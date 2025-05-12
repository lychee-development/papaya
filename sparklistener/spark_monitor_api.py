# complete_state_monitor.py

import uvicorn
import requests
import json
import time
import logging
import socket
from urllib.parse import urljoin
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
import hashlib

app = FastAPI(title="Complete Spark State Monitor")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SparkStateMonitor")

# Global configuration
config = {
    "spark_ui_url": "http://localhost:4040",
    "webhook_url": "https://webhook.site/7dc07478-a761-44ff-89bc-087a8e14183e",
    "polling_interval": 0.01,
    "port_check_interval": 0.1,
    "is_monitoring": False,
    "app_states": {},  # Track application states by app_id
    "sent_apps": set()  # Track applications we've sent data for
}

# API Models
class MonitorConfig(BaseModel):
    spark_ui_url: Optional[str] = "http://localhost:4040"
    webhook_url: Optional[str] = None
    polling_interval: Optional[float] = 0.5

class MonitorStatus(BaseModel):
    is_monitoring: bool
    spark_ui_url: str
    webhook_url: Optional[str]
    polling_interval: float
    tracked_apps: int
    sent_webhooks: int

# API Endpoints
@app.get("/", response_model=MonitorStatus)
def get_status():
    """Get the current monitoring status"""
    return {
        "is_monitoring": config["is_monitoring"],
        "spark_ui_url": config["spark_ui_url"],
        "webhook_url": config["webhook_url"],
        "polling_interval": config["polling_interval"],
        "tracked_apps": len(config["app_states"]),
        "sent_webhooks": len(config["sent_apps"])
    }

@app.post("/start", response_model=MonitorStatus)
def start_monitoring(background_tasks: BackgroundTasks, monitor_config: Optional[MonitorConfig] = None):
    """Start monitoring Spark UI for changes"""
    if monitor_config:
        if monitor_config.spark_ui_url:
            config["spark_ui_url"] = monitor_config.spark_ui_url
        if monitor_config.webhook_url:
            config["webhook_url"] = monitor_config.webhook_url
        if monitor_config.polling_interval:
            config["polling_interval"] = monitor_config.polling_interval
    
    if not config["is_monitoring"]:
        config["is_monitoring"] = True
        background_tasks.add_task(monitor_loop)
        logger.info(f"Started monitoring for Spark UI at {config['spark_ui_url']}")
    
    return get_status()

@app.post("/stop", response_model=MonitorStatus)
def stop_monitoring():
    """Stop monitoring Spark UI"""
    config["is_monitoring"] = False
    logger.info("Stopped monitoring Spark UI")
    return get_status()

@app.post("/reset", response_model=MonitorStatus)
def reset_monitoring():
    """Reset the tracking state"""
    config["app_states"] = {}
    config["sent_apps"] = set()
    logger.info("Reset monitor state")
    return get_status()

@app.post("/test-webhook")
def test_webhook():
    """Send a test webhook notification"""
    send_webhook({
        "message": "hello world",
        "test": True,
        "timestamp": time.time()
    })
    return {"status": "webhook sent"}

# Monitoring Functions
def is_port_open(host, port):
    """Check if a port is open using socket connection"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.1)
            s.connect((host, port))
            return True
    except:
        return False

def get_all_applications():
    """Get list of all applications from Spark UI"""
    try:
        apps_url = urljoin(config["spark_ui_url"], "/api/v1/applications")
        response = requests.get(apps_url, timeout=1)
        
        if response.status_code == 200:
            return response.json()
        return []
    except Exception as e:
        logger.error(f"Error getting applications: {e}")
        return []
def get_complete_app_state(app_id):
    """Get comprehensive state information for an application including detailed failure information"""
    app_state = {"app_id": app_id, "timestamp": time.time()}
    
    try:
        # Get basic app info
        app_url = urljoin(config["spark_ui_url"], f"/api/v1/applications/{app_id}")
        response = requests.get(app_url, timeout=1)
        if response.status_code == 200:
            app_state["app_info"] = response.json()
        
        # Get jobs
        jobs_url = urljoin(config["spark_ui_url"], f"/api/v1/applications/{app_id}/jobs")
        response = requests.get(jobs_url, timeout=1)
        if response.status_code == 200:
            app_state["jobs"] = response.json()
        
        # Get stages
        stages_url = urljoin(config["spark_ui_url"], f"/api/v1/applications/{app_id}/stages")
        response = requests.get(stages_url, timeout=1)
        if response.status_code == 200:
            app_state["stages"] = response.json()
        
        # Get executors
        executors_url = urljoin(config["spark_ui_url"], f"/api/v1/applications/{app_id}/executors")
        response = requests.get(executors_url, timeout=1)
        if response.status_code == 200:
            app_state["executors"] = response.json()
        
        # Get environment
        env_url = urljoin(config["spark_ui_url"], f"/api/v1/applications/{app_id}/environment")
        response = requests.get(env_url, timeout=1)
        if response.status_code == 200:
            app_state["environment"] = response.json()
        
        # ---- ENHANCED FAILURE DETECTION ----
        
        # 1. Track failures at job level
        if "jobs" in app_state:
            failed_jobs = [job for job in app_state["jobs"] if job.get("status") in ["FAILED", "ERROR"]]
            app_state["has_failed_jobs"] = len(failed_jobs) > 0
            
            # Extract detailed failure information
            if failed_jobs:
                app_state["failed_jobs_details"] = [{
                    "job_id": job.get("jobId"),
                    "name": job.get("name"),
                    "status": job.get("status"),
                    "failure_reason": job.get("failureReason")
                } for job in failed_jobs]
        
        # 2. Track failures at stage level
        if "stages" in app_state:
            failed_stages = [stage for stage in app_state["stages"] if stage.get("status") == "FAILED"]
            app_state["has_failed_stages"] = len(failed_stages) > 0
            
            # Extract detailed stage failure information
            if failed_stages:
                app_state["failed_stages_details"] = []
                for stage in failed_stages:
                    # Get detailed stage information including failure reason
                    stage_id = stage.get("stageId")
                    stage_attempt = stage.get("attemptId", 0)
                    
                    detail_url = urljoin(config["spark_ui_url"], 
                                        f"/api/v1/applications/{app_id}/stages/{stage_id}/{stage_attempt}")
                    try:
                        detail_resp = requests.get(detail_url, timeout=1)
                        if detail_resp.status_code == 200:
                            stage_details = detail_resp.json()
                            app_state["failed_stages_details"].append({
                                "stage_id": stage_id,
                                "attempt_id": stage_attempt,
                                "name": stage.get("name"),
                                "status": stage.get("status"),
                                "failure_reason": stage_details.get("failureReason"),
                                "exception": stage_details.get("exception")
                            })
                    except Exception as e:
                        logger.error(f"Error fetching stage details for {stage_id}: {e}")
        
        # 3. Track task failures across all stages - tasks with errors/exceptions
        app_state["task_failure_count"] = 0
        app_state["task_failures"] = []
        
        if "stages" in app_state:
            # Check active stages as well as completed ones to catch errors
            for stage in app_state["stages"]:
                stage_id = stage.get("stageId")
                stage_attempt = stage.get("attemptId", 0)
                
                # Get tasks for each stage
                tasks_url = urljoin(config["spark_ui_url"], 
                                   f"/api/v1/applications/{app_id}/stages/{stage_id}/{stage_attempt}/taskList")
                try:
                    tasks_resp = requests.get(tasks_url, timeout=1)
                    if tasks_resp.status_code == 200:
                        tasks = tasks_resp.json()
                        
                        # Check for failed tasks - detect errors in individual tasks
                        failed_tasks = [
                            task for task in tasks 
                            if (task.get("status") == "FAILED" or 
                                task.get("errorMessage") or 
                                task.get("hasException", False))
                        ]
                        
                        if failed_tasks:
                            app_state["task_failure_count"] += len(failed_tasks)
                            
                            # Collect detailed error information
                            for task in failed_tasks:
                                app_state["task_failures"].append({
                                    "stage_id": stage_id,
                                    "task_id": task.get("taskId"),
                                    "attempt": task.get("attempt"),
                                    "status": task.get("status"),
                                    "error_message": task.get("errorMessage"),
                                    "has_exception": task.get("hasException", False),
                                    "executor_id": task.get("executorId")
                                })
                except Exception as e:
                    logger.error(f"Error fetching tasks for stage {stage_id}: {e}")
        
        # 4. Check for executor failures
        if "executors" in app_state:
            app_state["executor_failure_count"] = sum(
                executor.get("failedTasks", 0) for executor in app_state["executors"]
            )
            app_state["has_executor_failures"] = app_state["executor_failure_count"] > 0
        
        # 5. Check if exceptions are present in appState
        if "app_info" in app_state and app_state["app_info"] and "attempts" in app_state["app_info"]:
            attempts = app_state["app_info"]["attempts"]
            if attempts and attempts[0]:
                # Look for exception field in latest attempt
                if "exception" in attempts[0] and attempts[0]["exception"]:
                    app_state["application_exception"] = attempts[0]["exception"]
                
                # Determine if application has failed by looking for termination causes
                termination_causes = ["killed", "failed", "exception"]
                if any(cause in attempts[0].get("endReason", "").lower() for cause in termination_causes):
                    app_state["application_failed"] = True
                    app_state["application_end_reason"] = attempts[0].get("endReason", "Unknown")
        
        # 6. Aggregate failure status from all sources
        app_state["has_failures"] = (
            app_state.get("has_failed_jobs", False) or 
            app_state.get("has_failed_stages", False) or 
            app_state.get("task_failure_count", 0) > 0 or
            app_state.get("has_executor_failures", False) or
            app_state.get("application_failed", False)
        )
            
        # Check if application is completed
        if "app_info" in app_state and "attempts" in app_state["app_info"]:
            attempts = app_state["app_info"]["attempts"]
            if attempts and "completed" in attempts[0]:
                app_state["is_completed"] = attempts[0]["completed"]
            else:
                app_state["is_completed"] = False
        
        return app_state
    
    except Exception as e:
        logger.error(f"Error getting state for application {app_id}: {e}")
        app_state["error"] = str(e)
        return app_state
def state_hash(state):
    """Create a hash of the important parts of the application state"""
    key_parts = []
    
    # Add job status summaries
    if "jobs" in state:
        for job in state["jobs"]:
            key_parts.append(f"{job.get('jobId')}:{job.get('status')}")
    
    # Add stage status summaries
    if "stages" in state:
        for stage in state["stages"]:
            key_parts.append(f"{stage.get('stageId')}:{stage.get('status')}")
    
    # Include task failure information
    key_parts.append(f"task_failures:{state.get('task_failure_count', 0)}")
    
    # Include executor failure information
    key_parts.append(f"executor_failures:{state.get('executor_failure_count', 0)}")
    
    # Include application exception status
    key_parts.append(f"app_exception:{bool(state.get('application_exception'))}")
    
    # Include completion state
    key_parts.append(f"completed:{state.get('is_completed', False)}")
    
    # Include overall failure status
    key_parts.append(f"has_failures:{state.get('has_failures', False)}")
    
    # Hash the combined string
    key = "|".join(key_parts)
    return hashlib.md5(key.encode()).hexdigest()

def state_changed(app_id, new_state):
    """Check if application state has changed meaningfully"""
    if app_id not in config["app_states"]:
        return True
    
    old_hash = config["app_states"][app_id].get("hash", "")
    new_hash = state_hash(new_state)
    
    return old_hash != new_hash

def send_webhook(data):
    """Send webhook notification with application state"""
    if not config["webhook_url"]:
        logger.warning("No webhook URL configured, skipping notification")
        return
    
    try:
        logger.info(f"Sending webhook with data length: {len(str(data))} characters")
        response = requests.post(
            config["webhook_url"],
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Webhook sent successfully: {response.status_code}")
            return True
        else:
            logger.error(f"Failed to send webhook: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error sending webhook: {e}")
        return False
def check_and_update_app(app_id):
    """Check application state and send webhook if changed"""
    try:
        # Get complete state
        app_state = get_complete_app_state(app_id)
        
        # Calculate state hash
        app_state["hash"] = state_hash(app_state)
        
        # Determine app name
        app_name = app_state.get("app_info", {}).get("name", "Unknown")
        
        # Check if this is a new app or state has changed
        is_new = app_id not in config["app_states"]
        has_changed = state_changed(app_id, app_state)
        
        if is_new:
            logger.info(f"Found new application: {app_name} ({app_id})")
        
        if has_changed:
            logger.info(f"Detected state change in application: {app_name} ({app_id})")
            
            # Save the new state
            config["app_states"][app_id] = app_state
            
            # Create a simplified payload with just the failure information
            simplified_failure_info = {
                "app_id": app_id,
                "app_name": app_name,
                "timestamp": app_state.get("timestamp"),
                "has_failures": app_state.get("has_failures", False),
                "is_completed": app_state.get("is_completed", False)
            }
            
            # Only include failure information if there are failures
            if app_state.get("has_failures", False):
                # Add failure details
                if app_state.get("has_failed_jobs", False):
                    simplified_failure_info["has_failed_jobs"] = True
                    simplified_failure_info["failed_jobs_details"] = app_state.get("failed_jobs_details", [])
                
                if app_state.get("has_failed_stages", False):
                    simplified_failure_info["has_failed_stages"] = True
                    simplified_failure_info["failed_stages_details"] = app_state.get("failed_stages_details", [])
                
                if app_state.get("task_failure_count", 0) > 0:
                    simplified_failure_info["task_failure_count"] = app_state.get("task_failure_count", 0)
                    simplified_failure_info["task_failures"] = app_state.get("task_failures", [])
                
                if app_state.get("has_executor_failures", False):
                    simplified_failure_info["executor_failure_count"] = app_state.get("executor_failure_count", 0)
                    simplified_failure_info["has_executor_failures"] = True
                
                if app_state.get("application_exception"):
                    simplified_failure_info["application_exception"] = app_state.get("application_exception")
                
                if app_state.get("application_failed", False):
                    simplified_failure_info["application_failed"] = True
                    simplified_failure_info["application_end_reason"] = app_state.get("application_end_reason", "Unknown")
            
            # Create the webhook payload
            payload = {
                "event_type": "application_state_change",
                "is_new_app": is_new,
                "failures": simplified_failure_info
            }
            
            # Send to webhook
            sent = send_webhook(payload)
            if sent:
                config["sent_apps"].add(app_id)
            
            return True
        
        # Always send at least one webhook per app
        if app_id not in config["sent_apps"]:
            logger.info(f"Sending initial state for application: {app_name} ({app_id})")
            
            # Save the new state
            config["app_states"][app_id] = app_state
            
            # Create a simplified failure information
            simplified_info = {
                "app_id": app_id,
                "app_name": app_name,
                "timestamp": app_state.get("timestamp"),
                "has_failures": app_state.get("has_failures", False),
                "is_completed": app_state.get("is_completed", False)
            }
            
            # Create the webhook payload
            payload = {
                "event_type": "application_initial_state",
                "failures": simplified_info
            }
            
            # Send to webhook
            sent = send_webhook(payload)
            if sent:
                config["sent_apps"].add(app_id)
            
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking application {app_id}: {e}")
        return False
def monitor_loop():
    """Main monitoring loop"""
    logger.info(f"Starting comprehensive state monitoring for Spark UI at {config['spark_ui_url']}")
    
    while config["is_monitoring"]:
        try:
            # Check if Spark UI port is open
            host = config["spark_ui_url"].split("://")[1].split(":")[0]
            port = int(config["spark_ui_url"].split(":")[-1].split("/")[0])
            if config["app_states"]:
                for app_id in list(config["app_states"]):
                    app_state = config["app_states"][app_id]
                    if not app_state.get("is_completed", False):
                        # Check if app still exists, if not mark as failed
                        app_exists = any(app.get("id") == app_id for app in get_all_applications())
                        if not app_exists:
                            logger.info(f"Application {app_id} appears to have ended abruptly")
                            
                            # Send final state notification
                            final_state = app_state.copy()
                            final_state["final_status"] = "FAILED_OR_ABRUPTLY_ENDED"
                            final_state["end_time"] = time.time()
                            final_state["has_failures"] = True

                            # Create simplified failure info
                            simplified_failure_info = {
                                "app_id": app_id,
                                "app_name": app_state.get("app_info", {}).get("name", "Unknown"),
                                "timestamp": time.time(),
                                "final_status": "FAILED_OR_ABRUPTLY_ENDED",
                                "has_failures": True
                            }

                            # Add specific failure details if available
                            if "has_failed_jobs" in app_state and app_state["has_failed_jobs"]:
                                simplified_failure_info["has_failed_jobs"] = True
                                simplified_failure_info["failed_jobs_details"] = app_state.get("failed_jobs_details", [])

                            if "has_failed_stages" in app_state and app_state["has_failed_stages"]:
                                simplified_failure_info["has_failed_stages"] = True
                                simplified_failure_info["failed_stages_details"] = app_state.get("failed_stages_details", [])

                            if "task_failure_count" in app_state and app_state["task_failure_count"] > 0:
                                simplified_failure_info["task_failure_count"] = app_state["task_failure_count"]
                                simplified_failure_info["task_failures"] = app_state.get("task_failures", [])

                            payload = {
                                "event_type": "application_failed",
                                "failures": simplified_failure_info
                            }

                            send_webhook(payload)
                            
                            # Remove from tracked apps
                            del config["app_states"][app_id]
            if is_port_open(host, port):
                # Get all running applications
                apps = get_all_applications()
                
                if apps:
                    logger.info(f"Found {len(apps)} Spark applications")
                    
                    # Check each application
                    for app in apps:
                        app_id = app.get("id")
                        if app_id:
                            check_and_update_app(app_id)
                else:
                    # If no apps are found but we have tracked apps, check if they're really gone
                    if config["app_states"]:
                        # This could indicate apps have completed and UI has reset
                        for app_id in list(config["app_states"]):
                            # Create final state notification
                            if app_id in config["app_states"] and app_id in config["sent_apps"]:
                                logger.info(f"Application {app_id} appears to have ended")
                                
                                # Send final state notification
                                final_state = config["app_states"][app_id]
                                final_state["final_status"] = "ENDED_OR_UI_RESET"
                                final_state["end_time"] = time.time()
                                
                                payload = {
                                    "message": "hello world",
                                    "event_type": "application_ended",
                                    "app_state": final_state
                                }
                                
                                send_webhook(payload)
                                
                                # Remove from tracked apps
                                del config["app_states"][app_id]
            
        except Exception as e:
            logger.error(f"Error in monitor loop: {e}")
        
        time.sleep(config["polling_interval"])
    
    logger.info("Monitoring stopped")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)