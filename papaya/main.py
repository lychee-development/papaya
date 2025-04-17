import sys
import uvicorn
import importlib
import inspect
import threading
import asyncio
from fastapi import FastAPI
from uvicorn.server import Server
import httpx

from mcp.server.fastmcp import FastMCP
from mcp.server.session import ServerSession

####################################################################################
# Temporary monkeypatch which avoids crashing when a POST message is received
# before a connection has been initialized, e.g: after a deployment.
# pylint: disable-next=protected-access
old__received_request = ServerSession._received_request


async def _received_request(self, *args, **kwargs):
    try:
        return await old__received_request(self, *args, **kwargs)
    except RuntimeError:
        pass


# pylint: disable-next=protected-access
ServerSession._received_request = _received_request
####################################################################################



### TODO: remove the excessive logging and make it a --verbose option

# ANSI color codes for purple-ish color
PAPAYA_COLOR = "\033[38;5;183m"  # Light coral/rose color
RESET_COLOR = "\033[0m"

# Function to color the Papaya prefix
def papaya_print(message):
    print(f"{PAPAYA_COLOR}PAPAYA:{RESET_COLOR} {message}")

# Attempt to import FastMCP, handle if not installed
try:
    from fastmcp.server import FastMCP
except ImportError:
    papaya_print("Error: FastMCP is not installed. Please install it (`pip install fastmcp`)")
    sys.exit(1)

# --- Uvicorn Monkey-Patching ---

# 1. Store the original Server.startup method
original_startup = Server.startup

# 2. Define the wrapped startup method
async def wrapped_startup(self, sockets=None) -> None:
    """
    A wrapper around uvicorn.Server.startup that finds the FastAPI app,
    starts a corresponding FastMCP server in a separate thread,
    and then proceeds with the original startup.
    """
    # --- Start FastMCP Server Logic ---
    mcp_server_started = False
    if hasattr(self.config, "loaded_app"):
        # Start with the potentially wrapped app
        current_app = self.config.loaded_app
        max_unwrap_depth = 10
        unwrap_count = 0

        # Traverse the middleware stack to find the base FastAPI app
        while hasattr(current_app, "app") and not isinstance(current_app, FastAPI) and unwrap_count < max_unwrap_depth:
            current_app = current_app.app
            unwrap_count += 1

        # Check if we found the FastAPI app
        if isinstance(current_app, FastAPI):
            try:
                print("PAPAYA: Found FastAPI app. Creating FastMCP server...")
                # Dynamically create MCP server from the discovered FastAPI app
                mcp_host = "0.0.0.0" # Or configure as needed
                mcp_port = 8001
                uvicorn_port = 3001 # TODO we need to actually properly fill this out

                http_client = httpx.AsyncClient(base_url=f"http://localhost:{uvicorn_port}")

                mcp_server = FastMCP.from_openapi(current_app.openapi(), http_client, host=mcp_host, port=mcp_port)


                print(f"PAPAYA: Starting FastMCP server on http://{mcp_host}:{mcp_port} in a background thread...")

                # Define the target function for the thread
                def run_mcp():
                    try:
                        # Note: mcp.run() might block here. It typically starts its own server.
                        # Consider using mcp_server.serve() if an async version exists
                        # or handle potential asyncio loop conflicts if run starts Uvicorn.
                        # For simplicity, running directly. Ensure FastMCP's run is thread-safe
                        # or doesn't conflict with the main Uvicorn loop.
                        # If FastMCP uses Uvicorn internally, this might need adjustment
                        # to avoid loop conflicts (e.g., running in a separate process
                        # or using advanced asyncio loop management).
                        mcp_server.run(transport="sse")
                        papaya_print("FastMCP server thread finished.")
                    except Exception as e_mcp:
                        papaya_print(f"Error running FastMCP server in thread: {e_mcp}")

                # Start the FastMCP server in a daemon thread
                # Daemon=True ensures the thread exits when the main program exits
                mcp_thread = threading.Thread(target=run_mcp, daemon=True)
                mcp_thread.start()
                mcp_server_started = True
                papaya_print("FastMCP server thread started.")
                # Give the thread a moment to potentially start logging
                await asyncio.sleep(0.5)

            except Exception as e_setup:
                papaya_print(f"Error setting up or starting FastMCP server: {e_setup}")
        else:
            papaya_print(f"Could not find FastAPI instance to generate MCP server (found type: {type(current_app).__name__}).")
            if unwrap_count >= max_unwrap_depth_global:
                 papaya_print("Warning - Reached maximum middleware unwrap depth.")

    else:
        papaya_print("Warning - Could not find loaded_app in uvicorn config before startup.")

    if not mcp_server_started:
         papaya_print("FastMCP server was not started.")

    # --- Call Original Uvicorn Startup ---
    papaya_print("Proceeding with original Uvicorn startup...")
    await original_startup(self, sockets=sockets)
    papaya_print("Original Uvicorn startup complete.")


# 3. Apply the monkey-patch
Server.startup = wrapped_startup


def configure_papaya(mcp_host="0.0.0.0", mcp_port=8001, max_unwrap_depth=10):
    """Configure Papaya settings - can be called programmatically"""
    global mcp_host_global, mcp_port_global, max_unwrap_depth_global
    mcp_host_global = mcp_host
    mcp_port_global = mcp_port
    max_unwrap_depth_global = max_unwrap_depth

# Store global settings
mcp_host_global = "0.0.0.0"
mcp_port_global = 8001
max_unwrap_depth_global = 10
