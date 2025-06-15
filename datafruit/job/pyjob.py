import ray
import random
import time 
import math 
import psutil
import pandas as pd
import logging
from typing import Optional, Union, Type, Dict, Callable, Any, List
from enum import Enum 
from datafruit import PostgresDB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from functools import wraps 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if not ray.is_initialized(): 
    ray.init()

# circuit breaker enum that ensure no time waste during parallel processes
class _CircuitState(Enum):
    """Circuit breaker states for fault tolerance in parallel processes."""
    CLOSED="CLOSED"
    OPEN="OPEN"
    HALF_OPEN="HALF_OPEN"
 

def pyjob(
    # Input/Output
    input_df: Optional[pd.DataFrame] = None,
    output: Optional[Union[str, Type]] = None,  # "dataframe", "list", or table class for auto-save
    
    # Database Configuration (alternative to input_df)
    db: Optional[PostgresDB] = None,
    query: Optional[str] = None,
    table_name: Optional[str] = None,
    
    # Processing config for ray
    batch_size: Optional[int] = None,
    num_cpus: int = 1,
    memory: Optional[int] = None,
    
    # error handling (optional, probably should update for more robust handling)
    max_retries: int = 3,
    retry_exceptions: Optional[List[Exception]] = None,
    enable_circuit_breaker: bool = True,
    circuit_failure_threshold: int = 5,
    circuit_timeout: int = 60,
    
    # added for future monitoring implementation 
    enable_monitoring: bool = True,
    auto_gc: bool = True,
    memory_threshold: float = 0.8
): 
    """
    A flexible decorator for parallel data transformations using Ray with DataFrame and Database support.
    
    This decorator handles all the parallelization complexity while giving you complete flexibility 
    in your transformation function. You write simple functions that process ONE row/record at a time, 
    and the decorator automatically applies it to ALL items in parallel.
    
    Supports seamless conversion between DataFrame ↔ Database with output specification.
    
    Examples:
        # DataFrame → process → convert to DataFrame
        @pyjob(input_df=df, output="dataframe")
        def clean_data(row):
            return {'clean_name': row['name'].title()}
        
        # Database → process → auto-save to table
        @pyjob(db=db, table_name="users", output=CleanUserTable)
        def process_users(user_dict):
            return CleanUserTable(name=user_dict['name'].title())
        
        # Default behavior (your function determines output)
        @pyjob(input_df=df)
        def process_row(row):
            return whatever_you_want(row)
    
    Args:
        input_df: DataFrame to process
        output: Output format - "dataframe", "list", or table class for auto-save to DB
        db: PostgresDB instance for database operations
        query: SQL query to fetch data from database
        table_name: Table name to fetch all records from (alternative to query)
        batch_size: Size of batches for parallel processing
        num_cpus: Number of CPUs per Ray task
        memory: Memory allocation per Ray task
        max_retries: Maximum retry attempts for failed tasks
        retry_exceptions: List of exceptions to retry on
        enable_circuit_breaker: Enable circuit breaker pattern
        circuit_failure_threshold: Number of failures before opening circuit
        circuit_timeout: Timeout before trying to close circuit
        enable_monitoring: Enable memory and performance monitoring
        auto_gc: Automatically trigger garbage collection
        memory_threshold: Memory threshold for triggering GC
    """ 
    circuit_breaker_state = {
        'state': _CircuitState.CLOSED,  
        'failure_count': 0, 
        'last_failure_time': None
    }
    
    def _update_circuit_breaker(success: bool):
        """Updates the circuit breaker state based on task success/failure."""
        if not enable_circuit_breaker:
            return
        
        if success:
            if circuit_breaker_state['state'] == _CircuitState.HALF_OPEN:
                circuit_breaker_state['state'] = _CircuitState.CLOSED
                circuit_breaker_state['failure_count'] = 0
                logging.info("Circuit breaker reset to CLOSED - system recovered.")
        else:
            circuit_breaker_state['failure_count'] += 1
            circuit_breaker_state['last_failure_time'] = time.time()
            if circuit_breaker_state['failure_count'] >= circuit_failure_threshold:
                if circuit_breaker_state['state'] != _CircuitState.OPEN:
                    circuit_breaker_state['state'] = _CircuitState.OPEN
                    logging.warning(f"Circuit breaker OPENED after {circuit_failure_threshold} failures.")

    def _check_circuit_breaker() -> bool:
        """Checks if requests should be allowed through the circuit breaker."""
        if not enable_circuit_breaker or circuit_breaker_state['state'] == _CircuitState.CLOSED:
            return True

        if circuit_breaker_state['state'] == _CircuitState.OPEN:
            if time.time() - circuit_breaker_state['last_failure_time'] > circuit_timeout:
                circuit_breaker_state['state'] = _CircuitState.HALF_OPEN
                logging.info("Circuit breaker HALF_OPEN - allowing test request.")
                return True
            return False
        
        return True  

    def _monitor_memory():
        """Monitors memory usage and triggers garbage collection if needed."""
        if not enable_monitoring:
            return 0
        
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > memory_threshold * 100 and auto_gc:
            import gc
            gc.collect()
            logging.warning(f"Memory usage ({memory_percent:.1f}%) exceeded threshold. Triggered GC.")
        return memory_percent
    
    def _get_optimal_batch_size(data_size: int) -> int:
        """Calculate optimal batch size based on data size and available resources."""
        if batch_size is not None:
            return batch_size
        
        # Adaptive batching based on available CPU cores
        num_workers = int(ray.available_resources().get("CPU", 4))
        optimal = max(50, data_size // (num_workers * 4))  # More granular batches
        return optimal

    def _handle_output_conversion(results: List[Any], output_spec) -> Any:
        """Handle output conversion and auto-saving based on output specification."""
        if not results:
            if output_spec == "dataframe":
                return pd.DataFrame()
            else:
                return []
        
        # Default case - just return filtered results
        if output_spec is None or output_spec == "list":
            return results
        
        # Convert to DataFrame
        if output_spec == "dataframe":
            sample = results[0]
            
            if isinstance(sample, pd.Series):
                # Convert Series back to DataFrame
                return pd.DataFrame(results).reset_index(drop=True)
            elif isinstance(sample, dict):
                # Convert dicts to DataFrame
                return pd.DataFrame(results)
            elif isinstance(sample, pd.DataFrame):
                # Concatenate DataFrames
                return pd.concat(results, ignore_index=True)
            else:
                # Try to convert other types to DataFrame
                try:
                    return pd.DataFrame(results)
                except Exception as e:
                    logging.warning(f"Could not convert results to DataFrame: {e}. Returning as list.")
                    return results
        
        # Auto-save to database table (if output is a table class)
        if hasattr(output_spec, '__tablename__') and db is not None:
            try:
                from sqlmodel import Session
                with Session(db.engine) as session:
                    # Ensure all results are instances of the target table class
                    table_objects = []
                    for result in results:
                        if isinstance(result, output_spec):
                            table_objects.append(result)
                        elif isinstance(result, dict):
                            # Try to convert dict to table object
                            table_objects.append(output_spec(**result))
                        else:
                            logging.warning(f"Cannot convert {type(result)} to {output_spec.__name__}")
                    
                    if table_objects:
                        session.add_all(table_objects)
                        session.commit()
                        logging.info(f"Auto-saved {len(table_objects)} records to {output_spec.__tablename__} table")
                        return table_objects
                    else:
                        logging.warning("No valid objects to save to database")
                        return results
            except Exception as e:
                logging.error(f"Database auto-save failed: {e}")
                return results
        
        # Return as-is for other cases
        return results
        
    def _process_batch_with_ray(func: Callable, data: List[Any]) -> List[Any]:
        """Process a list of items in parallel batches using Ray."""
        start_time = time.time()
        initial_mem = _monitor_memory()
        
        data_size = len(data)
        optimal_batch_size = _get_optimal_batch_size(data_size)

        # Small data - process directly (no parallelization overhead)
        if data_size <= optimal_batch_size:
            logging.info(f"Processing {data_size} items directly (small dataset).")
            results = []
            for item in data:
                try:
                    result = func(item)
                    if result is not None:  # Allow filtering by returning None
                        results.append(result)
                except Exception as e:
                    logging.error(f"Processing failed for item: {e}")
                    if max_retries > 0:
                        raise
                    continue
            return results

        # Large data - split into batches for parallel processing
        batches = [data[i:i + optimal_batch_size] for i in range(0, data_size, optimal_batch_size)]
        logging.info(f"Processing {data_size} items in {len(batches)} batches of size {optimal_batch_size}.")

        # Create Ray remote function that processes one batch
        @ray.remote(
            num_cpus=num_cpus, 
            memory=memory, 
            max_retries=max_retries,
            retry_exceptions=retry_exceptions or [ConnectionError, TimeoutError, Exception]
        )
        def remote_batch_processor(batch):
            batch_results = []
            for item in batch:
                try:
                    result = func(item)
                    if result is not None:
                        batch_results.append(result)
                except Exception as e:
                    logging.error(f"Processing failed for item: {e}")
                    continue
            return batch_results
        
        # Submit all batches to Ray workers
        futures = [remote_batch_processor.remote(batch) for batch in batches]
        
        # Monitor memory during processing
        for i in range(0, len(futures), 10):
            _monitor_memory()
        
        # Collect and combine results
        batch_results = ray.get(futures)
        final_results = []
        for batch_result in batch_results:
            if batch_result:
                final_results.extend(batch_result)

        if enable_monitoring:
            duration = time.time() - start_time
            final_mem = _monitor_memory()
            logging.info(
                f"Parallel processing completed in {duration:.2f}s. "
                f"Memory: {initial_mem:.1f}% → {final_mem:.1f}%. "
                f"Processed: {data_size} → {len(final_results)} items."
            )
        
        return final_results

    def _process_dataframe(func: Callable, df: pd.DataFrame) -> Any:
        """Process DataFrame row by row and handle output conversion."""
        logging.info(f"Processing DataFrame with {len(df)} rows.")
        
        # Convert DataFrame to list of Series (rows)
        rows = [df.iloc[i] for i in range(len(df))]
        
        # Process all rows
        processed_rows = _process_batch_with_ray(func, rows)
        
        # Handle output conversion
        result = _handle_output_conversion(processed_rows, output)
        
        logging.info(f"DataFrame processing complete: {len(df)} → {len(result) if hasattr(result, '__len__') else 1} items.")
        return result

    def _process_from_database(func: Callable, db, query=None, table_name=None) -> Any:
        """Handle memory-efficient database processing."""
        if not db:
            raise ValueError("Database instance must be provided for database mode.")
        
        if not query and not table_name:
            raise ValueError("Either `query` or `table_name` must be provided for database mode.")

        start_time = time.time()
        logging.info(f"Starting database processing")

        # Validate connection
        if not db.validate_connection():
            raise ConnectionError("Database connection failed.")

        # Build query
        if query:
            sql_query = query
        else:
            sql_query = f"SELECT * FROM {table_name}"

        # Memory-efficient data extraction
        SessionLocal = sessionmaker(bind=db.engine)
        with SessionLocal() as session:
            # Execute query and fetch results in chunks
            result = session.execute(text(sql_query))
            
            # Convert to list of dictionaries for processing
            columns = result.keys()
            all_rows = []
            
            # Fetch in chunks to manage memory
            chunk_size = _get_optimal_batch_size(1000)  # Default estimate
            
            while True:
                chunk = result.fetchmany(chunk_size)
                if not chunk:
                    break
                
                # Convert rows to dictionaries
                chunk_dicts = [dict(zip(columns, row)) for row in chunk]
                all_rows.extend(chunk_dicts)
                
                # Monitor memory usage
                if len(all_rows) % (chunk_size * 10) == 0:
                    _monitor_memory()

            if not all_rows:
                logging.warning("No records found matching criteria.")
                return _handle_output_conversion([], output)

            logging.info(f"Processing {len(all_rows)} records from database.")
            
            # Process all rows
            all_results = _process_batch_with_ray(func, all_rows)
            
            if enable_monitoring:
                duration = time.time() - start_time
                logging.info(f"Database processing completed in {duration:.2f}s.")
                
            return _handle_output_conversion(all_results, output)
        
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper():
            """
            Parameterless execution - all configuration comes from decorator.
            This provides a clean, declarative API where everything is specified upfront.
            """
            
            # Check circuit breaker
            if not _check_circuit_breaker():
                error_msg = f"Circuit breaker is OPEN for {func.__name__}. Call aborted."
                logging.error(error_msg)
                raise ConnectionError(error_msg)

            # Determine processing mode and execute
            try:
                result = None
                
                # Database Mode
                if db is not None:
                    logging.info(f"Executing {func.__name__} in DATABASE mode.")
                    result = _process_from_database(func, db=db, query=query, table_name=table_name)
                
                # DataFrame Mode
                elif input_df is not None:
                    logging.info(f"Executing {func.__name__} in DATAFRAME mode.")
                    result = _process_dataframe(func, input_df)
                
                # No valid input specified
                else:
                    raise ValueError(
                        "No valid input specified. Provide one of:\n"
                        "- input_df=dataframe for DataFrame processing\n"
                        "- db=postgres_db + (query=sql_query OR table_name=table) for database processing"
                    )

                _update_circuit_breaker(success=True)
                return result

            except Exception as e:
                logging.error(f"Error in {func.__name__}: {e}", exc_info=True)
                _update_circuit_breaker(success=False)
                raise

        # Attach utility methods
        wrapper.get_circuit_breaker_status = lambda: circuit_breaker_state['state'].value
        wrapper.reset_circuit_breaker = lambda: circuit_breaker_state.update({
            'state': _CircuitState.CLOSED, 'failure_count': 0, 'last_failure_time': None
        })
        wrapper.get_stats = lambda: {
            'input_type': 'dataframe' if input_df is not None else 'database',
            'output_format': str(output) if output else 'default',
            'table_name': table_name,
            'has_query': query is not None,
            'batch_size': batch_size,
            'num_cpus': num_cpus,
            'circuit_breaker': wrapper.get_circuit_breaker_status(),
            'monitoring_enabled': enable_monitoring
        }
        
        return wrapper
    return decorator
        
# this tests are ai generated so we gotta make it better
if __name__ == "__main__":
    from datetime import datetime
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    # Your actual database setup
    import datafruit as dft
    from sqlmodel import Field, SQLModel
    from typing import Optional
    
    class users(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        username: str = Field(unique=True)
        email: str = Field(unique=True)
        full_name: Optional[str] = None
        is_active: bool = Field(default=True)
        created_at: datetime = Field(default_factory=datetime.utcnow)

    class posts(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        title: str = Field(index=True)
        content: str
        published: bool = Field(default=False)
        created_at: datetime = Field(default_factory=datetime.utcnow)
        updated_at: Optional[datetime] = None

    # Your database connection
    postgres_db = dft.PostgresDB(
        "top secret stuff",
        [users, posts]
    )
    
    # Sample data
    sample_df = pd.DataFrame({
        'first_name': ['Alice', 'Bob', 'Carol', 'David'],
        'last_name': ['Johnson', 'Smith', 'Williams', 'Brown'],
        'score': [100, 50, 200, 75],  # Third column with numbers
        'age': [25, 17, 30, 45],
        'price': [100, 50, 200, 75]
    })
    
    print("🚀 Simplified PyJob Decorator - DataFrame and Database Support")
    print("=" * 70)
    
    # Test database connection
    print("\n🔌 Testing Database Connection...")
    try:
        if postgres_db.validate_connection():
            print("✅ Database connection successful!")
            postgres_db.sync_schema()  # Ensure tables exist
            print("✅ Schema synchronized!")
            
            # Optional: Add some test data to demonstrate database features
            print("\n💾 Adding Sample Data for Testing...")
            try:
                from sqlmodel import Session, select
                with Session(postgres_db.engine) as session:
                    # Check if we already have users
                    existing_users = session.exec(select(users)).first()
                    if not existing_users:
                        # Add sample users
                        sample_users = [
                            users(username="alice_demo", email="alice@gmail.com", full_name="Alice Johnson"),
                            users(username="bob_demo", email="bob@company.com", full_name="Bob Smith"),
                            users(username="carol_demo", email="carol@yahoo.com", full_name="Carol Williams"),
                        ]
                        for user in sample_users:
                            session.add(user)
                        
                        # Add sample posts
                        sample_posts = [
                            posts(title="Welcome Post", content="This is a welcome post with some content to analyze. It has multiple sentences and words.", published=True),
                            posts(title="Draft Post", content="This is a draft post that is not published yet.", published=False),
                            posts(title="Long Article", content="This is a much longer article with extensive content. " * 20, published=True),
                        ]
                        for post in sample_posts:
                            session.add(post)
                        
                        session.commit()
                        print("✅ Sample data added successfully!")
                    else:
                        print("📝 Sample data already exists, skipping...")
            except Exception as e:
                print(f"⚠️  Could not add sample data: {e}")
                print("   Database examples will show empty results")
                
        else:
            print("❌ Database connection failed!")
            # Continue with DataFrame examples only
    except Exception as e:
        print(f"❌ Database error: {e}")
        print("   Continuing with DataFrame examples only...")
    
    # Example 0: Simple test - multiply third column by 3
    print("\n🧮 Example 0: Multiply Third Column by 3")
    @pyjob(
        input_df=sample_df, 
        batch_size=2, 
        enable_monitoring=True
    )
    def multiply_third_column_by_3(row):
        """Multiply the third column (score) by 3."""
        modified_row = row.copy()
        modified_row['score'] = row['score'] * 3  # Third column times 3
        return modified_row
    
    try:
        result = multiply_third_column_by_3()
        print(f"✅ Processed {len(sample_df)} rows → {len(result)} items")
        print(f"   Output type: {type(result).__name__} of {type(result[0]).__name__}")
        print(f"   Original scores: {sample_df['score'].tolist()}")
        print(f"   Modified scores: {[r['score'] for r in result]}")
        print(f"   Sample: {result[0]['first_name']} score: {sample_df.iloc[0]['score']} → {result[0]['score']}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 1: DataFrame → Whatever your function returns
    print("\n📊 Example 1: DataFrame Processing")
    @pyjob(
        input_df=sample_df, 
        batch_size=2, 
        enable_monitoring=True
    )
    def clean_dataframe_row(row):
        """Process DataFrame row - return whatever you want."""
        cleaned_row = row.copy()
        cleaned_row['full_name'] = f"{row['first_name']} {row['last_name']}"
        cleaned_row['age_group'] = 'adult' if row['age'] >= 18 else 'minor'
        cleaned_row['score_category'] = 'high' if row['score'] > 100 else 'low'
        return cleaned_row
    
    try:
        cleaned_df = clean_dataframe_row()
        print(f"✅ DataFrame processing: {len(sample_df)} → {len(cleaned_df)} items")
        print(f"   Output type: {type(cleaned_df).__name__} of {type(cleaned_df[0]).__name__}")
        if hasattr(cleaned_df[0], 'get'):  # Check if it's a dict-like object
            print(f"   Sample: {cleaned_df[0].get('full_name', 'N/A')}")
        else:
            print(f"   Sample: {cleaned_df[0]}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 2: DataFrame → Return dictionaries
    print("\n📋 Example 2: DataFrame → Custom Objects")
    @pyjob(
        input_df=sample_df,
        batch_size=2,
        enable_monitoring=True
    )
    def extract_summary_data(row):
        """Extract summary data and return as dict - your choice of output."""
        return {
            'full_name': f"{row['first_name']} {row['last_name']}",
            'performance_level': 'excellent' if row['score'] > 150 else 'good' if row['score'] > 75 else 'needs_improvement',
            'age_category': 'adult' if row['age'] >= 18 else 'minor'
        }
    
    try:
        summary_list = extract_summary_data()
        print(f"✅ DataFrame processing: {len(sample_df)} → {len(summary_list)} items")
        print(f"   Output type: {type(summary_list).__name__} of {type(summary_list[0]).__name__}")
        print(f"   Sample: {summary_list[0]}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 3: Database Processing (with table name) - Your actual users table
    print("\n🗄️  Example 3: Database Processing (Your Users Table)")
    @pyjob(
        db=postgres_db,
        table_name="users",
        batch_size=10,
        enable_monitoring=True
    )
    def analyze_users_from_table(user_row):
        """Analyze users from your actual database table."""
        return {
            'user_id': user_row['id'],
            'username': user_row['username'],
            'email_domain': user_row['email'].split('@')[1] if '@' in user_row['email'] else 'unknown',
            'has_full_name': user_row['full_name'] is not None,
            'account_age_days': (datetime.utcnow() - user_row['created_at']).days if user_row['created_at'] else 0,
            'status': 'active' if user_row['is_active'] else 'inactive'
        }
    
    try:
        user_analysis = analyze_users_from_table()
        print(f"✅ Database processing: Retrieved and analyzed {len(user_analysis)} users")
        print(f"   Output type: {type(user_analysis).__name__}")
        if user_analysis:
            print(f"   Sample analysis: {user_analysis[0]}")
        else:
            print(f"   📝 Note: No users found in database - tables may be empty")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 4: Database Processing (with custom query) - Your posts
    print("\n🔍 Example 4: Database Processing (Your Posts with Custom Query)")
    @pyjob(
        db=postgres_db,
        query="SELECT * FROM posts WHERE published = true ORDER BY created_at DESC LIMIT 100",
        batch_size=5,
        enable_monitoring=True
    )
    def process_published_posts(post_row):
        """Process published posts with custom analysis."""
        content_length = len(post_row['content']) if post_row['content'] else 0
        return {
            'post_id': post_row['id'],
            'title': post_row['title'],
            'content_length': content_length,
            'word_count': len(post_row['content'].split()) if post_row['content'] else 0,
            'category': 'long' if content_length > 1000 else 'medium' if content_length > 500 else 'short',
            'days_since_created': (datetime.utcnow() - post_row['created_at']).days if post_row['created_at'] else 0
        }
    
    try:
        post_analysis = process_published_posts()
        print(f"✅ Database query processing: Analyzed {len(post_analysis)} published posts")
        print(f"   Output type: {type(post_analysis).__name__}")
        if post_analysis:
            print(f"   Sample analysis: {post_analysis[0]}")
        else:
            print(f"   📝 Note: No published posts found - add some posts to test this feature")
    except Exception as e:
        print(f"❌ Error: {e}")
        
    # Example 5: Database Processing - Get user email domains
    print("\n📧 Example 5: Extract Email Domains from Users")
    @pyjob(
        db=postgres_db,
        query="SELECT id, username, email, full_name FROM users WHERE is_active = true",
        batch_size=20,
        enable_monitoring=True
    )
    def extract_user_domains(user_row):
        """Extract email domains and user info."""
        if '@' not in user_row['email']:
            return None  # Skip invalid emails
            
        domain = user_row['email'].split('@')[1].lower()
        return {
            'user_id': user_row['id'],
            'username': user_row['username'],
            'email_domain': domain,
            'domain_type': 'gmail' if 'gmail' in domain else 'corporate' if '.' in domain else 'other',
            'has_full_name': bool(user_row['full_name'])
        }
    
    try:
        domain_analysis = extract_user_domains()
        print(f"✅ Email domain extraction: Processed {len(domain_analysis)} active users")
        print(f"   Output type: {type(domain_analysis).__name__}")
        if domain_analysis:
            print(f"   Sample: {domain_analysis[0]}")
            # Show domain distribution
            domain_counts = {}
            for item in domain_analysis:
                domain = item['email_domain']
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
            print(f"   Domain distribution: {dict(list(domain_counts.items())[:5])}")  # Top 5
        else:
            print(f"   📝 Note: No active users found - add some users to test this feature")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Show comprehensive stats
    print(f"\n📈 Function Statistics:")
    functions = [
        ('Multiply Third Column', multiply_third_column_by_3),
        ('DataFrame Processing', clean_dataframe_row),
        ('DataFrame→Custom Objects', extract_summary_data),
        ('Database Users Analysis', analyze_users_from_table),
        ('Database Posts Query', process_published_posts),
        ('Database Email Domains', extract_user_domains),
    ]
    
    for name, func in functions:
        stats = func.get_stats()
        print(f"   {name}: {stats}")
