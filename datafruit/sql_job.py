from sqlmodel import SQLModel
from datafruit import PostgresDB
from sqlalchemy import text, inspect
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import re

"""
Principles

This first iteration should just work as a sql query. The function should return a string that is a valid SQL query and reference the tables directly.
The function should be decorated with @sql_job.
The decorator should take the SQLModel input, db, and an optional output table as parameters. If no output table is provided, it should default to outputting as a dataframe.

"""

def sql_job(db: PostgresDB, output_table: SQLModel | None = None, plan: bool = False):
   def decorator(func):
       def wrapper(*args, **kwargs):
           sql_query = func(*args, **kwargs)

           if not isinstance(sql_query, str):
               raise ValueError("The function must return a valid SQL query string.")

           sql_query = sql_query.strip()

           def replace_ref(match):
               model_name = match.group(1)
               for table_model in db.tables:
                   if table_model.__name__ == model_name:
                       return table_model.__tablename__
               raise ValueError(f"Table model '{model_name}' not found")

           sql_query = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", replace_ref,  sql_query)

           try:
               with db.engine.connect() as conn:
                   inspector = inspect(db.engine)
                   existing_tables = inspector.get_table_names()

                   if output_table is None:
                       # Return as DataFrame
                       result = conn.execute(text(sql_query))
                       df = pd.DataFrame(result.fetchall(), columns=result.keys())
                       return df
                   else:
                       # Create output table if it doesn't exist
                       if output_table.__tablename__ not in existing_tables:
                           output_table.metadata.create_all(db.engine, tables=[output_table.__table__])

                       # Get all non-primary key columns (since primary keys are usually auto-increment)
                       columns = [col.name for col in output_table.__table__.columns if not col.primary_key]

                       if columns:
                           column_list = ", ".join(columns)
                           insert_query = f"INSERT INTO {output_table.__tablename__} ({column_list}) {sql_query}"
                       else:
                           # If no non-primary key columns, just insert without specifying columns
                           insert_query = f"INSERT INTO {output_table.__tablename__} {sql_query}"

                       result = conn.execute(text(insert_query))
                       conn.commit()

                       return f"Inserted {result.rowcount} rows into {output_table.__tablename__}"

           except SQLAlchemyError as e:
               raise RuntimeError(f"Database error: {str(e)}")

       return wrapper
   return decorator

if __name__ == "__main__":
    from typing import Optional
    from datetime import datetime, timedelta
    from sqlmodel import Field

    class users(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        username: str = Field(unique=True)
        email: str = Field(unique=True)
        full_name: Optional[str] = None
        is_active: bool = Field(default=True)
        created_at: datetime = Field(default_factory=datetime.utcnow)

    db = PostgresDB(
       "postgresql://myuser:mypassword@localhost:5432/mydatabase",
        tables=[users]
    )

    def insert_dummy_users_sqlmodel():
        from sqlalchemy import insert
        import random

        dummy_users_data = [
            # {"username": "john_doe", "email": "john.doe@example.com", "full_name": "John Doe", "is_active": True},
            # {"username": "jane_smith", "email": "jane.smith@example.com", "full_name": "Jane Smith", "is_active": True},
            # {"username": "bob_wilson", "email": "bob.wilson@example.com", "full_name": "Bob Wilson", "is_active": False},
            # {"username": "alice_brown", "email": "alice.brown@example.com", "full_name": "Alice Brown", "is_active": True},
            # {"username": "charlie_davis", "email": "charlie.davis@example.com", "full_name": "Charlie Davis", "is_active": True},
            # {"username": "diana_miller", "email": "diana.miller@example.com", "full_name": "Diana Miller", "is_active": False},
            # {"username": "testuser1", "email": "test1@test.com", "full_name": None, "is_active": True},
            # {"username": "testuser2", "email": "test2@test.com", "full_name": None, "is_active": True},
            # {"username": "testuser3", "email": "test3@test.com", "full_name": None, "is_active": True},
        ]

        # Add random created_at dates
        for user_data in dummy_users_data:
            days_ago = random.randint(1, 365)
            user_data["created_at"] = datetime.utcnow() - timedelta(days=days_ago)

        with db.engine.connect() as conn:
            try:
                trans = conn.begin()

                # Use SQLAlchemy insert statement
                stmt = insert(users).values(dummy_users_data)
                conn.execute(stmt)

                trans.commit()
                print(f"Successfully inserted {len(dummy_users_data)} users using SQLModel table!")

            except Exception as e:
                trans.rollback()
                print(f"Error inserting users: {e}")

    insert_dummy_users_sqlmodel()
    print("Dummy users inserted successfully.")

    @sql_job(db)
    def count_rows():
        return "SELECT COUNT(*) FROM {{ ref('users') }}"

    output = count_rows()

    print(output)
