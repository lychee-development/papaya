import pytest
import pandas as pd
from datafruit import PostgresDB
from datafruit.sql_job import sql_job
from datafruit.dummy_models import TestUser, TestOutput

# This fixture uses pytest_postgresql to create a PostgreSQL database for testing.
@pytest.fixture
def postgresql_db_conn_str(postgresql):
    connection = f'postgresql+psycopg2://{postgresql.info.user}:@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'
    return connection



@pytest.fixture
def db_with_data(postgresql_db_conn_str):
    from sqlalchemy import insert

    db = PostgresDB(postgresql_db_conn_str, tables=[TestUser, TestOutput])

    # Sync schema to create all tables
    db.sync_schema()

    # Insert test data using SQLAlchemy insert like in sql_job.py
    test_users_data = [
        {"username": "user1", "email": "user1@test.com", "full_name": "User One", "is_active": True},
        {"username": "user2", "email": "user2@test.com", "full_name": "User Two", "is_active": False},
        {"username": "user3", "email": "user3@test.com", "full_name": "User Three", "is_active": True},
    ]

    with db.engine.connect() as conn:
        trans = conn.begin()
        try:
            stmt = insert(TestUser).values(test_users_data)
            conn.execute(stmt)
            trans.commit()
        except Exception as e:
            trans.rollback()
            raise e

    return db

class TestSqlJob:
    def test_sql_job_returns_dataframe_by_default(self, db_with_data):
        @sql_job(db_with_data)
        def get_user_count():
            return "SELECT COUNT(*) as count FROM {{ ref('TestUser') }}"

        result = get_user_count()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['count'] == 3

    def test_sql_job_with_output_table(self, db_with_data):
        @sql_job(db_with_data, output_table=TestOutput)
        def insert_user_count():
            return "SELECT COUNT(*) as count_value FROM {{ ref('TestUser') }}"

        result = insert_user_count()

        assert isinstance(result, str)
        assert "Inserted 1 rows into test_output" in result

        # Verify data was inserted
        @sql_job(db_with_data)
        def check_output():
            return "SELECT * FROM {{ ref('TestOutput') }}"

        output_data = check_output()
        assert len(output_data) == 1
        assert output_data.iloc[0]['count_value'] == 3

    def test_ref_template_replacement(self, db_with_data):
        @sql_job(db_with_data)
        def test_ref_replacement():
            return "SELECT username FROM {{ ref('TestUser') }} WHERE is_active = true"

        result = test_ref_replacement()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # user1 and user3 are active
        active_usernames = result['username'].tolist()
        assert 'user1' in active_usernames
        assert 'user3' in active_usernames
        assert 'user2' not in active_usernames

    def test_multiple_ref_replacements(self, db_with_data):
        @sql_job(db_with_data)
        def test_multiple_refs():
            return """
            SELECT u1.username, u2.email
            FROM {{ ref('TestUser') }} u1
            JOIN {{ ref('TestUser') }} u2 ON u1.id = u2.id
            WHERE u1.is_active = true
            """

        result = test_multiple_refs()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_function_must_return_string(self, db_with_data):
        with pytest.raises(ValueError, match="The function must return a valid SQL query string"):
            @sql_job(db_with_data)
            def invalid_return():
                return 123

            invalid_return()

    def test_invalid_table_reference(self, db_with_data):
        with pytest.raises(ValueError, match="Table model 'NonExistentTable' not found"):
            @sql_job(db_with_data)
            def invalid_ref():
                return "SELECT * FROM {{ ref('NonExistentTable') }}"

            invalid_ref()

    def test_invalid_sql_query(self, db_with_data):
        with pytest.raises(RuntimeError, match="Database error"):
            @sql_job(db_with_data)
            def invalid_sql():
                return "INVALID SQL QUERY"

            invalid_sql()

    def test_plan_parameter(self, db_with_data):
        # This test verifies the plan parameter exists, though current implementation doesn't use it
        @sql_job(db_with_data, plan=True)
        def test_plan():
            return "SELECT COUNT(*) FROM {{ ref('TestUser') }}"

        result = test_plan()
        assert isinstance(result, pd.DataFrame)

    def test_ref_with_whitespace_variations(self, db_with_data):
        @sql_job(db_with_data)
        def test_whitespace_ref():
            return "SELECT COUNT(*) FROM {{ref('TestUser')}}"  # No spaces

        result = test_whitespace_ref()
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0].iloc[0] == 3

    def test_empty_result_set(self, db_with_data):
        @sql_job(db_with_data)
        def empty_result():
            return "SELECT * FROM {{ ref('TestUser') }} WHERE username = 'nonexistent'"

        result = empty_result()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_complex_query_with_aggregation(self, db_with_data):
        @sql_job(db_with_data)
        def complex_query():
            return """
            SELECT
                is_active,
                COUNT(*) as user_count,
                COUNT(full_name) as users_with_full_name
            FROM {{ ref('TestUser') }}
            GROUP BY is_active
            ORDER BY is_active
            """

        result = complex_query()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # True and False for is_active
        assert result.iloc[0]['user_count'] == 1  # False group
        assert result.iloc[1]['user_count'] == 2  # True group
