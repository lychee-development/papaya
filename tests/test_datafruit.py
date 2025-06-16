import pytest
from datafruit.datafruit import PostgresDB
from sqlmodel import SQLModel
from sqlalchemy import Engine, MetaData
import json
import tempfile
import hashlib
import sys
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch
import datafruit.cli as cli_module
from datafruit.dummy_models import TestUser, TestPost, TestProfile, TestComplexModel

# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def postgresql_db_conn_str(postgresql):
    connection = f'postgresql+psycopg2://{postgresql.info.user}:@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'
    return connection

@pytest.fixture
def db_instance(postgresql_db_conn_str):
    return PostgresDB(postgresql_db_conn_str, [TestUser, TestPost, TestProfile])

@pytest.fixture
def db_with_tables(db_instance):
    """Create a database instance with tables already created"""
    db_instance.sync_schema()
    return db_instance

@pytest.fixture
def empty_db_instance(postgresql_db_conn_str):
    """Create a database instance without any tables"""
    return PostgresDB(postgresql_db_conn_str, [])

@pytest.fixture
def complex_db_instance(postgresql_db_conn_str):
    return PostgresDB(postgresql_db_conn_str, [TestComplexModel])

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)

@pytest.fixture
def mock_dft_dir(temp_dir):
    dft_path = temp_dir / '.dft'
    dft_path.mkdir(parents=True, exist_ok=True)

    with patch.object(cli_module, 'DFT_DIR', dft_path):
        with patch.object(cli_module, 'PLAN_FILE', dft_path / 'plan.json'):
            yield dft_path

@pytest.fixture
def sample_schema_content():
    return '''import datafruit as dft
from sqlmodel import Field, SQLModel

class TestUserSample(SQLModel, table=True):
    __tablename__ = "test_users_sample"
    id: int = Field(primary_key=True)
    name: str

db = dft.PostgresDB("postgresql://localhost/test", [TestUserSample])
dft.export([db])'''

@pytest.fixture
def temp_project_dir():
    """Create a temporary project directory"""
    with tempfile.TemporaryDirectory() as temp_dir:
        project_dir = Path(temp_dir)
        yield project_dir

@pytest.fixture(autouse=True)
def cleanup_sqlmodel_registry():
    """Clean up SQLModel registry between tests to avoid conflicts."""
    yield
    try:
        if hasattr(SQLModel, 'registry') and hasattr(SQLModel.registry, '_class_registry'):
            SQLModel.registry._class_registry.clear()
        if hasattr(SQLModel, 'metadata'):
            SQLModel.metadata.clear()
    except Exception:
        pass

# =============================================================================
# ORIGINAL TESTS - Implementation Level
# =============================================================================

def test_init_creates_engine(postgresql_db_conn_str):
    db = PostgresDB(postgresql_db_conn_str, [TestUser])
    assert db.connection_string == postgresql_db_conn_str
    assert db.tables == [TestUser]
    assert db.engine is not None

def test_connect_returns_session(db_instance):
    engine = db_instance.engine
    assert isinstance(engine, Engine)

def test_get_engine_returns_new_engine_instance(db_instance):
    """Test that get_engine() returns a new Engine instance each time"""
    engine1 = db_instance.engine
    engine2 = db_instance.engine
    assert isinstance(engine1, Engine)
    assert isinstance(engine2, Engine)
    assert str(engine1.url) == str(engine2.url)

def test_get_local_metadata_returns_metadata_object(db_instance):
    """Test that _get_local_metadata() returns a MetaData object"""
    metadata = db_instance._get_local_metadata()
    assert isinstance(metadata, MetaData)

def test_get_local_metadata_contains_specified_tables(db_instance):
    """Test that local metadata contains all tables specified in __init__"""
    metadata = db_instance._get_local_metadata()
    table_names = [table.name for table in metadata.tables.values()]
    expected_tables = ["test_users", "test_posts", "test_profiles"]
    for expected_table in expected_tables:
        assert expected_table in table_names

def test_get_local_metadata_empty_when_no_tables(empty_db_instance):
    """Test that _get_local_metadata() returns empty metadata when no tables specified"""
    metadata = empty_db_instance._get_local_metadata()
    assert isinstance(metadata, MetaData)
    assert len(metadata.tables) == 0

# =============================================================================
# NEW TESTS - Declarative Configuration Management
# =============================================================================

@pytest.mark.config
class TestSchemaConfigurationParsing:
    """Test parsing and validation of declarative schema configurations"""

    def test_load_valid_schema_configuration(self, temp_dir, postgresql_db_conn_str):
        """Test loading a valid declarative schema file"""
        schema_content = f'''import datafruit as dft
from sqlmodel import Field, SQLModel
from typing import Optional

class TestUserConfig(SQLModel, table=True):
    __tablename__ = "test_users_config"
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True)
    email: str

db = dft.PostgresDB("{postgresql_db_conn_str}", [TestUserConfig])
dft.export([db])
'''
        schema_file = temp_dir / "dft.py"
        schema_file.write_text(schema_content)

        exported_dbs = cli_module.load_schema_from_file(schema_file)
        assert len(exported_dbs) == 1
        assert len(exported_dbs[0].tables) == 1

    def test_invalid_schema_configuration_syntax_error(self, temp_dir):
        """Test handling of invalid Python syntax in schema file"""
        invalid_schema = '''import datafruit as dft
from sqlmodel import SQLModel, Field

class TestUserInvalid(SQLModel, table=True)
    __tablename__ = "test_users_invalid"
    id: int = Field(primary_key=True)
'''
        schema_file = temp_dir / "dft.py"
        schema_file.write_text(invalid_schema)

        with pytest.raises(Exception):
            cli_module.load_schema_from_file(schema_file)

    def test_schema_configuration_missing_export(self, temp_dir, postgresql_db_conn_str):
        """Test schema file that doesn't call dft.export()"""
        schema_content = f'''import datafruit as dft
from sqlmodel import Field, SQLModel

class TestUserNoExport(SQLModel, table=True):
    __tablename__ = "test_users_no_export"
    id: int = Field(primary_key=True)

db = dft.PostgresDB("{postgresql_db_conn_str}", [TestUserNoExport])
'''
        schema_file = temp_dir / "dft.py"
        schema_file.write_text(schema_content)

        exported_dbs = cli_module.load_schema_from_file(schema_file)
        assert len(exported_dbs) == 1

    def test_schema_hash_generation(self, temp_dir):
        """Test that schema file hashing works for change detection"""
        content1 = "# Schema version 1"
        content2 = "# Schema version 2"

        schema_file = temp_dir / "schema.py"

        schema_file.write_text(content1)
        hash1 = cli_module.get_schema_hash(schema_file)

        schema_file.write_text(content2)
        hash2 = cli_module.get_schema_hash(schema_file)

        assert hash1 != hash2
        assert isinstance(hash1, str)
        assert len(hash1) > 0

# =============================================================================
# NEW TESTS - State Diffing (Core Declarative Feature)
# =============================================================================

@pytest.mark.diffing
class TestStateDiffing:
    """Test comparing desired state (config) vs actual state (database)"""

    def test_diff_detects_new_tables(self, db_instance):
        """Test that diff correctly identifies tables that need to be created"""
        diffs = db_instance.get_schema_diff()

        table_adds = [diff for diff in diffs if diff[0] == "add_table"]
        assert len(table_adds) == 3

    def test_diff_empty_when_states_match(self, db_instance):
        """Test that no diff is generated when database matches config"""
        success = db_instance.sync_schema()
        assert success is True

        diffs = db_instance.get_schema_diff()
        assert len(diffs) == 0

    def test_diff_with_empty_configuration(self, postgresql_db_conn_str):
        """Test diff when configuration declares no tables"""
        empty_db = PostgresDB(postgresql_db_conn_str, [])

        diffs = empty_db.get_schema_diff()
        assert isinstance(diffs, list)

# =============================================================================
# NEW TESTS - Plan Generation & Management
# =============================================================================

@pytest.mark.planning
class TestPlanGeneration:
    """Test plan generation and management (like terraform plan)"""

    def test_plan_serialization_and_deserialization(self, mock_dft_dir):
        """Test saving and loading plans"""
        diffs_by_db = [
            ("postgresql://test", [("add_table", Mock())])
        ]
        schema_hash = "test_hash"

        with patch('datafruit.cli.serialize_diff') as mock_serialize:
            mock_serialize.return_value = {"type": "add_table", "table": "test"}

            cli_module.save_plan(diffs_by_db, schema_hash)
            loaded_plan = cli_module.load_plan()

            assert loaded_plan is not None
            assert loaded_plan["schema_hash"] == schema_hash

    def test_plan_expiration(self, mock_dft_dir):
        """Test that old plans expire automatically"""
        expired_plan_data = {
            "timestamp": "2020-01-01T00:00:00",
            "schema_hash": "old_hash",
            "databases": [],
            "expiry_minutes": 10
        }

        plan_file = mock_dft_dir / "plan.json"
        plan_file.write_text(json.dumps(expired_plan_data))

        loaded_plan = cli_module.load_plan()
        assert loaded_plan is None
        assert not plan_file.exists()

    def test_plan_validation_schema_changed(self, mock_dft_dir, temp_dir):
        """Test plan invalidation when schema file changes"""
        schema_file = temp_dir / "dft.py"
        schema_file.write_text("# Original content")
        original_hash = cli_module.get_schema_hash(schema_file)

        diffs_by_db = []
        cli_module.save_plan(diffs_by_db, original_hash)

        schema_file.write_text("# Modified content")
        new_hash = cli_module.get_schema_hash(schema_file)

        assert original_hash != new_hash

    def test_diff_serialization(self):
        """Test that diffs can be properly serialized for plans"""
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.columns = []

        add_table_diff = ("add_table", mock_table)
        serialized = cli_module.serialize_diff(add_table_diff)

        assert serialized["type"] == "add_table"
        assert serialized["table_name"] == "test_table"

# =============================================================================
# NEW TESTS - Apply/Sync Operations
# =============================================================================

@pytest.mark.apply
class TestApplyOperations:
    """Test applying plans to reach desired state (like terraform apply)"""

    def test_sync_creates_declared_tables(self, db_instance):
        """Test that sync creates all tables declared in config"""
        initial_tables = db_instance.get_table_info()
        table_names = [table[0] for table in initial_tables]
        assert "test_users" not in table_names
        assert "test_posts" not in table_names

        success = db_instance.sync_schema()
        assert success is True

        final_tables = db_instance.get_table_info()
        table_names = [table[0] for table in final_tables]
        assert "test_users" in table_names
        assert "test_posts" in table_names

    def test_sync_is_idempotent(self, db_instance):
        """Test that running sync multiple times is safe (idempotent)"""
        success1 = db_instance.sync_schema()
        assert success1 is True

        success2 = db_instance.sync_schema()
        assert success2 is True

        diffs = db_instance.get_schema_diff()
        assert len(diffs) == 0

    def test_sync_handles_connection_failure(self):
        """Test graceful handling of connection failures during sync"""
        invalid_db = PostgresDB("postgresql+psycopg2://user:pass@nonexistent-host:5432/db", [TestUser])

        success = invalid_db.sync_schema()
        assert success is False

# =============================================================================
# NEW TESTS - CLI Interface
# =============================================================================

@pytest.mark.cli
class TestCLIInterface:
    """Test the declarative CLI commands (init, plan, apply)"""

    def test_cli_init_creates_project_structure(self, temp_dir):
        """Test that 'dft init' creates proper project structure"""
        project_dir = temp_dir / "test_project"

        success = cli_module.datafruit_default_init(str(project_dir))
        assert success is True

        dft_file = project_dir / "dft.py"
        assert dft_file.exists()

        content = dft_file.read_text()
        assert "import datafruit as dft" in content
        assert "SQLModel" in content

    def test_cli_project_name_validation(self):
        """Test project name validation rules"""
        valid_names = ["my-project", "project_123", "DataProject"]
        invalid_names = ["", "project with spaces", "project@special", None]

        for name in valid_names:
            assert cli_module.is_valid_project_name(name) is True

        for name in invalid_names:
            assert cli_module.is_valid_project_name(name) is False

    def test_cli_plan_workflow(self, mock_dft_dir):
        """Test the plan command workflow"""
        plan_file = mock_dft_dir / "plan.json"
        plan_file.write_text('{"test": "data"}')
        assert plan_file.exists()

        cli_module.clear_plan()
        assert not plan_file.exists()

    def test_get_schema_hash(self, temp_dir):
        schema_file = temp_dir / "test_schema.py"
        content = "test content for hashing"
        schema_file.write_text(content)

        hash_result = cli_module.get_schema_hash(schema_file)

        expected_hash = hashlib.sha256(content.encode()).hexdigest()
        assert hash_result == expected_hash

    def test_get_schema_hash_different_content(self, temp_dir):
        schema_file1 = temp_dir / "schema1.py"
        schema_file2 = temp_dir / "schema2.py"

        schema_file1.write_text("content 1")
        schema_file2.write_text("content 2")

        hash1 = cli_module.get_schema_hash(schema_file1)
        hash2 = cli_module.get_schema_hash(schema_file2)

        assert hash1 != hash2

    def test_ensure_dft_dir_creates_directory(self, mock_dft_dir):
        assert mock_dft_dir.exists()

        cli_module.ensure_dft_dir()

        assert mock_dft_dir.exists()
        assert mock_dft_dir.is_dir()

    def test_serialize_diff_add_table(self):
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_column1 = Mock()
        mock_column1.name = "col1"
        mock_column2 = Mock()
        mock_column2.name = "col2"
        mock_table.columns = [mock_column1, mock_column2]

        diff = ("add_table", mock_table)
        result = cli_module.serialize_diff(diff)

        expected = {
            "type": "add_table",
            "table_name": "test_table",
            "columns": ["col1", "col2"]
        }
        assert result == expected

    def test_serialize_diff_add_column(self):
        mock_column = Mock()
        mock_column.name = "new_column"
        mock_column.type = "VARCHAR(50)"

        diff = ("add_column", None, "test_table", mock_column)
        result = cli_module.serialize_diff(diff)

        expected = {
            "type": "add_column",
            "table_name": "test_table",
            "column_name": "new_column",
            "column_type": "VARCHAR(50)"
        }
        assert result == expected

    def test_serialize_diff_unknown_type(self):
        diff = ("unknown_diff_type", "some", "random", "data")
        result = cli_module.serialize_diff(diff)

        expected = {
            "type": "unknown_diff_type",
            "details": str(diff)
        }
        assert result == expected

# =============================================================================
# NEW TESTS - Connection & Validation
# =============================================================================

@pytest.mark.connection
class TestConnectionValidation:
    """Test database connection validation for declared resources"""

    def test_valid_connection_string(self, postgresql_db_conn_str):
        """Test validation of valid connection strings"""
        db = PostgresDB(postgresql_db_conn_str, [])
        assert db.validate_connection() is True

    def test_invalid_connection_string_format(self):
        """Test validation rejects malformed connection strings"""
        with pytest.raises(ValueError, match="Invalid database connection string"):
            PostgresDB("not-a-database-url-at-all", [])

    def test_unreachable_database_connection(self):
        """Test handling of unreachable database servers"""
        unreachable_db = PostgresDB("postgresql+psycopg2://user:pass@localhost:99999/db", [])
        assert unreachable_db.validate_connection() is False

# =============================================================================
# NEW TESTS - Integration Tests
# =============================================================================

@pytest.mark.integration
class TestPostgresDBInit:
    """Test PostgresDB initialization edge cases"""

    def test_init_creates_engine(self, postgresql_db_conn_str):
        db = PostgresDB(postgresql_db_conn_str, [TestUser])
        assert db.connection_string == postgresql_db_conn_str
        assert db.tables == [TestUser]
        assert db.engine is not None

    def test_init_with_multiple_tables(self, postgresql_db_conn_str):
        tables = [TestUser, TestPost, TestProfile]
        db = PostgresDB(postgresql_db_conn_str, tables)
        assert db.tables == tables
        assert len(db.tables) == 3

    def test_init_with_invalid_connection_string(self):
        with pytest.raises(ValueError, match="Invalid database connection string"):
            PostgresDB("invalid://connection", [TestUser])

    def test_init_creates_engine_instance(self, postgresql_db_conn_str):
        db = PostgresDB(postgresql_db_conn_str, [TestUser])
        assert hasattr(db, 'engine')
        assert db.engine.url.drivername == 'postgresql+psycopg2'

@pytest.mark.integration
class TestRealDatabaseConnections:
    """Test actual database connection scenarios"""

    def test_successful_connection_and_query(self, db_instance):
        """Test that we can connect and execute a simple query"""
        result = db_instance.execute_sql("SELECT version()")
        assert result is not None
        version_info = result.fetchone()
        assert version_info is not None
        assert "PostgreSQL" in str(version_info[0])

    def test_connection_with_invalid_host(self):
        """Test connection failure with invalid host"""
        invalid_db = PostgresDB("postgresql+psycopg2://user:pass@nonexistent-host:5432/db", [TestUser])
        assert invalid_db.validate_connection() is False

    def test_connection_with_invalid_port(self):
        """Test connection failure with invalid port"""
        invalid_db = PostgresDB("postgresql+psycopg2://user:pass@localhost:99999/db", [TestUser])
        assert invalid_db.validate_connection() is False

    def test_validate_connection_successful(self, db_instance):
        assert db_instance.validate_connection() is True

    def test_concurrent_connections(self, postgresql_db_conn_str):
        """Test multiple concurrent connections to the same database"""
        db1 = PostgresDB(postgresql_db_conn_str, [TestUser])
        db2 = PostgresDB(postgresql_db_conn_str, [TestPost])

        assert db1.validate_connection() is True
        assert db2.validate_connection() is True

        result1 = db1.execute_sql("SELECT 1 as test1")
        result2 = db2.execute_sql("SELECT 2 as test2")

        assert result1.fetchone()[0] == 1
        assert result2.fetchone()[0] == 2

@pytest.mark.integration
class TestSQLExecutionIntegration:
    """Test SQL execution with real database"""

    def test_execute_sql_successful(self, db_instance):
        result = db_instance.execute_sql("SELECT 1 as test_column")
        assert result is not None

    def test_execute_sql_with_invalid_sql(self, db_instance):
        result = db_instance.execute_sql("INVALID SQL STATEMENT")
        assert result is None

    def test_get_table_info_all_tables(self, db_instance):
        result = db_instance.get_table_info()
        assert isinstance(result, list)

    def test_get_table_info_nonexistent_table(self, db_instance):
        result = db_instance.get_table_info("nonexistent_table")
        assert result == []

# =============================================================================
# Test Runners
# =============================================================================

def run_declarative_tests():
    """Run all tests relevant for declarative data engineering"""
    import subprocess
    import sys

    cmd = [
        sys.executable, "-m", "pytest", __file__,
        "-v", "--tb=short"
    ]

    result = subprocess.run(cmd)
    return result.returncode == 0

def run_original_tests():
    """Run only the original implementation tests"""
    import subprocess
    import sys

    cmd = [
        sys.executable, "-m", "pytest", __file__,
        "-k", "not (config or diffing or planning or apply or cli or connection)",
        "-v"
    ]

    result = subprocess.run(cmd)
    return result.returncode == 0

def run_core_declarative_tests():
    """Run only the most critical declarative tests"""
    import subprocess
    import sys

    cmd = [
        sys.executable, "-m", "pytest", __file__,
        "-m", "config or diffing or planning",
        "-v"
    ]

    result = subprocess.run(cmd)
    return result.returncode == 0

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DataFruit Test Runner")
    parser.add_argument("category", nargs="?", default="all",
                       choices=["config", "diffing", "planning", "apply", "cli", "connection",
                               "integration", "original", "declarative", "core", "all"],
                       help="Test category to run")

    args = parser.parse_args()

    if args.category == "original":
        success = run_original_tests()
    elif args.category == "declarative":
        success = run_core_declarative_tests()
    elif args.category == "core":
        success = run_core_declarative_tests()
    elif args.category == "all":
        success = run_declarative_tests()
    else:
        import subprocess
        import sys
        cmd = [sys.executable, "-m", "pytest", __file__, "-m", args.category, "-v"]
        result = subprocess.run(cmd)
        success = result.returncode == 0

    sys.exit(0 if success else 1)
