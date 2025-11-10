"""
Integration tests for schema version tracking in BatchFileProcessor.

Tests that schema versions are recorded when schema evolution occurs.
"""
import pytest
import json
import tempfile
from pathlib import Path

from app.services.batch_file_processor import BatchFileProcessor
from app.services.file_state_service import FileStateService
from app.repositories.schema_version_repository import SchemaVersionRepository
from app.repositories.ingestion_repository import IngestionRepository
from app.models.domain import Ingestion, IngestionStatus


@pytest.mark.integration
class TestSchemaVersionTracking:
    """Test schema version tracking during file processing."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def ingestion(self, test_db):
        """Create test ingestion."""
        repo = IngestionRepository(test_db)
        ingestion = Ingestion(
            id="test-ingestion-123",
            tenant_id="test-tenant",
            name="Test Schema Version Tracking",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials=json.dumps({
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            }),
            format_type="json",
            destination_catalog="test_catalog",
            destination_database="default",
            destination_table="schema_version_test",
            status=IngestionStatus.ACTIVE,
            on_schema_change="append_new_columns",
            schema_version=1
        )
        test_db.add(ingestion)
        test_db.commit()
        test_db.refresh(ingestion)
        return ingestion

    def test_schema_version_recorded_on_new_columns(
        self, test_db, spark_session, ingestion, temp_dir
    ):
        """Test that schema version is recorded when new columns are added."""
        # Create initial table with base schema
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id INT,
                name STRING
            ) USING iceberg
        """)

        # Create test file with new column "email"
        file_path = temp_dir / "data_with_email.json"
        with open(file_path, 'w') as f:
            f.write('{"id": 1, "name": "Alice", "email": "alice@example.com"}\n')
            f.write('{"id": 2, "name": "Bob", "email": "bob@example.com"}\n')

        # Set up processor
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        # Process file - this should trigger schema evolution
        df = spark_session.read.json(str(file_path))
        processor._write_to_iceberg(df, str(file_path))

        # Verify schema version was recorded
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 1, "Schema version should be recorded"

        version = versions[0]
        assert version.version == 2, "Version should increment to 2"
        assert version.strategy_applied == "append_new_columns"
        assert str(file_path) in version.affected_files
        assert len(version.changes_json) == 1
        assert version.changes_json[0]["change_type"] == "NEW_COLUMN"
        assert version.changes_json[0]["column_name"] == "email"

        # Verify ingestion.schema_version was updated
        test_db.refresh(ingestion)
        assert ingestion.schema_version == 2

    def test_schema_version_not_recorded_on_ignore_strategy(
        self, test_db, spark_session, ingestion, temp_dir
    ):
        """Test that schema version is NOT recorded when strategy is 'ignore'."""
        # Change strategy to ignore
        ingestion.on_schema_change = "ignore"
        test_db.commit()

        # Create initial table
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id INT,
                name STRING
            ) USING iceberg
        """)

        # Create test file with new column
        file_path = temp_dir / "data_with_extra.json"
        with open(file_path, 'w') as f:
            f.write('{"id": 1, "name": "Alice", "extra": "ignored"}\n')

        # Process file
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        df = spark_session.read.json(str(file_path))
        processor._write_to_iceberg(df, str(file_path))

        # Verify NO schema version was recorded (because strategy is ignore)
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 0, "No version should be recorded with 'ignore' strategy"

        # Verify ingestion.schema_version unchanged
        test_db.refresh(ingestion)
        assert ingestion.schema_version == 1

    def test_schema_version_recorded_on_sync_strategy(
        self, test_db, spark_session, ingestion, temp_dir
    ):
        """Test schema version recording with sync_all_columns strategy."""
        # Change strategy to sync_all_columns
        ingestion.on_schema_change = "sync_all_columns"
        test_db.commit()

        # Create initial table
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id INT,
                name STRING,
                old_field STRING
            ) USING iceberg
        """)

        # Create test file with different schema (new column, removed column)
        file_path = temp_dir / "data_new_schema.json"
        with open(file_path, 'w') as f:
            f.write('{"id": 1, "name": "Alice", "email": "alice@example.com"}\n')

        # Process file
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        df = spark_session.read.json(str(file_path))
        processor._write_to_iceberg(df, str(file_path))

        # Verify schema version was recorded
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 1

        version = versions[0]
        assert version.version == 2
        assert version.strategy_applied == "sync_all_columns"
        assert str(file_path) in version.affected_files

        # Should have both added and removed columns in changes
        change_types = [change["change_type"] for change in version.changes_json]
        assert "NEW_COLUMN" in change_types
        assert "REMOVED_COLUMN" in change_types

    def test_multiple_schema_versions_increment(
        self, test_db, spark_session, ingestion, temp_dir
    ):
        """Test that version numbers increment correctly over multiple changes."""
        # Create initial table
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id INT,
                name STRING
            ) USING iceberg
        """)

        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        # First schema change: add "email"
        file1_path = temp_dir / "data1.json"
        with open(file1_path, 'w') as f:
            f.write('{"id": 1, "name": "Alice", "email": "alice@example.com"}\n')

        df1 = spark_session.read.json(str(file1_path))
        processor._write_to_iceberg(df1, str(file1_path))

        # Second schema change: add "phone"
        file2_path = temp_dir / "data2.json"
        with open(file2_path, 'w') as f:
            f.write('{"id": 2, "name": "Bob", "email": "bob@example.com", "phone": "555-1234"}\n')

        df2 = spark_session.read.json(str(file2_path))
        processor._write_to_iceberg(df2, str(file2_path))

        # Verify two versions were recorded
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 2
        assert versions[0].version == 2
        assert versions[1].version == 3

        # Verify ingestion.schema_version is 3
        test_db.refresh(ingestion)
        assert ingestion.schema_version == 3

    def test_no_schema_version_when_no_changes(
        self, test_db, spark_session, ingestion, temp_dir
    ):
        """Test that no version is recorded when schema doesn't change."""
        # Create initial table
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id INT,
                name STRING
            ) USING iceberg
        """)

        # Create test file with SAME schema
        file_path = temp_dir / "data_same_schema.json"
        with open(file_path, 'w') as f:
            f.write('{"id": 1, "name": "Alice"}\n')

        # Process file
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        df = spark_session.read.json(str(file_path))
        processor._write_to_iceberg(df, str(file_path))

        # Verify NO schema version was recorded (no changes)
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 0

        # Verify ingestion.schema_version unchanged
        test_db.refresh(ingestion)
        assert ingestion.schema_version == 1

    def test_schema_json_captured_correctly(
        self, test_db, spark_session, ingestion, temp_dir
    ):
        """Test that the full schema is captured correctly after evolution."""
        # Create initial table
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id INT,
                name STRING
            ) USING iceberg
        """)

        # Create test file with new column
        file_path = temp_dir / "data_with_email.json"
        with open(file_path, 'w') as f:
            f.write('{"id": 1, "name": "Alice", "email": "alice@example.com"}\n')

        # Process file
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        df = spark_session.read.json(str(file_path))
        processor._write_to_iceberg(df, str(file_path))

        # Verify schema_json contains all columns (including new one)
        schema_repo = SchemaVersionRepository(test_db)
        version = schema_repo.get_latest_version(ingestion.id)

        assert version is not None

        # Schema should be a dict with field names
        schema_json = version.schema_json
        assert "fields" in schema_json

        field_names = [field["name"] for field in schema_json["fields"]]
        assert "id" in field_names
        assert "name" in field_names
        assert "email" in field_names  # New column should be in schema
