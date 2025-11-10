"""
Integration tests for schema version tracking in BatchFileProcessor.

Tests that schema versions are recorded when schema evolution occurs.
"""
import pytest
import uuid

from app.services.batch_file_processor import BatchFileProcessor
from app.services.file_state_service import FileStateService
from app.repositories.schema_version_repository import SchemaVersionRepository
from app.repositories.ingestion_repository import IngestionRepository
from app.models.domain import Ingestion, IngestionStatus


@pytest.mark.integration
class TestSchemaVersionTracking:
    """Test schema version tracking during file processing."""

    @pytest.fixture
    def ingestion(self, test_db):
        """Create test ingestion with unique ID for each test."""
        repo = IngestionRepository(test_db)
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-{unique_id}",
            tenant_id="test-tenant",
            name="Test Schema Version Tracking",
            cluster_id="test-cluster-1",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials={
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            },
            format_type="json",
            destination_catalog="test_catalog",  # Use test_catalog (same as e2e tests)
            destination_database="test_db",
            destination_table=f"schema_version_test_{unique_id[:8]}",
            checkpoint_location=f"/tmp/test-checkpoint-{unique_id}",
            status=IngestionStatus.ACTIVE,
            on_schema_change="append_new_columns",
            schema_version=1,
            created_by="test-user"
        )
        test_db.add(ingestion)
        test_db.commit()
        test_db.refresh(ingestion)
        return ingestion

    def test_schema_version_recorded_on_new_columns(
        self, test_db, spark_session, ingestion
    ):
        """Test that schema version is recorded when new columns are added."""
        # Create initial table with base schema (use BIGINT to match JSON inference)
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id BIGINT,
                name STRING
            ) USING iceberg
        """)

        # Create DataFrame with new column "email" (directly from Python data)
        data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"}
        ]
        df = spark_session.createDataFrame(data)
        file_path = "test_file_with_email.json"  # Virtual path for tracking

        # Set up processor
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        # Process DataFrame - this should trigger schema evolution
        processor._write_to_iceberg(df, file_path)

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
        self, test_db, spark_session, ingestion
    ):
        """Test that schema version is NOT recorded when strategy is 'ignore'."""
        # Change strategy to ignore
        ingestion.on_schema_change = "ignore"
        test_db.commit()

        # Create initial table (use BIGINT to match JSON inference)
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id BIGINT,
                name STRING
            ) USING iceberg
        """)

        # Create DataFrame with new column (directly from Python data)
        data = [{"id": 1, "name": "Alice", "extra": "ignored"}]
        df = spark_session.createDataFrame(data)
        file_path = "test_file_with_extra.json"  # Virtual path for tracking

        # Process DataFrame
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        processor._write_to_iceberg(df, file_path)

        # Verify NO schema version was recorded (because strategy is ignore)
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 0, "No version should be recorded with 'ignore' strategy"

        # Verify ingestion.schema_version unchanged
        test_db.refresh(ingestion)
        assert ingestion.schema_version == 1

    def test_schema_version_recorded_on_sync_strategy(
        self, test_db, spark_session, ingestion
    ):
        """Test schema version recording with sync_all_columns strategy."""
        # Change strategy to sync_all_columns
        ingestion.on_schema_change = "sync_all_columns"
        test_db.commit()

        # Create initial table (use BIGINT to match JSON inference)
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id BIGINT,
                name STRING,
                old_field STRING
            ) USING iceberg
        """)

        # Create DataFrame with different schema (new column, removed column)
        data = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
        df = spark_session.createDataFrame(data)
        file_path = "test_file_new_schema.json"  # Virtual path for tracking

        # Process DataFrame
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        processor._write_to_iceberg(df, file_path)

        # Verify schema version was recorded
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 1

        version = versions[0]
        assert version.version == 2
        assert version.strategy_applied == "sync_all_columns"
        assert file_path in version.affected_files

        # Should have both added and removed columns in changes
        change_types = [change["change_type"] for change in version.changes_json]
        assert "NEW_COLUMN" in change_types
        assert "REMOVED_COLUMN" in change_types

    def test_multiple_schema_versions_increment(
        self, test_db, spark_session, ingestion
    ):
        """Test that version numbers increment correctly over multiple changes."""
        # Create initial table (use BIGINT to match JSON inference)
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id BIGINT,
                name STRING
            ) USING iceberg
        """)

        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        # First schema change: add "email"
        data1 = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
        df1 = spark_session.createDataFrame(data1)
        file1_path = "test_file_data1.json"  # Virtual path for tracking

        processor._write_to_iceberg(df1, file1_path)

        # Second schema change: add "phone"
        data2 = [{"id": 2, "name": "Bob", "email": "bob@example.com", "phone": "555-1234"}]
        df2 = spark_session.createDataFrame(data2)
        file2_path = "test_file_data2.json"  # Virtual path for tracking

        processor._write_to_iceberg(df2, file2_path)

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
        self, test_db, spark_session, ingestion
    ):
        """Test that no version is recorded when schema doesn't change."""
        # Create initial table (use BIGINT to match JSON inference)
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id BIGINT,
                name STRING
            ) USING iceberg
        """)

        # Create DataFrame with SAME schema
        data = [{"id": 1, "name": "Alice"}]
        df = spark_session.createDataFrame(data)
        file_path = "test_file_same_schema.json"  # Virtual path for tracking

        # Process DataFrame
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        processor._write_to_iceberg(df, file_path)

        # Verify NO schema version was recorded (no changes)
        schema_repo = SchemaVersionRepository(test_db)
        versions = schema_repo.get_all_versions(ingestion.id)

        assert len(versions) == 0

        # Verify ingestion.schema_version unchanged
        test_db.refresh(ingestion)
        assert ingestion.schema_version == 1

    def test_schema_json_captured_correctly(
        self, test_db, spark_session, ingestion
    ):
        """Test that the full schema is captured correctly after evolution."""
        # Create initial table (use BIGINT to match JSON inference)
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        spark_session.sql(f"""
            CREATE TABLE {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table} (
                id BIGINT,
                name STRING
            ) USING iceberg
        """)

        # Create DataFrame with new column (directly from Python data)
        data = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
        df = spark_session.createDataFrame(data)
        file_path = "test_file_with_email.json"  # Virtual path for tracking

        # Process DataFrame
        from app.spark.connect_client import SparkConnectClient
        spark_client = SparkConnectClient(spark_session=spark_session)
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        processor._write_to_iceberg(df, file_path)

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
