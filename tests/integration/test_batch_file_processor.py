"""
Integration tests for BatchFileProcessor._process_single_file.
"""
import pytest
import json
import uuid
import io
from app.services.batch_file_processor import BatchFileProcessor
from app.services.file_state_service import FileStateService
from app.repositories.ingestion_repository import IngestionRepository
from app.models.domain import Ingestion, IngestionStatus

@pytest.mark.integration
class TestBatchFileProcessor:
    """Test BatchFileProcessor._process_single_file method."""

    @pytest.fixture
    def ingestion(self, test_db):
        """Create test ingestion."""
        repo = IngestionRepository(test_db)
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-{unique_id}",
            tenant_id="test-tenant",
            name="Test Batch Processor",
            cluster_id="test-cluster-1",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials={
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            },
            format_type="json",
            destination_catalog="test_catalog",
            destination_database="test_db",
            destination_table=f"batch_test_{unique_id[:8]}",
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

    def _upload_file(self, minio_client, bucket, key, content):
        """Helper to upload file to MinIO."""
        data = json.dumps(content).encode('utf-8')
        minio_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=io.BytesIO(data)
        )
        return f"s3a://{bucket}/{key}"

    def test_process_single_file_success_infer_schema(
        self, test_db, spark_client, spark_session, minio_client, lakehouse_bucket, ingestion
    ):
        """Test processing a single file with schema inference."""
        # 1. Setup: Upload file
        file_data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88}
        ]
        file_key = f"data/test_infer_{uuid.uuid4()}.json"
        s3_path = self._upload_file(minio_client, lakehouse_bucket, file_key, file_data)
        
        # Ensure table is clean
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")

        # 2. Action
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        
        # Mock file info
        file_info = {"path": s3_path, "size": 100, "last_modified": 1234567890}
        
        result = processor._process_single_file(s3_path, file_info)

        # 3. Verification
        assert result['record_count'] == 2
        
        # Verify data in Iceberg
        df = spark_session.table(f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        rows = df.collect()
        assert len(rows) == 2
        
        # Check content (order might vary, so sort or check set)
        ids = {row['id'] for row in rows}
        assert ids == {1, 2}
        names = {row['name'] for row in rows}
        assert names == {"Alice", "Bob"}

    def test_process_single_file_success_predefined_schema(
        self, test_db, spark_client, spark_session, minio_client, lakehouse_bucket, ingestion
    ):
        """Test processing a single file with predefined schema."""
        # 1. Setup: Configure schema
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}}
            ]
        }
        ingestion.schema_json = json.dumps(schema)
        test_db.commit()
        
        # Upload file (has extra field 'score' which should be ignored if schema is strict? 
        # Spark read with schema usually fills missing with null, but extra fields in JSON might be ignored depending on mode.
        # Default mode is PERMISSIVE, but let's see. If we provide schema, Spark reads only those columns.)
        file_data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88}
        ]
        file_key = f"data/test_schema_{uuid.uuid4()}.json"
        s3_path = self._upload_file(minio_client, lakehouse_bucket, file_key, file_data)
        
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")

        # 2. Action
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        
        file_info = {"path": s3_path, "size": 100}
        result = processor._process_single_file(s3_path, file_info)

        # 3. Verification
        assert result['record_count'] == 2
        
        df = spark_session.table(f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        assert "score" not in df.columns
        assert "name" in df.columns
        assert df.count() == 2

    def test_process_single_file_empty_file(
        self, test_db, spark_client, spark_session, minio_client, lakehouse_bucket, ingestion
    ):
        """Test processing an empty file."""
        # 1. Setup: Upload empty list JSON
        file_data = []
        file_key = f"data/test_empty_{uuid.uuid4()}.json"
        s3_path = self._upload_file(minio_client, lakehouse_bucket, file_key, file_data)
        
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")

        # 2. Action
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        
        file_info = {"path": s3_path, "size": 10}
        result = processor._process_single_file(s3_path, file_info)

        # 3. Verification
        assert result['record_count'] == 0
        
        # Table should NOT exist because we haven't written anything (saveAsTable is called in _write_to_iceberg which is skipped if count is 0)
        # Wait, the code says:
        # if record_count == 0: return ...
        # So _write_to_iceberg is NOT called.
        # Let's verify table does not exist.
        table_exists = spark_session.catalog.tableExists(f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        assert not table_exists

    def test_process_single_file_malformed_file(
        self, test_db, spark_client, spark_session, minio_client, lakehouse_bucket, ingestion
    ):
        """Test processing a malformed file."""
        # 1. Setup: Upload malformed content (not JSON)
        content = b"{ this is not valid json }"
        file_key = f"data/test_malformed_{uuid.uuid4()}.json"
        minio_client.put_object(
            Bucket=lakehouse_bucket,
            Key=file_key,
            Body=io.BytesIO(content)
        )
        s3_path = f"s3a://{lakehouse_bucket}/{file_key}"
        
        # 2. Action & Verification
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        
        file_info = {"path": s3_path, "size": 100}
        
        # Spark might not raise exception immediately on read if lazy, but count() triggers it.
        # However, Spark's JSON reader in PERMISSIVE mode (default) might just return nulls or corrupt record column.
        # If we want it to fail, we might need FAILFAST mode or check for corruption.
        # But let's see what happens. If it returns 0 records or raises, that's "handling" it.
        # Ideally we want it to raise or log error.
        # Let's assume we expect it to raise or return 0 valid records (if it parses as empty).
        # Actually, for "not valid json", Spark often treats it as a corrupt record if schema is inferred.
        
        # If we want to enforce failure, we might need to set mode to FAILFAST in format_options.
        ingestion.format_options = json.dumps({"mode": "FAILFAST"})
        test_db.commit()
        
        with pytest.raises(Exception):
            processor._process_single_file(s3_path, file_info)

    def test_process_single_file_schema_evolution(
        self, test_db, spark_client, spark_session, minio_client, lakehouse_bucket, ingestion
    ):
        """Test schema evolution across two files."""
        # 1. Setup: File 1
        file1_data = [{"id": 1, "name": "Alice"}]
        file1_key = f"data/test_evolve_1_{uuid.uuid4()}.json"
        s3_path1 = self._upload_file(minio_client, lakehouse_bucket, file1_key, file1_data)
        
        spark_session.sql(f"DROP TABLE IF EXISTS {ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        # Process File 1
        processor._process_single_file(s3_path1, {"path": s3_path1, "size": 100})
        
        # 2. Setup: File 2 with NEW column
        file2_data = [{"id": 2, "name": "Bob", "email": "bob@example.com"}]
        file2_key = f"data/test_evolve_2_{uuid.uuid4()}.json"
        s3_path2 = self._upload_file(minio_client, lakehouse_bucket, file2_key, file2_data)
        
        # Process File 2
        processor._process_single_file(s3_path2, {"path": s3_path2, "size": 100})
        
        # 3. Verification
        df = spark_session.table(f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}")
        assert "email" in df.columns
        
        rows = df.collect()
        assert len(rows) == 2
        alice = next(r for r in rows if r['name'] == "Alice")
        bob = next(r for r in rows if r['name'] == "Bob")
        
        assert alice['email'] is None
        assert bob['email'] == "bob@example.com"
