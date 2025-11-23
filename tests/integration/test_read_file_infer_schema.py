"""
Integration tests for BatchFileProcessor._read_file_infer_schema.
"""
import json
import uuid
import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType
from chispa import assert_df_equality

from app.services.batch_file_processor import (
    BatchFileProcessor,
    FileProcessingError,
    FileErrorCategory,
)
from app.services.file_state_service import FileStateService
from app.repositories.ingestion_repository import IngestionRepository
from app.models.domain import Ingestion, IngestionStatus
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestReadFileInferSchema:
    """Tests for _read_file_infer_schema error handling and success paths."""

    @pytest.fixture
    def ingestion(self, test_db):
        """Create ingestion configured for schema inference."""
        repo = IngestionRepository(test_db)
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-{unique_id}",
            tenant_id="test-tenant",
            name="Test Read Infer Schema",
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
        repo.create(ingestion)
        test_db.refresh(ingestion)
        return ingestion

    def test_read_infer_schema_success(
        self, test_db, spark_client, spark_session, upload_file, ingestion
    ):
        """Reads a valid JSON file and infers schema/data correctly."""
        logger = TestLogger()
        logger.section("Integration Test: _read_file_infer_schema success")

        # Upload valid file
        file_data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88}
        ]
        s3_path = upload_file(key=f"data/read_infer_ok_{uuid.uuid4()}.json", content=file_data)
        logger.step(f"Uploaded file to {s3_path}")

        # Invoke
        processor = BatchFileProcessor(spark_client, FileStateService(test_db), ingestion, test_db)
        df = processor._read_file_infer_schema(s3_path)

        # Verify schema and rows
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("score", LongType(), True)
        ])
        assert df.schema == expected_schema

        # Trigger evaluation to ensure no lazy errors
        expected_rows = [
            (1, "Alice", 95),
            (2, "Bob", 88)
        ]
        df_expected = spark_session.createDataFrame(expected_rows, schema=expected_schema)
        assert_df_equality(df, df_expected, ignore_row_order=True)
        logger.success("Schema inferred and rows read as expected")

    def test_read_infer_schema_malformed_file_failfast(
        self, test_db, spark_client, upload_file, ingestion
    ):
        """
        Malformed JSON should raise FileProcessingError with category=data_malformed
        when FAILFAST is configured.
        """
        logger = TestLogger()
        logger.section("Integration Test: _read_file_infer_schema malformed file FAILFAST")

        # Configure ingestion to fail fast on malformed records
        ingestion.format_options = json.dumps({"mode": "FAILFAST"})
        test_db.commit()

        # Upload malformed content manually (not JSON-serializable)
        content = b"{ this is not valid json }"
        s3_path = upload_file(key=f"data/read_infer_bad_{uuid.uuid4()}.json", content=content)
        logger.step(f"Uploaded malformed file to {s3_path}")

        processor = BatchFileProcessor(spark_client, FileStateService(test_db), ingestion, test_db)

        with pytest.raises(FileProcessingError) as excinfo:
            processor._read_file_infer_schema(s3_path)

        err: FileProcessingError  = excinfo.value

        assert (err.category, err.retryable, err.file_path) == (
            FileErrorCategory.DATA_MALFORMED, False, s3_path,
        )
        assert err.user_message == "Malformed data encountered. Fix the source file or switch to PERMISSIVE mode."
        assert "(org.apache.spark.SparkException) Job aborted due to stage failure" in err.raw_error

        logger.success("Malformed file raised FileProcessingError with correct category")

    def test_read_infer_schema_missing_bucket(
        self, test_db, spark_client, ingestion
    ):
        """
        Missing bucket should be categorized as bucket_not_found.
        """
        logger = TestLogger()
        logger.section("Integration Test: _read_file_infer_schema missing bucket")

        missing_path = f"s3a://nonexistent-bucket-{uuid.uuid4()}/missing/file.json"
        processor = BatchFileProcessor(spark_client, FileStateService(test_db), ingestion, test_db)

        with pytest.raises(FileProcessingError) as excinfo:
            df = processor._read_file_infer_schema(missing_path)

        err: FileProcessingError = excinfo.value
        assert (err.category, err.retryable) == (
            FileErrorCategory.BUCKET_NOT_FOUND,
            False,
        )
        assert err.user_message == "Source bucket not found. Verify bucket name and permissions."

        assert "(org.apache.hadoop.fs.s3a.UnknownStoreException)" in err.raw_error
        assert "The specified bucket does not exist" in err.raw_error

        logger.success("Missing bucket categorized as BUCKET_NOT_FOUND")

    def test_read_infer_schema_missing_file(
        self, test_db, spark_client, upload_file, ingestion, lakehouse_bucket
    ):
        """
        Missing file (valid bucket) should be categorized as path_not_found.
        """
        logger = TestLogger()
        logger.section("Integration Test: _read_file_infer_schema missing file")

        # Create a valid bucket by uploading a dummy file (MinIO creates bucket on upload)
        upload_file(key=f"setup/dummy_{uuid.uuid4()}.txt", content="setup")
        
        # Now try to read a non-existent file in that bucket
        missing_file_path = f"s3a://{lakehouse_bucket}/nonexistent/file.json"
        
        processor = BatchFileProcessor(spark_client, FileStateService(test_db), ingestion, test_db)

        with pytest.raises(FileProcessingError) as excinfo:
            df = processor._read_file_infer_schema(missing_file_path)

        err: FileProcessingError = excinfo.value
        assert (err.category, err.retryable) == (
            FileErrorCategory.PATH_NOT_FOUND,
            False,
        )
        assert err.user_message == "Source path not found. Verify bucket/key/prefix and retry."
        
        # Spark/Hadoop error for missing file usually contains "does not exist" or "FileNotFoundException"
        assert "does not exist" in err.raw_error or "FileNotFoundException" in err.raw_error

        logger.success("Missing file categorized as PATH_NOT_FOUND")

    def test_read_infer_schema_invalid_format_option(
        self, test_db, spark_client, upload_file, ingestion
    ):
        """
        Invalid format option should be categorized as format_options_invalid.
        """
        logger = TestLogger()
        logger.section("Integration Test: _read_file_infer_schema invalid format option")

        ingestion.format_options = json.dumps({"samplingRatio": "not-a-number"})
        test_db.commit()

        s3_path = upload_file(
            key=f"data/read_infer_invalid_mode_{uuid.uuid4()}.json",
            content=[{"id": 1}]
        )
        processor = BatchFileProcessor(spark_client, FileStateService(test_db), ingestion, test_db)

        with pytest.raises(FileProcessingError) as excinfo:
            df = processor._read_file_infer_schema(s3_path)

        err: FileProcessingError = excinfo.value

        assert (err.category, err.retryable, err.file_path) == (
            FileErrorCategory.FORMAT_OPTIONS_INVALID, False, s3_path,
        )

        assert err.user_message == "Invalid format options. Check mode/options for the reader."
        assert err.raw_error == """For input string: \"not-a-number\""""
        logger.success("Invalid format option categorized as FORMAT_OPTIONS_INVALID")
