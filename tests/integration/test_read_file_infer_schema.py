"""
Integration tests for SparkFileReader.read_file_infer_schema.
"""
import json
import uuid
import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType
from chispa import assert_df_equality

from app.services.batch.reader import SparkFileReader
from app.services.batch.errors import FileProcessingError, FileErrorCategory
from app.repositories.ingestion_repository import IngestionRepository
from app.models.domain import Ingestion, IngestionStatus
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestReadFileInferSchema:
    """Tests for SparkFileReader.read_file_infer_schema error handling and success paths."""

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
        logger.section("Integration Test: read_file_infer_schema success")

        # Upload valid file
        file_data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88}
        ]
        s3_path = upload_file(key=f"data/read_infer_ok_{uuid.uuid4()}.json", content=file_data)
        logger.step(f"Uploaded file to {s3_path}")

        # Invoke
        reader = SparkFileReader(spark_client)
        df = reader.read_file_infer_schema(s3_path, ingestion)

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





    def test_read_infer_schema_missing_file(
        self, test_db, spark_client, upload_file, ingestion, lakehouse_bucket
    ):
        """
        Missing file (valid bucket) should be categorized as path_not_found.
        """
        logger = TestLogger()
        logger.section("Integration Test: read_file_infer_schema missing file")

        # Create a valid bucket by uploading a dummy file (MinIO creates bucket on upload)
        upload_file(key=f"setup/dummy_{uuid.uuid4()}.txt", content="setup")
        
        # Now try to read a non-existent file in that bucket
        missing_file_path = f"s3a://{lakehouse_bucket}/nonexistent/file.json"
        
        reader = SparkFileReader(spark_client)

        with pytest.raises(FileProcessingError) as excinfo:
            reader.read_file_infer_schema(missing_file_path, ingestion)

        err: FileProcessingError = excinfo.value
        assert (err.category, err.retryable) == (
            FileErrorCategory.PATH_NOT_FOUND,
            False,
        )
        assert err.user_message == "Source path not found."
        
        # Spark/Hadoop error for missing file usually contains "does not exist" or "FileNotFoundException"
        assert "does not exist" in err.raw_error or "FileNotFoundException" in err.raw_error

        logger.success("Missing file categorized as PATH_NOT_FOUND")


