"""
Integration tests for SparkFileReader.read_file_infer_schema.
"""
import json
import uuid
import pytest
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from chispa import assert_df_equality

from app.services.batch.reader import SparkFileReader
from app.services.batch.errors import FileProcessingError, FileErrorCategory
from app.models.domain import Ingestion, IngestionStatus
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestReadFileInferSchema:
    """Tests for SparkFileReader.read_file_infer_schema error handling and success paths."""

    @pytest.fixture
    def ingestion(self, test_db):
        """Create ingestion configured for schema inference."""
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
        test_db.add(ingestion)
        test_db.commit()
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


@pytest.mark.integration
class TestReadFileInferSchemaFormats:
    """Tests for SparkFileReader.read_file_infer_schema with different file formats."""

    @pytest.fixture
    def csv_ingestion(self, test_db):
        """Create ingestion configured for CSV format."""
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-csv-{unique_id}",
            tenant_id="test-tenant",
            name="Test CSV Infer Schema",
            cluster_id="test-cluster-1",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials={
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            },
            format_type="csv",
            format_options=json.dumps({"header": "true"}),
            destination_catalog="test_catalog",
            destination_database="test_db",
            destination_table=f"batch_csv_{unique_id[:8]}",
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

    @pytest.fixture
    def parquet_ingestion(self, test_db):
        """Create ingestion configured for Parquet format."""
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-parquet-{unique_id}",
            tenant_id="test-tenant",
            name="Test Parquet Infer Schema",
            cluster_id="test-cluster-1",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials={
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            },
            format_type="parquet",
            destination_catalog="test_catalog",
            destination_database="test_db",
            destination_table=f"batch_parquet_{unique_id[:8]}",
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

    def test_read_infer_schema_csv_success(
        self, test_db, spark_client, spark_session, upload_file, csv_ingestion
    ):
        """Reads a valid CSV file and infers schema correctly."""
        logger = TestLogger()
        logger.section("Integration Test: read_file_infer_schema CSV success")

        # Upload CSV file with header
        csv_content = "id,name,score\n1,Alice,95\n2,Bob,88"
        s3_path = upload_file(key=f"data/read_csv_{uuid.uuid4()}.csv", content=csv_content)
        logger.step(f"Uploaded CSV file to {s3_path}")

        # Invoke
        reader = SparkFileReader(spark_client)
        df = reader.read_file_infer_schema(s3_path, csv_ingestion)

        # Verify schema - CSV inferSchema produces IntegerType for small integers
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("score", IntegerType(), True)
        ])
        assert df.schema == expected_schema, f"Expected {expected_schema}, got {df.schema}"

        # Verify data
        expected_rows = [(1, "Alice", 95), (2, "Bob", 88)]
        df_expected = spark_session.createDataFrame(expected_rows, schema=expected_schema)
        assert_df_equality(df, df_expected, ignore_row_order=True)
        logger.success("CSV schema inferred and rows read as expected")

    def test_read_infer_schema_parquet_success(
        self, test_db, spark_client, spark_session, upload_file, parquet_ingestion
    ):
        """Reads a valid Parquet file and infers schema correctly."""
        logger = TestLogger()
        logger.section("Integration Test: read_file_infer_schema Parquet success")

        # Create a valid Parquet file using pyarrow (local, not Spark Connect)
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io

        # Create table with explicit types
        table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"], type=pa.string()),
            "score": pa.array([95, 88], type=pa.int64())
        })

        # Write to bytes buffer
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        parquet_bytes = buffer.getvalue()

        # Upload Parquet file
        s3_path = upload_file(key=f"data/read_parquet_{uuid.uuid4()}.parquet", content=parquet_bytes)
        logger.step(f"Uploaded Parquet file to {s3_path}")

        # Invoke
        reader = SparkFileReader(spark_client)
        df = reader.read_file_infer_schema(s3_path, parquet_ingestion)

        # Verify schema and data
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("score", LongType(), True)
        ])
        assert df.schema == expected_schema, f"Expected {expected_schema}, got {df.schema}"

        expected_rows = [(1, "Alice", 95), (2, "Bob", 88)]
        df_expected = spark_session.createDataFrame(expected_rows, schema=expected_schema)
        assert_df_equality(df, df_expected, ignore_row_order=True)
        logger.success("Parquet schema inferred and rows read as expected")


@pytest.mark.integration
class TestReadFileFormatOptions:
    """Tests for SparkFileReader with custom format options."""

    @pytest.fixture
    def csv_custom_delimiter_ingestion(self, test_db):
        """Create ingestion with custom CSV delimiter."""
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-csv-delim-{unique_id}",
            tenant_id="test-tenant",
            name="Test CSV Custom Delimiter",
            cluster_id="test-cluster-1",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials={
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            },
            format_type="csv",
            format_options=json.dumps({"header": "true", "delimiter": "|"}),
            destination_catalog="test_catalog",
            destination_database="test_db",
            destination_table=f"batch_csv_delim_{unique_id[:8]}",
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

    @pytest.fixture
    def json_multiline_ingestion(self, test_db):
        """Create ingestion with multiline JSON option."""
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-json-multi-{unique_id}",
            tenant_id="test-tenant",
            name="Test JSON Multiline",
            cluster_id="test-cluster-1",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials={
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            },
            format_type="json",
            format_options=json.dumps({"multiLine": "true"}),
            destination_catalog="test_catalog",
            destination_database="test_db",
            destination_table=f"batch_json_multi_{unique_id[:8]}",
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

    def test_read_csv_with_custom_delimiter(
        self, test_db, spark_client, spark_session, upload_file, csv_custom_delimiter_ingestion
    ):
        """Reads CSV with pipe delimiter and verifies columns are parsed correctly."""
        logger = TestLogger()
        logger.section("Integration Test: CSV with custom delimiter")

        # Upload CSV with pipe delimiter
        csv_content = "id|name|score\n1|Alice|95\n2|Bob|88"
        s3_path = upload_file(key=f"data/read_csv_pipe_{uuid.uuid4()}.csv", content=csv_content)
        logger.step(f"Uploaded pipe-delimited CSV to {s3_path}")

        # Invoke
        reader = SparkFileReader(spark_client)
        df = reader.read_file_infer_schema(s3_path, csv_custom_delimiter_ingestion)

        # Verify columns are parsed correctly (3 columns, not 1)
        assert len(df.columns) == 3, f"Expected 3 columns, got {len(df.columns)}: {df.columns}"
        assert df.columns == ["id", "name", "score"], f"Unexpected columns: {df.columns}"

        # Verify data - CSV inferSchema produces IntegerType for small integers
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("score", IntegerType(), True)
        ])
        expected_rows = [(1, "Alice", 95), (2, "Bob", 88)]
        df_expected = spark_session.createDataFrame(expected_rows, schema=expected_schema)
        assert_df_equality(df, df_expected, ignore_row_order=True)
        logger.success("Pipe delimiter applied correctly")

    def test_read_json_with_multiline_option(
        self, test_db, spark_client, spark_session, upload_file, json_multiline_ingestion
    ):
        """Reads multiline JSON and verifies it parses correctly."""
        logger = TestLogger()
        logger.section("Integration Test: JSON with multiline option")

        # Upload multiline JSON (pretty-printed, spans multiple lines)
        json_content = """{
  "id": 1,
  "name": "Alice",
  "score": 95
}"""
        s3_path = upload_file(key=f"data/read_json_multi_{uuid.uuid4()}.json", content=json_content)
        logger.step(f"Uploaded multiline JSON to {s3_path}")

        # Invoke
        reader = SparkFileReader(spark_client)
        df = reader.read_file_infer_schema(s3_path, json_multiline_ingestion)

        # Verify data
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("score", LongType(), True)
        ])
        expected_rows = [(1, "Alice", 95)]
        df_expected = spark_session.createDataFrame(expected_rows, schema=expected_schema)
        assert_df_equality(df, df_expected, ignore_row_order=True, ignore_column_order=True)
        logger.success("Multiline JSON parsed correctly")
