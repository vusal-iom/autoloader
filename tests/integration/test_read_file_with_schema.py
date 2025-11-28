"""
Integration tests for SparkFileReader.read_file_with_schema.
"""
import json
import uuid
import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType
from chispa import assert_df_equality

from app.services.batch.reader import SparkFileReader
from tests.fixtures.ingestion_factory import create_test_ingestion
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestReadFileWithSchema:
    """Tests for SparkFileReader.read_file_with_schema."""

    @pytest.fixture
    def schema(self):
        """Predefined schema for tests."""
        return StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("score", LongType(), True)
        ])

    @pytest.fixture
    def ingestion_with_schema(self, test_db, schema):
        """Create ingestion with predefined schema."""
        return create_test_ingestion(
            test_db,
            schema_json=json.dumps(schema.jsonValue()),
            name_prefix="Test Read With Schema"
        )

    def test_read_with_schema_success(
        self, test_db, spark_client, spark_session, upload_file, ingestion_with_schema, schema
    ):
        """Reads a JSON file with predefined schema and verifies data."""
        logger = TestLogger()
        logger.section("Integration Test: read_file_with_schema success")

        # Upload valid file
        file_data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88}
        ]
        s3_path = upload_file(key=f"data/read_schema_ok_{uuid.uuid4()}.json", content=file_data)
        logger.step(f"Uploaded file to {s3_path}")

        # Invoke
        reader = SparkFileReader(spark_client)
        df = reader.read_file_with_schema(s3_path, ingestion_with_schema)

        # Verify schema matches predefined schema
        assert df.schema == schema, f"Expected {schema}, got {df.schema}"

        # Verify data
        expected_rows = [
            (1, "Alice", 95),
            (2, "Bob", 88)
        ]
        df_expected = spark_session.createDataFrame(expected_rows, schema=schema)
        assert_df_equality(df, df_expected, ignore_row_order=True)
        logger.success("Schema applied and rows read as expected")

    def test_read_with_schema_uses_provided_schema(
        self, test_db, spark_client, spark_session, upload_file, ingestion_with_schema, schema
    ):
        """Verifies that the provided schema is used instead of inference."""
        logger = TestLogger()
        logger.section("Integration Test: read_file_with_schema uses provided schema")

        # Upload file - schema should be applied from ingestion config
        file_data = [
            {"id": 100, "name": "Charlie", "score": 75}
        ]
        s3_path = upload_file(key=f"data/read_schema_apply_{uuid.uuid4()}.json", content=file_data)
        logger.step(f"Uploaded file to {s3_path}")

        # Invoke
        reader = SparkFileReader(spark_client)
        df = reader.read_file_with_schema(s3_path, ingestion_with_schema)

        # Verify schema matches the predefined schema (LongType, not inferred IntegerType)
        assert df.schema == schema, f"Expected {schema}, got {df.schema}"

        # Verify data reads correctly
        expected_rows = [(100, "Charlie", 75)]
        df_expected = spark_session.createDataFrame(expected_rows, schema=schema)
        assert_df_equality(df, df_expected, ignore_row_order=True)
        logger.success("Predefined schema applied correctly")
