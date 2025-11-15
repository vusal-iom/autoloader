"""
Integration tests for assertion helpers (verify_table_content).

Tests validate the helper function with real Spark DataFrames and Iceberg tables.
"""
import pytest
import uuid

from tests.helpers.assertions import verify_table_content, verify_table_schema
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestVerifyTableContent:
    """Integration tests for verify_table_content helper."""

    def test_exact_match_with_dataframe(self, spark_session):
        """
        Test exact match verification with DataFrame input.

        Validates that verify_table_content correctly passes when
        DataFrame content matches expected data exactly.
        """
        # Create test DataFrame
        data = [
            {"id": 1, "name": "Alice", "email": None},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
        df = spark_session.createDataFrame(data)

        # Verify exact match
        expected_data = [
            {"id": 1, "name": "Alice", "email": None},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]

        result_df = verify_table_content(
            df_or_table=df,
            expected_data=expected_data,
            spark_session=spark_session
        )

        # Should return the DataFrame
        assert result_df is not None
        print("Exact match verification passed")

    def test_exact_match_with_table_identifier(self, spark_session):
        """
        Test exact match verification with table identifier input.

        Validates that verify_table_content can read from Iceberg table
        and verify content.
        """
        table_name = f"test_table_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            # Create table and insert data
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    name STRING,
                    active BOOLEAN
                ) USING iceberg
            """)

            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES
                    (1, 'Alice', true),
                    (2, 'Bob', false),
                    (3, 'Charlie', true)
            """)

            # Verify with table identifier
            expected_data = [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False},
                {"id": 3, "name": "Charlie", "active": True},
            ]

            verify_table_content(
                df_or_table=table_id,
                expected_data=expected_data,
                spark_session=spark_session
            )

            print(f"Table content verification passed for {table_id}")

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")

    def test_null_value_verification(self, spark_session):
        """
        Test that null values are correctly verified.

        Validates that verify_table_content properly handles NULL values
        and distinguishes them from non-null values.
        """
        data = [
            {"id": 1, "name": "Alice", "email": None, "age": None},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 30},
            {"id": 3, "name": "Charlie", "email": None, "age": 25},
        ]
        df = spark_session.createDataFrame(data)

        expected_data = [
            {"id": 1, "name": "Alice", "email": None, "age": None},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 30},
            {"id": 3, "name": "Charlie", "email": None, "age": 25},
        ]

        verify_table_content(
            df_or_table=df,
            expected_data=expected_data,
            spark_session=spark_session
        )

        print("NULL value verification passed")

    def test_column_order_ignored_by_default(self, spark_session):
        """
        Test that column order is ignored by default.

        Validates that verify_table_content passes when columns are
        in different order but content is the same.
        """
        # DataFrame with columns in one order
        df = spark_session.createDataFrame([
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ])

        # Expected data with columns in different order
        expected_data = [
            {"email": "alice@example.com", "name": "Alice", "id": 1},
            {"email": "bob@example.com", "name": "Bob", "id": 2},
        ]

        verify_table_content(
            df_or_table=df,
            expected_data=expected_data,
            spark_session=spark_session,
            ignore_column_order=True
        )

        print("Column order verification passed")

    def test_row_order_ignored_by_default(self, spark_session):
        """
        Test that row order is ignored by default.

        Validates that verify_table_content passes when rows are
        in different order but content is the same.
        """
        df = spark_session.createDataFrame([
            {"id": 3, "name": "Charlie"},
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])

        # Expected data in different order
        expected_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        verify_table_content(
            df_or_table=df,
            expected_data=expected_data,
            spark_session=spark_session,
            ignore_row_order=True
        )

        print("Row order verification passed")

    def test_subset_column_verification(self, spark_session):
        """
        Test verification of subset of columns.

        Validates that verify_table_content can verify only
        specified columns, ignoring others.
        """
        df = spark_session.createDataFrame([
            {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25},
        ])

        # Verify only id and name columns
        expected_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        verify_table_content(
            df_or_table=df,
            expected_data=expected_data,
            spark_session=spark_session,
            columns=["id", "name"]
        )

        print("Subset column verification passed")

    def test_mismatch_row_count_raises_error(self, spark_session):
        """
        Test that mismatched row counts raise AssertionError.

        Validates that verify_table_content fails with clear error
        when row counts don't match.
        """
        df = spark_session.createDataFrame([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])

        expected_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        with pytest.raises(AssertionError) as exc_info:
            verify_table_content(
                df_or_table=df,
                expected_data=expected_data,
                spark_session=spark_session
            )

        error_message = str(exc_info.value)
        assert "Expected 3 rows, got 2 rows" in error_message
        assert "MISSING ROWS" in error_message
        print("Row count mismatch correctly raised error")

    def test_mismatch_values_raises_error(self, spark_session):
        """
        Test that mismatched values raise AssertionError.

        Validates that verify_table_content fails with clear diff
        when values don't match.
        """
        df = spark_session.createDataFrame([
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "wrong@example.com"},
        ])

        expected_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]

        with pytest.raises(AssertionError) as exc_info:
            verify_table_content(
                df_or_table=df,
                expected_data=expected_data,
                spark_session=spark_session
            )

        error_message = str(exc_info.value)
        assert "Content verification failed" in error_message
        assert "MISSING ROWS" in error_message or "EXTRA ROWS" in error_message
        print("Value mismatch correctly raised error")

    def test_null_vs_nonnull_mismatch_raises_error(self, spark_session):
        """
        Test that NULL vs non-NULL mismatches raise AssertionError.

        Validates that verify_table_content correctly distinguishes
        between NULL and non-NULL values.
        """
        from pyspark.sql.types import StructType, StructField, LongType, StringType

        # Create DataFrame with explicit schema to handle None values
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ])

        df = spark_session.createDataFrame([
            {"id": 1, "name": "Alice", "email": None},
        ], schema)

        expected_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
        ]

        with pytest.raises(AssertionError) as exc_info:
            verify_table_content(
                df_or_table=df,
                expected_data=expected_data,
                spark_session=spark_session
            )

        error_message = str(exc_info.value)
        assert "email" in error_message.lower()
        print("NULL vs non-NULL mismatch correctly raised error")

    def test_with_logger(self, spark_session):
        """
        Test that logger integration works correctly.

        Validates that verify_table_content logs metrics when
        logger is provided.
        """
        logger = TestLogger()

        df = spark_session.createDataFrame([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])

        expected_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        verify_table_content(
            df_or_table=df,
            expected_data=expected_data,
            spark_session=spark_session,
            logger=logger
        )

        print("Logger integration test passed")

    def test_empty_dataframe(self, spark_session):
        """
        Test verification of empty DataFrame.

        Validates that verify_table_content handles empty DataFrames correctly.
        """
        from pyspark.sql.types import StructType, StructField, LongType, StringType

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ])

        df = spark_session.createDataFrame([], schema)

        expected_data = []

        verify_table_content(
            df_or_table=df,
            expected_data=expected_data,
            spark_session=spark_session
        )

        print("Empty DataFrame verification passed")

    def test_invalid_input_raises_error(self, spark_session):
        """
        Test that invalid input types raise ValueError.

        Validates that verify_table_content properly validates input types.
        """
        with pytest.raises(ValueError) as exc_info:
            verify_table_content(
                df_or_table=123,  # Invalid type
                expected_data=[],
                spark_session=spark_session
            )

        assert "must be DataFrame or string" in str(exc_info.value)
        print("Invalid input type correctly raised error")

    def test_table_identifier_without_spark_session_raises_error(self, spark_session):
        """
        Test that table identifier without spark_session raises ValueError.

        Validates that verify_table_content requires spark_session when
        table identifier is provided.
        """
        with pytest.raises(ValueError) as exc_info:
            verify_table_content(
                df_or_table="test_catalog.test_db.some_table",
                expected_data=[],
                spark_session=None
            )

        assert "spark_session is required" in str(exc_info.value)
        print("Missing spark_session correctly raised error")


@pytest.mark.integration
class TestVerifyTableSchema:
    """Integration tests for verify_table_schema helper."""

    def test_schema_verification_with_table(self, spark_session):
        table_name = f"schema_helper_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    profile STRUCT<
                        name: STRING,
                        age: INT
                    >
                ) USING iceberg
            """)

            verify_table_schema(
                df_or_table=table_id,
                expected_schema=[
                    ("id", "bigint"),
                    ("profile", "struct<name:string,age:int>"),
                ],
                spark_session=spark_session,
            )

            print("Schema verification passed for table input")

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")

    def test_schema_mismatch_raises_error(self, spark_session):
        table_name = f"schema_helper_mismatch_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    name STRING,
                    email STRING
                ) USING iceberg
            """)

            with pytest.raises(AssertionError) as exc_info:
                verify_table_schema(
                    df_or_table=table_id,
                    expected_schema=[
                        {"name": "id", "type": "bigint"},
                        {"name": "name", "type": "string"},
                        {"name": "created_at", "type": "timestamp"},
                    ],
                    spark_session=spark_session,
                )

            assert "Schema verification failed" in str(exc_info.value)
            print("Schema mismatch correctly raised error")

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")

    def test_dataframe_input_with_ignore_case(self, spark_session):
        df = spark_session.createDataFrame([
            {"id": 1, "name": "Alice"},
        ])

        verify_table_schema(
            df_or_table=df,
            expected_schema=[
                ("ID", "BIGINT"),
                ("NAME", "STRING"),
            ],
            ignore_case=True,
        )

        print("Schema verification passed for DataFrame input")
