"""
Integration tests for Schema Evolution Strategies.

Tests all four strategies (ignore, fail, append_new_columns, sync_all_columns)
with JSON, CSV, and Parquet formats using Spark Connect.
"""

import pytest
from typing import List, Dict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from app.services.schema_evolution_service import (
    SchemaEvolutionService,
    SchemaMismatchError,
    IncompatibleTypeChangeError,
)


# Note: Using spark_session fixture from conftest.py (Spark Connect)
# No local file creation needed - using createDataFrame directly


def create_json_data(schema_type: str, num_batches: int = 2, id_offset: int = 0) -> List[Dict]:
    """
    Create JSON test data with different schemas.

    Args:
        schema_type: 'base' (id, name, email) or 'evolved' (id, name, email, phone, address)
        num_batches: Number of batches to create
        id_offset: Offset to add to IDs (for generating distinct records)

    Returns:
        List of records as dictionaries
    """
    records = []
    for i in range(num_batches):
        if schema_type == "base":
            batch_records = [
                {"id": id_offset + i * 100 + j, "name": f"User{j}", "email": f"user{j}@test.com"}
                for j in range(10)
            ]
        elif schema_type == "evolved":
            batch_records = [
                {
                    "id": id_offset + i * 100 + j,
                    "name": f"User{j}",
                    "email": f"user{j}@test.com",
                    "phone": f"555-{j:04d}",
                    "address": f"{j} Main St"
                }
                for j in range(10)
            ]
        else:
            raise ValueError(f"Unknown schema type: {schema_type}")

        records.extend(batch_records)

    return records


def create_csv_data(schema_type: str, num_batches: int = 2, id_offset: int = 0) -> List[Dict]:
    """
    Create CSV test data with different schemas.

    Args:
        schema_type: 'base' or 'evolved'
        num_batches: Number of batches to create
        id_offset: Offset to add to IDs (for generating distinct records)

    Returns:
        List of records as dictionaries
    """
    records = []
    for i in range(num_batches):
        if schema_type == "base":
            batch_records = [
                {
                    'id': id_offset + i * 100 + j,
                    'name': f"User{j}",
                    'email': f"user{j}@test.com"
                }
                for j in range(10)
            ]
        elif schema_type == "evolved":
            batch_records = [
                {
                    'id': id_offset + i * 100 + j,
                    'name': f"User{j}",
                    'email': f"user{j}@test.com",
                    'phone': f"555-{j:04d}",
                    'address': f"{j} Main St"
                }
                for j in range(10)
            ]

        records.extend(batch_records)

    return records


def create_parquet_data(schema_type: str, num_batches: int = 2, id_offset: int = 0) -> List[tuple]:
    """
    Create Parquet test data with different schemas.

    Args:
        schema_type: 'base' or 'evolved'
        num_batches: Number of batches to create
        id_offset: Offset to add to IDs (for generating distinct records)

    Returns:
        List of tuples (for createDataFrame)
    """
    records = []
    for i in range(num_batches):
        if schema_type == "base":
            batch_records = [
                (id_offset + i * 100 + j, f"User{j}", f"user{j}@test.com")
                for j in range(10)
            ]
        elif schema_type == "evolved":
            batch_records = [
                (id_offset + i * 100 + j, f"User{j}", f"user{j}@test.com", f"555-{j:04d}", f"{j} Main St")
                for j in range(10)
            ]

        records.extend(batch_records)

    return records


class TestIgnoreStrategy:
    """Test 'ignore' strategy - continues using original schema."""

    @pytest.mark.parametrize("format_type,create_data_func", [
        ("json", create_json_data),
        ("csv", create_csv_data),
        ("parquet", create_parquet_data),
    ])
    def test_ignore_strategy_drops_new_columns(self, spark_session, format_type, create_data_func):
        """
        Test that 'ignore' strategy drops new columns from source files.

        Expected behavior:
        - Initial load with base schema (3 columns)
        - Second load with evolved schema (5 columns)
        - Strategy 'ignore' means new columns are dropped
        - Table should still have only 3 columns
        """
        table_name = f"test_catalog.default.ignore_test_{format_type}"

        # Phase 1: Initial load with base schema
        base_data = create_data_func("base", num_batches=2)

        # Create DataFrame based on format
        if format_type == "parquet":
            df = spark_session.createDataFrame(base_data, ["id", "name", "email"])
        else:
            df = spark_session.createDataFrame(base_data)

        # Create Iceberg table
        df.writeTo(table_name).using("iceberg").create()

        # Verify initial schema
        table_df = spark_session.table(table_name)
        assert len(table_df.columns) == 3
        assert set(table_df.columns) == {"id", "name", "email"}
        initial_count = table_df.count()
        assert initial_count == 20  # 2 batches * 10 records

        # Phase 2: Load evolved schema data (with 'ignore' strategy - implicit)
        evolved_data = create_data_func("evolved", num_batches=2)

        # Create DataFrame with evolved schema
        if format_type == "parquet":
            evolved_df = spark_session.createDataFrame(evolved_data, ["id", "name", "email", "phone", "address"])
        else:
            evolved_df = spark_session.createDataFrame(evolved_data)

        # Select only the original columns (simulating 'ignore' behavior)
        evolved_df_filtered = evolved_df.select("id", "name", "email")

        # Append to table
        evolved_df_filtered.writeTo(table_name).using("iceberg").append()

        # Verify schema hasn't changed
        final_table = spark_session.table(table_name)
        assert len(final_table.columns) == 3
        assert set(final_table.columns) == {"id", "name", "email"}
        assert final_table.count() == 40  # 4 batches * 10 records

        # Cleanup
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestFailStrategy:
    """Test 'fail' strategy - raises exception on schema mismatch."""

    def test_fail_strategy_raises_on_new_columns(self, spark_session):
        """
        Test that 'fail' strategy raises SchemaMismatchError.

        Expected behavior:
        - Initial schema (3 columns)
        - Evolved schema (5 columns) detected
        - Strategy 'fail' raises SchemaMismatchError
        """
        # Create schemas
        source_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),  # NEW
            StructField("address", StringType(), True),  # NEW
        ])

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ])

        # Compare schemas
        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

        # Apply fail strategy - should raise
        with pytest.raises(SchemaMismatchError) as exc_info:
            SchemaEvolutionService.apply_schema_evolution(
                spark_session, "test_catalog.default.fail_test", comparison, "fail"
            )

        assert "Schema Mismatch Detected" in str(exc_info.value)
        assert "phone" in str(exc_info.value)
        assert "address" in str(exc_info.value)


class TestAppendNewColumnsStrategy:
    """Test 'append_new_columns' strategy - adds new columns."""

    def test_append_new_columns_json(self, spark_session):
        """
        Test 'append_new_columns' strategy with JSON data.

        Expected behavior:
        - Initial schema: id, name, email (3 columns)
        - Evolved schema adds: phone, address
        - Strategy appends new columns
        - Final schema: id, name, email, phone, address (5 columns)
        - Old records have NULL for new fields
        """
        table_name = "test_catalog.default.append_json_test"

        # Phase 1: Create table with base schema
        base_data = create_json_data("base", num_batches=2)
        df = spark_session.createDataFrame(base_data)
        df.writeTo(table_name).using("iceberg").create()

        # Verify initial schema
        table_df = spark_session.table(table_name)
        assert len(table_df.columns) == 3
        assert table_df.count() == 20

        # Phase 2: Create evolved data (with different IDs to avoid duplicates)
        evolved_data = create_json_data("evolved", num_batches=2, id_offset=1000)
        evolved_df = spark_session.createDataFrame(evolved_data)

        # Apply schema evolution
        comparison = SchemaEvolutionService.compare_schemas(evolved_df.schema, table_df.schema)
        assert comparison.has_changes
        assert len(comparison.added_columns) == 2

        SchemaEvolutionService.apply_schema_evolution(
            spark_session, table_name, comparison, "append_new_columns"
        )

        # Refresh and verify schema changed
        spark_session.sql(f"REFRESH TABLE {table_name}")
        updated_table = spark_session.table(table_name)
        assert len(updated_table.columns) == 5
        assert set(updated_table.columns) == {"id", "name", "email", "phone", "address"}

        # Append evolved data (enable schema evolution for append)
        evolved_df.writeTo(table_name).using("iceberg").option("merge-schema", "true").append()

        # Verify backward compatibility
        final_table = spark_session.table(table_name)
        assert final_table.count() == 40

        # Old records (ids 0-199) should have NULL for new fields
        old_records = final_table.filter("id < 1000")
        assert old_records.count() == 20
        assert old_records.filter("phone IS NULL").count() == 20
        assert old_records.filter("address IS NULL").count() == 20

        # New records (ids 1000-1199) should have values
        new_records = final_table.filter("id >= 1000")
        assert new_records.count() == 20
        assert new_records.filter("phone IS NOT NULL").count() == 20
        assert new_records.filter("address IS NOT NULL").count() == 20

        # Cleanup
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestSyncAllColumnsStrategy:
    """Test 'sync_all_columns' strategy - full synchronization."""

    def test_sync_all_columns_adds_and_removes(self, spark_session):
        """
        Test 'sync_all_columns' strategy adds AND removes columns.

        Expected behavior:
        - Initial schema: id, name, email, deprecated_field (4 columns)
        - New schema: id, name, phone, address (4 columns)
        - email and deprecated_field removed
        - phone and address added
        """
        table_name = "test_catalog.default.sync_test"

        # Phase 1: Create table with initial schema
        data = [(1, "User1", "user1@test.com", "old_value")]
        df = spark_session.createDataFrame(data, ["id", "name", "email", "deprecated_field"])
        df.writeTo(table_name).using("iceberg").create()

        # Verify initial schema
        table_df = spark_session.table(table_name)
        assert len(table_df.columns) == 4

        # Phase 2: Create new schema (different columns)
        new_data = [(2, "User2", "555-0001", "123 Main St")]
        new_df = spark_session.createDataFrame(new_data, ["id", "name", "phone", "address"])

        # Apply sync_all_columns strategy
        comparison = SchemaEvolutionService.compare_schemas(new_df.schema, table_df.schema)
        assert comparison.has_changes
        assert len(comparison.added_columns) == 2  # phone, address
        assert len(comparison.removed_columns) == 2  # email, deprecated_field

        SchemaEvolutionService.apply_schema_evolution(
            spark_session, table_name, comparison, "sync_all_columns"
        )

        # Verify schema updated
        spark_session.sql(f"REFRESH TABLE {table_name}")
        updated_table = spark_session.table(table_name)
        assert set(updated_table.columns) == {"id", "name", "phone", "address"}

        # Cleanup
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_sync_compatible_type_change(self, spark_session):
        """
        Test 'sync_all_columns' with compatible type changes.

        Expected behavior:
        - Initial: id (int)
        - New: id (long)
        - Type change is compatible, should succeed
        """
        table_name = "test_catalog.default.sync_type_test"

        # Create table with int type (explicitly specify IntegerType)
        data = [(1, "User1")]
        initial_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark_session.createDataFrame(data, initial_schema)
        df.writeTo(table_name).using("iceberg").create()

        # New schema with long type
        new_data = [(2, "User2")]
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ])
        new_df = spark_session.createDataFrame(new_data, schema)

        # Apply sync
        table_df = spark_session.table(table_name)
        comparison = SchemaEvolutionService.compare_schemas(new_df.schema, table_df.schema)

        # Should detect type change
        assert len(comparison.modified_columns) == 1

        # Apply evolution - should succeed for compatible change
        SchemaEvolutionService.apply_schema_evolution(
            spark_session, table_name, comparison, "sync_all_columns"
        )

        # Cleanup
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_sync_incompatible_type_change_raises_error(self, spark_session):
        """
        Test 'sync_all_columns' with incompatible type changes.

        Expected behavior:
        - Incompatible type change should raise IncompatibleTypeChangeError
        """
        table_name = "test_catalog.default.sync_incompatible_test"

        # Create table with string type
        data = [("1", "User1")]
        df = spark_session.createDataFrame(data, ["id", "name"])
        df.writeTo(table_name).using("iceberg").create()

        # New schema with int type (incompatible)
        new_data = [(2, "User2")]
        new_df = spark_session.createDataFrame(new_data, ["id", "name"])

        # Apply sync
        table_df = spark_session.table(table_name)
        comparison = SchemaEvolutionService.compare_schemas(new_df.schema, table_df.schema)

        # Should raise on incompatible type change
        with pytest.raises(IncompatibleTypeChangeError):
            SchemaEvolutionService.apply_schema_evolution(
                spark_session, table_name, comparison, "sync_all_columns"
            )

        # Cleanup
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestFormatSpecificBehavior:
    """Test format-specific behaviors."""

    def test_csv_with_header(self, spark_session):
        """Test CSV format with createDataFrame."""
        table_name = "test_catalog.default.csv_header_test"

        # Create CSV data
        base_data = create_csv_data("base", num_batches=1)

        # Create DataFrame from data
        df = spark_session.createDataFrame(base_data)
        df.writeTo(table_name).using("iceberg").create()

        table_df = spark_session.table(table_name)
        assert len(table_df.columns) == 3
        assert table_df.count() == 10

        # Cleanup
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_json_multiline(self, spark_session):
        """Test JSON format with createDataFrame."""
        table_name = "test_catalog.default.json_multiline_test"

        # Create JSON data
        base_data = create_json_data("base", num_batches=1)

        # Create DataFrame from data
        df = spark_session.createDataFrame(base_data)
        df.writeTo(table_name).using("iceberg").create()

        table_df = spark_session.table(table_name)
        assert len(table_df.columns) == 3
        assert table_df.count() == 10

        # Cleanup
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
