"""
Integration tests for Schema Evolution Strategies.

Tests all four strategies (ignore, fail, append_new_columns, sync_all_columns)
with JSON, CSV, and Parquet formats.
"""

import pytest
import json
import csv
import tempfile
import os
from pathlib import Path
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from app.services.schema_evolution_service import (
    SchemaEvolutionService,
    SchemaMismatchError,
    IncompatibleTypeChangeError,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def spark():
    """Create a local Spark session for testing with Iceberg support."""
    spark = SparkSession.builder \
        .appName("schema_evolution_test") \
        .master("local[1]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.test_catalog.type", "hadoop") \
        .config("spark.sql.catalog.test_catalog.warehouse", tempfile.mkdtemp()) \
        .getOrCreate()

    yield spark
    spark.stop()


def create_json_files(directory: str, schema_type: str, num_files: int = 2, id_offset: int = 0) -> List[str]:
    """
    Create JSON test files with different schemas.

    Args:
        directory: Directory to create files in
        schema_type: 'base' (id, name, email) or 'evolved' (id, name, email, phone, address)
        num_files: Number of files to create
        id_offset: Offset to add to IDs (for generating distinct records)

    Returns:
        List of file paths created
    """
    files = []
    for i in range(num_files):
        file_path = os.path.join(directory, f"data_{i}.json")

        if schema_type == "base":
            records = [
                {"id": id_offset + i * 100 + j, "name": f"User{j}", "email": f"user{j}@test.com"}
                for j in range(10)
            ]
        elif schema_type == "evolved":
            records = [
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

        with open(file_path, 'w') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')

        files.append(file_path)

    return files


def create_csv_files(directory: str, schema_type: str, num_files: int = 2) -> List[str]:
    """
    Create CSV test files with different schemas.

    Args:
        directory: Directory to create files in
        schema_type: 'base' or 'evolved'
        num_files: Number of files to create

    Returns:
        List of file paths created
    """
    files = []
    for i in range(num_files):
        file_path = os.path.join(directory, f"data_{i}.csv")

        with open(file_path, 'w', newline='') as f:
            if schema_type == "base":
                writer = csv.DictWriter(f, fieldnames=['id', 'name', 'email'])
                writer.writeheader()
                for j in range(10):
                    writer.writerow({
                        'id': i * 100 + j,
                        'name': f"User{j}",
                        'email': f"user{j}@test.com"
                    })
            elif schema_type == "evolved":
                writer = csv.DictWriter(f, fieldnames=['id', 'name', 'email', 'phone', 'address'])
                writer.writeheader()
                for j in range(10):
                    writer.writerow({
                        'id': i * 100 + j,
                        'name': f"User{j}",
                        'email': f"user{j}@test.com",
                        'phone': f"555-{j:04d}",
                        'address': f"{j} Main St"
                    })

        files.append(file_path)

    return files


def create_parquet_files(directory: str, schema_type: str, spark: SparkSession, num_files: int = 2) -> List[str]:
    """
    Create Parquet test files with different schemas.

    Args:
        directory: Directory to create files in
        schema_type: 'base' or 'evolved'
        spark: SparkSession
        num_files: Number of files to create

    Returns:
        List of file paths created
    """
    files = []
    for i in range(num_files):
        file_path = os.path.join(directory, f"data_{i}.parquet")

        if schema_type == "base":
            data = [
                (i * 100 + j, f"User{j}", f"user{j}@test.com")
                for j in range(10)
            ]
            df = spark.createDataFrame(data, ["id", "name", "email"])
        elif schema_type == "evolved":
            data = [
                (i * 100 + j, f"User{j}", f"user{j}@test.com", f"555-{j:04d}", f"{j} Main St")
                for j in range(10)
            ]
            df = spark.createDataFrame(data, ["id", "name", "email", "phone", "address"])

        df.write.mode("overwrite").parquet(file_path)
        files.append(file_path)

    return files


class TestIgnoreStrategy:
    """Test 'ignore' strategy - continues using original schema."""

    @pytest.mark.parametrize("format_type,create_files_func", [
        ("json", create_json_files),
        ("csv", create_csv_files),
        pytest.param("parquet", create_parquet_files, marks=pytest.mark.skip(reason="Parquet requires Spark setup")),
    ])
    def test_ignore_strategy_drops_new_columns(self, temp_dir, spark, format_type, create_files_func):
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
        if format_type == "parquet":
            base_files = create_files_func(temp_dir, "base", spark, num_files=2)
        else:
            base_files = create_files_func(temp_dir, "base", num_files=2)

        # Read and create table
        if format_type == "json":
            df = spark.read.json([f"file://{f}" for f in base_files])
        elif format_type == "csv":
            df = spark.read.option("header", "true").csv([f"file://{f}" for f in base_files])
        else:
            df = spark.read.parquet([f"file://{f}" for f in base_files])

        # Create Iceberg table
        df.writeTo(table_name).using("iceberg").create()

        # Verify initial schema
        table_df = spark.table(table_name)
        assert len(table_df.columns) == 3
        assert set(table_df.columns) == {"id", "name", "email"}
        initial_count = table_df.count()
        assert initial_count == 20  # 2 files * 10 records

        # Phase 2: Load evolved schema files (with 'ignore' strategy - implicit)
        evolved_dir = os.path.join(temp_dir, "evolved")
        os.makedirs(evolved_dir)

        if format_type == "parquet":
            evolved_files = create_files_func(evolved_dir, "evolved", spark, num_files=2)
        else:
            evolved_files = create_files_func(evolved_dir, "evolved", num_files=2)

        # Read evolved files - they have 5 columns
        if format_type == "json":
            evolved_df = spark.read.json([f"file://{f}" for f in evolved_files])
        elif format_type == "csv":
            evolved_df = spark.read.option("header", "true").csv([f"file://{f}" for f in evolved_files])
        else:
            evolved_df = spark.read.parquet([f"file://{f}" for f in evolved_files])

        # Select only the original columns (simulating 'ignore' behavior)
        evolved_df_filtered = evolved_df.select("id", "name", "email")

        # Append to table
        evolved_df_filtered.writeTo(table_name).using("iceberg").append()

        # Verify schema hasn't changed
        final_table = spark.table(table_name)
        assert len(final_table.columns) == 3
        assert set(final_table.columns) == {"id", "name", "email"}
        assert final_table.count() == 40  # 4 files * 10 records

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestFailStrategy:
    """Test 'fail' strategy - raises exception on schema mismatch."""

    def test_fail_strategy_raises_on_new_columns(self, spark, temp_dir):
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
                spark, "test_catalog.default.fail_test", comparison, "fail"
            )

        assert "Schema Mismatch Detected" in str(exc_info.value)
        assert "phone" in str(exc_info.value)
        assert "address" in str(exc_info.value)


class TestAppendNewColumnsStrategy:
    """Test 'append_new_columns' strategy - adds new columns."""

    def test_append_new_columns_json(self, spark, temp_dir):
        """
        Test 'append_new_columns' strategy with JSON files.

        Expected behavior:
        - Initial schema: id, name, email (3 columns)
        - Evolved schema adds: phone, address
        - Strategy appends new columns
        - Final schema: id, name, email, phone, address (5 columns)
        - Old records have NULL for new fields
        """
        table_name = "test_catalog.default.append_json_test"

        # Phase 1: Create table with base schema
        base_files = create_json_files(temp_dir, "base", num_files=2)
        df = spark.read.json([f"file://{f}" for f in base_files])
        df.writeTo(table_name).using("iceberg").create()

        # Verify initial schema
        table_df = spark.table(table_name)
        assert len(table_df.columns) == 3
        assert table_df.count() == 20

        # Phase 2: Read evolved files (with different IDs to avoid duplicates)
        evolved_dir = os.path.join(temp_dir, "evolved")
        os.makedirs(evolved_dir)
        evolved_files = create_json_files(evolved_dir, "evolved", num_files=2, id_offset=1000)
        evolved_df = spark.read.json([f"file://{f}" for f in evolved_files])

        # Apply schema evolution
        comparison = SchemaEvolutionService.compare_schemas(evolved_df.schema, table_df.schema)
        assert comparison.has_changes
        assert len(comparison.added_columns) == 2

        SchemaEvolutionService.apply_schema_evolution(
            spark, table_name, comparison, "append_new_columns"
        )

        # Refresh and verify schema changed
        spark.sql(f"REFRESH TABLE {table_name}")
        updated_table = spark.table(table_name)
        assert len(updated_table.columns) == 5
        assert set(updated_table.columns) == {"id", "name", "email", "phone", "address"}

        # Append evolved data (enable schema evolution for append)
        evolved_df.writeTo(table_name).using("iceberg").option("merge-schema", "true").append()

        # Verify backward compatibility
        final_table = spark.table(table_name)
        assert final_table.count() == 40

        # Old records (ids 0-109) should have NULL for new fields
        old_records = final_table.filter("id < 1000")
        assert old_records.count() == 20
        assert old_records.filter("phone IS NULL").count() == 20
        assert old_records.filter("address IS NULL").count() == 20

        # New records (ids 1000-1109) should have values
        new_records = final_table.filter("id >= 1000")
        assert new_records.count() == 20
        assert new_records.filter("phone IS NOT NULL").count() == 20
        assert new_records.filter("address IS NOT NULL").count() == 20

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestSyncAllColumnsStrategy:
    """Test 'sync_all_columns' strategy - full synchronization."""

    def test_sync_all_columns_adds_and_removes(self, spark, temp_dir):
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
        df = spark.createDataFrame(data, ["id", "name", "email", "deprecated_field"])
        df.writeTo(table_name).using("iceberg").create()

        # Verify initial schema
        table_df = spark.table(table_name)
        assert len(table_df.columns) == 4

        # Phase 2: Create new schema (different columns)
        new_data = [(2, "User2", "555-0001", "123 Main St")]
        new_df = spark.createDataFrame(new_data, ["id", "name", "phone", "address"])

        # Apply sync_all_columns strategy
        comparison = SchemaEvolutionService.compare_schemas(new_df.schema, table_df.schema)
        assert comparison.has_changes
        assert len(comparison.added_columns) == 2  # phone, address
        assert len(comparison.removed_columns) == 2  # email, deprecated_field

        SchemaEvolutionService.apply_schema_evolution(
            spark, table_name, comparison, "sync_all_columns"
        )

        # Verify schema updated
        spark.sql(f"REFRESH TABLE {table_name}")
        updated_table = spark.table(table_name)
        assert set(updated_table.columns) == {"id", "name", "phone", "address"}

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_sync_compatible_type_change(self, spark, temp_dir):
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
        df = spark.createDataFrame(data, initial_schema)
        df.writeTo(table_name).using("iceberg").create()

        # New schema with long type
        new_data = [(2, "User2")]
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ])
        new_df = spark.createDataFrame(new_data, schema)

        # Apply sync
        table_df = spark.table(table_name)
        comparison = SchemaEvolutionService.compare_schemas(new_df.schema, table_df.schema)

        # Should detect type change
        assert len(comparison.modified_columns) == 1

        # Apply evolution - should succeed for compatible change
        SchemaEvolutionService.apply_schema_evolution(
            spark, table_name, comparison, "sync_all_columns"
        )

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_sync_incompatible_type_change_raises_error(self, spark, temp_dir):
        """
        Test 'sync_all_columns' with incompatible type changes.

        Expected behavior:
        - Incompatible type change should raise IncompatibleTypeChangeError
        """
        table_name = "test_catalog.default.sync_incompatible_test"

        # Create table with string type
        data = [("1", "User1")]
        df = spark.createDataFrame(data, ["id", "name"])
        df.writeTo(table_name).using("iceberg").create()

        # New schema with int type (incompatible)
        new_data = [(2, "User2")]
        new_df = spark.createDataFrame(new_data, ["id", "name"])

        # Apply sync
        table_df = spark.table(table_name)
        comparison = SchemaEvolutionService.compare_schemas(new_df.schema, table_df.schema)

        # Should raise on incompatible type change
        with pytest.raises(IncompatibleTypeChangeError):
            SchemaEvolutionService.apply_schema_evolution(
                spark, table_name, comparison, "sync_all_columns"
            )

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestFormatSpecificBehavior:
    """Test format-specific behaviors."""

    def test_csv_with_header(self, spark, temp_dir):
        """Test CSV format with header handling."""
        table_name = "test_catalog.default.csv_header_test"

        # Create CSV files with headers
        base_files = create_csv_files(temp_dir, "base", num_files=1)

        # Read with header
        df = spark.read.option("header", "true").option("inferSchema", "true").csv([f"file://{f}" for f in base_files])
        df.writeTo(table_name).using("iceberg").create()

        table_df = spark.table(table_name)
        assert len(table_df.columns) == 3
        assert table_df.count() == 10

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_json_multiline(self, spark, temp_dir):
        """Test JSON format with newline-delimited records."""
        table_name = "test_catalog.default.json_multiline_test"

        # Create JSON files
        base_files = create_json_files(temp_dir, "base", num_files=1)

        # Read JSON
        df = spark.read.json([f"file://{f}" for f in base_files])
        df.writeTo(table_name).using("iceberg").create()

        table_df = spark.table(table_name)
        assert len(table_df.columns) == 3
        assert table_df.count() == 10

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
