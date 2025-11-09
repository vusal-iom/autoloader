"""
Unit tests for Schema Evolution Service.

Tests schema comparison logic and evolution strategies without requiring Spark/database.
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, FloatType

from app.services.schema_evolution_service import (
    SchemaEvolutionService,
    SchemaComparison,
    SchemaChange,
    SchemaChangeType,
    SchemaMismatchError,
    IncompatibleTypeChangeError,
)


class TestSchemaComparison:
    """Test schema comparison logic."""

    def test_no_changes_same_schema(self):
        """Test that identical schemas report no changes."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ])

        comparison = SchemaEvolutionService.compare_schemas(schema, schema)

        assert not comparison.has_changes
        assert len(comparison.added_columns) == 0
        assert len(comparison.removed_columns) == 0
        assert len(comparison.modified_columns) == 0

    def test_detect_added_columns(self):
        """Test detection of new columns in source schema."""
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

        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

        assert comparison.has_changes
        assert len(comparison.added_columns) == 2
        assert len(comparison.removed_columns) == 0
        assert len(comparison.modified_columns) == 0

        added_names = [f.name for f in comparison.added_columns]
        assert "phone" in added_names
        assert "address" in added_names

    def test_detect_removed_columns(self):
        """Test detection of columns removed from source schema."""
        source_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),  # REMOVED from source
            StructField("phone", StringType(), True),  # REMOVED from source
        ])

        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

        assert comparison.has_changes
        assert len(comparison.added_columns) == 0
        assert len(comparison.removed_columns) == 2
        assert len(comparison.modified_columns) == 0

        removed_names = [f.name for f in comparison.removed_columns]
        assert "email" in removed_names
        assert "phone" in removed_names

    def test_detect_type_changes(self):
        """Test detection of column type changes."""
        source_schema = StructType([
            StructField("id", LongType(), True),  # Changed from int
            StructField("price", DoubleType(), True),  # Changed from float
        ])

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("price", FloatType(), True),
        ])

        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

        assert comparison.has_changes
        assert len(comparison.added_columns) == 0
        assert len(comparison.removed_columns) == 0
        assert len(comparison.modified_columns) == 2

        # Check that both type changes are detected
        modified_names = [col[0] for col in comparison.modified_columns]
        assert "id" in modified_names
        assert "price" in modified_names

    def test_mixed_changes(self):
        """Test detection of multiple types of changes at once."""
        source_schema = StructType([
            StructField("id", LongType(), True),  # Type changed
            StructField("name", StringType(), True),  # Same
            StructField("email", StringType(), True),  # Same
            StructField("phone", StringType(), True),  # NEW
        ])

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),  # REMOVED from source
        ])

        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

        assert comparison.has_changes
        assert len(comparison.added_columns) == 1  # phone
        assert len(comparison.removed_columns) == 1  # address
        assert len(comparison.modified_columns) == 1  # id type change

    def test_case_insensitive_comparison(self):
        """Test that schema comparison is case-insensitive."""
        source_schema = StructType([
            StructField("ID", IntegerType(), True),
            StructField("Name", StringType(), True),
        ])

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])

        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

        # Should be considered the same despite different case
        assert not comparison.has_changes

    def test_get_changes_list(self):
        """Test conversion of comparison to list of SchemaChange objects."""
        source_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("phone", StringType(), True),  # NEW
        ])

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),  # REMOVED
        ])

        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)
        changes = comparison.get_changes()

        assert len(changes) == 2
        change_types = [c.change_type for c in changes]
        assert SchemaChangeType.NEW_COLUMN in change_types
        assert SchemaChangeType.REMOVED_COLUMN in change_types


class TestTypeCompatibility:
    """Test type compatibility checks."""

    def test_compatible_widening_conversions(self):
        """Test that widening numeric conversions are compatible."""
        assert SchemaEvolutionService.is_type_change_compatible("int", "long")
        assert SchemaEvolutionService.is_type_change_compatible("integer", "bigint")
        assert SchemaEvolutionService.is_type_change_compatible("float", "double")

    def test_compatible_string_conversions(self):
        """Test that string/varchar conversions are compatible."""
        assert SchemaEvolutionService.is_type_change_compatible("string", "varchar")
        assert SchemaEvolutionService.is_type_change_compatible("varchar", "string")

    def test_compatible_date_timestamp(self):
        """Test that date to timestamp is compatible."""
        assert SchemaEvolutionService.is_type_change_compatible("date", "timestamp")

    def test_same_type_is_compatible(self):
        """Test that same type is always compatible."""
        assert SchemaEvolutionService.is_type_change_compatible("string", "string")
        assert SchemaEvolutionService.is_type_change_compatible("int", "int")

    def test_incompatible_conversions(self):
        """Test that incompatible conversions are detected."""
        assert not SchemaEvolutionService.is_type_change_compatible("string", "int")
        assert not SchemaEvolutionService.is_type_change_compatible("long", "int")  # Narrowing
        assert not SchemaEvolutionService.is_type_change_compatible("double", "float")  # Narrowing
        assert not SchemaEvolutionService.is_type_change_compatible("timestamp", "date")

    def test_case_insensitive_type_comparison(self):
        """Test that type comparison is case-insensitive."""
        assert SchemaEvolutionService.is_type_change_compatible("INT", "LONG")
        assert SchemaEvolutionService.is_type_change_compatible("String", "VARCHAR")


class TestStrategyValidation:
    """Test strategy validation."""

    def test_evolution_strategies_require_iceberg(self):
        """Test that evolution strategies require Iceberg format."""
        with pytest.raises(ValueError, match="requires Iceberg"):
            SchemaEvolutionService.validate_strategy_compatibility("parquet", "append_new_columns")

        with pytest.raises(ValueError, match="requires Iceberg"):
            SchemaEvolutionService.validate_strategy_compatibility("csv", "sync_all_columns")

    def test_ignore_and_fail_work_with_any_format(self):
        """Test that ignore and fail strategies work with any format."""
        # Should not raise
        SchemaEvolutionService.validate_strategy_compatibility("parquet", "ignore")
        SchemaEvolutionService.validate_strategy_compatibility("csv", "fail")
        SchemaEvolutionService.validate_strategy_compatibility("json", "ignore")

    def test_evolution_strategies_work_with_iceberg(self):
        """Test that evolution strategies work with Iceberg."""
        # Should not raise
        SchemaEvolutionService.validate_strategy_compatibility("iceberg", "append_new_columns")
        SchemaEvolutionService.validate_strategy_compatibility("iceberg", "sync_all_columns")

    def test_case_insensitive_format_check(self):
        """Test that format validation is case-insensitive."""
        # Should not raise
        SchemaEvolutionService.validate_strategy_compatibility("ICEBERG", "append_new_columns")
        SchemaEvolutionService.validate_strategy_compatibility("Iceberg", "sync_all_columns")


class TestSchemaMismatchError:
    """Test SchemaMismatchError exception."""

    def test_error_message_format(self):
        """Test that error message is properly formatted."""
        source_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("phone", StringType(), True),  # NEW
        ])

        target_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),  # REMOVED
        ])

        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)
        error = SchemaMismatchError(comparison)

        assert "Schema Mismatch Detected" in error.message
        assert "phone" in error.message
        assert "email" in error.message
        assert "[NEW]" in error.message
        assert "[REMOVED]" in error.message
        assert "Action Required" in error.message


class TestIncompatibleTypeChangeError:
    """Test IncompatibleTypeChangeError exception."""

    def test_error_attributes(self):
        """Test that error has correct attributes."""
        error = IncompatibleTypeChangeError("user_id", "string", "int")

        assert error.column == "user_id"
        assert error.old_type == "string"
        assert error.new_type == "int"
        assert "user_id" in error.message
        assert "string" in error.message
        assert "int" in error.message
