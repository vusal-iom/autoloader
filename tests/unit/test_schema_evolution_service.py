"""
Unit tests for SchemaEvolutionService.

Tests schema comparison, type compatibility validation, and evolution strategies.
"""
import pytest
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType,
    DoubleType, TimestampType, BooleanType, FloatType, ArrayType, MapType
)

from app.services.schema_evolution_service import (
    SchemaEvolutionService,
    SchemaChangeType,
)


# ============================================================================
# Fixtures: Schema Test Data
# ============================================================================

@pytest.fixture
def simple_source_schema():
    """Source schema with basic types for testing."""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("created_at", TimestampType(), True)
    ])


@pytest.fixture
def simple_target_schema():
    """Target schema with slight differences for testing."""
    return StructType([
        StructField("id", LongType(), False),  # Type evolved
        StructField("name", StringType(), True),
        StructField("deleted", BooleanType(), True)  # Removed from source
    ])


@pytest.fixture
def complex_schema_with_nested():
    """Schema with nested structures for advanced testing."""
    return StructType([
        StructField("id", LongType(), False),
        StructField("metadata", StructType([
            StructField("version", IntegerType()),
            StructField("tags", ArrayType(StringType()))
        ])),
        StructField("mappings", MapType(StringType(), DoubleType()))
    ])


# ============================================================================
# P0 Tests: Schema Comparison
# ============================================================================

class TestSchemaComparison:
    """Tests for schema comparison logic."""

    def test_compare_schemas_identical(self):
        """
        Test that identical schemas return no changes.

        Why: Validates baseline - no changes should trigger no actions.
        Business Value: Prevents unnecessary schema operations that could impact performance.
        Edge Cases: Case sensitivity, nullable flags, metadata differences.
        """
        # Create identical schemas
        schema1 = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("created_at", TimestampType(), True)
        ])

        schema2 = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("created_at", TimestampType(), True)
        ])

        # Compare schemas
        comparison = SchemaEvolutionService.compare_schemas(schema1, schema2)

        # Assert no changes detected
        assert comparison.has_changes is False, "Identical schemas should have no changes"
        assert len(comparison.added_columns) == 0, "Should have no added columns"
        assert len(comparison.removed_columns) == 0, "Should have no removed columns"
        assert len(comparison.modified_columns) == 0, "Should have no modified columns"

        # Verify get_changes returns empty list
        changes = comparison.get_changes()
        assert len(changes) == 0, "get_changes() should return empty list for identical schemas"

    def test_compare_schemas_new_columns(self):
        """
        Test that new columns in source are detected correctly.

        Why: Most common evolution scenario in data lakes.
        Business Value: Enables automatic schema expansion as data sources evolve.
        Edge Cases: Complex types (arrays, structs), special characters in names.
        """
        # Target schema (existing table)
        target_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
        ])

        # Source schema (new data with additional columns)
        source_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),  # NEW column
            StructField("created_at", TimestampType(), True),  # NEW column
            StructField("tags", ArrayType(StringType()), True),  # NEW complex type
            StructField("metadata", StructType([  # NEW nested struct
                StructField("version", IntegerType()),
                StructField("source", StringType())
            ]), True),
            StructField("column_with_special_chars", StringType(), True),  # NEW with special chars
        ])

        # Compare schemas
        comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

        # Assert changes detected
        assert comparison.has_changes is True, "Should detect new columns as changes"

        # Assert added columns
        assert len(comparison.added_columns) == 5, "Should detect 5 new columns"
        added_names = {field.name for field in comparison.added_columns}
        expected_new_columns = {
            "email", "created_at", "tags", "metadata", "column_with_special_chars"
        }
        assert added_names == expected_new_columns, f"Expected {expected_new_columns}, got {added_names}"

        # Verify complex types are preserved
        tags_field = next(f for f in comparison.added_columns if f.name == "tags")
        assert isinstance(tags_field.dataType, ArrayType), "Array type should be preserved"

        metadata_field = next(f for f in comparison.added_columns if f.name == "metadata")
        assert isinstance(metadata_field.dataType, StructType), "Struct type should be preserved"

        # Assert no removed or modified columns
        assert len(comparison.removed_columns) == 0, "Should have no removed columns"
        assert len(comparison.modified_columns) == 0, "Should have no modified columns"

        # Verify get_changes returns NEW_COLUMN changes
        changes = comparison.get_changes()
        assert len(changes) == 5, "Should have 5 changes"
        assert all(
            change.change_type == SchemaChangeType.NEW_COLUMN for change in changes
        ), "All changes should be NEW_COLUMN type"

        # Verify change details
        email_change = next(c for c in changes if c.column_name == "email")
        assert email_change.new_type == "string", "Should capture new column type"
        assert email_change.old_type is None, "Old type should be None for new columns"
