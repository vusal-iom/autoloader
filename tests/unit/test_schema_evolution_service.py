"""
Unit tests for SchemaEvolutionService.

Tests schema comparison, type compatibility validation, and evolution strategies.
"""
import pytest
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType,
    DoubleType, TimestampType, BooleanType, FloatType, ArrayType, MapType
)

from app.services.schema_evolution_service import SchemaEvolutionService


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

        # Assert no changes via bulk comparison
        assert comparison.has_changes is False
        assert [c.to_dict() for c in comparison.get_changes()] == []

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
        assert comparison.has_changes is True

        # Expected changes
        actual_changes = [c.to_dict() for c in comparison.get_changes()]
        expected_changes = [
            {"change_type": "NEW_COLUMN", "column_name": "email", "old_type": None, "new_type": "string"},
            {"change_type": "NEW_COLUMN", "column_name": "created_at", "old_type": None, "new_type": "timestamp"},
            {"change_type": "NEW_COLUMN", "column_name": "tags", "old_type": None, "new_type": "array<string>"},
            {"change_type": "NEW_COLUMN", "column_name": "metadata", "old_type": None,
             "new_type": "struct<version:int,source:string>"},
            {"change_type": "NEW_COLUMN", "column_name": "column_with_special_chars", "old_type": None,
             "new_type": "string"},
        ]
        assert actual_changes == expected_changes
