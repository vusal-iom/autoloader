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
