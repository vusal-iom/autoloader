"""
Schema Evolution Demo

Demonstrates the four schema evolution strategies with sample data.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from app.services.schema_evolution_service import (
    SchemaEvolutionService,
    SchemaMismatchError,
    IncompatibleTypeChangeError,
)


def demo_schema_comparison():
    """Demonstrate schema comparison."""
    print("=" * 60)
    print("DEMO 1: Schema Comparison")
    print("=" * 60)

    # Original schema
    original_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ])

    # Evolved schema (added 2 columns)
    evolved_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
    ])

    comparison = SchemaEvolutionService.compare_schemas(evolved_schema, original_schema)

    print(f"Has changes: {comparison.has_changes}")
    print(f"Added columns: {[f.name for f in comparison.added_columns]}")
    print(f"Removed columns: {[f.name for f in comparison.removed_columns]}")
    print(f"Modified columns: {comparison.modified_columns}")

    changes = comparison.get_changes()
    print(f"\nDetailed changes:")
    for change in changes:
        print(f"  - {change.change_type}: {change.column_name}")

    print()


def demo_fail_strategy():
    """Demonstrate fail strategy."""
    print("=" * 60)
    print("DEMO 2: Fail Strategy")
    print("=" * 60)

    original_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])

    evolved_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("phone", StringType(), True),
    ])

    comparison = SchemaEvolutionService.compare_schemas(evolved_schema, original_schema)

    print("Applying 'fail' strategy with schema changes...")
    try:
        # This will raise SchemaMismatchError
        SchemaEvolutionService.apply_schema_evolution(
            spark=None,  # Not needed for this demo
            table_identifier="test.table",
            comparison=comparison,
            strategy="fail"
        )
    except SchemaMismatchError as e:
        print("Expected error caught:")
        print(e.message)

    print()


def demo_type_compatibility():
    """Demonstrate type compatibility checks."""
    print("=" * 60)
    print("DEMO 3: Type Compatibility")
    print("=" * 60)

    compatible_pairs = [
        ("int", "long"),
        ("float", "double"),
        ("string", "varchar"),
        ("date", "timestamp"),
    ]

    incompatible_pairs = [
        ("string", "int"),
        ("long", "int"),
        ("double", "float"),
    ]

    print("Compatible type changes:")
    for old_type, new_type in compatible_pairs:
        is_compatible = SchemaEvolutionService.is_type_change_compatible(old_type, new_type)
        print(f"  {old_type:10} -> {new_type:10}: {is_compatible}")

    print("\nIncompatible type changes:")
    for old_type, new_type in incompatible_pairs:
        is_compatible = SchemaEvolutionService.is_type_change_compatible(old_type, new_type)
        print(f"  {old_type:10} -> {new_type:10}: {is_compatible}")

    print()


def demo_strategy_validation():
    """Demonstrate strategy validation."""
    print("=" * 60)
    print("DEMO 4: Strategy Validation")
    print("=" * 60)

    strategies = ["ignore", "fail", "append_new_columns", "sync_all_columns"]
    formats = ["json", "csv", "parquet", "iceberg"]

    print("Strategy compatibility matrix:")
    print(f"{'Format':<12} | " + " | ".join(f"{s:<20}" for s in strategies))
    print("-" * 100)

    for format_type in formats:
        results = []
        for strategy in strategies:
            try:
                SchemaEvolutionService.validate_strategy_compatibility(format_type, strategy)
                results.append("OK")
            except ValueError:
                results.append("Not Supported")

        print(f"{format_type:<12} | " + " | ".join(f"{r:<20}" for r in results))

    print()


def main():
    """Run all demos."""
    print("\n" + "=" * 60)
    print("IOMETE Autoloader - Schema Evolution Demos")
    print("=" * 60 + "\n")

    demo_schema_comparison()
    demo_fail_strategy()
    demo_type_compatibility()
    demo_strategy_validation()

    print("=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)

    print("\nKey Takeaways:")
    print("1. 'ignore' - Safe default, works with all formats")
    print("2. 'fail' - Explicit control, requires manual review")
    print("3. 'append_new_columns' - Additive evolution (Iceberg only)")
    print("4. 'sync_all_columns' - Full sync (Iceberg only)")
    print("\nFor production use, start with 'ignore' or 'fail' strategy.")


if __name__ == "__main__":
    main()
