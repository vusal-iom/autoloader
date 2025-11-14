# Schema Evolution Service - Integration Tests

## Focus

Test schema comparison with **real files and Spark**, not mocked objects. Goal is to understand how Spark infers schemas from actual JSON/Parquet files and validate our comparison logic handles real-world scenarios.

## Why Integration Tests

Unit tests mock Spark schemas - we construct StructType manually. Integration tests use real files:
- Discover how Spark actually infers types from JSON (int vs long, nested structures)
- Test with realistic file evolution scenarios
- Validate our comparison logic against Spark's actual behavior

## Test Cases

### Test 1: Happy Path - New Columns Added

**Scenario:** Initial JSON files, then new files with additional fields

**Files:**

Initial (`data_v1.json`):
```json
{"id": 1, "name": "Alice", "amount": 100.50}
{"id": 2, "name": "Bob", "amount": 200.75}
```

Evolved (`data_v2.json`):
```json
{"id": 3, "name": "Charlie", "amount": 150.25, "email": "charlie@example.com", "created_at": "2024-01-15T10:30:00"}
{"id": 4, "name": "Diana", "amount": 300.00, "email": "diana@example.com", "created_at": "2024-01-15T11:00:00"}
```

**Test Implementation:**

```python
import pytest
import json
import tempfile
from pathlib import Path
from app.services.schema_evolution_service import SchemaEvolutionService, SchemaChangeType

@pytest.mark.integration
class TestSchemaComparison:
    """Integration tests for schema comparison with real files."""

    def test_new_columns_detected_from_json_files(self, spark_session):
        """
        Test that new columns in evolved JSON files are detected correctly.

        This validates:
        - Spark's JSON schema inference
        - Our schema comparison logic with real schemas
        - Change detection accuracy
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write initial JSON files
            v1_path = Path(tmpdir) / "v1"
            v1_path.mkdir()
            with open(v1_path / "data.json", "w") as f:
                f.write('{"id": 1, "name": "Alice", "amount": 100.50}\n')
                f.write('{"id": 2, "name": "Bob", "amount": 200.75}\n')

            # Write evolved JSON files
            v2_path = Path(tmpdir) / "v2"
            v2_path.mkdir()
            with open(v2_path / "data.json", "w") as f:
                f.write('{"id": 3, "name": "Charlie", "amount": 150.25, "email": "charlie@example.com", "created_at": "2024-01-15T10:30:00"}\n')
                f.write('{"id": 4, "name": "Diana", "amount": 300.00, "email": "diana@example.com", "created_at": "2024-01-15T11:00:00"}\n')

            # Read schemas via Spark
            df_v1 = spark_session.read.json(str(v1_path))
            df_v2 = spark_session.read.json(str(v2_path))

            schema_v1 = df_v1.schema
            schema_v2 = df_v2.schema

            # Print for learning
            print("\n=== Initial Schema ===")
            print(schema_v1)
            print("\n=== Evolved Schema ===")
            print(schema_v2)

            # Compare schemas
            comparison = SchemaEvolutionService.compare_schemas(
                source_schema=schema_v2,
                target_schema=schema_v1
            )

            # Validate detection
            assert comparison.has_changes, "Should detect schema changes"

            changes = comparison.get_changes()
            change_dict = [c.to_dict() for c in changes]

            print("\n=== Detected Changes ===")
            for change in change_dict:
                print(f"  {change}")

            # Verify new columns detected
            new_column_names = [c["column_name"] for c in change_dict if c["change_type"] == "NEW_COLUMN"]
            assert "email" in new_column_names, "email column should be detected as new"
            assert "created_at" in new_column_names, "created_at column should be detected as new"

            # Verify no false positives (existing columns not flagged)
            removed_columns = [c["column_name"] for c in change_dict if c["change_type"] == "REMOVED_COLUMN"]
            assert len(removed_columns) == 0, "No columns should be detected as removed"

            # Learn: What type did Spark infer for created_at?
            created_at_change = next(c for c in change_dict if c["column_name"] == "created_at")
            print(f"\n=== Spark inferred type for created_at: {created_at_change['new_type']} ===")
```

---

### Test 2: Complex Nested Schema - Struct Evolution

**Scenario:** JSON with nested objects, then files with additional nested fields

**Files:**

Initial (`events_v1.json`):
```json
{
  "event_id": "e1",
  "user": {
    "id": "u1",
    "name": "Alice"
  },
  "metadata": {
    "source": "web",
    "ip": "192.168.1.1"
  }
}
```

Evolved (`events_v2.json`):
```json
{
  "event_id": "e2",
  "user": {
    "id": "u2",
    "name": "Bob",
    "email": "bob@example.com"
  },
  "metadata": {
    "source": "mobile",
    "ip": "192.168.1.2",
    "device": {
      "type": "iPhone",
      "os": "iOS 17"
    }
  },
  "tags": ["login", "success"]
}
```

**Test Implementation:**

```python
def test_nested_struct_evolution_from_json(self, spark_session):
    """
    Test complex nested structure evolution.

    This validates:
    - Spark's handling of nested JSON structures
    - Struct field addition detection
    - New top-level array columns
    - Deeply nested schema changes
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write initial nested JSON
        v1_path = Path(tmpdir) / "v1"
        v1_path.mkdir()
        initial_data = {
            "event_id": "e1",
            "user": {
                "id": "u1",
                "name": "Alice"
            },
            "metadata": {
                "source": "web",
                "ip": "192.168.1.1"
            }
        }
        with open(v1_path / "event.json", "w") as f:
            json.dump(initial_data, f)

        # Write evolved nested JSON
        v2_path = Path(tmpdir) / "v2"
        v2_path.mkdir()
        evolved_data = {
            "event_id": "e2",
            "user": {
                "id": "u2",
                "name": "Bob",
                "email": "bob@example.com"
            },
            "metadata": {
                "source": "mobile",
                "ip": "192.168.1.2",
                "device": {
                    "type": "iPhone",
                    "os": "iOS 17"
                }
            },
            "tags": ["login", "success"]
        }
        with open(v2_path / "event.json", "w") as f:
            json.dump(evolved_data, f)

        # Read schemas
        df_v1 = spark_session.read.json(str(v1_path))
        df_v2 = spark_session.read.json(str(v2_path))

        schema_v1 = df_v1.schema
        schema_v2 = df_v2.schema

        # Print for learning
        print("\n=== Initial Nested Schema ===")
        df_v1.printSchema()
        print("\n=== Evolved Nested Schema ===")
        df_v2.printSchema()

        # Compare schemas
        comparison = SchemaEvolutionService.compare_schemas(
            source_schema=schema_v2,
            target_schema=schema_v1
        )

        assert comparison.has_changes, "Should detect nested schema changes"

        changes = comparison.get_changes()
        change_dict = [c.to_dict() for c in changes]

        print("\n=== Detected Changes ===")
        for change in change_dict:
            print(f"  Type: {change['change_type']}")
            print(f"  Column: {change['column_name']}")
            if change['old_type']:
                print(f"  Old: {change['old_type']}")
            if change['new_type']:
                print(f"  New: {change['new_type']}")
            print()

        # Validate top-level new column (tags array)
        new_columns = [c["column_name"] for c in change_dict if c["change_type"] == "NEW_COLUMN"]
        assert "tags" in new_columns, "tags array column should be detected"

        # Validate struct changes detected
        # user struct: gained email field (detected as TYPE_CHANGE)
        # metadata struct: gained device nested struct (detected as TYPE_CHANGE)
        type_changes = [c for c in change_dict if c["change_type"] == "TYPE_CHANGE"]

        user_change = next((c for c in type_changes if c["column_name"] == "user"), None)
        metadata_change = next((c for c in type_changes if c["column_name"] == "metadata"), None)

        print("\n=== User Struct Change ===")
        if user_change:
            print(f"  Old: {user_change['old_type']}")
            print(f"  New: {user_change['new_type']}")
            assert "email" in user_change['new_type'], "email should be in new user struct"

        print("\n=== Metadata Struct Change ===")
        if metadata_change:
            print(f"  Old: {metadata_change['old_type']}")
            print(f"  New: {metadata_change['new_type']}")
            assert "device" in metadata_change['new_type'], "device should be in new metadata struct"

        # Learning goals
        print("\n=== LEARNING OBSERVATIONS ===")
        print(f"Total changes detected: {len(changes)}")
        print(f"New top-level columns: {len([c for c in changes if c.change_type == SchemaChangeType.NEW_COLUMN])}")
        print(f"Modified columns (structs): {len([c for c in changes if c.change_type == SchemaChangeType.TYPE_CHANGE])}")
        print("\nQuestion: How does our current implementation handle nested field additions?")
        print("Answer: Detects as TYPE_CHANGE on parent struct (column-level), not field-level")
```

---

## Running Tests

```bash
# Ensure Spark Connect is running
docker-compose -f docker-compose.test.yml up -d

# Wait for Spark to start (1-2 min)

# Run integration tests
pytest tests/integration/test_schema_evolution_service.py -v -s

# -s flag shows print statements for learning
```

## Expected Learnings

After running these tests, we'll understand:

1. **Spark Type Inference:**
   - Does Spark infer `string` or `timestamp` for ISO date strings?
   - Does Spark infer `int`, `bigint`, or `long` for integers in JSON?
   - How does Spark represent nested structs in schema.simpleString()?

2. **Our Comparison Logic:**
   - Does our case-insensitive matching work correctly?
   - Are nested changes detected at the right granularity?
   - Do we have false positives/negatives?

3. **Next Steps:**
   - Do we need field-level change detection for nested structs?
   - How should we handle struct evolution in DDL?
   - What edge cases should we add tests for?

## Test File Location

Create: `tests/integration/test_schema_evolution_service.py`

Follow pattern from existing `tests/integration/test_schema_version_tracking.py`

## Expand As We Learn

After running these 2 tests, we'll add:
- Type change scenarios (compatible/incompatible)
- Array element evolution
- Map value type changes
- Removed columns detection
- Case sensitivity edge cases

Keep it iterative and practical.
