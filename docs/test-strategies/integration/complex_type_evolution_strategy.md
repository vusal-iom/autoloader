# Complex Type Evolution - Analysis & Integration Test Strategy

## Executive Summary

This document analyzes how schema evolution should handle complex types (arrays, structs, maps) when ingesting JSON files from cloud storage. Currently, the `SchemaEvolutionService` treats any complex type change as incompatible, which may be overly restrictive for real-world JSON evolution scenarios.

**Key Questions:**
1. How should we detect and validate changes in nested structures?
2. What complex type changes are safe vs. risky?
3. How do we test this with real Spark integration using JSON files?

## The Problem: JSON Files with Evolving Nested Structures

### Real-World Scenario

Imagine ingesting AWS CloudTrail logs, Stripe webhook payloads, or application event logs as JSON files. These structures naturally evolve:

**Day 1: Initial JSON**
```json
{
  "user_id": "123",
  "action": "login",
  "metadata": {
    "ip": "192.168.1.1",
    "browser": "Chrome"
  },
  "tags": ["security", "audit"]
}
```

**Day 30: Evolved JSON**
```json
{
  "user_id": "123",
  "action": "login",
  "metadata": {
    "ip": "192.168.1.1",
    "browser": "Chrome",
    "device_id": "abc-123",          // ← NEW field in struct
    "location": {                     // ← NEW nested struct
      "country": "US",
      "city": "NYC"
    }
  },
  "tags": ["security", "audit", "mfa"],
  "session_metadata": {               // ← NEW top-level struct
    "session_id": "xyz",
    "duration_ms": 1500
  }
}
```

**Day 60: Type Evolution**
```json
{
  "user_id": "123",
  "action": "login",
  "metadata": {
    "ip": "192.168.1.1",
    "browser": "Chrome",
    "device_id": "abc-123",
    "timestamp": 1234567890,          // ← Was int, now long (widening)
    "location": {
      "country": "US",
      "city": "NYC",
      "coordinates": [40.7, -74.0]    // ← NEW array in nested struct
    }
  },
  "tags": ["security", "audit", "mfa"],
  "attempt_count": 3                  // ← Was string in some old files, now int (breaking!)
}
```

### Current Behavior Analysis

When Spark reads these JSON files:

**Scenario 1: Struct Field Addition**
```python
# Old schema (Day 1)
StructType([
    StructField("metadata", StructType([
        StructField("ip", StringType()),
        StructField("browser", StringType())
    ]))
])

# New schema (Day 30)
StructType([
    StructField("metadata", StructType([
        StructField("ip", StringType()),
        StructField("browser", StringType()),
        StructField("device_id", StringType())  # ADDED
    ]))
])
```

**Current Implementation:**
```python
old_type = "struct<ip:string,browser:string>"
new_type = "struct<ip:string,browser:string,device_id:string>"

if old_type != new_type:  # True! Strings differ
    # Detected as TYPE_CHANGE
    if is_type_change_compatible(old_type, new_type):  # False! Not in compatible_transitions
        # ❌ Raises IncompatibleTypeChangeError
```

**Problem:** Adding a field to a struct is **semantically compatible** but currently treated as incompatible.

**Scenario 2: Array Element Type Widening**
```python
# Old: tags: array<string>
# New: tags: array<string>  (same)

# Old: counts: array<int>
# New: counts: array<long>  (widened)
```

**Current Implementation:**
```python
old_type = "array<int>"
new_type = "array<long>"

if old_type != new_type:  # True!
    if is_type_change_compatible(old_type, new_type):  # False! String comparison only
        # ❌ Raises IncompatibleTypeChangeError
```

**Problem:** Array element widening (`int` → `long`) is safe but not recognized.

## Design Decisions Required

### Decision 1: Granularity of Change Detection

**Option A: Column-Level Reporting (Current)**
```python
SchemaChange(
    change_type=TYPE_CHANGE,
    column_name="metadata",
    old_type="struct<ip:string,browser:string>",
    new_type="struct<ip:string,browser:string,device_id:string>"
)
```

**Option B: Field-Level Drilling (Proposed)**
```python
SchemaChange(
    change_type=NESTED_FIELD_ADDED,
    column_name="metadata.device_id",
    old_type=None,
    new_type="string"
)
```

**Recommendation:** Start with **Option A** for simplicity, but enhance the compatibility checker to understand complex types. Field-level reporting can be added later if needed.

### Decision 2: Compatibility Rules for Complex Types

#### Arrays

| Old Type | New Type | Compatible? | Reason |
|----------|----------|-------------|--------|
| `array<int>` | `array<long>` | ✅ YES | Element widening (safe) |
| `array<long>` | `array<int>` | ❌ NO | Element narrowing (data loss) |
| `array<string>` | `array<int>` | ❌ NO | Element type incompatible |
| `array<struct<a:int>>` | `array<struct<a:int,b:string>>` | ✅ YES | Nested struct evolution |

**Rule:** Array types are compatible if element types are compatible (recursively).

#### Structs

| Old Type | New Type | Compatible? | Reason |
|----------|----------|-------------|--------|
| `struct<a:int>` | `struct<a:int,b:string>` | ✅ YES | Field added (nullable) |
| `struct<a:int,b:string>` | `struct<a:int>` | ⚠️ MAYBE | Field removed (data loss, but reads work) |
| `struct<a:int>` | `struct<a:long>` | ✅ YES | Field type widened |
| `struct<a:long>` | `struct<a:int>` | ❌ NO | Field type narrowed |
| `struct<a:int>` | `struct<b:int>` | ❌ NO | Field renamed (not detectable) |

**Rules:**
- **Added fields:** Compatible (Spark can read old data with nulls for new fields)
- **Removed fields:** Compatible for reading (old data has extra fields that will be ignored), but may indicate data loss
- **Modified field types:** Check type compatibility recursively
- **Renamed fields:** Appears as remove + add, incompatible

#### Maps

| Old Type | New Type | Compatible? | Reason |
|----------|----------|-------------|--------|
| `map<string,int>` | `map<string,long>` | ✅ YES | Value type widened |
| `map<string,long>` | `map<string,int>` | ❌ NO | Value type narrowed |
| `map<string,int>` | `map<int,int>` | ❌ NO | Key type changed (breaks lookups) |
| `map<string,struct<a:int>>` | `map<string,struct<a:int,b:string>>` | ✅ YES | Nested struct evolution |

**Rules:**
- **Key type changes:** Always incompatible (changes map semantics)
- **Value type changes:** Check type compatibility recursively

### Decision 3: Evolution Strategy Behavior

Given these compatibility rules, how should strategies handle complex type changes?

**Strategy: `ignore`**
- No DDL changes
- Spark will read with projection (selecting common fields)
- Works well for struct field additions

**Strategy: `append_new_columns`**
- Top-level new columns: Add to table
- Struct field additions: **Should we support?**
  - ❌ Current: No (can't ALTER nested fields easily)
  - ✅ Proposed: Detect but don't fail, rely on Spark's schema evolution in merge

**Strategy: `sync_all_columns`**
- Array/Map element widening: **Should we support?**
  - ❌ Current: No
  - ✅ Proposed: Allow if element type change is compatible
  - Implementation: May require table recreation or Iceberg schema evolution APIs

**Strategy: `fail`**
- Any complex type change fails with detailed error message
- Same as current behavior

## Proposed Implementation Approach

### Phase 1: Enhanced Type Compatibility Checker (P0)

Implement recursive type compatibility checking:

```python
@staticmethod
def is_type_change_compatible(old_type: DataType, new_type: DataType) -> bool:
    """
    Check if type change is compatible, including complex types.

    Now accepts DataType objects instead of strings for recursive checking.
    """
    # Same type
    if old_type == new_type:
        return True

    # Primitive type widening
    if isinstance(old_type, (IntegerType, LongType, FloatType, DoubleType, ...)):
        return _check_primitive_widening(old_type, new_type)

    # Array: element type must be compatible
    if isinstance(old_type, ArrayType) and isinstance(new_type, ArrayType):
        return is_type_change_compatible(
            old_type.elementType,
            new_type.elementType
        )

    # Struct: all common fields must be compatible
    if isinstance(old_type, StructType) and isinstance(new_type, StructType):
        return _check_struct_compatibility(old_type, new_type)

    # Map: key type must match, value type must be compatible
    if isinstance(old_type, MapType) and isinstance(new_type, MapType):
        if old_type.keyType != new_type.keyType:
            return False  # Key type change not allowed
        return is_type_change_compatible(
            old_type.valueType,
            new_type.valueType
        )

    return False


@staticmethod
def _check_struct_compatibility(old_struct: StructType, new_struct: StructType) -> bool:
    """
    Check if struct evolution is compatible.

    Rules:
    - New fields added: Compatible (nullable)
    - Fields removed: Compatible for read (with warning)
    - Field types changed: Must be compatible recursively
    """
    old_fields = {f.name.lower(): f for f in old_struct.fields}
    new_fields = {f.name.lower(): f for f in new_struct.fields}

    # Check all common fields for type compatibility
    for field_name in old_fields.keys():
        if field_name in new_fields:
            if not is_type_change_compatible(
                old_fields[field_name].dataType,
                new_fields[field_name].dataType
            ):
                return False

    # New fields added: Always compatible (will be null in old data)
    # Removed fields: Compatible for reading (will be ignored in new data)
    return True
```

### Phase 2: Better Change Reporting (P1)

Enhance `SchemaChange` to include context for complex types:

```python
@dataclass
class SchemaChange:
    change_type: SchemaChangeType
    column_name: str
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    is_complex_type: bool = False
    nested_changes: Optional[List[str]] = None  # Human-readable description

    def to_dict(self) -> Dict:
        return {
            "change_type": self.change_type.value,
            "column_name": self.column_name,
            "old_type": self.old_type,
            "new_type": self.new_type,
            "is_complex_type": self.is_complex_type,
            "nested_changes": self.nested_changes
        }
```

Example output:
```python
SchemaChange(
    change_type=TYPE_CHANGE,
    column_name="metadata",
    old_type="struct<ip:string,browser:string>",
    new_type="struct<ip:string,browser:string,device_id:string>",
    is_complex_type=True,
    nested_changes=["Added field: device_id (string)"]
)
```

### Phase 3: DDL Generation for Compatible Complex Changes (P2)

For `sync_all_columns` strategy, generate appropriate DDL:

**Challenge:** Iceberg/Spark SQL doesn't support `ALTER TABLE ... ALTER COLUMN nested_field.subfield`

**Options:**
1. **Schema Evolution via Merge:** Let Spark handle it during write with `mergeSchema` option
2. **Iceberg Schema Evolution API:** Use Iceberg's schema evolution directly
3. **Fail-Safe:** Treat complex type changes as incompatible for now, require manual intervention

**Recommendation:** Start with Option 1 (rely on Spark's merge behavior) for Phase 1.

## Integration Test Strategy

### Test Environment Setup

**Prerequisites:**
- Real Spark session (SparkConnect in integration tests)
- Temporary test database and tables
- S3/local file system for JSON test files
- Iceberg table format

**Test Fixtures:**
```python
@pytest.fixture(scope="module")
def spark_session():
    """Real Spark session for integration tests."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("ComplexTypeEvolutionTest") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.test_catalog.type", "hadoop") \
        .getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture
def json_storage_path(tmp_path):
    """Temporary directory for JSON test files."""
    json_dir = tmp_path / "json_files"
    json_dir.mkdir()
    return str(json_dir)
```

### Test Scenarios

#### Test 1: Struct Field Addition (JSON Evolution)

**Scenario:** JSON files gain new nested fields over time

**Setup:**
```python
def test_json_struct_field_addition(spark_session, json_storage_path):
    """
    Test schema evolution when JSON adds new fields to nested structures.

    Day 1: Basic metadata struct
    Day 2: Metadata gains new fields
    Expected: Compatible evolution, new fields added
    """

    # Day 1: Initial JSON files
    day1_data = [
        {"user_id": "1", "metadata": {"ip": "1.1.1.1", "browser": "Chrome"}},
        {"user_id": "2", "metadata": {"ip": "2.2.2.2", "browser": "Firefox"}}
    ]
    write_json_files(json_storage_path, "day1", day1_data)

    # Create initial table
    df1 = spark_session.read.json(f"{json_storage_path}/day1")
    initial_schema = df1.schema
    df1.write.format("iceberg").mode("overwrite").saveAsTable("test_catalog.db.events")

    # Day 2: JSON with additional nested fields
    day2_data = [
        {
            "user_id": "3",
            "metadata": {
                "ip": "3.3.3.3",
                "browser": "Safari",
                "device_id": "abc-123",        # NEW
                "timestamp": 1234567890         # NEW
            }
        }
    ]
    write_json_files(json_storage_path, "day2", day2_data)

    # Read new files
    df2 = spark_session.read.json(f"{json_storage_path}/day2")
    evolved_schema = df2.schema

    # Compare schemas
    from app.services.schema_evolution_service import SchemaEvolutionService

    comparison = SchemaEvolutionService.compare_schemas(
        source_schema=evolved_schema,
        target_schema=initial_schema
    )

    # ASSERTIONS
    assert comparison.has_changes, "Should detect schema changes"

    changes = comparison.get_changes()
    assert len(changes) == 1, "Should detect 1 change (metadata struct modified)"

    change = changes[0]
    assert change.change_type == SchemaChangeType.TYPE_CHANGE
    assert change.column_name == "metadata"
    assert "device_id" in change.new_type
    assert "timestamp" in change.new_type

    # Test compatibility
    old_metadata_type = initial_schema["metadata"].dataType
    new_metadata_type = evolved_schema["metadata"].dataType

    is_compatible = SchemaEvolutionService.is_type_change_compatible(
        old_metadata_type,
        new_metadata_type
    )

    assert is_compatible, "Adding fields to struct should be compatible"

    # Test evolution strategy
    SchemaEvolutionService.apply_schema_evolution(
        spark=spark_session,
        table_identifier="test_catalog.db.events",
        comparison=comparison,
        strategy="sync_all_columns"
    )

    # Verify table schema updated
    final_schema = spark_session.table("test_catalog.db.events").schema
    assert "device_id" in final_schema["metadata"].dataType.simpleString()

    # Verify old data readable (null for new fields)
    result_df = spark_session.table("test_catalog.db.events")
    assert result_df.count() == 3  # All records readable

    row1 = result_df.filter("user_id = '1'").first()
    assert row1["metadata"]["ip"] == "1.1.1.1"
    assert row1["metadata"]["device_id"] is None  # Null for old data
```

#### Test 2: Array Element Type Widening

**Scenario:** Array elements evolve from smaller to larger types

```python
def test_json_array_element_widening(spark_session, json_storage_path):
    """
    Test array element type widening (int → long).

    Initial: response_times: [100, 200, 300] (int array)
    Evolved: response_times: [1234567890123, ...] (long array due to large values)
    Expected: Compatible widening
    """

    # Initial data: small integers
    initial_data = [
        {"request_id": "1", "response_times": [100, 200, 300]},
        {"request_id": "2", "response_times": [150, 250]}
    ]
    write_json_files(json_storage_path, "initial", initial_data)

    df1 = spark_session.read.json(f"{json_storage_path}/initial")
    initial_schema = df1.schema

    # Evolved data: large integers requiring long
    evolved_data = [
        {"request_id": "3", "response_times": [9999999999, 8888888888]}  # Forces long type
    ]
    write_json_files(json_storage_path, "evolved", evolved_data)

    df2 = spark_session.read.json(f"{json_storage_path}/evolved")
    evolved_schema = df2.schema

    # Compare
    comparison = SchemaEvolutionService.compare_schemas(evolved_schema, initial_schema)

    # Get type change
    changes = comparison.get_changes()
    type_change = next(c for c in changes if c.column_name == "response_times")

    assert "array<bigint>" in type_change.new_type or "array<long>" in type_change.new_type
    assert "array<int>" in type_change.old_type or "array<bigint>" in type_change.old_type

    # Test compatibility
    old_array_type = initial_schema["response_times"].dataType
    new_array_type = evolved_schema["response_times"].dataType

    is_compatible = SchemaEvolutionService.is_type_change_compatible(
        old_array_type,
        new_array_type
    )

    assert is_compatible, "Array element widening (int → long) should be compatible"
```

#### Test 3: Deeply Nested Structure Evolution

**Scenario:** Multi-level nested JSON structures evolve

```python
def test_deeply_nested_json_evolution(spark_session, json_storage_path):
    """
    Test evolution in deeply nested structures.

    Initial:
      user.profile.settings.theme = "dark"

    Evolved:
      user.profile.settings.theme = "dark"
      user.profile.settings.notifications.email = true  (NEW nested level)
    """

    initial_data = [
        {
            "user": {
                "id": "1",
                "profile": {
                    "name": "Alice",
                    "settings": {
                        "theme": "dark"
                    }
                }
            }
        }
    ]
    write_json_files(json_storage_path, "initial", initial_data)

    evolved_data = [
        {
            "user": {
                "id": "2",
                "profile": {
                    "name": "Bob",
                    "settings": {
                        "theme": "light",
                        "notifications": {           # NEW nested struct
                            "email": True,
                            "push": False
                        }
                    }
                }
            }
        }
    ]
    write_json_files(json_storage_path, "evolved", evolved_data)

    df1 = spark_session.read.json(f"{json_storage_path}/initial")
    df2 = spark_session.read.json(f"{json_storage_path}/evolved")

    comparison = SchemaEvolutionService.compare_schemas(df2.schema, df1.schema)

    # Should detect change in 'user' column (struct modified)
    assert comparison.has_changes

    changes = comparison.get_changes()
    user_change = next(c for c in changes if c.column_name == "user")

    # Verify the new nested structure is reflected
    assert "notifications" in user_change.new_type

    # Test compatibility
    is_compatible = SchemaEvolutionService.is_type_change_compatible(
        df1.schema["user"].dataType,
        df2.schema["user"].dataType
    )

    assert is_compatible, "Adding deeply nested fields should be compatible"
```

#### Test 4: Incompatible Complex Type Change

**Scenario:** Breaking changes in complex types should be detected

```python
def test_incompatible_array_element_change(spark_session, json_storage_path):
    """
    Test that incompatible array element type changes are detected.

    Initial: tags: ["security", "audit"]  (array<string>)
    Evolved: tags: [1, 2, 3]  (array<int>)
    Expected: Incompatible change, error raised
    """

    initial_data = [{"id": "1", "tags": ["security", "audit"]}]
    write_json_files(json_storage_path, "initial", initial_data)

    evolved_data = [{"id": "2", "tags": [1, 2, 3]}]
    write_json_files(json_storage_path, "evolved", evolved_data)

    df1 = spark_session.read.json(f"{json_storage_path}/initial")
    df2 = spark_session.read.json(f"{json_storage_path}/evolved")

    comparison = SchemaEvolutionService.compare_schemas(df2.schema, df1.schema)

    # Detect change
    changes = comparison.get_changes()
    tags_change = next(c for c in changes if c.column_name == "tags")

    assert "array<string>" in tags_change.old_type
    assert "array<bigint>" in tags_change.new_type or "array<long>" in tags_change.new_type

    # Test incompatibility
    is_compatible = SchemaEvolutionService.is_type_change_compatible(
        df1.schema["tags"].dataType,
        df2.schema["tags"].dataType
    )

    assert not is_compatible, "Array element type change (string → int) should be incompatible"

    # Verify error raised with sync_all_columns strategy
    with pytest.raises(IncompatibleTypeChangeError) as exc_info:
        SchemaEvolutionService.apply_schema_evolution(
            spark=spark_session,
            table_identifier="test_catalog.db.test_table",
            comparison=comparison,
            strategy="sync_all_columns"
        )

    assert "tags" in str(exc_info.value)
    assert "array<string>" in str(exc_info.value)
```

#### Test 5: Map Value Type Evolution

**Scenario:** Map value types evolve compatibly

```python
def test_map_value_type_widening(spark_session, json_storage_path):
    """
    Test map value type widening.

    Initial: counters: {"page_views": 100, "clicks": 50}  (map<string,int>)
    Evolved: counters: {"page_views": 9999999999}  (map<string,long>)
    Expected: Compatible widening
    """

    initial_data = [{"id": "1", "counters": {"page_views": 100, "clicks": 50}}]
    write_json_files(json_storage_path, "initial", initial_data)

    evolved_data = [{"id": "2", "counters": {"page_views": 9999999999}}]
    write_json_files(json_storage_path, "evolved", evolved_data)

    df1 = spark_session.read.json(f"{json_storage_path}/initial")
    df2 = spark_session.read.json(f"{json_storage_path}/evolved")

    comparison = SchemaEvolutionService.compare_schemas(df2.schema, df1.schema)

    is_compatible = SchemaEvolutionService.is_type_change_compatible(
        df1.schema["counters"].dataType,
        df2.schema["counters"].dataType
    )

    assert is_compatible, "Map value widening (int → long) should be compatible"
```

#### Test 6: Struct Field Removal

**Scenario:** Fields removed from nested structures

```python
def test_struct_field_removal(spark_session, json_storage_path):
    """
    Test struct field removal scenario.

    Initial: metadata: {ip, browser, user_agent}
    Evolved: metadata: {ip, browser}  (user_agent removed)
    Expected: Depends on strategy
      - ignore: Compatible (old data ignored)
      - fail: Incompatible (data loss)
      - sync_all_columns: Compatible but with warning
    """

    initial_data = [
        {"id": "1", "metadata": {"ip": "1.1.1.1", "browser": "Chrome", "user_agent": "Mozilla/5.0"}}
    ]
    write_json_files(json_storage_path, "initial", initial_data)

    evolved_data = [
        {"id": "2", "metadata": {"ip": "2.2.2.2", "browser": "Firefox"}}
    ]
    write_json_files(json_storage_path, "evolved", evolved_data)

    df1 = spark_session.read.json(f"{json_storage_path}/initial")
    df2 = spark_session.read.json(f"{json_storage_path}/evolved")

    comparison = SchemaEvolutionService.compare_schemas(df2.schema, df1.schema)

    changes = comparison.get_changes()
    metadata_change = next(c for c in changes if c.column_name == "metadata")

    # Verify field was removed
    assert "user_agent" in metadata_change.old_type
    assert "user_agent" not in metadata_change.new_type

    # Test compatibility (should be compatible for reading, but data loss warning)
    is_compatible = SchemaEvolutionService.is_type_change_compatible(
        df1.schema["metadata"].dataType,
        df2.schema["metadata"].dataType
    )

    # Decision: Are removed struct fields compatible?
    # Recommendation: YES for reading (Spark handles projection), but log warning
    assert is_compatible, "Struct field removal should be compatible (with warning)"
```

### Test Utilities

```python
import json
from pathlib import Path


def write_json_files(base_path: str, subdir: str, data: list):
    """
    Write list of dictionaries as JSON files.

    Args:
        base_path: Base directory for test files
        subdir: Subdirectory name (e.g., "day1", "day2")
        data: List of dictionaries to write as JSON
    """
    dir_path = Path(base_path) / subdir
    dir_path.mkdir(parents=True, exist_ok=True)

    for idx, record in enumerate(data):
        file_path = dir_path / f"record_{idx}.json"
        with open(file_path, 'w') as f:
            json.dump(record, f)


def assert_schema_field_type(schema: StructType, field_path: str, expected_type: str):
    """
    Assert that a field (possibly nested) has expected type.

    Args:
        schema: Spark StructType schema
        field_path: Dot-separated path (e.g., "metadata.device_id")
        expected_type: Expected type string (e.g., "string", "bigint")
    """
    parts = field_path.split(".")
    current_type = schema

    for part in parts:
        if isinstance(current_type, StructType):
            field = next((f for f in current_type.fields if f.name == part), None)
            assert field is not None, f"Field '{part}' not found in schema"
            current_type = field.dataType
        else:
            raise AssertionError(f"Cannot navigate to '{part}' in non-struct type")

    actual_type = current_type.simpleString()
    assert expected_type in actual_type, f"Expected '{expected_type}' but got '{actual_type}'"
```

## Test Execution Plan

### Phase 1: Unit Tests First (Current Implementation)
- Test current behavior with string-based type comparison
- Document limitations with complex types
- Establish baseline coverage

### Phase 2: Integration Test Infrastructure
- Set up real Spark session fixture
- Create JSON file writing utilities
- Test Iceberg table operations

### Phase 3: Complex Type Integration Tests
- Implement tests for struct field addition
- Implement tests for array element widening
- Implement tests for map value evolution
- Document actual Spark behavior with JSON schema inference

### Phase 4: Enhanced Implementation
- Refactor `is_type_change_compatible` to accept DataType objects
- Implement recursive compatibility checking
- Update integration tests to verify new behavior
- Add performance benchmarks for large nested schemas

## Open Questions & Decisions Needed

### Question 1: Struct Field Removal Strategy

**Scenario:** Old JSON has `metadata.user_agent`, new JSON doesn't.

**Options:**
- **A) Compatible:** Allow it (Spark can read old data, ignores extra fields)
- **B) Incompatible:** Treat as breaking change (data loss concern)
- **C) Warning:** Allow but log warning about potential data loss

**Recommendation:** Option C - Allow but warn. This matches Spark's permissive read behavior.

### Question 2: Nullable Changes in Nested Structs

**Scenario:** Field changes from `nullable=true` to `nullable=false` or vice versa.

**Current Behavior:** Not checked (only type string compared)

**Should we check?**
- **YES:** Changing nullable=true → false is a breaking change (can cause write failures)
- **NO:** Too strict, Spark often handles this gracefully

**Recommendation:** Ignore nullable changes for complex types in Phase 1, revisit in Phase 2.

### Question 3: Performance Concerns

**Scenario:** Large schemas with 100+ nested fields, frequent comparisons.

**Concern:** Recursive type checking may be expensive.

**Mitigation:**
- Cache schema comparison results
- Optimize recursive traversal
- Add performance benchmarks in integration tests

### Question 4: DDL Generation Complexity

**Scenario:** How to apply evolution for nested field additions in `sync_all_columns`?

**Challenge:** Standard SQL doesn't support:
```sql
ALTER TABLE my_table ALTER COLUMN metadata ADD FIELD device_id STRING
```

**Options:**
- **A) Table Scan:** Read all data, write with new schema (expensive!)
- **B) Iceberg API:** Use Iceberg's schema evolution API directly
- **C) Merge Write:** Use Spark's `mergeSchema` option on next write
- **D) Manual:** Require manual intervention for complex type changes

**Recommendation:** Option C for Phase 1 (least invasive), Option B for Phase 2 (proper solution).

## Success Criteria

1. **Correctness**
   - All compatible complex type evolutions detected and allowed
   - All incompatible changes rejected with clear error messages
   - No data corruption or loss in any scenario

2. **Real-World Validation**
   - Integration tests pass with actual Spark session
   - JSON file evolution scenarios work end-to-end
   - Performance acceptable for schemas up to 200 fields

3. **Developer Experience**
   - Clear error messages explain what changed and why it's incompatible
   - Warnings logged for potentially risky changes (field removals)
   - Documentation explains complex type evolution rules

## Next Steps

1. **Review & Decide:** Team reviews this document and makes decisions on open questions
2. **Unit Tests First:** Implement unit tests for current string-based behavior (baseline)
3. **Integration Test Setup:** Create Spark fixtures and JSON utilities
4. **Run Discovery Tests:** Execute integration tests to observe actual Spark behavior with JSON evolution
5. **Enhanced Implementation:** Based on findings, implement recursive type compatibility
6. **Validation:** Run full integration test suite to verify behavior
7. **Documentation:** Update user-facing docs with complex type evolution examples

## Appendix: Spark's Actual Behavior with JSON Schema Inference

To guide our implementation, we need to understand how Spark **actually** handles these scenarios:

**Experiment 1:** Write initial JSON, read back schema, write to Iceberg table.
**Experiment 2:** Write evolved JSON, read back schema, compare with table schema.
**Experiment 3:** Attempt to merge/append evolved JSON to existing table with `mergeSchema` option.

**Key Observations to Document:**
- Does Spark infer `int` vs `long` based on value ranges in JSON?
- How does Spark handle struct field additions in merge operations?
- What errors occur when writing incompatible data to existing tables?
- Does Iceberg's schema evolution handle nested changes gracefully?

These experiments will inform our compatibility rules and ensure we align with Spark's behavior.
