# Schema Evolution Service - Integration Test Strategy

## Executive Summary

This document outlines integration test strategy for the `SchemaEvolutionService` module. While unit tests verify the logic in isolation with mocked Spark sessions, integration tests validate the service behavior with real Spark Connect, actual Iceberg tables, and PostgreSQL database to ensure the DDL operations work correctly in production-like environments.

## Module Overview

**Location:** `app/services/schema_evolution_service.py`

**Core Responsibilities:**
1. Compare Spark schemas (source files vs target table) to detect changes
2. Validate type compatibility for schema changes
3. Apply evolution strategies by executing DDL statements
4. Track schema versions in PostgreSQL

**Integration Points:**
- **Spark Connect:** Executes DDL (ALTER TABLE) on Iceberg tables
- **Iceberg Tables:** Target for schema evolution operations
- **PostgreSQL:** Stores schema version history
- **BatchFileProcessor:** Consumer of schema evolution service

## Existing Test Coverage

### Unit Tests (`test_schema_evolution_service.py`)
**What they cover:**
- Schema comparison logic with various field changes
- Type compatibility validation (primitive widening)
- Error handling (SchemaMismatchError, IncompatibleTypeChangeError)
- Change detection and reporting
- Complex types (arrays, structs, maps) detection

**What they mock:**
- Spark session and SQL execution
- Table metadata retrieval
- DDL execution results

### Existing Integration Test (`test_schema_version_tracking.py`)
**What it covers:**
- Schema version recording during file processing (via BatchFileProcessor)
- Version incrementing across multiple changes
- Strategy behavior (ignore, append_new_columns, sync_all_columns)
- Schema JSON capture

**What it tests:**
- End-to-end flow through BatchFileProcessor._write_to_iceberg
- Schema version repository integration

## Integration Test Value Proposition

Integration tests add value beyond unit tests by:

1. **Validating DDL Execution:** Ensure ALTER TABLE statements work with real Iceberg
2. **Iceberg Constraints:** Discover Iceberg-specific limitations (e.g., type changes, partitioned tables)
3. **Real Type System:** Test Spark's actual type inference and conversion behavior
4. **Concurrency Safety:** Verify locking and race condition handling
5. **Error Scenarios:** Test actual failure modes with real database and Spark
6. **Performance:** Measure actual DDL execution time on real tables

## Integration Test Scenarios

### P0 Tests: Critical DDL Operations

#### Test 1: Direct Schema Evolution - Append New Columns

**Why:** Validates the most common evolution scenario with real DDL execution

**What:** Tests SchemaEvolutionService.apply_schema_evolution directly with real Iceberg table

**Business Value:** Ensures append_new_columns strategy works in production

**Test Flow:**
1. Create Iceberg table with base schema (id, name)
2. Create DataFrame with additional columns (id, name, email, created_at)
3. Call SchemaEvolutionService.compare_schemas to detect changes
4. Call SchemaEvolutionService.apply_schema_evolution with "append_new_columns"
5. Verify table schema has new columns via Spark SQL DESCRIBE
6. Verify old data readable and new columns are NULL

**Expected Behavior:**
- ALTER TABLE ADD COLUMN statements execute successfully
- Table schema reflects new columns
- Existing data preserved with NULL values for new columns
- No errors or exceptions

**Edge Cases:**
- Multiple columns added simultaneously
- Complex types (arrays, structs) as new columns
- Columns with special characters in names

**Implementation:**
```python
def test_direct_schema_evolution_append_columns(spark_session, test_db):
    """Test append_new_columns strategy with real Iceberg table."""
    # Create initial table
    spark_session.sql("""
        CREATE TABLE test_catalog.test_db.users (
            id BIGINT,
            name STRING
        ) USING iceberg
    """)

    # Insert initial data
    spark_session.sql("""
        INSERT INTO test_catalog.test_db.users VALUES (1, 'Alice'), (2, 'Bob')
    """)

    # Create evolved schema DataFrame
    from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
    evolved_schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    # Get target schema
    target_table = spark_session.table("test_catalog.test_db.users")
    target_schema = target_table.schema

    # Compare schemas
    from app.services.schema_evolution_service import SchemaEvolutionService
    comparison = SchemaEvolutionService.compare_schemas(evolved_schema, target_schema)

    assert comparison.has_changes
    assert len(comparison.added_columns) == 2

    # Apply evolution
    SchemaEvolutionService.apply_schema_evolution(
        spark=spark_session,
        table_identifier="test_catalog.test_db.users",
        comparison=comparison,
        strategy="append_new_columns"
    )

    # Verify schema updated
    updated_table = spark_session.table("test_catalog.test_db.users")
    field_names = [f.name for f in updated_table.schema.fields]

    assert "id" in field_names
    assert "name" in field_names
    assert "email" in field_names
    assert "created_at" in field_names

    # Verify old data readable
    result = updated_table.collect()
    assert len(result) == 2
    assert result[0]["email"] is None
    assert result[0]["created_at"] is None
```

---

#### Test 2: Direct Schema Evolution - Sync All Columns

**Why:** Tests the most complex strategy with add, remove, and type changes

**What:** Validates sync_all_columns with multiple DDL operations

**Business Value:** Ensures full schema synchronization works correctly

**Test Flow:**
1. Create Iceberg table (id, name, old_field)
2. Create evolved schema (id, name, new_field) - old_field removed, new_field added
3. Apply sync_all_columns strategy
4. Verify old_field removed, new_field added

**Expected Behavior:**
- Multiple ALTER TABLE statements executed in sequence
- Columns added and removed correctly
- No partial updates on failure

**Edge Cases:**
- Removing partitioning columns (should fail or warn)
- Type changes that Iceberg doesn't support
- Large number of changes (performance)

---

#### Test 3: Fail Strategy with Schema Mismatch

**Why:** Validates that fail strategy prevents any changes

**What:** Tests SchemaMismatchError is raised correctly

**Business Value:** Ensures strict environments can prevent automatic changes

**Test Flow:**
1. Create Iceberg table with base schema
2. Detect schema changes
3. Apply "fail" strategy
4. Verify SchemaMismatchError raised with detailed message
5. Verify NO changes applied to table

**Expected Behavior:**
- SchemaMismatchError raised before any DDL
- Error message includes all detected changes
- Table schema unchanged
- No side effects

---

#### Test 4: Incompatible Type Change Detection

**Why:** Validates that unsafe type changes are blocked

**What:** Tests IncompatibleTypeChangeError with real type change scenario

**Business Value:** Prevents data loss from unsafe type conversions

**Test Flow:**
1. Create Iceberg table (amount DOUBLE)
2. Create evolved schema (amount STRING) - incompatible narrowing
3. Apply sync_all_columns strategy
4. Verify IncompatibleTypeChangeError raised
5. Verify table schema unchanged

**Expected Behavior:**
- Error raised before ALTER TABLE execution
- Clear error message explaining incompatibility
- Table remains in original state

---

#### Test 5: Case-Insensitive Column Matching

**Why:** Spark and Iceberg handle column names case-insensitively

**What:** Validates schema comparison works with mixed case

**Business Value:** Prevents false positives in column detection

**Test Flow:**
1. Create Iceberg table (UserId BIGINT, UserName STRING)
2. Create DataFrame (userid, username) - lowercase
3. Compare schemas
4. Verify NO changes detected (case-insensitive match)

**Expected Behavior:**
- Schema comparison ignores case differences
- No false "removed" or "added" columns
- Works correctly with real Iceberg table

---

### P1 Tests: Important Scenarios

#### Test 6: DDL Execution Failure Handling

**Why:** DDL can fail due to Iceberg constraints or permissions

**What:** Tests error propagation and cleanup on DDL failure

**Business Value:** Ensures system stability on failures

**Test Flow:**
1. Create Iceberg table
2. Force DDL failure (invalid column name, reserved keyword, etc.)
3. Apply evolution strategy
4. Verify exception propagated correctly
5. Verify no partial schema changes

**Expected Behavior:**
- Exception raised with clear error message
- No orphaned schema changes
- Table remains in valid state

---

#### Test 7: Multiple Evolution Steps in Sequence

**Why:** Real ingestion jobs evolve schema multiple times

**What:** Tests sequential schema changes across multiple batches

**Business Value:** Validates long-term schema evolution behavior

**Test Flow:**
1. Create initial Iceberg table (v1 schema)
2. Apply first evolution (add column A)
3. Apply second evolution (add column B)
4. Apply third evolution (add column C)
5. Verify all changes persisted correctly

**Expected Behavior:**
- Each evolution succeeds independently
- Final schema contains all additions
- No conflicts or race conditions

---

#### Test 8: Compatible Type Widening

**Why:** Validates safe type promotions work

**What:** Tests INT → BIGINT, FLOAT → DOUBLE promotions

**Business Value:** Allows safe evolution without data loss

**Test Flow:**
1. Create Iceberg table (count INT, price FLOAT)
2. Create evolved schema (count BIGINT, price DOUBLE)
3. Apply sync_all_columns
4. Verify types updated via ALTER COLUMN

**Expected Behavior:**
- ALTER COLUMN TYPE statements execute
- Existing data readable with new types
- No data loss or precision issues

---

#### Test 9: Schema Evolution on Partitioned Table

**Why:** Iceberg has special rules for partitioned tables

**What:** Tests evolution with partition columns present

**Business Value:** Ensures partitioned tables evolve safely

**Test Flow:**
1. Create partitioned Iceberg table (PARTITIONED BY year)
2. Add non-partition column
3. Attempt to remove partition column (should fail)
4. Verify appropriate behavior

**Expected Behavior:**
- Non-partition columns can be added
- Partition columns cannot be removed
- Clear error if partition schema violated

---

### P2 Tests: Advanced and Edge Cases

#### Test 10: Large Schema Evolution

**Why:** Performance validation for wide tables

**What:** Tests evolution with 50+ columns

**Business Value:** Ensures scalability

**Test Flow:**
1. Create table with 50 columns
2. Add 20 new columns
3. Measure execution time
4. Verify all changes applied

**Expected Behavior:**
- Completes within acceptable time (< 30s)
- All DDL statements execute correctly
- No performance degradation

---

#### Test 11: Empty Table Evolution

**Why:** Edge case with no existing data

**What:** Tests schema evolution on empty Iceberg table

**Business Value:** Handles new table initialization

**Test Flow:**
1. Create empty Iceberg table
2. Apply schema evolution
3. Verify schema updated
4. Insert data with new schema

**Expected Behavior:**
- Evolution succeeds on empty table
- New schema immediately usable
- No special case errors

---

#### Test 12: Ignore Strategy with Extra Columns

**Why:** Validates ignore strategy drops extra columns correctly

**What:** Tests DataFrame column filtering with ignore strategy

**Business Value:** Allows reading evolved files without table changes

**Test Flow:**
1. Create Iceberg table (id, name)
2. Create DataFrame (id, name, extra1, extra2)
3. Apply "ignore" strategy
4. Verify NO DDL executed
5. Verify extra columns would be dropped on write

**Expected Behavior:**
- No ALTER TABLE statements
- DataFrame filtered to table columns
- Write succeeds without errors

---

## Implementation Best Practices

### Test Structure

Follow existing integration test pattern from `test_schema_version_tracking.py`:

```python
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType
from app.services.schema_evolution_service import SchemaEvolutionService

@pytest.mark.integration
class TestSchemaEvolutionDDL:
    """Integration tests for SchemaEvolutionService DDL execution."""

    def test_scenario_name(self, test_db, spark_session):
        """Test description with business reason."""
        # Setup: Create Iceberg table
        table_name = f"test_table_{uuid.uuid4().hex[:8]}"
        spark_session.sql(f"""
            CREATE TABLE test_catalog.test_db.{table_name} (
                id BIGINT,
                name STRING
            ) USING iceberg
        """)

        try:
            # Test logic here
            pass
        finally:
            # Cleanup
            spark_session.sql(f"DROP TABLE IF EXISTS test_catalog.test_db.{table_name}")
```

### Fixtures

Use existing fixtures from `tests/conftest.py` and `tests/integration/conftest.py`:

- `test_db`: SQLite database session (function-scoped)
- `spark_session`: Real Spark Connect session (session-scoped)
- `spark_client`: SparkConnectClient instance

### Table Naming

Always use unique table names to avoid conflicts:

```python
import uuid
table_name = f"schema_evolution_test_{uuid.uuid4().hex[:8]}"
```

### Cleanup

Always drop test tables in finally block:

```python
try:
    # Test logic
    pass
finally:
    spark_session.sql(f"DROP TABLE IF EXISTS test_catalog.test_db.{table_name}")
```

### Assertions

Use descriptive assertions with clear messages:

```python
assert "email" in field_names, \
    f"Expected 'email' column in schema, got: {field_names}"
```

## Test Execution

### Prerequisites

```bash
# Start Spark Connect service
docker-compose -f docker-compose.test.yml up -d

# Wait 1-2 minutes for Spark to start
# Verify Spark UI at http://localhost:4040
```

### Running Tests

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run specific test file
pytest tests/integration/test_schema_evolution_service.py -v

# Run specific test
pytest tests/integration/test_schema_evolution_service.py::TestSchemaEvolutionDDL::test_direct_schema_evolution_append_columns -v
```

## Success Criteria

### Test Quality Metrics
- All P0 tests passing (100%)
- 90%+ P1 tests passing
- Test execution time < 5 minutes
- Zero flaky tests (deterministic results)

### Coverage Goals
- Integration test covers DDL execution paths
- All evolution strategies tested with real Iceberg
- All error scenarios validated

### Business Impact
- Zero data corruption incidents
- Schema evolution success rate > 95%
- Reduced production issues related to DDL failures

## Risks and Mitigations

### Risk 1: Spark Connect Dependency
**Risk:** Tests fail if Spark service not available
**Mitigation:**
- Clear documentation on prerequisites
- Service health checks in conftest
- Fail-fast with helpful error messages

### Risk 2: Iceberg Version Differences
**Risk:** DDL behavior differs across Iceberg versions
**Mitigation:**
- Document tested Iceberg version
- Add version detection in tests
- Skip unsupported scenarios with pytest.skip

### Risk 3: Test Data Conflicts
**Risk:** Parallel test runs interfere with each other
**Mitigation:**
- Use unique table names with UUID
- Function-scoped test_db fixture
- Explicit cleanup in finally blocks

### Risk 4: Long Test Execution Time
**Risk:** Integration tests slow down CI/CD
**Mitigation:**
- Session-scoped Spark fixture (reuse connection)
- Minimal test data (1-10 rows)
- Parallel test execution where safe

## Comparison with Unit Tests

| Aspect | Unit Tests | Integration Tests |
|--------|-----------|-------------------|
| **Spark Session** | Mocked | Real Spark Connect |
| **DDL Execution** | Mocked (returns None) | Real ALTER TABLE |
| **Database** | In-memory SQLite | SQLite (same as unit) |
| **Iceberg Tables** | Not tested | Real Iceberg operations |
| **Speed** | < 1s per test | 5-10s per test |
| **Scope** | Logic verification | System behavior |
| **Value** | Algorithm correctness | Production readiness |

## Comparison with E2E Tests

| Aspect | Integration Tests | E2E Tests |
|--------|------------------|-----------|
| **Database** | SQLite | PostgreSQL |
| **Scope** | SchemaEvolutionService | Full ingestion pipeline |
| **MinIO** | Not required | Required (file upload) |
| **Prefect** | Not required | Required (scheduling) |
| **Data Flow** | Direct service call | API → Service → Spark |
| **Test Focus** | DDL correctness | User workflow |

Integration tests sit between unit and E2E tests, focusing on SchemaEvolutionService behavior with real Spark while avoiding the complexity of full system setup.

## Next Steps

1. **Phase 1:** Implement P0 tests (1-5)
   - Test direct DDL execution
   - Validate core strategies
   - Test error scenarios

2. **Phase 2:** Implement P1 tests (6-9)
   - Add failure handling tests
   - Test sequential evolution
   - Test partitioned tables

3. **Phase 3:** Add P2 tests (10-12)
   - Performance validation
   - Edge case coverage

4. **Phase 4:** CI/CD Integration
   - Add to pytest markers
   - Configure parallel execution
   - Set up coverage reporting

## References

- Unit Test Strategy: `docs/test-strategies/unit/schema_evolution_test_strategy.md`
- Complex Type Evolution (Future): `docs/test-strategies/integration/complex_type_evolution_strategy.md`
- Existing Integration Test: `tests/integration/test_schema_version_tracking.py`
- Service Implementation: `app/services/schema_evolution_service.py`
