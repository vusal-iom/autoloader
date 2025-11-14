# Schema Evolution Service - Integration Tests V2

## Philosophy

**Unit tests validate logic. Integration tests validate reality.**

Unit tests already cover `compare_schemas` thoroughly with mocked Spark sessions. Integration tests should focus on what unit tests CAN'T test: **Does the DDL actually work with real Iceberg tables?**

## Why Test `apply_schema_evolution` Not `compare_schemas`

### What Unit Tests Already Cover
- Schema comparison logic (new/removed/modified columns)
- Type compatibility validation
- Change detection with various data types
- Edge cases (case sensitivity, nullability)

### What Integration Tests Add
- **Real DDL execution:** Does `ALTER TABLE ADD COLUMN` actually work?
- **Iceberg constraints:** What does Iceberg accept/reject?
- **Table state verification:** Did the schema actually change?
- **Data compatibility:** Can we read old data with new schema?
- **Error scenarios:** How do real failures manifest?

### Implicit Coverage
`apply_schema_evolution` internally calls `compare_schemas`. If comparison is wrong, apply fails. Testing apply tests the full flow.

## Test Scenarios

### Test 1: Append New Columns Strategy - Happy Path

**Goal:** Validate most common evolution scenario with real DDL

**Setup:**
```python
# Create Iceberg table
spark.sql("""
    CREATE TABLE test_catalog.test_db.users (
        id BIGINT,
        name STRING
    ) USING iceberg
""")

# Insert initial data
spark.sql("INSERT INTO test_catalog.test_db.users VALUES (1, 'Alice'), (2, 'Bob')")
```

**Action:**
```python
# Read JSON with evolved schema
json_data = [
    {"id": 3, "name": "Charlie", "email": "charlie@example.com", "created_at": "2024-01-15T10:30:00"}
]
df = spark.createDataFrame(json_data)

# Get target table schema
target_schema = spark.table("test_catalog.test_db.users").schema

# Compare
comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)

# Apply evolution
SchemaEvolutionService.apply_schema_evolution(
    spark=spark,
    table_identifier="test_catalog.test_db.users",
    comparison=comparison,
    strategy="append_new_columns"
)
```

**Verify:**
1. Table schema has new columns: `DESCRIBE test_catalog.test_db.users`
2. Old records have NULL for new columns: `SELECT * WHERE id <= 2`
3. Can insert new records: `INSERT with all columns`
4. No data loss: `SELECT COUNT(*) = 2`

**Learn:**
- Does Iceberg accept our ALTER TABLE syntax?
- Are column types inferred correctly from JSON?
- Do NULL defaults work as expected?

---

### Test 2: Sync All Columns - Add and Remove

**Goal:** Test full synchronization with real column removal

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.products (
        id BIGINT,
        name STRING,
        old_category STRING
    ) USING iceberg
""")
```

**Action:**
```python
# JSON without old_category, with new_field
json_data = [{"id": 1, "name": "Widget", "new_field": "value"}]
df = spark.createDataFrame(json_data)

# Apply sync_all_columns
SchemaEvolutionService.apply_schema_evolution(
    spark=spark,
    table_identifier="test_catalog.test_db.products",
    comparison=comparison,
    strategy="sync_all_columns"
)
```

**Verify:**
1. old_category removed: `DESCRIBE` should not show it
2. new_field added: `DESCRIBE` should show it
3. Table schema matches DataFrame schema exactly

**Learn:**
- Does Iceberg allow DROP COLUMN?
- Are there restrictions on removing columns?
- Does order matter (add first or remove first)?

---

### Test 3: Fail Strategy - Reject Changes

**Goal:** Validate strict mode prevents any schema changes

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.strict_table (
        id BIGINT,
        name STRING
    ) USING iceberg
""")
```

**Action:**
```python
# JSON with extra column
json_data = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
df = spark.createDataFrame(json_data)

# Apply with "fail" strategy
try:
    SchemaEvolutionService.apply_schema_evolution(
        spark=spark,
        table_identifier="test_catalog.test_db.strict_table",
        comparison=comparison,
        strategy="fail"
    )
    assert False, "Should have raised SchemaMismatchError"
except SchemaMismatchError as e:
    # Expected
    print(f"Error message: {e.message}")
```

**Verify:**
1. SchemaMismatchError raised before any DDL
2. Error message includes all detected changes
3. Table schema unchanged: `DESCRIBE` shows original schema
4. No partial updates

**Learn:**
- Is error message helpful for users?
- Does it explain what changed and how to fix it?

---

### Test 4: Incompatible Type Change Detection

**Goal:** Validate unsafe type changes are blocked with real Spark types

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.orders (
        id BIGINT,
        amount DOUBLE
    ) USING iceberg
""")
```

**Action:**
```python
# JSON where amount is a string (incompatible)
json_data = [{"id": 1, "amount": "not_a_number"}]
df = spark.createDataFrame(json_data)

# Try to apply sync_all_columns
try:
    SchemaEvolutionService.apply_schema_evolution(
        spark=spark,
        table_identifier="test_catalog.test_db.orders",
        comparison=comparison,
        strategy="sync_all_columns"
    )
    assert False, "Should have raised IncompatibleTypeChangeError"
except IncompatibleTypeChangeError as e:
    print(f"Blocked incompatible change: {e.message}")
```

**Verify:**
1. IncompatibleTypeChangeError raised
2. Table schema unchanged
3. Error clearly states: column, old type, new type

**Learn:**
- Does our type compatibility logic match Spark's reality?
- Are there types we allow that Iceberg rejects?
- Are there safe changes we block unnecessarily?

---

### Test 5: Compatible Type Widening

**Goal:** Test safe type promotions work in practice

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.metrics (
        id BIGINT,
        count INT,
        price FLOAT
    ) USING iceberg
""")
```

**Action:**
```python
# JSON with larger types
json_data = [{"id": 1, "count": 9999999999, "price": 123.456789}]  # Forces BIGINT and DOUBLE
df = spark.createDataFrame(json_data)

# Apply evolution
SchemaEvolutionService.apply_schema_evolution(
    spark=spark,
    table_identifier="test_catalog.test_db.metrics",
    comparison=comparison,
    strategy="sync_all_columns"
)
```

**Verify:**
1. count changed from INT to BIGINT
2. price changed from FLOAT to DOUBLE
3. ALTER COLUMN TYPE executed successfully

**Learn:**
- Does Iceberg support ALTER COLUMN TYPE?
- Which type widenings actually work?
- Do we need to update compatibility matrix?

---

### Test 6: Nested Struct Evolution (Discovery Test)

**Goal:** Discover how Spark/Iceberg handle nested field additions

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.events (
        id BIGINT,
        metadata STRUCT<ip: STRING, browser: STRING>
    ) USING iceberg
""")
```

**Action:**
```python
# JSON with evolved struct (new field)
json_data = [{
    "id": 1,
    "metadata": {"ip": "1.1.1.1", "browser": "Chrome", "device": "iPhone"}
}]
df = spark.createDataFrame(json_data)

# Try to apply
try:
    SchemaEvolutionService.apply_schema_evolution(
        spark=spark,
        table_identifier="test_catalog.test_db.events",
        comparison=comparison,
        strategy="append_new_columns"
    )
    print("SUCCESS: Nested field addition worked!")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")
```

**Verify:**
- Does it succeed or fail?
- If succeeds: How did Iceberg handle nested field addition?
- If fails: What's the error? Do we need special handling?

**Learn:**
- Can Iceberg ALTER nested struct fields?
- Does our current TYPE_CHANGE detection handle this?
- Do we need to implement special logic for structs?

---

### Test 7: Ignore Strategy - No DDL

**Goal:** Validate ignore strategy doesn't execute any DDL

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.flexible_table (
        id BIGINT,
        name STRING
    ) USING iceberg
""")
```

**Action:**
```python
# JSON with extra columns
json_data = [{"id": 1, "name": "Alice", "extra1": "value1", "extra2": "value2"}]
df = spark.createDataFrame(json_data)

# Apply ignore strategy
SchemaEvolutionService.apply_schema_evolution(
    spark=spark,
    table_identifier="test_catalog.test_db.flexible_table",
    comparison=comparison,
    strategy="ignore"
)
```

**Verify:**
1. No DDL executed (check Spark logs if possible)
2. Table schema unchanged
3. Strategy returns without error

**Learn:**
- Does ignore strategy work as documented?
- Should it modify the DataFrame (drop extra columns)?

---

### Test 8: Case-Insensitive Column Matching

**Goal:** Verify Iceberg's case handling matches our logic

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.case_test (
        UserId BIGINT,
        UserName STRING
    ) USING iceberg
""")
```

**Action:**
```python
# JSON with lowercase names
json_data = [{"userid": 1, "username": "Alice"}]
df = spark.createDataFrame(json_data)

# Compare and apply
comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
```

**Verify:**
1. No changes detected (case-insensitive match)
2. No DDL executed
3. comparison.has_changes == False

**Learn:**
- Does Spark infer lowercase column names from JSON?
- Does our case-insensitive matching work with real schemas?

---

### Test 9: Partitioned Table Evolution

**Goal:** Discover constraints on partitioned tables

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.partitioned_events (
        id BIGINT,
        event_date DATE,
        data STRING
    ) USING iceberg
    PARTITIONED BY (event_date)
""")
```

**Action:**
```python
# Try to add column to partitioned table
json_data = [{"id": 1, "event_date": "2024-01-15", "data": "test", "new_field": "value"}]
df = spark.createDataFrame(json_data)

SchemaEvolutionService.apply_schema_evolution(
    spark=spark,
    table_identifier="test_catalog.test_db.partitioned_events",
    comparison=comparison,
    strategy="append_new_columns"
)
```

**Verify:**
- Does it work or fail?
- Can we add columns to partitioned tables?
- Are there restrictions?

**Learn:**
- Iceberg's actual behavior with partitions
- Do we need special handling?

---

### Test 10: Empty Table Evolution

**Goal:** Verify evolution works on tables with no data

**Setup:**
```python
spark.sql("""
    CREATE TABLE test_catalog.test_db.empty_table (
        id BIGINT,
        name STRING
    ) USING iceberg
""")
# No data inserted
```

**Action:**
```python
# Apply evolution
json_data = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
df = spark.createDataFrame(json_data)

SchemaEvolutionService.apply_schema_evolution(
    spark=spark,
    table_identifier="test_catalog.test_db.empty_table",
    comparison=comparison,
    strategy="append_new_columns"
)
```

**Verify:**
1. Schema updated successfully
2. Can insert data with new schema
3. No special case errors

---

## Test Implementation Pattern

```python
import pytest
import uuid
from app.services.schema_evolution_service import SchemaEvolutionService

@pytest.mark.integration
class TestSchemaEvolutionApply:
    """Integration tests for apply_schema_evolution with real Iceberg tables."""

    def test_append_new_columns_happy_path(self, spark_session):
        """Test append_new_columns strategy with real Iceberg table."""
        # Generate unique table name
        table_name = f"test_users_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            # Create table
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    name STRING
                ) USING iceberg
            """)

            # Insert data
            spark_session.sql(f"INSERT INTO {table_id} VALUES (1, 'Alice'), (2, 'Bob')")

            # Create evolved schema
            json_data = [{"id": 3, "name": "Charlie", "email": "charlie@example.com"}]
            df = spark_session.createDataFrame(json_data)

            # Get target schema
            target_schema = spark_session.table(table_id).schema

            # Compare
            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes

            # Apply evolution
            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns"
            )

            # Verify schema updated
            updated_table = spark_session.table(table_id)
            field_names = [f.name for f in updated_table.schema.fields]
            assert "email" in field_names, "email column should be added"

            # Verify old data has NULL for new column
            old_records = updated_table.filter("id <= 2").collect()
            assert all(row["email"] is None for row in old_records), "Old records should have NULL for email"

            print(f"\n✓ Test passed: append_new_columns worked on {table_id}")

        finally:
            # Cleanup
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
```

## Running Tests

```bash
# Start Spark Connect
docker-compose -f docker-compose.test.yml up -d

# Run all integration tests
pytest tests/integration/test_schema_evolution_service.py -v -s

# Run specific test
pytest tests/integration/test_schema_evolution_service.py::TestSchemaEvolutionApply::test_append_new_columns_happy_path -v -s
```

## Expected Learnings

After running these tests, we'll know:

1. **What Actually Works:**
   - Which DDL statements Iceberg accepts
   - Which type changes are truly compatible
   - How nested structures evolve in practice

2. **What Doesn't Work:**
   - Iceberg limitations we didn't know about
   - Edge cases our logic doesn't handle
   - Scenarios that need special treatment

3. **What to Fix:**
   - Update compatibility matrix based on real results
   - Add error handling for discovered edge cases
   - Improve error messages based on actual failures

## Success Criteria

- All P0 tests (1-5) pass with real Iceberg tables
- Discovery tests (6, 9) document actual behavior
- Edge case tests (7, 8, 10) validate robustness
- Zero data corruption or loss in any scenario
- Clear error messages for all failure cases

## Next Steps

1. Implement Test 1 (happy path)
2. Run and verify DDL works
3. Implement Tests 2-5 (core scenarios)
4. Run discovery tests (6, 9) to learn constraints
5. Document findings and update service if needed
6. Add more tests based on discoveries
