# Schema Evolution Strategies - Test Review Documentation

## Overview
This test suite validates the four schema evolution strategies for handling schema changes during data ingestion:
1. **ignore** - Drops new columns, maintains original schema
2. **fail** - Raises errors on schema mismatches
3. **append_new_columns** - Adds new columns, preserves existing ones
4. **sync_all_columns** - Full sync (adds/removes columns, handles type changes)

Tests cover JSON, CSV, and Parquet formats using Spark Connect with Iceberg tables.

---

## Test Data Helpers

### `create_json_data(schema_type, num_batches, id_offset)`
**Purpose:** Generate JSON test records with different schemas

**Schemas:**
- `base`: 3 columns → `{id, name, email}`
- `evolved`: 5 columns → `{id, name, email, phone, address}`

**Parameters:**
- `num_batches`: Number of batches (default: 2)
- `id_offset`: ID starting point for unique records (default: 0)

**Output:** List of dictionaries, 10 records per batch

---

### `create_csv_data(schema_type, num_batches, id_offset)`
**Purpose:** Generate CSV test records (same schemas as JSON)

**Output:** List of dictionaries suitable for CSV format

---

### `create_parquet_data(schema_type, num_batches, id_offset)`
**Purpose:** Generate Parquet test records (same schemas as JSON)

**Output:** List of tuples for createDataFrame with explicit schema

---

## Test Classes

### 1. TestIgnoreStrategy

#### `test_ignore_strategy_drops_new_columns`
**File:** `test_schema_evolution_strategies.py:139`

**What it tests:** The 'ignore' strategy correctly drops new columns from incoming data

**Test flow:**
1. **Phase 1: Initial load**
   - Create Iceberg table with base schema (3 columns: id, name, email)
   - Load 20 records (2 batches × 10 records)
   - Verify table has exactly 3 columns

2. **Phase 2: Evolved schema load**
   - Create evolved data with 5 columns (adds phone, address)
   - Manually filter to keep only original 3 columns (simulating 'ignore' behavior)
   - Append filtered data to table

3. **Validation:**
   - Schema unchanged: still 3 columns
   - Row count increased: 40 records (20 original + 20 new)
   - New columns (phone, address) not present in table

**Formats tested:** JSON, CSV, Parquet (via `@pytest.mark.parametrize`)

**Key assertion:** `set(final_table.columns) == {"id", "name", "email"}`

---

### 2. TestFailStrategy

#### `test_fail_strategy_raises_on_new_columns`
**File:** `test_schema_evolution_strategies.py:198`

**What it tests:** The 'fail' strategy raises `SchemaMismatchError` when schemas differ

**Test flow:**
1. Define source schema with 5 columns (id, name, email, phone, address)
2. Define target schema with 3 columns (id, name, email)
3. Compare schemas using `SchemaEvolutionService.compare_schemas()`
4. Apply 'fail' strategy

**Validation:**
- Raises `SchemaMismatchError` exception
- Error message contains "Schema Mismatch Detected"
- Error message lists new columns: "phone" and "address"

**Purpose:** Ensures fail strategy prevents ingestion when schemas don't match

---

### 3. TestAppendNewColumnsStrategy

#### `test_append_new_columns_json`
**File:** `test_schema_evolution_strategies.py:239`

**What it tests:** The 'append_new_columns' strategy adds new columns while preserving old data

**Test flow:**
1. **Phase 1: Initial table**
   - Create table with base schema (3 columns)
   - Load 20 records with IDs 0-199

2. **Phase 2: Schema evolution**
   - Detect schema changes (2 new columns: phone, address)
   - Apply 'append_new_columns' strategy using `SchemaEvolutionService.apply_schema_evolution()`
   - Verify table schema expanded to 5 columns

3. **Phase 3: Load evolved data**
   - Load 20 new records with IDs 1000-1199 (different IDs to avoid duplicates)
   - Use `merge-schema: true` option for Iceberg append

4. **Validation:**
   - Total records: 40
   - Old records (IDs < 1000): phone and address are NULL (20 records)
   - New records (IDs >= 1000): phone and address have values (20 records)
   - Schema has all 5 columns

**Key concept:** Backward compatibility - old records get NULL for new columns

---

### 4. TestSyncAllColumnsStrategy

#### `test_sync_all_columns_adds_and_removes`
**File:** `test_schema_evolution_strategies.py:307`

**What it tests:** The 'sync_all_columns' strategy both adds AND removes columns

**Test flow:**
1. **Phase 1: Initial table**
   - Create table with 4 columns: `{id, name, email, deprecated_field}`
   - Load 1 record

2. **Phase 2: Complete schema change**
   - New schema has 4 columns: `{id, name, phone, address}`
   - Removed: email, deprecated_field
   - Added: phone, address

3. **Apply sync_all_columns strategy**
   - Detect 2 added columns
   - Detect 2 removed columns
   - Apply evolution

4. **Validation:**
   - Final schema exactly matches new schema: `{id, name, phone, address}`
   - Old columns (email, deprecated_field) no longer exist

**Purpose:** Validates full schema synchronization (not just additive)

---

#### `test_sync_compatible_type_change`
**File:** `test_schema_evolution_strategies.py:350`

**What it tests:** Compatible type changes are allowed (int → long)

**Test flow:**
1. Create table with `id` as IntegerType
2. New schema has `id` as LongType (compatible widening)
3. Apply 'sync_all_columns' strategy

**Validation:**
- Schema evolution detects type change (1 modified column)
- Evolution succeeds without errors
- Type change is applied

**Compatible changes:** Widening conversions (int → long, float → double)

---

#### `test_sync_incompatible_type_change_raises_error`
**File:** `test_schema_evolution_strategies.py:393`

**What it tests:** Incompatible type changes raise `IncompatibleTypeChangeError`

**Test flow:**
1. Create table with `id` as StringType
2. New schema has `id` as IntegerType (incompatible)
3. Apply 'sync_all_columns' strategy

**Validation:**
- Raises `IncompatibleTypeChangeError` exception
- Evolution is blocked

**Incompatible changes:** String → Int, Int → String, etc.

---

### 5. TestFormatSpecificBehavior

#### `test_csv_with_header`
**File:** `test_schema_evolution_strategies.py:428`

**What it tests:** CSV format handling with createDataFrame

**Test flow:**
1. Create CSV data using helper function
2. Create DataFrame from CSV data
3. Write to Iceberg table
4. Verify schema and row count

**Validation:**
- 3 columns created correctly
- 10 records loaded
- CSV-specific parsing works

**Purpose:** Ensures CSV format works with schema evolution framework

---

#### `test_json_multiline`
**File:** `test_schema_evolution_strategies.py:446`

**What it tests:** JSON format handling with createDataFrame

**Test flow:**
1. Create JSON data using helper function
2. Create DataFrame from JSON data
3. Write to Iceberg table
4. Verify schema and row count

**Validation:**
- 3 columns created correctly
- 10 records loaded
- JSON-specific parsing works

**Purpose:** Ensures JSON format works with schema evolution framework

---

## Test Coverage Summary

| Strategy | Test Count | Formats | Key Behaviors Tested |
|----------|-----------|---------|---------------------|
| **ignore** | 1 | JSON, CSV, Parquet | Drops new columns, maintains original schema |
| **fail** | 1 | Schema comparison | Raises error on mismatch |
| **append_new_columns** | 1 | JSON | Adds columns, preserves old data with NULLs |
| **sync_all_columns** | 3 | Schema comparison | Add/remove columns, compatible/incompatible type changes |
| **Format-specific** | 2 | CSV, JSON | Format parsing validation |

---

## Key Testing Patterns

### Pattern 1: Two-Phase Testing
Most tests follow a two-phase approach:
1. **Phase 1:** Create table with initial schema
2. **Phase 2:** Introduce schema changes and validate behavior

### Pattern 2: Schema Validation
Each test verifies:
- Column names: `set(df.columns) == expected_set`
- Column count: `len(df.columns) == expected_count`
- Row counts: `df.count() == expected_count`

### Pattern 3: NULL Handling
Tests verify backward compatibility:
- Old records get NULL for new columns
- New records have values for all columns

### Pattern 4: Cleanup
All tests include cleanup: `spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")`

---

## Testing Dependencies

**Services tested:**
- `SchemaEvolutionService.compare_schemas()` - Detects schema differences
- `SchemaEvolutionService.apply_schema_evolution()` - Applies evolution strategy

**Infrastructure:**
- Spark Connect (real connection, not mocked)
- Iceberg tables (writeTo/using "iceberg")
- Test catalog: `test_catalog.default.*`

**Fixtures:**
- `spark_session` - Provided by conftest.py (Spark Connect)

---

## Observations for Review

### Strengths
1. **Comprehensive coverage** of all 4 evolution strategies
2. **Multi-format testing** via parametrization (JSON, CSV, Parquet)
3. **Clear test phases** with comments
4. **Backward compatibility** validation (NULL handling)
5. **Error case testing** (fail strategy, incompatible types)

### Potential Improvements
1. **Missing column removal test** for 'append_new_columns' strategy (should it handle column removal?)
2. **No nested schema tests** (struct types, arrays, maps)
3. **Limited type change coverage** (only int→long and string→int tested)
4. **No concurrent schema evolution tests**
5. **Parquet format underutilized** (only used in 'ignore' strategy test)

### Edge Cases Not Covered
- Schema evolution with NULL values in source data
- Multiple simultaneous schema changes (add + remove + type change)
- Schema evolution with partitioned tables
- Schema evolution with table constraints/primary keys
- Very large schema changes (100+ columns)

---

## Quick Test Reference

```bash
# Run all schema evolution tests
pytest tests/integration/test_schema_evolution_strategies.py -v

# Run specific strategy
pytest tests/integration/test_schema_evolution_strategies.py::TestIgnoreStrategy -v
pytest tests/integration/test_schema_evolution_strategies.py::TestFailStrategy -v
pytest tests/integration/test_schema_evolution_strategies.py::TestAppendNewColumnsStrategy -v
pytest tests/integration/test_schema_evolution_strategies.py::TestSyncAllColumnsStrategy -v

# Run specific test
pytest tests/integration/test_schema_evolution_strategies.py::TestSyncAllColumnsStrategy::test_sync_incompatible_type_change_raises_error -v
```

---

## Related Files
- `app/services/schema_evolution_service.py` - Service implementation
- `tests/integration/conftest.py` - Spark Connect fixture
- `tests/integration/test_schema_version_tracking.py` - Schema version tracking tests
