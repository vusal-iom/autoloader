# Schema Evolution Implementation Summary

## Overview

Successfully implemented schema evolution strategies for IOMETE Autoloader as specified in `docs/schema-evolution-strategies.md`.

## Implementation Details

### 1. Database Schema Changes

**Migration:** `alembic/versions/c933db976380_add_schema_evolution_strategy.py`

- Added `on_schema_change` column to `ingestions` table (default: 'ignore')
- Added `changes_json` and `strategy_applied` columns to `schema_versions` table

**Models Updated:**
- `app/models/domain.py`: Updated `Ingestion` and `SchemaVersion` models
- `app/models/schemas.py`: Added `SchemaEvolutionStrategy` enum

### 2. Core Services

**New Files Created:**

1. `app/services/schema_evolution_service.py` (400+ lines)
   - `SchemaEvolutionService`: Main service class
   - `SchemaComparison`: Schema comparison results
   - `SchemaChange`: Individual change representation
   - `SchemaMismatchError`: Exception for 'fail' strategy
   - `IncompatibleTypeChangeError`: Exception for incompatible type changes

2. `app/repositories/schema_version_repository.py`
   - Repository for schema version tracking
   - Methods: create_version, get_latest_version, get_all_versions

**Files Modified:**

1. `app/services/batch_file_processor.py`
   - Integrated schema evolution into `_write_to_iceberg`
   - Replaced `_evolve_schema_if_needed` with `_apply_schema_evolution`
   - Added strategy validation

2. `app/services/ingestion_service.py`
   - Added `on_schema_change` to `_build_ingestion_from_create`
   - Updated `_to_response` to include strategy in API responses

### 3. Schema Evolution Strategies

#### ignore (Default)
- Continues using original table schema
- New columns in source files are dropped
- Missing columns result in NULL values
- Works with all file formats

#### fail
- Detects schema mismatches
- Raises `SchemaMismatchError` with detailed diff
- Forces manual intervention
- Works with all file formats

#### append_new_columns
- Adds new columns from source files
- Preserves all existing columns
- Historical data has NULL for new fields
- Iceberg tables only

#### sync_all_columns
- Fully synchronizes table schema with source
- Adds new columns and removes old columns
- Updates compatible type changes
- Iceberg tables only

### 4. Testing

**Unit Tests:** `tests/unit/test_schema_evolution_service.py` (19 tests)

Test Coverage:
- Schema comparison logic (7 tests)
  - No changes detection
  - Added/removed/modified columns
  - Mixed changes
  - Case-insensitive comparison
- Type compatibility (6 tests)
  - Widening conversions (int→long, float→double)
  - String/varchar conversions
  - Compatible date→timestamp
  - Incompatible conversions
- Strategy validation (3 tests)
  - Iceberg requirement for evolution strategies
  - ignore/fail work with any format
- Error handling (2 tests)
  - SchemaMismatchError formatting
  - IncompatibleTypeChangeError attributes

**Integration Tests:** `tests/integration/test_schema_evolution_strategies.py`

Test Coverage:
- 'ignore' strategy (JSON, CSV, Parquet)
- 'fail' strategy (raises on mismatch)
- 'append_new_columns' strategy (adds columns, backward compatibility)
- 'sync_all_columns' strategy (adds/removes, type changes)
- Format-specific behaviors

**Result:** All 19 unit tests passing

### 5. API Changes

**Request Schema:**
```json
{
  "name": "my-ingestion",
  "on_schema_change": "append_new_columns",
  ...
}
```

**Response Schema:**
```json
{
  "id": "ing_123",
  "on_schema_change": "append_new_columns",
  ...
}
```

### 6. Type Compatibility Matrix

| Old Type | New Type | Compatible? |
|----------|----------|-------------|
| int      | long     | Yes         |
| float    | double   | Yes         |
| string   | varchar  | Yes         |
| date     | timestamp| Yes         |
| long     | int      | No (narrowing) |
| string   | int      | No          |

### 7. Migration Applied

```bash
$ alembic upgrade head
INFO  [alembic.runtime.migration] Running upgrade f789e2f419ac -> c933db976380, add_schema_evolution_strategy
```

## Usage Examples

### Example 1: Append New Columns (Safe for log/event data)

```python
# API Request
{
  "name": "event-logs",
  "format": {"type": "json"},
  "destination": {
    "catalog": "spark_catalog",
    "database": "default",
    "table": "events"
  },
  "on_schema_change": "append_new_columns"
}
```

**Behavior:**
- Day 1: Files have `id, timestamp, event_type`
- Day 5: Files add `user_id, metadata`
- Table automatically evolves to 5 columns
- Old records have NULL for new fields

### Example 2: Fail on Change (Strict governance)

```python
{
  "name": "financial-data",
  "on_schema_change": "fail"
}
```

**Behavior:**
- Schema change detected → SchemaMismatchError raised
- Ingestion stops, requires manual review
- Suitable for regulated data

### Example 3: Sync All Columns (Single authoritative source)

```python
{
  "name": "staging-table",
  "on_schema_change": "sync_all_columns"
}
```

**Behavior:**
- Source adds columns → added to table
- Source removes columns → removed from table
- Table always mirrors source

## Files Created/Modified Summary

**Created:**
- `app/services/schema_evolution_service.py`
- `app/repositories/schema_version_repository.py`
- `tests/unit/test_schema_evolution_service.py`
- `tests/integration/test_schema_evolution_strategies.py`
- `alembic/versions/c933db976380_add_schema_evolution_strategy.py`

**Modified:**
- `app/models/domain.py`
- `app/models/schemas.py`
- `app/services/batch_file_processor.py`
- `app/services/ingestion_service.py`

**Total Lines Added:** ~1,800 lines (including tests and docs)

## Backward Compatibility

- Default strategy is 'ignore' (preserves existing behavior)
- Existing ingestions without `on_schema_change` will use 'ignore'
- Migration adds column with default value
- No breaking changes to existing API endpoints

## Next Steps

To use the new feature:

1. Update ingestion via API with desired strategy
2. Monitor schema_versions table for change history
3. Review schema evolution logs in run history
4. Test with non-production data first

## Testing the Implementation

```bash
# Run unit tests
pytest tests/unit/test_schema_evolution_service.py -v

# Run integration tests (requires Spark)
pytest tests/integration/test_schema_evolution_strategies.py -v

# Run E2E tests (requires full stack)
pytest tests/e2e/test_schema_evolution.py -v
```

## Performance Considerations

- Schema comparison is done once per run
- Evolution DDL operations are fast (Iceberg metadata operations)
- No performance impact when strategy is 'ignore'
- Type changes may require table scan (depending on Iceberg version)

## Monitoring and Observability

Schema changes are logged at INFO level:
```
INFO: Schema changes detected: 2 added, 0 removed, 0 modified
INFO: Adding 2 new columns to spark_catalog.default.events
INFO: Added column: phone (string)
INFO: Added column: address (string)
```

Schema versions are tracked in `schema_versions` table for audit trail.
