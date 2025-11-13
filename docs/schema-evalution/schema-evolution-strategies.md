# Schema Evolution Strategies for IOMETE Autoloader

## Overview

When ingesting files from cloud storage (S3, Azure Blob, GCS) into Iceberg tables, source file schemas can change over time. This document outlines strategies for handling schema evolution automatically during batch ingestion.

**Common Schema Change Scenarios:**
- New columns added to source files
- Columns removed from source files
- Column data types changed
- Column order changed
- Nested structure modifications (for JSON/Parquet)

## Schema Evolution Strategies

### 1. `ignore` (Default - Safe & Conservative)

**Behavior:**
- Ignores any schema changes in source files
- Continues using the original table schema
- New columns in source files are silently dropped
- Missing columns in source files result in NULL values in target table

**Use Cases:**
- Stable, well-defined schemas
- Strict data contracts with upstream systems
- When schema changes should be explicitly reviewed
- Testing and preview mode

**Advantages:**
- Predictable behavior
- No unexpected schema changes
- Works with all file formats

**Disadvantages:**
- May silently lose data from new columns
- Requires manual intervention for schema updates

**Example:**
```
Original Table Schema: id, name, email
New File Schema:       id, name, email, phone, address

Result: phone and address columns are ignored, only id/name/email loaded
```

---

### 2. `append_new_columns` (Additive Evolution)

**Behavior:**
- Automatically adds new columns found in source files to target table
- Preserves all existing columns in target table (even if removed from source)
- New columns will have NULL values for previously ingested rows
- Column order may differ between source and target
- **Iceberg tables only** (leverages Iceberg schema evolution)

**Use Cases:**
- Log/event data that accumulates fields over time
- Semi-structured data with evolving attributes
- Append-only analytics where historical schema matters
- API response ingestion with backward compatibility

**Advantages:**
- Never loses new data
- Maintains historical column presence
- Safe for append-only workloads
- No data loss from schema expansion

**Disadvantages:**
- Table schema can grow indefinitely
- Removed columns remain (with NULLs) - potential confusion
- Doesn't handle data type changes

**Example:**
```
Original Table:  id, name, email
File Batch 1:    id, name, email, phone        → Table: id, name, email, phone
File Batch 2:    id, name, address             → Table: id, name, email, phone, address
                                                  (email has NULLs for batch 2)
```

---

### 3. `sync_all_columns` (Full Synchronization)

**Behavior:**
- Fully synchronizes target table schema with source file schema
- Adds new columns from source
- Removes columns no longer in source
- Updates column data types when compatible
- **Iceberg tables only** (uses Iceberg schema evolution APIs)

**Use Cases:**
- Single authoritative source with evolving schema
- Data warehouse staging tables
- Regular full refreshes expected
- When source schema is the source of truth

**Advantages:**
- Target schema always matches current source
- Handles both additions and removals
- Clean schema without legacy columns
- Can adapt to type changes (with constraints)

**Disadvantages:**
- Can lose historical columns
- Requires careful monitoring
- Type changes may fail if incompatible
- More aggressive - higher risk

**Example:**
```
Original Table:  id, name, email, phone
New File Schema: id, name, address, country

Result: Table schema becomes: id, name, address, country
        (email and phone columns are dropped)
```

**Type Change Handling:**
```
Compatible:     int → long, float → double, string → varchar
Incompatible:   string → int, date → timestamp (may require explicit handling)
```

---

### 4. `fail` (Strict Validation)

**Behavior:**
- Detects schema mismatches between source and target
- Fails the ingestion run with detailed error message
- Requires manual intervention to proceed
- No automatic changes to table schema

**Use Cases:**
- Production tables with strict governance
- Regulated data with schema contracts
- When schema changes require approval workflow
- Critical business tables

**Advantages:**
- Explicit control over schema changes
- Prevents unexpected modifications
- Forces review and decision-making
- Audit trail of schema evolution

**Disadvantages:**
- Requires manual intervention
- Can block automated pipelines
- May cause ingestion delays

**Error Message Example:**
```
Schema Mismatch Detected!

Source Schema:
  - id (long)
  - name (string)
  - email (string)
  - phone (string)      [NEW]

Target Schema:
  - id (long)
  - name (string)
  - email (string)

Action Required:
1. Update on_schema_change setting to 'append_new_columns' or 'sync_all_columns'
2. Perform manual schema migration
3. Trigger full refresh with --force-schema-update
```

---

## Configuration

### API Schema Definition

```python
# In schemas.py
class SchemaEvolutionStrategy(str, Enum):
    IGNORE = "ignore"
    APPEND_NEW_COLUMNS = "append_new_columns"
    SYNC_ALL_COLUMNS = "sync_all_columns"
    FAIL = "fail"

class IngestionCreate(BaseModel):
    # ... existing fields ...
    on_schema_change: SchemaEvolutionStrategy = SchemaEvolutionStrategy.IGNORE
```

### Database Schema

```python
# In domain.py - Ingestion model
class Ingestion(Base):
    # ... existing columns ...
    on_schema_change: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="ignore"
    )
```

### Usage Examples

**REST API:**
```json
{
  "name": "aws-logs-ingestion",
  "source_type": "s3",
  "format_type": "json",
  "on_schema_change": "append_new_columns",
  "source_config": { ... },
  "destination_config": { ... }
}
```

**UI Configuration:**
```
Schema Evolution Strategy: [dropdown]
  ○ Ignore changes (recommended for stable schemas)
  ○ Append new columns automatically (requires Iceberg)
  ○ Sync all columns with source (requires Iceberg)
  ○ Fail on schema mismatch (strict validation)
```

---

## Implementation Considerations

### Schema Detection Flow

```
1. Discover new files from source
2. Infer schema from sample files
3. Compare with current table schema
4. Detect differences (new columns, removed columns, type changes)
5. Apply strategy:
   - ignore: proceed with existing schema
   - append_new_columns: ALTER TABLE ADD COLUMN ...
   - sync_all_columns: ALTER TABLE ADD/DROP/ALTER COLUMN ...
   - fail: raise exception with detailed diff
6. Record schema version in schema_versions table
7. Proceed with ingestion
```

### Schema Comparison Logic

```python
class SchemaChange:
    added_columns: List[str]
    removed_columns: List[str]
    modified_columns: List[Tuple[str, str, str]]  # (name, old_type, new_type)

def compare_schemas(source_schema: StructType, target_schema: StructType) -> SchemaChange:
    """
    Compare Spark schemas and return detected changes
    """
    # Implementation details...
```

### Iceberg Schema Evolution APIs

```python
# Using Spark SQL for Iceberg tables
def add_columns(table_name: str, columns: List[Tuple[str, str]]):
    for col_name, col_type in columns:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")

def drop_columns(table_name: str, columns: List[str]):
    for col_name in columns:
        spark.sql(f"ALTER TABLE {table_name} DROP COLUMN {col_name}")

def alter_column_type(table_name: str, col_name: str, new_type: str):
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {new_type}")
```

### Version Tracking

Every schema change should be tracked in the `schema_versions` table:

```python
class SchemaVersion(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    ingestion_id: Mapped[int] = mapped_column(ForeignKey("ingestions.id"))
    version_number: Mapped[int]
    schema_json: Mapped[str]  # Full schema as JSON
    changes_json: Mapped[str]  # SchemaChange object as JSON
    strategy_applied: Mapped[str]  # Which strategy was used
    created_at: Mapped[datetime]
```

---

## Testing Strategy

### Unit Tests

```python
def test_ignore_strategy_drops_new_columns():
    """Verify new columns are ignored"""
    pass

def test_append_strategy_adds_new_columns():
    """Verify new columns are added to table"""
    pass

def test_append_strategy_keeps_removed_columns():
    """Verify removed columns remain in table"""
    pass

def test_sync_strategy_full_sync():
    """Verify full schema synchronization"""
    pass

def test_fail_strategy_raises_exception():
    """Verify exception raised on mismatch"""
    pass

def test_type_change_handling():
    """Verify compatible and incompatible type changes"""
    pass
```

### Integration Tests

```python
def test_schema_evolution_end_to_end():
    """
    1. Create ingestion with initial schema
    2. Ingest first batch of files
    3. Modify source file schema
    4. Ingest second batch
    5. Verify table schema matches expected strategy
    """
    pass
```

### Test Data

Create test files with evolving schemas:
```
test_data/
  schema_v1/
    file1.json   # id, name, email
    file2.json   # id, name, email
  schema_v2/
    file3.json   # id, name, email, phone
    file4.json   # id, name, email, phone
  schema_v3/
    file5.json   # id, name, address, country
```

---

## Migration Path

### Phase 1: Foundation
- Add `on_schema_change` column to ingestions table
- Create `SchemaEvolutionStrategy` enum
- Implement schema comparison logic
- Add schema version tracking

### Phase 2: Basic Strategies
- Implement `ignore` (default, current behavior)
- Implement `fail` (validation only)
- Add API support and validation

### Phase 3: Iceberg Evolution
- Implement `append_new_columns` for Iceberg
- Implement `sync_all_columns` for Iceberg
- Add format validation (Iceberg only)

### Phase 4: UI & Monitoring
- Add UI configuration options
- Schema change notifications
- Schema version history UI
- Alerts for schema mismatches

---

## Error Handling

### Incompatible Type Changes

```python
class IncompatibleTypeChangeError(Exception):
    def __init__(self, column: str, old_type: str, new_type: str):
        self.message = f"Cannot change column '{column}' from {old_type} to {new_type}"
        super().__init__(self.message)
```

### Non-Iceberg Format Restrictions

```python
def validate_strategy_compatibility(format_type: FormatType, strategy: SchemaEvolutionStrategy):
    """
    Validate that strategy is compatible with table format
    """
    if strategy in [SchemaEvolutionStrategy.APPEND_NEW_COLUMNS,
                    SchemaEvolutionStrategy.SYNC_ALL_COLUMNS]:
        if format_type != FormatType.ICEBERG:
            raise ValueError(
                f"Strategy '{strategy}' requires Iceberg table format. "
                f"Current format: {format_type}"
            )
```

---

## Best Practices

### Recommendation Matrix

| Use Case | Recommended Strategy | Why |
|----------|---------------------|-----|
| Production fact tables | `fail` | Requires governance and review |
| Event/log streaming | `append_new_columns` | Accumulates fields over time |
| Staging from single source | `sync_all_columns` | Source is authoritative |
| Development/testing | `ignore` | Predictable behavior |
| Highly regulated data | `fail` | Explicit approval needed |
| API response ingestion | `append_new_columns` | APIs evolve additively |

### Monitoring Recommendations

1. **Alert on schema changes** (when using append/sync)
2. **Track schema version metrics** (how often changes occur)
3. **Monitor failed runs** (when using fail strategy)
4. **Review schema change history** regularly
5. **Set up approval workflows** for production tables

### Documentation Requirements

When schema changes occur, record:
- Date and time of change
- Files that triggered the change
- Columns added/removed/modified
- Strategy that was applied
- Run ID associated with the change

---

## API Examples

### Get Schema Version History

```
GET /api/v1/ingestions/{ingestion_id}/schema-versions

Response:
[
  {
    "version_number": 1,
    "schema": {...},
    "created_at": "2025-01-01T00:00:00Z"
  },
  {
    "version_number": 2,
    "schema": {...},
    "changes": {
      "added_columns": ["phone"],
      "removed_columns": [],
      "modified_columns": []
    },
    "strategy_applied": "append_new_columns",
    "created_at": "2025-01-15T12:00:00Z"
  }
]
```

### Preview Schema Changes (Before Activation)

```
POST /api/v1/ingestions/{ingestion_id}/preview-schema-changes

Response:
{
  "current_schema": {...},
  "detected_source_schema": {...},
  "changes": {
    "added_columns": ["phone", "address"],
    "removed_columns": ["deprecated_field"],
    "modified_columns": []
  },
  "strategy": "append_new_columns",
  "action_preview": "Will add 2 new columns: phone, address"
}
```

---

## Future Enhancements

### Potential Advanced Features

1. **Schema Evolution Rules**
   - Allow/deny lists for specific columns
   - Column naming conventions enforcement
   - Data type restrictions

2. **Custom Transformation on Schema Change**
   - Default values for new columns
   - Derivation rules for computed columns
   - Data type conversion rules

3. **Schema Change Notifications**
   - Email/Slack notifications
   - Webhook triggers
   - Integration with data catalog

4. **Schema Validation Rules**
   - Required columns enforcement
   - Column type constraints
   - Nullable/Not-null validation

5. **Rollback Capability**
   - Revert to previous schema version
   - Rollback with data consistency checks

6. **Partial Column Sync**
   - Allowlist columns to always sync
   - Denylist columns to never add
   - Column prefix/suffix rules

---

## References

- [DBT Incremental Models Schema Changes](https://iomete.com/resources/integrations/dbt/dbt-incremental-models-by-examples#schema-changes)
- [Apache Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)
- [Spark Structured Streaming Schema Evolution](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovery-semantics-after-changes-in-a-streaming-query)

---

## Decision Log

| Decision | Rationale |
|----------|-----------|
| Default to `ignore` | Safest option, backward compatible, no surprises |
| Iceberg-only for evolution | Leverages Iceberg's native schema evolution capabilities |
| Track schema versions | Auditability, debugging, rollback capability |
| Fail strategy for validation | Explicit control for critical tables |
| Support all 4 strategies | Covers wide range of use cases from strict to permissive |
