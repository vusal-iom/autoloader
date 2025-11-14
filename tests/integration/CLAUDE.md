# Integration Tests

## Requirements

Integration tests **MUST** use real Spark Connect (no mocks, no local mode).

**Prerequisites:**
```bash
# Start Spark Connect service
docker-compose -f docker-compose.test.yml up -d

# Wait 1-2 minutes for Spark to fully start
# Verify Spark UI is accessible at http://localhost:4040
```

## Configuration

- **Spark Connect**: `sc://localhost:15002` (from Docker)
- **MinIO**: `http://localhost:9000` (for Iceberg warehouse)
- **Catalog**: `test_catalog.test_db` (same as e2e tests)
- **Database**: SQLite (local, no PostgreSQL needed)
- **Service Check**: Tests will fail-fast if Spark Connect or MinIO are not available

## Running Tests

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run specific test
pytest tests/integration/test_schema_version_tracking.py -v
```

## Test Structure

Integration tests verify business logic with real Spark operations:
- Schema evolution detection and tracking
- Iceberg table operations
- File processing logic
- Repository and service layer integration

Unlike e2e tests, integration tests:
- Use SQLite instead of PostgreSQL
- Don't require MinIO or Prefect
- Focus on component integration, not full system workflows

## Best Practices

### Schema Field Verification

Always use set comparison for complete schema verification.

**Do this:**
```python
field_names = {f.name for f in updated_table.schema.fields}
assert field_names == {"id", "name", "email", "created_at"}
```

**Not this:**
```python
field_names = [f.name for f in updated_table.schema.fields]
assert "id" in field_names
assert "name" in field_names
assert "email" in field_names
assert "created_at" in field_names
```

**Why:** Set comparison verifies exact field match (no extra fields, no missing fields), provides better error messages showing actual vs expected, and uses a single assertion instead of multiple.

### Table Content Verification

Always test full table content using the `verify_table_content` helper. This makes it easy to see and understand exactly what you expect from the table.

**Do this:**
```python
verify_table_content(
    df_or_table=table_id,
    expected_data=[
        {"id": 1, "name": "Alice", "email": None, "created_at": None},
        {"id": 2, "name": "Bob", "email": None, "created_at": None},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com", "created_at": "2024-01-15T10:30:00"},
    ],
    spark_session=spark_session,
    logger=logger
)
```

**Not this:**
```python
assert df.count() == 3
# Missing: verification of actual values, NULL handling, column values, etc.
```

**Why:**
- Comprehensive verification of all values including NULLs
- Excellent detailed diff output on mismatch (shows missing rows, extra rows, value differences)
- Handles row and column ordering automatically
- Accepts both DataFrames and table identifiers
- Self-documenting: shows complete expected state in one place