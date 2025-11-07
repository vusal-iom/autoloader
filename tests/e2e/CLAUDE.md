# E2E Integration Tests

## Rules (Non-Negotiable)

1. **API-Only**: All operations through API. Never query database directly.
2. **Real Systems**: Use Docker services (MinIO, Postgres, Spark). Never mock.
3. **Discuss First**: Show proposed test structure before implementing.
4. **One Test**: Focus on ONE test at a time.

## Test Structure (Follow This Pattern)

```python
@pytest.mark.e2e
class TestFeature:
    def test_scenario(self, api_client, minio_client, test_bucket,
                      spark_session, test_cluster_id, minio_config, ...):
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: My Feature")

        # Phase 1: Setup
        table_name = generate_unique_table_name("test")  # Always unique!
        ingestion = create_standard_ingestion(
            api_client, test_cluster_id, test_bucket,
            minio_config, table_name
        )

        # Phase 2: Execute
        run_id = trigger_run(api_client, ingestion["id"])
        run = wait_for_run_completion(api_client, ingestion["id"], run_id, logger=logger)

        # Phase 3: Verify
        assert_run_metrics(run, expected_files=3, expected_records=3000, logger=logger)
        verify_table_data(
            spark_session,
            get_table_identifier(ingestion),
            expected_count=3000,
            logger=logger
        )

        print_test_summary([("Status", "SUCCESS ✅")])
```

## Critical Do's and Don'ts

### Always Do:
- ✅ Use `generate_unique_table_name()` (prevents test collisions)
- ✅ Use helpers from `helpers.py` (consistency)
- ✅ Use `E2ELogger()` for output
- ✅ Wait for completion with `wait_for_run_completion()`
- ✅ Verify data in Spark, not just API responses
- ✅ End with `print_test_summary()`

### Never Do:
- ❌ Query database directly with SQLAlchemy
- ❌ Hardcode table names (use `generate_unique_table_name()`)
- ❌ Poll manually (use `wait_for_run_completion()`)
- ❌ Skip Spark verification
- ❌ Reimplement helpers

## Key Files & Their Purpose

- `conftest.py` - Shared fixtures (DB, API, service health)
- `tests/e2e/conftest.py` - E2E fixtures (MinIO, Spark, buckets)
- `tests/e2e/helpers.py` - Test utilities (read docstrings)
- Existing test files - Your templates

## File Upload Patterns

```python
# Initial files (automatic via fixture)
def test_something(sample_json_files, ...):
    # 3 files × 1000 records already uploaded to test_bucket/data/

    # Add more files during test
    new_files = upload_json_files(
        minio_client, test_bucket,
        start_file_idx=3,  # Continue numbering
        num_files=2,
        schema_type="evolved"  # "base" or "evolved"
    )
```

## What Each Test Validates (Learn From These)

| Test | Key Validation |
|------|----------------|
| `test_basic_ingestion.py` | Happy path: API → Spark → Iceberg |
| `test_incremental_load.py` | **Files NOT reprocessed on 2nd run** |
| `test_schema_evolution.py` | **Backward/forward compatibility** |
| `test_refresh_workflows.py` | **Full/new-only/dry-run modes** |

The bold items are the CRITICAL validations that prevent regressions.

## Quick Start

```bash
# Start services (wait 1-2 min for Spark)
docker-compose -f docker-compose.test.yml up -d

# Run tests
pytest tests/e2e/test_basic_ingestion.py -v

# With verbose logging
E2E_VERBOSE=true pytest tests/e2e/ -v
```

## Common Fixtures

- `api_client` - FastAPI TestClient (function-scoped)
- `test_bucket` - Unique per test: `test-bucket-{timestamp}`
- `lakehouse_bucket` - Shared: `test-lakehouse` (session-scoped)
- `sample_json_files` - 3 files auto-uploaded (function-scoped)
- `spark_session` - For Iceberg verification (function-scoped)

Read `conftest.py` for full fixture list and docstrings.
