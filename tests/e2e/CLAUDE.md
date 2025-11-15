# E2E Integration Tests

> **See also**: [Parent Test Guidelines](../CLAUDE.md) for logging best practices and NO emoji policy.

## Rules (Non-Negotiable)

1. **API-Only**: All operations through API. Never query database directly.
2. **Real Systems**: Use Docker services (MinIO, Postgres, Spark). Never mock.
3. **Discuss First**: Show proposed test structure before implementing.
4. **One Test**: Focus on ONE test at a time.

## Test Structure

```python
from tests.e2e.helpers import (
    TestLogger, create_standard_ingestion, trigger_run,
    wait_for_run_completion, assert_run_metrics, verify_table_data,
    verify_table_content, generate_unique_table_name, get_table_identifier,
    print_test_summary
)

@pytest.mark.e2e
class TestFeature:
    def test_scenario(self, api_client, minio_client, test_bucket, spark_session, ...):
        logger = TestLogger()
        logger.section("E2E TEST: My Feature")

        # Phase 1: Setup
        table_name = generate_unique_table_name("test")
        ingestion = create_standard_ingestion(api_client, test_cluster_id, test_bucket, minio_config, table_name)

        # Phase 2: Execute
        run_id = trigger_run(api_client, ingestion["id"])
        run = wait_for_run_completion(api_client, ingestion["id"], run_id, logger=logger)

        # Phase 3: Verify
        assert_run_metrics(run, expected_files=3, expected_records=3000, logger=logger)
        verify_table_data(spark_session, get_table_identifier(ingestion), expected_count=3000, logger=logger)

        print_test_summary([("Status", "SUCCESS")])
```

## Critical Do's and Don'ts

### Always Do:
- Use `generate_unique_table_name()` (prevents test collisions)
- Import helpers from `tests.e2e.helpers` package
- Use constants from `helpers.constants` instead of magic numbers
- Wait for completion with `wait_for_run_completion()`
- Verify data in Spark, not just API responses
- End with `print_test_summary()`

### Never Do:
- Query database directly with SQLAlchemy
- Hardcode table names or magic numbers
- Poll manually (use helpers)
- Skip Spark verification
- Reimplement helpers
- Use emojis or excessive logging

## Helper Reference

All helpers available from `tests.e2e.helpers`:

> Shared schema/content helpers live in `tests.helpers.assertions`. You can import them directly from there or via `tests.e2e.helpers`.

**Constants:**
- `DEFAULT_RUN_TIMEOUT`, `PREFECT_FLOW_TIMEOUT`, `DEFAULT_POLL_INTERVAL`
- `SUCCESS_STATUSES`, `FAILED_STATUSES`, `RUNNING_STATUSES`
- `SCHEMA_TYPE_BASE`, `SCHEMA_TYPE_EVOLVED`

**Factories:**
- `generate_unique_table_name(prefix)` - Always use for table names
- `create_standard_ingestion(...)` - Standard S3 JSON ingestion
- `upload_json_files(...)` - Upload test data with specific schema
- `get_table_identifier(ingestion)` - Get full table path

**Run Helpers:**
- `trigger_run(api_client, ingestion_id)` - Trigger manual run
- `wait_for_run_completion(...)` - Poll until run completes
- `get_latest_run_id(api_client, ingestion_id)` - Get most recent run

**Prefect Helpers:**
- `wait_for_prefect_flow_completion(flow_run_id, logger=logger)`
- `verify_prefect_deployment_exists(deployment_id, ...)`
- `verify_prefect_deployment_active/paused/deleted(deployment_id, logger=logger)`

**Assertions:**
- `assert_run_metrics(run, expected_files, expected_records, logger=logger)`
- `verify_table_data(spark_session, table_id, expected_count, ...)` - Verify schema and count
- `verify_table_schema(df_or_table, expected_schema, ...)` - Compare actual vs expected schema (names + types)
- `verify_table_content(df_or_table, expected_data, ...)` - Verify complete table content including all values and NULLs
- `verify_schema_evolution(spark_session, table_id, ...)` - Verify backward/forward compatibility
- `print_test_summary([("Label", value), ...])`

**Logger:**
- `logger.section(msg)` - Test section header
- `logger.phase(msg)` - Phase description
- `logger.step(msg, always=False)` - Step detail (verbose only by default)
- `logger.success(msg, always=True)` - Success message
- `logger.timed_phase(msg)` - Context manager that auto-logs duration

## What Each Test Validates

| Test | Critical Validation |
|------|---------------------|
| `test_incremental_load.py` | Files NOT reprocessed on 2nd run |
| `test_schema_evolution.py` | Backward/forward compatibility |
| `test_prefect_workflows.py` | Prefect deployment lifecycle |

## Quick Start

```bash
# Start services (wait 1-2 min for Spark)
docker-compose -f docker-compose.test.yml up -d

# Run all e2e tests
pytest tests/e2e/ -v

# Run specific test
pytest tests/e2e/test_incremental_load.py -v

# With verbose logging (both are supported)
E2E_VERBOSE=true pytest tests/e2e/ -v
TEST_VERBOSE=true pytest tests/e2e/ -v
```

## Common Fixtures

- `api_client` - FastAPI TestClient (function-scoped)
- `test_bucket` - Unique per test: `test-bucket-{timestamp}` (function-scoped)
- `lakehouse_bucket` - Shared: `test-lakehouse` (session-scoped)
- `sample_json_files` - 3 files auto-uploaded (function-scoped)
- `spark_session` - For Iceberg verification (function-scoped)
- `minio_client` - Boto3 S3 client for MinIO
- `minio_config` - MinIO config for test client (localhost:9000)
- `minio_config_for_ingestion` - MinIO config for worker (minio:9000)

See `conftest.py` for full fixture list and docstrings.
