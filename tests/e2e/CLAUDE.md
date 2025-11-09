# E2E Integration Tests

## Rules (Non-Negotiable)

1. **API-Only**: All operations through API. Never query database directly.
2. **Real Systems**: Use Docker services (MinIO, Postgres, Spark). Never mock.
3. **Discuss First**: Show proposed test structure before implementing.
4. **One Test**: Focus on ONE test at a time.

## Test Structure (Follow This Pattern)

```python
from tests.e2e.helpers import (
    E2ELogger,
    create_standard_ingestion,
    trigger_run,
    wait_for_run_completion,
    assert_run_metrics,
    verify_table_data,
    generate_unique_table_name,
    get_table_identifier,
    print_test_summary,
)

@pytest.mark.e2e
class TestFeature:
    def test_scenario(self, api_client, minio_client, test_bucket,
                      spark_session, test_cluster_id, minio_config, ...):
        logger = E2ELogger()
        logger.section("E2E TEST: My Feature")

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

        print_test_summary([("Status", "SUCCESS")])
```

## Critical Do's and Don'ts

### Always Do:
- Use `generate_unique_table_name()` (prevents test collisions)
- Import helpers from `tests.e2e.helpers` package
- Use `E2ELogger()` for output (minimal logging only)
- Wait for completion with `wait_for_run_completion()`
- Verify data in Spark, not just API responses
- End with `print_test_summary()`
- Use constants from `helpers.constants` instead of magic numbers
- Keep logging clean and minimal (avoid excessive output)
- No emojis in log messages (keep professional)

### Never Do:
- Query database directly with SQLAlchemy
- Hardcode table names (use `generate_unique_table_name()`)
- Poll manually (use `wait_for_run_completion()`)
- Skip Spark verification
- Reimplement helpers
- Use magic numbers (import constants instead)
- Log every minor step (only log important phases)
- Use emojis in logger output

## Logging and Comments

**Keep it clean, professional, and minimal.**

- **No emojis** - anywhere in code, comments, logs, or test output
- **Simple comments** - single-line phase markers, no decorative separators (`===`, `---`)
- **Concise messages** - no trailing punctuation (`...`, `!!!`), no SHOUTING CASE
- **Log phases, not steps** - let helpers and assertions do the detailed logging
- **Professional tone** - factual, informative, suitable for CI/CD logs

```python
# Good example
logger.section("E2E TEST: Incremental Load")
logger.phase("Phase 1: Setup")
logger.phase("Phase 2: Execute")
logger.success("Run completed in 5s")
print_test_summary([("Status", "SUCCESS")])
```

This keeps test output readable in terminals, CI/CD systems, and log aggregators.

## Directory Structure

```
tests/e2e/
├── helpers/                          # Helper package (organized by responsibility)
│   ├── __init__.py                  # Package exports (import from here)
│   ├── constants.py                 # Timeouts, status values, defaults
│   ├── logger.py                    # E2ELogger class
│   ├── factories.py                 # Ingestion creation, data generation
│   ├── run_helpers.py               # Run execution and monitoring
│   ├── assertions.py                # Verification functions
│   └── prefect_helpers.py           # Prefect deployment operations
│
├── conftest.py                       # E2E fixtures (MinIO, Spark, buckets)
├── CLAUDE.md                         # This file (test guidelines)
├── README.md                         # Detailed documentation
│
├── test_incremental_load.py         # Test files
├── test_schema_evolution.py
└── test_prefect_workflows.py
```

## Helper Modules Guide

### Constants (`helpers/constants.py`)
Use named constants instead of magic numbers:
```python
from tests.e2e.helpers import DEFAULT_RUN_TIMEOUT, SCHEMA_TYPE_BASE, SCHEMA_TYPE_EVOLVED

# Don't: timeout = 180
# Do: timeout = DEFAULT_RUN_TIMEOUT
```

Available constants:
- `DEFAULT_RUN_TIMEOUT`, `PREFECT_FLOW_TIMEOUT` - Timeouts in seconds
- `DEFAULT_POLL_INTERVAL`, `PREFECT_POLL_INTERVAL` - Poll intervals
- `SUCCESS_STATUSES`, `FAILED_STATUSES` - Status value lists
- `SCHEMA_TYPE_BASE`, `SCHEMA_TYPE_EVOLVED` - Schema types for test data

### Logger (`helpers/logger.py`)
**Keep logging minimal and clean - no emojis, only essential information.**

```python
from tests.e2e.helpers import E2ELogger

logger = E2ELogger()  # Auto-reads E2E_VERBOSE env var

# Clean, professional logging
logger.section("E2E TEST: Incremental Load")
logger.phase("Phase 1: Setup")
logger.step("Created ingestion", always=True)  # Important milestones only
logger.success("Test completed", always=True)

# Use timed phases for performance tracking
with logger.timed_phase("Phase 2: Processing"):
    # work happens
    pass
# Logs: "Completed in 2.3s"
```

**Logging Best Practices:**
- Only log important phases and milestones
- Use `always=True` sparingly (only for critical info)
- No emojis - keep output professional
- Use `logger.step()` for verbose-only details
- Prefer assertions over logging for verification

### Factories (`helpers/factories.py`)
```python
from tests.e2e.helpers import (
    create_standard_ingestion,
    generate_unique_table_name,
    upload_json_files,
    get_table_identifier,
    SCHEMA_TYPE_BASE,
    SCHEMA_TYPE_EVOLVED,
)

# Generate unique table name
table_name = generate_unique_table_name("my_test")

# Create ingestion
ingestion = create_standard_ingestion(
    api_client, cluster_id, test_bucket, minio_config, table_name
)

# Get table identifier
table_id = get_table_identifier(ingestion)  # "catalog.database.table"

# Upload test files
files = upload_json_files(
    minio_client, test_bucket,
    start_file_idx=0,
    num_files=3,
    schema_type=SCHEMA_TYPE_BASE,  # or SCHEMA_TYPE_EVOLVED
    logger=logger
)
```

### Run Helpers (`helpers/run_helpers.py`)
```python
from tests.e2e.helpers import (
    trigger_run,
    wait_for_run_completion,
    get_run,
    get_latest_run_id,
)

# Trigger run
run_id = trigger_run(api_client, ingestion_id)

# Wait for completion
run = wait_for_run_completion(
    api_client, ingestion_id, run_id,
    timeout=180,  # or use DEFAULT_RUN_TIMEOUT
    logger=logger
)

# Get latest run ID (useful after Prefect flow completion)
run_id = get_latest_run_id(api_client, ingestion_id)
# Or exclude a specific run
run2_id = get_latest_run_id(api_client, ingestion_id, exclude_run_id=run1_id)
```

### Assertions (`helpers/assertions.py`)
```python
from tests.e2e.helpers import (
    assert_run_metrics,
    verify_table_data,
    verify_schema_evolution,
    print_test_summary,
)

# Assert run metrics
assert_run_metrics(
    run,
    expected_files=3,
    expected_records=3000,
    expected_errors=0,
    logger=logger
)

# Verify table data
verify_table_data(
    spark_session, table_id,
    expected_count=3000,
    expected_fields=["id", "timestamp", "user_id"],
    check_duplicates=True,
    logger=logger
)

# Verify schema evolution
verify_schema_evolution(
    spark_session, table_id,
    old_record_filter="id < 3000",
    new_record_filter="id >= 3000",
    old_count=3000,
    new_count=2000,
    new_fields=["region", "metadata"],
    logger=logger
)

# Print summary at end (no emojis)
print_test_summary([
    ("Test", "My Test"),
    ("Files", 3),
    ("Records", 3000),
    ("Status", "SUCCESS")
])
```

### Prefect Helpers (`helpers/prefect_helpers.py`)
```python
from tests.e2e.helpers import (
    wait_for_prefect_flow_completion,
    verify_prefect_deployment_exists,
    verify_prefect_deployment_active,
    verify_prefect_deployment_paused,
    verify_prefect_deployment_deleted,
)

# Wait for Prefect flow to complete
await wait_for_prefect_flow_completion(flow_run_id, logger=logger)

# Verify deployment exists
deployment = await verify_prefect_deployment_exists(
    deployment_id,
    expected_name=f"ingestion-{ingestion_id}",
    expected_tags=["autoloader"],
    logger=logger
)

# Verify deployment state
await verify_prefect_deployment_active(deployment_id, logger=logger)
await verify_prefect_deployment_paused(deployment_id, logger=logger)
await verify_prefect_deployment_deleted(deployment_id, logger=logger)
```

## File Upload Patterns

```python
from tests.e2e.helpers import upload_json_files, SCHEMA_TYPE_BASE, SCHEMA_TYPE_EVOLVED

# Initial files (automatic via fixture)
def test_something(sample_json_files, ...):
    # 3 files × 1000 records already uploaded to test_bucket/data/

    # Add more files with base schema
    new_files = upload_json_files(
        minio_client, test_bucket,
        start_file_idx=3,  # Continue numbering
        num_files=2,
        schema_type=SCHEMA_TYPE_BASE,
        logger=logger
    )

    # Add files with evolved schema (2 additional fields)
    evolved_files = upload_json_files(
        minio_client, test_bucket,
        start_file_idx=5,
        num_files=2,
        schema_type=SCHEMA_TYPE_EVOLVED,
        logger=logger
    )
```

## What Each Test Validates (Learn From These)

| Test | Key Validation |
|------|----------------|
| `test_incremental_load.py` | **Files NOT reprocessed on 2nd run** |
| `test_schema_evolution.py` | **Backward/forward compatibility** |
| `test_prefect_workflows.py` | **Prefect deployment lifecycle** |

The bold items are the CRITICAL validations that prevent regressions.

## Quick Start

```bash
# Start services (wait 1-2 min for Spark)
docker-compose -f docker-compose.test.yml up -d

# Run all e2e tests
pytest tests/e2e/ -v

# Run specific test
pytest tests/e2e/test_incremental_load.py -v

# With verbose logging
E2E_VERBOSE=true pytest tests/e2e/ -v
```

## Common Fixtures

- `api_client` - FastAPI TestClient (function-scoped)
- `test_bucket` - Unique per test: `test-bucket-{timestamp}`
- `lakehouse_bucket` - Shared: `test-lakehouse` (session-scoped)
- `sample_json_files` - 3 files auto-uploaded (function-scoped)
- `spark_session` - For Iceberg verification (function-scoped)
- `minio_client` - Boto3 S3 client for MinIO
- `minio_config` - MinIO config for test client (localhost:9000)
- `minio_config_for_ingestion` - MinIO config for worker (minio:9000)

Read `conftest.py` for full fixture list and docstrings.

## Best Practices

### 1. Minimal, Clean Logging
**Keep test output clean and professional. Only log what matters.**

```python
# BAD - Excessive logging with emojis
logger.section("🧪 E2E TEST: My Feature")
logger.step("✅ Created bucket", always=True)
logger.step("✅ Uploaded file 1", always=True)
logger.step("✅ Uploaded file 2", always=True)
logger.step("✅ Uploaded file 3", always=True)
logger.step("🚀 Triggering run", always=True)
logger.step("⏱️ Waiting...", always=True)
logger.success("🎉 All done!", always=True)

# GOOD - Minimal, clean logging
logger.section("E2E TEST: Incremental Load")
logger.phase("Phase 1: Setup")
# Setup happens silently unless E2E_VERBOSE=true
logger.phase("Phase 2: Execute")
run = wait_for_run_completion(...)  # Helper logs completion time
logger.phase("Phase 3: Verify")
# Assertions fail loudly if something is wrong
print_test_summary([("Status", "SUCCESS")])
```

**Guidelines:**
- Log phases, not individual steps
- No emojis (❌ 🧪 ✅ 🚀 ⏱️ 🎉 etc.)
- Let assertions speak for themselves
- Use `logger.step()` without `always=True` for details
- Only set `always=True` for critical milestones

### 2. Use Constants
```python
# ❌ Bad
timeout = 180
poll_interval = 2

# ✅ Good
from tests.e2e.helpers import DEFAULT_RUN_TIMEOUT, DEFAULT_POLL_INTERVAL
timeout = DEFAULT_RUN_TIMEOUT
poll_interval = DEFAULT_POLL_INTERVAL
```

### 3. DRY - Don't Repeat Yourself
```python
# BAD - Duplicating Prefect flow waiting logic
async with get_client() as client:
    while time.time() - start < timeout:
        flow_run = await client.read_flow_run(flow_run_id)
        # ... 40 lines of polling code

# GOOD - Use helper
await wait_for_prefect_flow_completion(flow_run_id, logger=logger)
```

### 4. Use Timed Phases
```python
# BAD - Manual timing
logger.phase("Phase 1: Loading data")
start = time.time()
# ... work ...
elapsed = time.time() - start
logger.step(f"Completed in {elapsed}s")

# GOOD - Automatic timing
with logger.timed_phase("Phase 1: Loading data"):
    # ... work ...
# Logs: "Completed in 2.3s"
```

### 5. Clear Test Structure
```python
# Always follow: Setup → Execute → Verify → Summary
def test_my_feature(...):
    logger.section("E2E TEST: My Feature")  # No emojis

    # Setup (minimal logging)
    logger.phase("Phase 1: Setup")
    table_name = generate_unique_table_name("test")
    ingestion = create_standard_ingestion(...)

    # Execute (helpers log important events)
    logger.phase("Phase 2: Execute")
    run_id = trigger_run(...)
    run = wait_for_run_completion(...)  # Logs completion time

    # Verify (assertions speak for themselves)
    logger.phase("Phase 3: Verify")
    assert_run_metrics(run, expected_files=3, expected_records=3000)
    verify_table_data(...)

    # Summary (cleanup happens in fixtures)
    print_test_summary([("Status", "SUCCESS")])
```

## Troubleshooting

### Import Errors
If you see import errors, ensure you're importing from the package:
```python
# CORRECT
from tests.e2e.helpers import E2ELogger, create_standard_ingestion

# WRONG
from tests.e2e.helpers.helpers import E2ELogger  # No such module
```

### Service Not Ready
If tests fail with connection errors:
```bash
# Check services are running
docker-compose -f docker-compose.test.yml ps

# Check logs
docker-compose -f docker-compose.test.yml logs spark
docker-compose -f docker-compose.test.yml logs prefect-worker

# Restart services
docker-compose -f docker-compose.test.yml restart
```

## Further Reading

For detailed documentation on the helper modules, see [README.md](README.md).
