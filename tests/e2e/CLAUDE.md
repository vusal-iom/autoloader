# E2E Integration Tests - Guidelines

## Core Principles

### 1. API-Only Testing
**CRITICAL:** All operations and verifications MUST go through the API.
- ✅ Create ingestions via API, verify via API
- ❌ Never query database directly
- Exception: Setting up test files in MinIO is OK

### 2. Real Systems
Tests use **real services** from `docker-compose.test.yml`:
- MinIO (localhost:9000) - S3-compatible storage
- PostgreSQL (localhost:5432) - Metadata database
- Spark Connect (localhost:15002) - Data processing

Start services: `docker-compose -f docker-compose.test.yml up -d`

### 3. Collaborative Development
When writing new tests:
1. Discuss approach with user FIRST
2. Show proposed structure
3. Wait for confirmation
4. Never make changes directly without agreement

### 4. One Test at a Time
Focus on ONE test case per session. Avoid batch modifications.

---

## Test Infrastructure

### Key Files
- `conftest.py` - Shared fixtures (DB, API client, service health checks)
- `tests/e2e/conftest.py` - E2E-specific fixtures (MinIO, Spark, buckets)
- `tests/e2e/helpers.py` - Reusable test utilities

### Important Fixtures

**Session-scoped** (created once):
- `minio_client` - Boto3 S3 client for MinIO
- `lakehouse_bucket` - Persistent bucket for Iceberg warehouse (`test-lakehouse`)
- `ensure_services_ready` - Health check for all Docker services

**Function-scoped** (fresh per test):
- `api_client` - FastAPI TestClient with DB override (standard tests)
- `test_bucket` - Unique bucket per test (`test-bucket-{timestamp}`)
- `sample_json_files` - 3 JSON files with 1000 records each (auto-uploaded)
- `spark_session` - Spark Connect session for verification

### Helper Functions (helpers.py)

**Creating tests:**
- `create_standard_ingestion()` - Standard S3→Iceberg ingestion config
- `trigger_run()` - Start ingestion run, returns run_id
- `wait_for_run_completion()` - Poll until success/failure (timeout: 180s)

**Verification:**
- `assert_run_metrics()` - Verify files/records processed
- `verify_table_data()` - Check Iceberg table via Spark (count, schema, duplicates)
- `verify_schema_evolution()` - Check backward/forward compatibility

**Utilities:**
- `E2ELogger()` - Structured output (use `E2E_VERBOSE=true` for details)
- `generate_unique_table_name()` - Timestamped table names
- `upload_json_files()` - Upload test data with base or evolved schema
- `print_test_summary()` - Format test results

---

## Test Patterns

### Standard Test Structure

```python
@pytest.mark.e2e
class TestMyFeature:
    def test_my_scenario(
        self, api_client, minio_client, minio_config, test_bucket,
        spark_session, test_cluster_id, ...
    ):
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: My Feature")

        # Phase 1: Setup
        table_name = generate_unique_table_name("my_test")
        ingestion = create_standard_ingestion(...)

        # Phase 2: Execute
        run_id = trigger_run(api_client, ingestion["id"])
        run = wait_for_run_completion(..., timeout=180, logger=logger)

        # Phase 3: Verify
        assert_run_metrics(run, expected_files=3, expected_records=3000)
        verify_table_data(spark_session, table_identifier, expected_count=3000)

        # Summary
        print_test_summary([("Status", "SUCCESS ✅")])
```

### Bucket Management

- **Lakehouse bucket** (`test-lakehouse`) - Shared, stores Iceberg data, cleaned at session end
- **Test bucket** (`test-bucket-{timestamp}`) - Per-test, stores source files, auto-cleaned

### File Upload Patterns

```python
# Initial files (automatic via fixture)
def test_example(sample_json_files):  # Already uploaded to test_bucket/data/

# Additional files during test
new_files = upload_json_files(
    minio_client, test_bucket,
    start_file_idx=3,  # Continue numbering
    num_files=2,
    schema_type="evolved"  # or "base"
)
```

---

## Existing Tests (tests/e2e/)

### test_basic_ingestion.py
Happy path: Create ingestion → Run → Verify 3 files/3000 records in Iceberg table

### test_incremental_load.py
**Key validation:** Second run processes ONLY new files (not all files)
- Run 1: 3 files, 3000 records
- Run 2: 2 NEW files, 2000 records (total 5000, NOT 6000)
- Critical: No duplicates, file state tracking works

### test_schema_evolution.py
**Key validation:** Schema evolution + backward/forward compatibility
- Run 1: 3 files, 5 fields (base schema)
- Run 2: 2 files, 7 fields (evolved schema: +region, +metadata)
- Old records have NULL for new fields
- New records have all 7 fields

### test_refresh_workflows.py
Three workflows: Full refresh, new-only refresh, dry-run mode
- Full: Drop table + clear history + reprocess all
- New-only: Drop table + keep history + process new only
- Dry-run: Preview operations without executing

---

## Best Practices

1. **Use helpers** - Don't reinvent `create_standard_ingestion()`, `wait_for_run_completion()`, etc.
2. **Use E2ELogger** - Structured output with phase/step/success methods
3. **Unique table names** - Always use `generate_unique_table_name()`
4. **Meaningful assertions** - Add context: `assert x == y, f"Expected {y}, got {x}. This means..."`
5. **One scenario per test** - Don't mix unrelated workflows
6. **Clear success criteria** - Document in test docstring
7. **Test summaries** - End with `print_test_summary()`

---

## Common Issues

**Services not ready:**
```bash
docker-compose -f docker-compose.test.yml up -d
docker-compose -f docker-compose.test.yml ps  # Check health
docker-compose -f docker-compose.test.yml logs -f spark-connect
```

**Spark takes 1-2 min to start** - Wait for health check, verify http://localhost:4040

**Port conflicts:** Check with `lsof -i :9000` (MinIO), `:5432` (Postgres), `:15002` (Spark)

**Full cleanup:** `docker-compose -f docker-compose.test.yml down -v`

---

## Running Tests

```bash
# All e2e tests
pytest tests/e2e/ -v -m e2e

# Specific test
pytest tests/e2e/test_basic_ingestion.py -v

# With verbose logging
E2E_VERBOSE=true pytest tests/e2e/test_basic_ingestion.py -v

# Single test method
pytest tests/e2e/test_basic_ingestion.py::TestBasicIngestion::test_basic_ingestion -v
```

---

## Key Takeaways

- ✅ API-only interactions (no direct DB access)
- ✅ Real Docker services (no mocking)
- ✅ Use existing helpers and fixtures
- ✅ One test at a time, discuss before implementing
- ✅ Clear phases: Setup → Execute → Verify → Summary
