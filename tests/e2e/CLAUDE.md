# E2E Integration Tests - CLAUDE.md

## Overview

End-to-end integration tests validate the complete IOMETE Autoloader workflow using **real infrastructure components** running in Docker containers. These tests ensure the entire system works together correctly, from API requests through database persistence to Spark data processing.

## Core Testing Philosophy

### 1. API-Only Interactions

**CRITICAL RULE: All test operations and verifications MUST go through the API.**

- ✅ **DO:** Create ingestions via `POST /api/v1/ingestions`
- ✅ **DO:** Trigger runs via `POST /api/v1/ingestions/{id}/run`
- ✅ **DO:** Verify run status via `GET /api/v1/ingestions/{id}/runs/{run_id}`
- ❌ **DON'T:** Query the database directly using SQLAlchemy
- ❌ **DON'T:** Manipulate database state outside of the API

**Exception:** Test data setup in MinIO is acceptable before running tests (uploading JSON files).

### 2. Real System Integration

**All tests use REAL services from `docker-compose.test.yml`:**

- **MinIO** (localhost:9000) - S3-compatible object storage
- **PostgreSQL** (localhost:5432) - Database for ingestion metadata and run history
- **Spark Connect** (localhost:15002) - Remote Spark execution for data processing

This provides production-like testing without mocking.

### 3. Single Test Focus

- Work on **ONE test case at a time**
- Avoid batch modifications across multiple tests
- Makes code review easier and changes more traceable

### 4. Collaborative Development

**When writing new tests:**
1. **First**, discuss the test approach and design with the user
2. **Show** proposed test structure and assertions
3. **Wait** for confirmation before implementing
4. **Never** make changes directly without agreement

### 5. Test Execution

Use **pytest** with appropriate markers:

```bash
# Run all e2e tests
pytest tests/e2e/ -v -m e2e

# Run specific test file
pytest tests/e2e/test_basic_ingestion.py -v

# Run with verbose logging
E2E_VERBOSE=true pytest tests/e2e/test_basic_ingestion.py -v

# Run single test method
pytest tests/e2e/test_basic_ingestion.py::TestBasicIngestion::test_basic_ingestion -v
```

---

## Infrastructure Setup

### Docker Compose Services

**File:** `docker-compose.test.yml`

#### MinIO Configuration
```yaml
Service: minio
Image: minio/minio:latest
Ports: 9000 (API), 9001 (Console)
Credentials: minioadmin / minioadmin
Health Check: /minio/health/live
Volume: minio_test_data
```

#### PostgreSQL Configuration
```yaml
Service: postgres
Image: postgres:15
Port: 5432
Database: autoloader_test
User: test_user / test_password
Health Check: pg_isready
Volume: postgres_test_data
```

#### Spark Connect Configuration
```yaml
Service: spark-connect
Image: apache/spark:3.5.0
Port: 15002 (Spark Connect), 4040 (UI)
Packages:
  - iceberg-spark-runtime-3.5_2.12:1.4.3
  - hadoop-aws:3.3.4
  - spark-connect_2.12:3.5.0
AWS Config: minioadmin credentials for S3/MinIO
Health Check: Spark UI at port 4040
Volumes: warehouse, checkpoints, ivy_cache
```

### Starting Test Services

```bash
# Start all services
docker-compose -f docker-compose.test.yml up -d

# Check service health
docker-compose -f docker-compose.test.yml ps

# View logs
docker-compose -f docker-compose.test.yml logs -f spark-connect
docker-compose -f docker-compose.test.yml logs -f minio
docker-compose -f docker-compose.test.yml logs -f postgres

# Stop services
docker-compose -f docker-compose.test.yml down

# Clean up volumes (DESTRUCTIVE)
docker-compose -f docker-compose.test.yml down -v
```

---

## Fixture Architecture

### Session-Scoped Fixtures (Once Per Test Session)

#### `ensure_services_ready` (tests/conftest.py:103-160)
- **Purpose:** Health check for all Docker services before tests run
- **Checks:** MinIO, PostgreSQL, Spark UI availability
- **Timeout:** 30s for MinIO/Postgres, 60s for Spark
- **Raises:** RuntimeError if services not ready
- **Usage:** Auto-injected as dependency for e2e fixtures

#### `test_engine` (tests/conftest.py:40-51)
- **Purpose:** SQLAlchemy engine for test database
- **Creates:** All tables via `Base.metadata.create_all()`
- **Cleanup:** Drops all tables after session
- **Database:** PostgreSQL (configurable via `DATABASE_URL` env var)

#### `minio_config` (tests/e2e/conftest.py:21-28)
- **Purpose:** MinIO connection configuration
- **Returns:** Dict with endpoint_url, credentials, region
- **Environment Variables:**
  - `TEST_MINIO_ENDPOINT` (default: http://localhost:9000)
  - `TEST_MINIO_ACCESS_KEY` (default: minioadmin)
  - `TEST_MINIO_SECRET_KEY` (default: minioadmin)

#### `minio_client` (tests/e2e/conftest.py:32-52)
- **Purpose:** Boto3 S3 client for MinIO
- **Verifies:** Connection via `list_buckets()` call
- **Dependencies:** `minio_config`, `ensure_services_ready`
- **Raises:** RuntimeError if connection fails

#### `lakehouse_bucket` (tests/e2e/conftest.py:56-103)
- **Purpose:** Persistent bucket for Iceberg warehouse data
- **Bucket Name:** `test-lakehouse`
- **Lifecycle:**
  - **Setup:** Creates bucket (idempotent)
  - **Teardown:** Deletes all objects (paginated), then bucket
- **Cleanup:** Batched deletion (1000 objects/request, S3 limit)
- **Used By:** Spark to store Iceberg table data

#### `spark_connect_url` (tests/e2e/conftest.py:206-208)
- **Purpose:** Spark Connect URL
- **Default:** `sc://localhost:15002`
- **Environment Variable:** `TEST_SPARK_CONNECT_URL`

### Function-Scoped Fixtures (Fresh Per Test)

#### `test_db` (tests/conftest.py:55-74)
- **Purpose:** Database session with automatic rollback
- **Isolation:** Each test gets clean state
- **Cleanup:** Rolls back all changes after test

#### `api_client` (tests/conftest.py:78-99)
- **Purpose:** FastAPI TestClient with DB override
- **Override:** Injects `test_db` session via dependency injection
- **Cleanup:** Clears dependency overrides after test
- **Use Case:** Standard unit/integration tests with DB rollback

#### `e2e_api_client_no_override` (tests/e2e/conftest.py:252-270)
- **Purpose:** FastAPI TestClient WITHOUT DB override
- **Critical Difference:** Uses real database commits
- **Use Case:** Prefect e2e tests where background tasks create own DB sessions
- **Why:** Both API and background tasks need to see same committed data

#### `test_bucket` (tests/e2e/conftest.py:107-144)
- **Purpose:** Unique S3 bucket per test
- **Naming:** `test-bucket-{timestamp}` (YYYYMMDDHHmmss)
- **Lifecycle:**
  - **Setup:** Creates fresh bucket
  - **Teardown:** Deletes all objects, then bucket
- **Isolation:** Each test gets clean storage namespace

#### `sample_json_files` (tests/e2e/conftest.py:148-202)
- **Purpose:** Generate and upload test JSON files
- **Default:** 3 files × 1000 records = 3000 total records
- **Schema:** id, timestamp, user_id, event_type, value
- **Format:** Newline-delimited JSON (NDJSON)
- **Path:** `s3://{test_bucket}/data/batch_{idx}.json`
- **Configurable:**
  - `TEST_DATA_NUM_FILES` (default: 3)
  - `TEST_DATA_RECORDS_PER_FILE` (default: 1000)
- **Returns:** List of file metadata dicts (path, size, records)

#### `spark_session` (tests/e2e/conftest.py:212-236)
- **Purpose:** Spark Connect session for data verification
- **Connection:** Remote Spark via Spark Connect protocol
- **App Name:** `E2E-Test-Verification`
- **Cleanup:** Stops session after test
- **Raises:** RuntimeError if Spark not available

#### `test_tenant_id` (tests/e2e/conftest.py:240-242)
- **Purpose:** Test tenant identifier
- **Default:** `test-tenant-001`
- **Environment Variable:** `TEST_TENANT_ID`

#### `test_cluster_id` (tests/e2e/conftest.py:246-248)
- **Purpose:** Test cluster identifier
- **Default:** `test-cluster-001`
- **Environment Variable:** `TEST_CLUSTER_ID`

---

## Helper Functions (tests/e2e/helpers.py)

### Logging Utilities

#### `E2ELogger` (helpers.py:28-70)
- **Purpose:** Structured test output with verbosity control
- **Environment Variable:** `E2E_VERBOSE=true` for detailed logs
- **Methods:**
  - `section(title)` - Major test section header (always shown)
  - `phase(title)` - Phase within test (always shown)
  - `step(msg, always=False)` - Detailed step (verbose or always)
  - `metric(key, value)` - Key-value output (verbose only)
  - `success(msg, always=True)` - Success message
  - `error(msg)` - Error message (always shown)

**Example Usage:**
```python
logger = E2ELogger()
logger.section("🧪 E2E TEST: Basic Ingestion")
logger.phase("📝 Creating ingestion...")
logger.step("Configuring S3 source...")
logger.success("Ingestion created")
logger.metric("Files Processed", 3)
```

### Ingestion Helpers

#### `create_standard_ingestion()` (helpers.py:76-166)
- **Purpose:** Create standard S3 JSON ingestion configuration
- **Parameters:**
  - `api_client` - FastAPI test client
  - `cluster_id` - Target cluster
  - `test_bucket` - S3 bucket name
  - `minio_config` - MinIO credentials
  - `table_name` - Destination Iceberg table
  - `name` - Ingestion name (default: "E2E Test Ingestion")
  - `evolution_enabled` - Schema evolution flag (default: True)
  - `**overrides` - Override any payload fields
- **Returns:** Created ingestion response dict
- **Default Configuration:**
  - Source: S3 with `data/*.json` pattern
  - Format: JSON with auto schema inference
  - Destination: Iceberg append mode, no partitioning
  - Schedule: Manual trigger (no cron)
  - Quality: No thresholds or alerts
- **Assertions:** 201 status code, fails if creation unsuccessful

### Run Execution Helpers

#### `trigger_run()` (helpers.py:173-193)
- **Purpose:** Trigger manual ingestion run
- **Parameters:**
  - `api_client` - FastAPI test client
  - `ingestion_id` - Ingestion UUID
- **Returns:** Run ID (UUID string)
- **Assertions:**
  - 202 ACCEPTED status
  - Run ID not null
  - Status is "accepted" or "running"

#### `wait_for_run_completion()` (helpers.py:196-250)
- **Purpose:** Poll for run completion with timeout
- **Parameters:**
  - `api_client` - FastAPI test client
  - `ingestion_id` - Ingestion UUID
  - `run_id` - Run UUID
  - `timeout` - Max wait seconds (default: 180)
  - `poll_interval` - Poll frequency seconds (default: 2)
  - `logger` - Optional E2ELogger for progress
- **Returns:** Final run data dict
- **Behavior:**
  - Polls run status every 2 seconds
  - Shows elapsed time if verbose logging enabled
  - Returns immediately on success/completed
  - Fails test immediately on failed status
  - Fails test if timeout exceeded
- **Raises:** `pytest.fail()` on failure or timeout

#### `get_run()` (helpers.py:253-267)
- **Purpose:** Get run details (one-time fetch)
- **Parameters:**
  - `api_client` - FastAPI test client
  - `ingestion_id` - Ingestion UUID
  - `run_id` - Run UUID
- **Returns:** Run data dict
- **Assertions:** 200 status code

### Verification Helpers

#### `assert_run_metrics()` (helpers.py:274-316)
- **Purpose:** Assert run metrics match expectations
- **Parameters:**
  - `run` - Run data dict
  - `expected_files` - Expected files processed (None to skip)
  - `expected_records` - Expected records ingested (None to skip)
  - `expected_errors` - Expected error count (default: 0)
  - `logger` - Optional E2ELogger for output
- **Assertions:**
  - Status is "success" or "completed"
  - `started_at` and `ended_at` not null
  - Files processed matches expected
  - Records ingested matches expected
  - Error count matches expected
- **Logs:** Status, files, records, bytes, duration (if logger provided)

#### `verify_table_data()` (helpers.py:319-378)
- **Purpose:** Verify Iceberg table data via Spark
- **Parameters:**
  - `spark_session` - PySpark SparkSession
  - `table_identifier` - Full table name (catalog.database.table)
  - `expected_count` - Expected record count (None to skip)
  - `expected_fields` - Fields that MUST exist (None to skip)
  - `unexpected_fields` - Fields that MUST NOT exist (None to skip)
  - `check_duplicates` - Check for duplicate IDs (default: True)
  - `logger` - Optional E2ELogger for output
- **Returns:** DataFrame for further verification
- **Assertions:**
  - Record count matches expected
  - All expected fields present in schema
  - No unexpected fields in schema
  - Distinct ID count equals record count (no duplicates)
- **Logs:** Record count, schema fields, distinct IDs

#### `verify_schema_evolution()` (helpers.py:381-434)
- **Purpose:** Verify backward and forward compatibility after schema evolution
- **Parameters:**
  - `spark_session` - PySpark SparkSession
  - `table_identifier` - Full table name
  - `old_record_filter` - SQL filter for old records (e.g., "id < 3000")
  - `new_record_filter` - SQL filter for new records (e.g., "id >= 3000")
  - `old_count` - Expected count of old records
  - `new_count` - Expected count of new records
  - `new_fields` - List of fields added during evolution
  - `logger` - Optional E2ELogger
- **Assertions:**
  - **Backward Compatibility:** Old records have NULL for new fields
  - **Forward Compatibility:** New records have values for new fields
- **Use Case:** Schema evolution tests (e.g., adding "region" and "metadata" fields)

### Test Data Helpers

#### `generate_unique_table_name()` (helpers.py:441-444)
- **Purpose:** Generate unique table name with timestamp
- **Parameters:** `prefix` - Table name prefix (default: "e2e_test")
- **Returns:** `{prefix}_{YYYYMMDDHHmmss}`
- **Example:** `e2e_test_20250107123456`

#### `upload_json_files()` (helpers.py:447-525)
- **Purpose:** Upload JSON files to MinIO with specified schema
- **Parameters:**
  - `minio_client` - Boto3 S3 client
  - `test_bucket` - Target bucket name
  - `start_file_idx` - Starting file index for numbering
  - `num_files` - Number of files to create
  - `records_per_file` - Records per file (default: 1000)
  - `schema_type` - "base" (5 fields) or "evolved" (7 fields)
  - `logger` - Optional E2ELogger
- **Returns:** List of file metadata dicts (path, key, size, records)
- **Base Schema (5 fields):**
  - `id` (int) - Unique record ID
  - `timestamp` (string) - ISO 8601 timestamp
  - `user_id` (string) - User identifier
  - `event_type` (string) - Event category
  - `value` (float) - Numeric value
- **Evolved Schema (7 fields):**
  - All base fields PLUS:
  - `region` (string) - AWS region
  - `metadata` (dict) - Browser, version, device
- **File Path:** `data/batch_{idx}.json`
- **Format:** Newline-delimited JSON (NDJSON)

#### `get_table_identifier()` (helpers.py:528-539)
- **Purpose:** Extract full table identifier from ingestion config
- **Parameters:** `ingestion` - Ingestion data dict
- **Returns:** `{catalog}.{database}.{table}`
- **Example:** `test_catalog.test_db.e2e_test_20250107123456`

#### `print_test_summary()` (helpers.py:542-567)
- **Purpose:** Print formatted test summary
- **Parameters:**
  - `details` - List of (label, value) tuples
  - `footer_message` - Optional footer message
- **Example:**
```python
print_test_summary([
    ("Ingestion ID", ingestion_id),
    ("Run ID", run_id),
    ("Files Processed", 3),
    ("Records Ingested", 3000),
    ("Status", "SUCCESS ✅")
], footer_message="🎉 INCREMENTAL LOAD VALIDATED!")
```

---

## Test Organization Patterns

### Test Class Structure

All e2e tests follow this pattern:

```python
@pytest.mark.e2e
@pytest.mark.requires_spark  # Optional marker
@pytest.mark.requires_minio  # Optional marker
class TestFeatureName:
    """E2E test for feature description"""

    def test_scenario_name(
        self,
        api_client: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        spark_session: SparkSession,
        test_tenant_id: str,
        test_cluster_id: str,
        spark_connect_url: str
    ):
        """
        Test description with success criteria.

        Steps:
        1. Setup phase
        2. Execution phase
        3. Verification phase
        """
```

### Phase-Based Test Flow

Tests are organized into logical phases:

```python
logger = E2ELogger()
logger.section("🧪 E2E TEST: Feature Name")

# Phase 1: Setup
logger.phase("📝 Phase 1: Setup")
# ... setup code ...
logger.success("Setup complete")

# Phase 2: Initial Action
logger.phase("🚀 Phase 2: Initial Action")
# ... action code ...
logger.success("Action complete")

# Phase 3: Verification
logger.phase("📊 Phase 3: Verification")
# ... verification code ...
logger.success("Verification passed")

# Summary
logger.section("✅ E2E TEST PASSED")
print_test_summary([...])
```

### Assertion Patterns

**Use helper functions for common assertions:**

```python
# Run metrics verification
assert_run_metrics(
    run=run,
    expected_files=3,
    expected_records=3000,
    logger=logger
)

# Table data verification
verify_table_data(
    spark_session=spark_session,
    table_identifier=table_identifier,
    expected_count=3000,
    expected_fields=["id", "timestamp", "user_id"],
    check_duplicates=True,
    logger=logger
)
```

**Direct assertions for business logic:**

```python
# Critical assertions with explanatory messages
assert total_count == 5000, \
    f"Expected 5000 total records (3000 + 2000), got {total_count}. " \
    f"If you got 6000, files were re-processed (FAILED)."

assert distinct_count == total_count, \
    f"Found duplicate IDs: {total_count - distinct_count} duplicates. " \
    f"This means data was ingested twice (FAILED)."
```

---

## Existing Test Cases

### 1. Basic Ingestion (test_basic_ingestion.py)

**File:** `tests/e2e/test_basic_ingestion.py`

**Purpose:** Validate happy path for S3 JSON ingestion

**Workflow:**
1. Create ingestion configuration for S3 JSON files
2. Trigger manual run
3. Poll until completion (max 3 minutes)
4. Verify data in Iceberg table via Spark
5. Verify run history

**Success Criteria:**
- Ingestion created in "draft" status
- Run completes with "success" status
- 3 files processed, 3000 records ingested
- Table has 3000 records with correct schema (5 fields)
- No duplicate IDs
- Run appears in history

**Key Validations:**
- API responses (201, 202, 200)
- Run metrics accuracy
- Iceberg table data integrity
- Schema correctness

---

### 2. Incremental Load (test_incremental_load.py)

**File:** `tests/e2e/test_incremental_load.py`

**Purpose:** Validate incremental file processing across multiple runs

**Workflow:**
1. Upload 3 JSON files, trigger first run
2. Verify first run: 3 files, 3000 records
3. Upload 2 additional JSON files
4. Trigger second run
5. Verify second run: **ONLY 2 NEW files processed** (not 5)
6. Verify total: 5000 records (not 6000)
7. Verify no duplicate data

**Success Criteria:**
- First run: 3 files, 3000 records
- Second run: **2 files, 2000 records** (incremental)
- Total: 5 files, 5000 records
- No duplicate IDs (distinct count = record count)
- File state tracking prevents re-processing

**Critical Validations:**
- **Core Value Prop:** Files not re-processed on subsequent runs
- Checkpoint management works correctly
- Metrics accurate across multiple runs
- No data duplication

**Why This Test Matters:**
- Validates the primary feature: automatic file state tracking
- Ensures cost efficiency (don't reprocess already-loaded files)
- Tests the foundation for scheduled ingestion

---

### 3. Schema Evolution (test_schema_evolution.py)

**File:** `tests/e2e/test_schema_evolution.py`

**Purpose:** Validate automatic schema evolution in Iceberg tables

**Workflow:**
1. Create ingestion with evolution enabled
2. Upload 3 files with **base schema** (5 fields)
3. Trigger first run, verify 5-field schema
4. Upload 2 files with **evolved schema** (7 fields: +region, +metadata)
5. Trigger second run
6. Verify schema evolved to 7 fields
7. Verify backward compatibility (old records have NULL for new fields)
8. Verify forward compatibility (new records have all fields)

**Success Criteria:**
- First run: 3 files, 3000 records, 5 columns
- Second run: 2 files, 2000 records, schema evolved to 7 columns
- **Backward Compatibility:** Old 3000 records have NULL for region/metadata
- **Forward Compatibility:** New 2000 records have values for all 7 fields
- All 5000 records queryable
- No data loss during schema change

**Key Validations:**
- Schema detection on new files
- Iceberg schema evolution (ADD COLUMN operations)
- Existing data preserved with NULLs
- New data contains evolved fields
- Incremental processing still works with schema changes

**Why This Test Matters:**
- Real-world data sources evolve over time
- Ensures no manual intervention needed for schema changes
- Validates Iceberg's schema evolution capabilities
- Tests that evolution + incremental loading work together

---

### 4. Refresh Workflows (test_refresh_workflows.py)

**File:** `tests/e2e/test_refresh_workflows.py`

**Purpose:** Validate table refresh operations (full, new-only, dry-run)

#### Test 4a: Full Refresh

**Workflow:**
1. Create ingestion with 3 files (3000 records)
2. Run initial ingestion
3. Execute **full refresh** (drop table + clear history + auto-run)
4. Verify operations: table_dropped, processed_files_cleared, run_triggered
5. Verify all 3 files reprocessed
6. Verify table recreated with same 3000 records

**Success Criteria:**
- Initial run: 3 files, 3000 records
- Refresh operations: 3 operations executed
- Refresh run: 3 files, 3000 records (reprocessed)
- Impact estimation: 3 files to process, 0 skipped
- File history cleared and rebuilt

**Use Case:** Table corruption, schema incompatibility, full reprocessing needed

---

#### Test 4b: New-Only Refresh

**Workflow:**
1. Create ingestion with 3 initial files (3000 records)
2. Run initial ingestion
3. Add 2 new files (2000 records)
4. Execute **new-only refresh** (drop table + keep history + auto-run)
5. Verify operations: table_dropped, run_triggered (NO files_cleared)
6. Verify only 2 new files processed
7. Verify table has 2000 records (new data only)

**Success Criteria:**
- Initial run: 3 files, 3000 records
- Refresh operations: 2 operations (no processed_files_cleared)
- Refresh run: 2 new files, 2000 records
- Impact estimation: 2 files to process, 3 skipped
- File history preserved (5 files total in history)
- Table has only new data (IDs 3000-4999)

**Use Case:** Table corruption but want to avoid reprocessing old files

---

#### Test 4c: Dry Run Mode

**Workflow:**
1. Create ingestion with 3 files (3000 records)
2. Run initial ingestion
3. Execute **full refresh in DRY RUN mode**
4. Verify response status is "dry_run"
5. Verify "would_perform" operations listed
6. Verify impact estimates provided
7. Verify NO actual changes made (table, files, runs)

**Success Criteria:**
- Response status: "dry_run"
- `would_perform` field lists 3 operations
- Impact estimates: 3 files, cost calculation
- `next_steps` provided (how to proceed/cancel)
- **No changes made:**
  - Table still exists with 3000 records
  - File history unchanged
  - No new run created
- Original data preserved (IDs 0-2999)

**Use Case:** Preview refresh impact before execution, cost estimation

---

## Bucket and File Management Patterns

### Bucket Lifecycle

**Lakehouse Bucket (Session-Scoped):**
- Created once at session start
- Shared across all tests
- Stores Iceberg table data (warehouse)
- Cleaned up at session end (all objects deleted)
- Name: `test-lakehouse`

**Test Bucket (Function-Scoped):**
- Created fresh for each test
- Unique name: `test-bucket-{timestamp}`
- Stores source data files (JSON files)
- Cleaned up after each test
- Isolation: No cross-test contamination

### File Upload Patterns

**Initial Files (via `sample_json_files` fixture):**
```python
# Automatically creates 3 files with 1000 records each
def test_example(sample_json_files):
    # sample_json_files already uploaded to test_bucket/data/
    assert len(sample_json_files) == 3
```

**Additional Files (via `upload_json_files` helper):**
```python
# Upload more files during test
new_files = upload_json_files(
    minio_client=minio_client,
    test_bucket=test_bucket,
    start_file_idx=3,  # Continue numbering from 3
    num_files=2,       # Upload 2 more files
    records_per_file=1000,
    schema_type="evolved",  # Use evolved schema
    logger=logger
)
```

### Cleanup Guarantees

**Automatic Cleanup (via fixtures):**
- Test buckets deleted after each test
- Lakehouse bucket cleaned at session end
- Spark sessions stopped after each test
- Database sessions rolled back (if using `test_db`)

**Manual Cleanup (if needed):**
```bash
# Full cleanup including volumes
docker-compose -f docker-compose.test.yml down -v

# Restart fresh
docker-compose -f docker-compose.test.yml up -d
```

---

## Environment Variables

### Test Configuration

```bash
# Database
DATABASE_URL=postgresql://test_user:test_password@localhost:5432/autoloader_test

# MinIO
TEST_MINIO_ENDPOINT=http://localhost:9000
TEST_MINIO_ACCESS_KEY=minioadmin
TEST_MINIO_SECRET_KEY=minioadmin

# Spark
TEST_SPARK_CONNECT_URL=sc://localhost:15002

# Test Identity
TEST_TENANT_ID=test-tenant-001
TEST_CLUSTER_ID=test-cluster-001

# Test Data
TEST_DATA_NUM_FILES=3
TEST_DATA_RECORDS_PER_FILE=1000

# Logging
E2E_VERBOSE=true  # Enable detailed test logging
```

### Default Values

All environment variables have sensible defaults matching `docker-compose.test.yml` configuration. Tests work out-of-the-box without setting any environment variables.

---

## Common Troubleshooting

### Services Not Ready

**Error:** `RuntimeError: MinIO not ready`

**Solution:**
```bash
# Start services
docker-compose -f docker-compose.test.yml up -d

# Wait for health checks
docker-compose -f docker-compose.test.yml ps

# Check logs
docker-compose -f docker-compose.test.yml logs -f
```

### Spark Takes Long to Start

**Symptom:** Tests timeout waiting for Spark

**Solution:**
- Spark UI health check has 60s start period
- Wait 1-2 minutes after `docker-compose up -d`
- Check Spark logs: `docker-compose logs spark-connect`
- Verify Spark UI accessible: http://localhost:4040

### Port Conflicts

**Error:** `port is already allocated`

**Solution:**
```bash
# Find conflicting process
lsof -i :9000  # MinIO
lsof -i :5432  # PostgreSQL
lsof -i :15002 # Spark Connect

# Stop conflicting service or change port in docker-compose.test.yml
```

### Database Migration Issues

**Error:** `relation does not exist`

**Solution:**
```bash
# Recreate database schema
docker-compose -f docker-compose.test.yml down -v
docker-compose -f docker-compose.test.yml up -d

# Alembic migrations applied automatically by test_engine fixture
```

### Bucket Already Exists

**Error:** `BucketAlreadyExists`

**Solution:**
- `lakehouse_bucket` fixture handles this gracefully
- If manual bucket creation needed, check for existence first
- Clean up: `docker-compose down -v` to remove MinIO volumes

### Test Isolation Issues

**Symptom:** Tests pass individually but fail when run together

**Solution:**
- Use unique table names via `generate_unique_table_name()`
- Each test gets fresh `test_bucket` via fixture
- Verify no shared state between tests
- Check for race conditions in Spark operations

---

## Best Practices for New Tests

### 1. Use Existing Helpers

**DON'T reinvent the wheel:**
```python
# ❌ Bad - Manual API calls
response = api_client.post("/api/v1/ingestions", json={...})
ingestion = response.json()
ingestion_id = ingestion["id"]
```

**DO use helpers:**
```python
# ✅ Good - Use helper
ingestion = create_standard_ingestion(
    api_client=api_client,
    cluster_id=test_cluster_id,
    test_bucket=test_bucket,
    minio_config=minio_config,
    table_name=generate_unique_table_name("my_test")
)
ingestion_id = ingestion["id"]
```

### 2. Use E2ELogger for Output

**DON'T use raw print:**
```python
# ❌ Bad
print("Creating ingestion...")
print(f"Files processed: {files_processed}")
```

**DO use logger:**
```python
# ✅ Good
logger = E2ELogger()
logger.phase("Creating ingestion...")
logger.metric("Files Processed", files_processed)
logger.success("Ingestion created")
```

### 3. Use Unique Table Names

**ALWAYS generate unique table names:**
```python
# ✅ Always do this
table_name = generate_unique_table_name("my_feature")
# Result: my_feature_20250107123456
```

### 4. Clean Up After Yourself

**DON'T rely on manual cleanup:**
- Use fixtures for automatic cleanup
- Buckets, sessions, databases auto-cleaned
- If creating resources manually, ensure cleanup in finally block

### 5. Provide Clear Success Criteria

**Include in docstring:**
```python
def test_my_feature(self, ...):
    """
    Test description.

    Success criteria:
    - First run: X files, Y records
    - Second run: Z files, W records
    - Table has expected schema
    - No duplicates
    """
```

### 6. Use Meaningful Assertions

**DON'T just assert true/false:**
```python
# ❌ Bad
assert actual_count == expected_count
```

**DO provide context:**
```python
# ✅ Good
assert actual_count == expected_count, \
    f"Expected {expected_count} records, got {actual_count}. " \
    f"This indicates file re-processing (FAILED)."
```

### 7. Test One Scenario Per Method

**DON'T combine unrelated scenarios:**
- One test method per workflow
- Makes debugging easier
- Clearer test intent

### 8. Use Pytest Markers

**Mark tests appropriately:**
```python
@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
@pytest.mark.slow  # If test takes > 30s
class TestMyFeature:
    ...
```

### 9. Document Expected Behavior

**Add comments for non-obvious assertions:**
```python
# CRITICAL: Second run should process only NEW files (2),
# not all files (5). This validates incremental load.
assert_run_metrics(run2, expected_files=2, expected_records=2000)
```

### 10. Use Test Summaries

**Always end with summary:**
```python
print_test_summary([
    ("Test", "My Feature E2E"),
    ("Ingestion ID", ingestion_id),
    ("Status", "SUCCESS ✅")
])
```

---

## Writing a New E2E Test - Template

```python
"""
E2E Test: [Feature Name]

Tests the [feature description] workflow:
1. [Step 1]
2. [Step 2]
3. [Step 3]

This test validates:
- [Validation 1]
- [Validation 2]

This test uses REAL services from docker-compose.test.yml:
- MinIO (S3-compatible storage) on localhost:9000
- Spark Connect on localhost:15002
- PostgreSQL on localhost:5432

All interactions are API-only (no direct database manipulation).
"""

import pytest
from typing import Dict
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession

from .helpers import (
    E2ELogger,
    create_standard_ingestion,
    trigger_run,
    wait_for_run_completion,
    assert_run_metrics,
    verify_table_data,
    generate_unique_table_name,
    get_table_identifier,
    print_test_summary
)


@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestMyFeature:
    """E2E test for [feature description]"""

    def test_my_scenario(
        self,
        api_client: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        sample_json_files: list,
        spark_session: SparkSession,
        test_tenant_id: str,
        test_cluster_id: str,
        spark_connect_url: str
    ):
        """
        Test [scenario description].

        Success criteria:
        - [Criterion 1]
        - [Criterion 2]
        - [Criterion 3]
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: [Feature Name]")

        table_name = generate_unique_table_name("my_feature")

        # =====================================================================
        # Phase 1: Setup
        # =====================================================================
        logger.phase("📝 Phase 1: Setup")

        ingestion = create_standard_ingestion(
            api_client=api_client,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="My Feature E2E Test"
        )

        ingestion_id = ingestion["id"]
        table_identifier = get_table_identifier(ingestion)
        logger.success(f"Created ingestion: {ingestion_id}")

        # =====================================================================
        # Phase 2: Execution
        # =====================================================================
        logger.phase("🚀 Phase 2: Execution")

        run_id = trigger_run(api_client, ingestion_id)
        logger.success(f"Triggered run: {run_id}")

        run = wait_for_run_completion(
            api_client=api_client,
            ingestion_id=ingestion_id,
            run_id=run_id,
            timeout=180,
            logger=logger
        )

        # =====================================================================
        # Phase 3: Verification
        # =====================================================================
        logger.phase("📊 Phase 3: Verification")

        assert_run_metrics(
            run=run,
            expected_files=3,
            expected_records=3000,
            logger=logger
        )

        verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=3000,
            expected_fields=["id", "timestamp", "user_id", "event_type", "value"],
            logger=logger
        )

        logger.success("All verifications passed")

        # =====================================================================
        # Summary
        # =====================================================================
        logger.section("✅ E2E TEST PASSED: [Feature Name]")
        print_test_summary([
            ("Test", "My Feature E2E"),
            ("Ingestion ID", ingestion_id),
            ("Run ID", run_id),
            ("Files Processed", 3),
            ("Records Ingested", 3000),
            ("Table", table_identifier),
            ("Status", "SUCCESS ✅")
        ])
```

---

## Future Test Ideas

Based on the existing patterns, consider adding:

1. **Multi-Source Tests**
   - Different file formats (CSV, Parquet, Avro)
   - Different cloud providers (Azure Blob, GCS)

2. **Error Handling Tests**
   - Malformed JSON files
   - Schema incompatibility
   - Spark errors and retries

3. **Scheduling Tests**
   - Cron-based scheduling
   - Backfill operations

4. **Partitioning Tests**
   - Partitioned table ingestion
   - Z-ordering optimization

5. **Quality Tests**
   - Row count thresholds
   - Alert triggering

6. **Performance Tests**
   - Large file ingestion (10k+ records)
   - Many small files (100+ files)
   - Concurrent runs

---

## Summary

E2E tests provide confidence that the entire IOMETE Autoloader system works correctly from API to data lake. By using real infrastructure, comprehensive helpers, and structured test patterns, we ensure production-like validation without sacrificing developer experience.

**Key Takeaways:**
- ✅ Always use API-only interactions
- ✅ Use real Docker services (MinIO, Postgres, Spark)
- ✅ Leverage existing helper functions
- ✅ Follow phase-based test structure
- ✅ Provide clear success criteria
- ✅ Use E2ELogger for structured output
- ✅ Ensure automatic cleanup via fixtures
- ✅ One test scenario per method
- ✅ Meaningful assertions with context
- ✅ Summarize test results
