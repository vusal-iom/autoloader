# E2E-02: Incremental Load Test Specification

**Test ID:** E2E-02
**Test Name:** Incremental Load - S3 JSON Files
**Category:** End-to-End Integration Test
**Priority:** HIGH
**Status:** Planned
**Created:** 2025-01-05
**Author:** System

## Test Overview

### Objective

Validate that IOMETE Autoloader correctly implements incremental file processing across multiple ingestion runs. The system must:
1. Track which files have been processed
2. Only ingest NEW files in subsequent runs
3. Maintain accurate run metrics
4. Prevent data duplication in the target Iceberg table

### Scope

**In Scope:**
- Multiple ingestion runs on the same configuration
- File state tracking and persistence
- Checkpoint-based file filtering
- Data deduplication verification
- Run metrics accuracy across runs
- File state querying via processed files endpoint

**Out of Scope:**
- Schema evolution between runs (covered in E2E-04)
- Error scenarios (covered in E2E-03)
- Different file formats (CSV covered in E2E-05)
- Scheduled/cron-based execution (covered in E2E-06)
- Concurrent ingestion runs (future test)

### Prerequisites

1. **Services Running:**
   - MinIO (S3-compatible storage) on localhost:9000
   - PostgreSQL database on localhost:5432
   - Spark Connect on localhost:15002
   - FastAPI application running

2. **Test Infrastructure:**
   - Docker Compose test environment (`docker-compose.test.yml`)
   - Test fixtures initialized
   - Lakehouse bucket created in MinIO

3. **Dependencies:**
   - E2E-01 (Basic S3 JSON Ingestion) must be passing
   - All test fixtures from `tests/conftest.py` and `tests/e2e/conftest.py`

## Test Environment

### Configuration

**Source:**
- Type: AWS S3 (MinIO)
- Endpoint: http://localhost:9000
- Bucket: `test-bucket-{unique_id}`
- File Pattern: `data-*.json`
- Format: JSON (newline-delimited)

**Destination:**
- Catalog: `test_catalog`
- Database: `test_db`
- Table: `test_incremental_load_{timestamp}`
- Format: Apache Iceberg

**Test Data:**
- Total Files: 5
- Initial Batch: 3 files
- Second Batch: 2 additional files
- Records per File: 1000
- Total Expected Records: 5000

### Test Data Structure

**JSON Record Schema:**
```json
{
  "id": "integer",
  "timestamp": "string (ISO 8601)",
  "user_id": "string",
  "event_type": "string",
  "value": "float"
}
```

**File Naming Convention:**
```
data-001.json  # IDs: 0-999
data-002.json  # IDs: 1000-1999
data-003.json  # IDs: 2000-2999
data-004.json  # IDs: 3000-3999 (added in second batch)
data-005.json  # IDs: 4000-4999 (added in second batch)
```

**Sample Record:**
```json
{"id": 0, "timestamp": "2025-01-05T10:00:00.000000Z", "user_id": "user-0", "event_type": "login", "value": 0.0}
{"id": 1, "timestamp": "2025-01-05T10:00:01.000000Z", "user_id": "user-1", "event_type": "click", "value": 1.0}
...
```

## Test Procedure

### Phase 1: Setup and Initial Ingestion

#### Step 1.1: Prepare Test Bucket
**Action:**
```python
# Fixture creates unique bucket
test_bucket = "test-bucket-{uuid}"
```

**Expected Result:**
- Bucket created in MinIO
- Bucket is empty and ready for test files

#### Step 1.2: Upload Initial Files (Batch 1)
**Action:**
```python
# Upload 3 JSON files
upload_json_file(test_bucket, "data-001.json", records_range=(0, 999))
upload_json_file(test_bucket, "data-002.json", records_range=(1000, 1999))
upload_json_file(test_bucket, "data-003.json", records_range=(2000, 2999))
```

**Expected Result:**
- 3 files in bucket
- Each file contains 1000 newline-delimited JSON records
- Files match pattern `data-*.json`

#### Step 1.3: Create Ingestion Configuration
**Action:**
```python
POST /api/v1/ingestions

{
  "name": "test-incremental-load",
  "source_type": "S3",
  "source_config": {
    "bucket": test_bucket,
    "path": "",
    "file_pattern": "data-*.json",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "endpoint": "http://localhost:9000"
  },
  "destination_config": {
    "catalog": "test_catalog",
    "database": "test_db",
    "table": f"test_incremental_load_{timestamp}"
  },
  "format_type": "JSON",
  "schedule": null,
  "tenant_id": test_tenant_id,
  "cluster_id": test_cluster_id
}
```

**Expected Result:**
- Status: 201 Created
- Response contains `ingestion_id`
- Ingestion status: `ACTIVE`

#### Step 1.4: Trigger First Ingestion Run
**Action:**
```python
POST /api/v1/ingestions/{ingestion_id}/run
```

**Expected Result:**
- Status: 202 Accepted
- Response contains `run_id`
- Run status: `RUNNING` or `PENDING`

#### Step 1.5: Poll for First Run Completion
**Action:**
```python
# Poll every 5 seconds, max 180 seconds
while time_elapsed < 180:
    response = GET /api/v1/runs/{run_id}
    if status in ["COMPLETED", "FAILED", "ERROR"]:
        break
    sleep(5)
```

**Expected Result:**
- Run completes within 180 seconds
- Final status: `COMPLETED`
- No errors in response

#### Step 1.6: Verify First Run Metrics
**Action:**
```python
GET /api/v1/runs/{run_id}
```

**Expected Result:**
```json
{
  "run_id": "{run_id}",
  "status": "COMPLETED",
  "metrics": {
    "files_processed": 3,
    "records_written": 3000,
    "bytes_processed": "~300KB"
  },
  "error": null
}
```

**Critical Assertions:**
- ✅ `metrics.files_processed == 3`
- ✅ `metrics.records_written == 3000`
- ✅ `status == "COMPLETED"`
- ✅ `error is None`

#### Step 1.7: Verify Data in Iceberg Table (First Run)
**Action:**
```python
# Query via Spark session
table_name = f"test_catalog.test_db.test_incremental_load_{timestamp}"

# Count total records
count_query = f"SELECT COUNT(*) as count FROM {table_name}"
result = spark.sql(count_query).collect()
total_records = result[0].count

# Check for duplicates
distinct_query = f"SELECT COUNT(DISTINCT id) as count FROM {table_name}"
result = spark.sql(distinct_query).collect()
distinct_records = result[0].count

# Verify ID range
minmax_query = f"SELECT MIN(id) as min_id, MAX(id) as max_id FROM {table_name}"
result = spark.sql(minmax_query).collect()
min_id = result[0].min_id
max_id = result[0].max_id
```

**Expected Result:**
- `total_records == 3000`
- `distinct_records == 3000` (no duplicates)
- `min_id == 0`
- `max_id == 2999`

**Critical Assertions:**
- ✅ Exactly 3000 records in table
- ✅ No duplicate IDs
- ✅ ID range is continuous [0, 2999]

#### Step 1.8: Verify File State After First Run
**Action:**
```python
GET /api/v1/ingestions/{ingestion_id}/processed-files
```

**Expected Result:**
```json
{
  "processed_files": [
    {
      "file_path": "s3://test-bucket-{uuid}/data-001.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:30:00Z"
    },
    {
      "file_path": "s3://test-bucket-{uuid}/data-002.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:30:00Z"
    },
    {
      "file_path": "s3://test-bucket-{uuid}/data-003.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:30:00Z"
    }
  ],
  "total": 3
}
```

**Critical Assertions:**
- ✅ Exactly 3 files marked as `PROCESSED`
- ✅ All file paths are correct
- ✅ All have `processed_at` timestamps

---

### Phase 2: Add New Files

#### Step 2.1: Upload Additional Files (Batch 2)
**Action:**
```python
# Upload 2 more JSON files to SAME bucket
upload_json_file(test_bucket, "data-004.json", records_range=(3000, 3999))
upload_json_file(test_bucket, "data-005.json", records_range=(4000, 4999))
```

**Expected Result:**
- 5 total files now in bucket
- New files: `data-004.json` and `data-005.json`
- Each new file contains 1000 records
- All 5 files match pattern `data-*.json`

#### Step 2.2: Verify Bucket Contents
**Action:**
```python
# List objects in bucket
objects = minio_client.list_objects(test_bucket)
file_names = [obj.object_name for obj in objects]
```

**Expected Result:**
```python
file_names == [
    "data-001.json",
    "data-002.json",
    "data-003.json",
    "data-004.json",  # NEW
    "data-005.json"   # NEW
]
```

---

### Phase 3: Second Ingestion Run (Incremental)

#### Step 3.1: Trigger Second Ingestion Run
**Action:**
```python
POST /api/v1/ingestions/{ingestion_id}/run
# Same ingestion_id as before
```

**Expected Result:**
- Status: 202 Accepted
- Response contains NEW `run_id` (different from first run)
- Run status: `RUNNING` or `PENDING`

#### Step 3.2: Poll for Second Run Completion
**Action:**
```python
# Poll every 5 seconds, max 180 seconds
while time_elapsed < 180:
    response = GET /api/v1/runs/{run_id_2}
    if status in ["COMPLETED", "FAILED", "ERROR"]:
        break
    sleep(5)
```

**Expected Result:**
- Run completes within 180 seconds
- Final status: `COMPLETED`
- No errors in response

#### Step 3.3: Verify Second Run Metrics (CRITICAL)
**Action:**
```python
GET /api/v1/runs/{run_id_2}
```

**Expected Result:**
```json
{
  "run_id": "{run_id_2}",
  "status": "COMPLETED",
  "metrics": {
    "files_processed": 2,        // ONLY 2 new files!
    "records_written": 2000,     // ONLY 2000 new records!
    "bytes_processed": "~200KB"
  },
  "error": null
}
```

**CRITICAL ASSERTIONS (Core Test Validation):**
- ✅ `metrics.files_processed == 2` (NOT 5!)
- ✅ `metrics.records_written == 2000` (NOT 5000!)
- ✅ `status == "COMPLETED"`
- ✅ `error is None`

**Failure Modes:**
- ❌ If `files_processed == 5`: All files were re-processed (file tracking broken)
- ❌ If `files_processed == 3`: Checkpoint issue or wrong file pattern
- ❌ If `records_written == 5000`: Likely re-processed all files

#### Step 3.4: Verify Data in Iceberg Table (After Second Run)
**Action:**
```python
# Query via Spark session
table_name = f"test_catalog.test_db.test_incremental_load_{timestamp}"

# Count total records
count_query = f"SELECT COUNT(*) as count FROM {table_name}"
result = spark.sql(count_query).collect()
total_records = result[0].count

# Check for duplicates
distinct_query = f"SELECT COUNT(DISTINCT id) as count FROM {table_name}"
result = spark.sql(distinct_query).collect()
distinct_records = result[0].count

# Verify ID range
minmax_query = f"SELECT MIN(id) as min_id, MAX(id) as max_id FROM {table_name}"
result = spark.sql(minmax_query).collect()
min_id = result[0].min_id
max_id = result[0].max_id
```

**Expected Result:**
- `total_records == 5000` (NOT 6000!)
- `distinct_records == 5000` (no duplicates)
- `min_id == 0`
- `max_id == 4999`

**CRITICAL ASSERTIONS (Core Test Validation):**
- ✅ Exactly 5000 records in table (NOT 6000!)
- ✅ No duplicate IDs (total == distinct)
- ✅ ID range is continuous [0, 4999]

**Failure Modes:**
- ❌ If `total_records == 6000`: Data duplication occurred (checkpoint failure)
- ❌ If `distinct_records < total_records`: Duplicate records present
- ❌ If records missing: Some files not processed correctly

#### Step 3.5: Verify File State After Second Run
**Action:**
```python
GET /api/v1/ingestions/{ingestion_id}/processed-files
```

**Expected Result:**
```json
{
  "processed_files": [
    {
      "file_path": "s3://test-bucket-{uuid}/data-001.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:30:00Z"
    },
    {
      "file_path": "s3://test-bucket-{uuid}/data-002.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:30:00Z"
    },
    {
      "file_path": "s3://test-bucket-{uuid}/data-003.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:30:00Z"
    },
    {
      "file_path": "s3://test-bucket-{uuid}/data-004.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:35:00Z"  // NEW, different timestamp
    },
    {
      "file_path": "s3://test-bucket-{uuid}/data-005.json",
      "status": "PROCESSED",
      "processed_at": "2025-01-05T10:35:00Z"  // NEW, different timestamp
    }
  ],
  "total": 5
}
```

**Critical Assertions:**
- ✅ ALL 5 files marked as `PROCESSED`
- ✅ All file paths are correct
- ✅ Files 4-5 have later `processed_at` timestamps than files 1-3

#### Step 3.6: Verify Run History
**Action:**
```python
GET /api/v1/ingestions/{ingestion_id}/runs
```

**Expected Result:**
```json
{
  "runs": [
    {
      "run_id": "{run_id_2}",
      "status": "COMPLETED",
      "metrics": {
        "files_processed": 2,
        "records_written": 2000
      },
      "started_at": "2025-01-05T10:35:00Z",
      "completed_at": "2025-01-05T10:37:00Z"
    },
    {
      "run_id": "{run_id_1}",
      "status": "COMPLETED",
      "metrics": {
        "files_processed": 3,
        "records_written": 3000
      },
      "started_at": "2025-01-05T10:30:00Z",
      "completed_at": "2025-01-05T10:32:00Z"
    }
  ],
  "total": 2
}
```

**Critical Assertions:**
- ✅ 2 runs in history
- ✅ Each run has correct metrics
- ✅ Runs are ordered by recency (latest first)

---

## Test Validation Summary

### Primary Validation Points

| # | Validation | Expected Value | Failure Impact |
|---|------------|----------------|----------------|
| 1 | First run files processed | 3 | High - Basic ingestion broken |
| 2 | First run records written | 3000 | High - Data ingestion broken |
| 3 | First run table record count | 3000 | High - Data verification failed |
| 4 | **Second run files processed** | **2** | **CRITICAL - Incremental load broken** |
| 5 | **Second run records written** | **2000** | **CRITICAL - Incremental load broken** |
| 6 | **Total table records after both runs** | **5000** | **CRITICAL - Duplication occurred** |
| 7 | Distinct IDs in table | 5000 | Critical - Duplicate data |
| 8 | Processed files count | 5 | High - File tracking incomplete |
| 9 | Run history count | 2 | Medium - History tracking issue |

### Success Criteria

The test is considered **PASSED** if ALL of the following are true:
1. ✅ First run processes exactly 3 files and writes 3000 records
2. ✅ Second run processes exactly 2 NEW files (not all 5)
3. ✅ Second run writes exactly 2000 NEW records (not 5000)
4. ✅ Total records in table is 5000 (not 6000)
5. ✅ No duplicate records (COUNT(*) == COUNT(DISTINCT id))
6. ✅ All 5 files marked as PROCESSED in database
7. ✅ Run history shows both runs with correct metrics
8. ✅ No errors reported in any run
9. ✅ Test completes within 6 minutes total

### Failure Criteria

The test is considered **FAILED** if ANY of the following occur:
1. ❌ Second run processes more than 2 files
2. ❌ Table contains more than 5000 records (duplication)
3. ❌ Table contains fewer than 5000 records (data loss)
4. ❌ Duplicate IDs found in table
5. ❌ Any run fails with error status
6. ❌ Test times out (> 10 minutes)
7. ❌ Processed files count != 5
8. ❌ Run metrics don't match actual data

## Expected Behavior

### What Should Happen

**Checkpoint Management:**
- Spark Structured Streaming maintains checkpoint at `/tmp/iomete-autoloader/checkpoints/{ingestion_id}`
- Checkpoint tracks which files have been processed
- Checkpoint persists between runs
- New run reads checkpoint and skips already-processed files

**File State Tracking:**
- `file_state_service.py` records processed files in database
- `ProcessedFile` model stores: `ingestion_id`, `file_path`, `status`, `processed_at`
- Second run queries processed files before execution
- Only unprocessed files are added to processing queue

**Batch Orchestrator:**
- `batch_orchestrator.py` coordinates with `file_state_service`
- Calculates accurate metrics based on actual files processed
- Updates run metrics in database
- Marks run as COMPLETED when done

**Data Flow:**
```
Second Run Triggered
  ↓
Load Checkpoint (3 files processed)
  ↓
List S3 Bucket (5 files found)
  ↓
Filter Out Processed Files (2 files remaining)
  ↓
Process Only New Files (data-004.json, data-005.json)
  ↓
Write 2000 Records to Iceberg
  ↓
Update File State (mark 2 new files as PROCESSED)
  ↓
Update Checkpoint (5 files now tracked)
  ↓
Record Metrics (files_processed=2, records_written=2000)
  ↓
Mark Run COMPLETED
```

### What Should NOT Happen

❌ **Re-processing all files:**
- System should NOT process data-001.json, data-002.json, data-003.json again
- Checkpoint prevents this

❌ **Duplicate data writes:**
- System should NOT write 6000 total records
- Iceberg append mode should only add new records

❌ **Incorrect metrics:**
- System should NOT report files_processed=5 for second run
- Metrics must reflect actual work done

## Test Execution

### Running the Test

```bash
# Start test services
make setup-test

# Wait for services to be ready
sleep 60

# Run the specific test
pytest tests/e2e/test_incremental_load.py::TestIncrementalLoad::test_incremental_load_s3_json -v -s

# Or run all e2e tests
make test-e2e-full

# Cleanup
make teardown-test
```

### Test Duration

- **Setup:** ~60 seconds (service startup)
- **First ingestion run:** ~120 seconds
- **Add new files:** ~5 seconds
- **Second ingestion run:** ~90 seconds (faster, fewer files)
- **Verification:** ~10 seconds
- **Total:** ~5-6 minutes

### Test Markers

```python
@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
@pytest.mark.incremental  # New marker specific to this test
```

## Debugging Guide

### If Second Run Processes All 5 Files

**Symptom:** `metrics.files_processed == 5` instead of 2

**Possible Causes:**
1. Checkpoint not being used or corrupted
2. File state tracking not querying correctly
3. Checkpoint location incorrect

**Investigation Steps:**
```bash
# Check checkpoint directory
ls -la /tmp/iomete-autoloader/checkpoints/{ingestion_id}/

# Check processed_files table
psql -c "SELECT * FROM processed_files WHERE ingestion_id = '{ingestion_id}';"

# Check Spark logs
docker-compose -f docker-compose.test.yml logs spark-connect | grep checkpoint
```

### If Table Has 6000 Records

**Symptom:** `total_records == 6000` instead of 5000

**Possible Causes:**
1. Checkpoint bypassed
2. Iceberg append mode issue
3. Files re-processed despite checkpoint

**Investigation Steps:**
```python
# Check for duplicate IDs
spark.sql(f"""
    SELECT id, COUNT(*) as count
    FROM {table_name}
    GROUP BY id
    HAVING COUNT(*) > 1
""").show()

# Check file metadata in Iceberg
spark.sql(f"SELECT input_file_name(), COUNT(*) FROM {table_name} GROUP BY input_file_name()").show()
```

### If Run Metrics Are Wrong

**Symptom:** Metrics don't match actual data

**Investigation Steps:**
1. Check `batch_orchestrator.py` metrics calculation
2. Verify `run_repository.py` updates
3. Check logs for metrics recording:
```bash
docker-compose -f docker-compose.test.yml logs app | grep metrics
```

## Related Test Cases

- **E2E-01:** Basic S3 JSON Ingestion (prerequisite)
- **E2E-03:** Failed Run and Retry (error handling)
- **E2E-04:** Schema Evolution Detection (schema changes)
- **E2E-05:** CSV Format Ingestion (format variation)
- **E2E-06:** Scheduled Ingestion (cron-based)

## Implementation Checklist

### Pre-Implementation
- [ ] E2E-01 test is passing consistently
- [ ] Test services are running (MinIO, PostgreSQL, Spark)
- [ ] Documentation reviewed and approved
- [ ] Test data generation helper implemented

### Implementation
- [ ] Create `tests/e2e/test_incremental_load.py`
- [ ] Implement helper to upload additional files mid-test
- [ ] Implement Phase 1 (initial ingestion)
- [ ] Implement Phase 2 (add new files)
- [ ] Implement Phase 3 (second ingestion + verification)
- [ ] Add all critical assertions
- [ ] Add test docstring with clear description

### Verification
- [ ] Test passes on first run
- [ ] Test passes on 3 consecutive runs
- [ ] Test duration < 6 minutes
- [ ] All assertions validate correctly
- [ ] No flaky behavior observed
- [ ] Cleanup works correctly (no test pollution)

### Documentation
- [ ] Update `tests/README.md` with new test
- [ ] Document any fixture changes
- [ ] Add troubleshooting notes if needed
- [ ] Mark test as complete in project docs

## Approval & Sign-off

**Test Specification Approved By:** _________________
**Date:** _________________
**Implementation Ready:** [ ] Yes [ ] No
**Blocked By:** _________________

---

**Document Version:** 1.0
**Last Updated:** 2025-01-05
**Next Review:** After implementation
