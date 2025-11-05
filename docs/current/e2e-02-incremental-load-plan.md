# E2E Test Next Step: Incremental Load (E2E-02)

**Status:** Planned (Documentation Phase)
**Priority:** HIGH
**Date:** 2025-01-05

## Executive Summary

After successfully implementing and passing our first e2e test (`test_basic_s3_json_ingestion.py`), the next logical step is to implement **E2E-02: Incremental Load Test**. This test validates the core value proposition of IOMETE Autoloader: scheduled batch processing that only ingests new files without re-processing already-ingested data.

## Why Incremental Load is the Right Next Step

### 1. Core Value Proposition

Incremental loading is **THE** differentiator for IOMETE Autoloader. From the original problem statement:

> "What I'd really like is a simple scheduled ingestion: check an S3 bucket, Azure Blob, GCS, or FTP hourly, daily, or on whatever schedule, and automatically load new files into a table — no Spark code or extra effort from the user."

The key word is "new files" - the system must not re-process files that have already been ingested. This distinguishes our scheduled batch approach from:
- Always-on streaming jobs (resource-intensive)
- Manual batch jobs (require custom code)
- Simple scheduled jobs without state tracking (will re-process everything)

### 2. Validates Critical Architecture Components

This test validates the core architectural decisions:

**File State Tracking:**
- `file_state_service.py` - Tracks which files have been processed
- `processed_file_repository.py` - Persists file state to database
- `ProcessedFile` model - Stores file metadata and processing status

**Spark Checkpoint Management:**
- Checkpoint location per ingestion (`/tmp/iomete-autoloader/checkpoints/{ingestion_id}`)
- Spark Structured Streaming with `availableNow` trigger
- File-based checkpointing to track processed files

**Batch Orchestrator Logic:**
- `batch_orchestrator.py` - Coordinates multiple batch runs
- `batch_file_processor.py` - Processes only new files
- Run metrics accuracy across multiple runs

### 3. Natural Test Progression

Our testing journey:
1. ✅ **Phase 1 (E2E-01):** Prove "one batch works" - Configuration → Execution → Data Verification
2. ⏭️ **Phase 2 (E2E-02):** Prove "multiple batches work correctly" - No duplication, correct metrics
3. 🔮 **Phase 3+:** Error scenarios, format variations, scheduling

This is the logical next step before expanding to error cases or additional formats.

### 4. High-Impact Early Detection

If incremental load doesn't work correctly, the entire product concept needs rework. Problems that could surface:
- Checkpoint corruption or misconfiguration
- File state tracking bugs
- Race conditions in concurrent runs
- Incorrect metrics calculation
- Spark Structured Streaming issues with file-based sources

**Better to discover these issues now** before building error handling, CSV support, or scheduling features on top of broken foundations.

### 5. Builds on Existing Foundation

Implementation efficiency:
- ✅ Same test infrastructure (MinIO, PostgreSQL, Spark Connect)
- ✅ Same fixtures (`test_bucket`, `spark_session`, `api_client`)
- ✅ Similar test pattern (just adds a second ingestion run)
- ✅ Can reuse `test_basic_s3_json_ingestion.py` as template

## What This Test Will Validate

### Success Criteria

The test MUST verify:

1. **File Processing Accuracy**
   - First run: 3 files processed
   - Second run: Only 2 NEW files processed (not all 5)
   - Total files in system: 5 files marked as processed

2. **Data Correctness**
   - First run: 3000 records in Iceberg table
   - Second run: 5000 TOTAL records (not 6000 due to duplication)
   - No duplicate records based on ID field

3. **Run Metrics Accuracy**
   - First run metrics: `files_processed=3`, `records_written=3000`
   - Second run metrics: `files_processed=2`, `records_written=2000`

4. **File State Tracking**
   - All 5 files show `status=PROCESSED` in database
   - Each file has correct processing timestamp
   - File state persists across runs

5. **Checkpoint Integrity**
   - Checkpoint directory exists and contains state
   - Checkpoint prevents re-processing of files
   - Checkpoint survives between runs

### Failure Scenarios That Would Fail This Test

❌ **All files re-processed:**
- Second run shows `files_processed=5` instead of 2
- Indicates checkpoint or file tracking failure

❌ **Duplicate data:**
- Table contains 6000 records instead of 5000
- Indicates deduplication failure or checkpoint issue

❌ **Incorrect metrics:**
- Run history shows wrong file counts
- Indicates metrics calculation bug

❌ **File state not persisted:**
- Database doesn't show all 5 files as processed
- Indicates repository or service layer bug

## High-Level Test Approach

### Test Structure

```python
@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestIncrementalLoad:
    """
    Test that validates incremental file processing across multiple runs.
    Ensures that already-processed files are not re-ingested.
    """

    def test_incremental_load_s3_json(
        self,
        api_client,
        minio_client,
        test_bucket,
        spark_session,
        test_tenant_id,
        test_cluster_id
    ):
        # Test implementation
        pass
```

### Test Flow

**Phase 1: Initial Ingestion (3 files)**
1. Upload 3 JSON files to test bucket (1000 records each)
2. Create ingestion configuration via API
3. Trigger manual run via API
4. Poll for completion (max 3 minutes)
5. Verify run metrics: `files_processed=3`, `records_written=3000`
6. Query Iceberg table: should have exactly 3000 records
7. Verify file state: 3 files marked as `PROCESSED`

**Phase 2: Add New Files (2 more files)**
8. Upload 2 additional JSON files to same bucket (1000 records each)
9. File pattern should match both old and new files (total 5 files in bucket)

**Phase 3: Second Ingestion Run (incremental)**
10. Trigger second manual run via same API endpoint
11. Poll for completion (max 3 minutes)
12. **Critical verification:** Run metrics show `files_processed=2`, `records_written=2000`
13. Query Iceberg table: should have exactly 5000 TOTAL records (not 6000)
14. Verify no duplicate records (check unique IDs)
15. Verify file state: ALL 5 files marked as `PROCESSED`
16. Verify run history: shows both runs with correct metrics

### Key Implementation Details

**Test Data Strategy:**
- Use sequential IDs across files to detect duplicates:
  - File 1: IDs 0-999
  - File 2: IDs 1000-1999
  - File 3: IDs 2000-2999
  - File 4 (new): IDs 3000-3999
  - File 5 (new): IDs 4000-4999
- This makes duplicate detection straightforward

**Fixture Reuse:**
- Existing `test_bucket` fixture can be used
- Need helper function to add files mid-test
- Existing `sample_json_files` fixture uploads 3 files - need similar function for 2 more

**Verification Strategy:**
- API-only interactions (per CLAUDE.md guidelines)
- Use Spark session to query Iceberg table
- Count total records
- Check for duplicate IDs: `SELECT COUNT(*) vs COUNT(DISTINCT id)`
- Query run history via API

## Expected Outcomes

### If Test Passes ✅

- Validates core autoloader functionality works correctly
- Proves file state tracking is reliable
- Confirms checkpoint management is correct
- Demonstrates metrics accuracy
- Provides confidence to build error handling and additional features

### If Test Fails ❌

**All files re-processed:**
- Problem: Checkpoint not working or file tracking broken
- Investigation: Check checkpoint location, Spark logs, processed_files table

**Duplicate data in table:**
- Problem: Deduplication failing or checkpoint bypassed
- Investigation: Iceberg table writes, Spark execution plan

**Wrong metrics:**
- Problem: Metrics calculation bug in batch orchestrator
- Investigation: `batch_orchestrator.py` logic, run repository

**File state not persisted:**
- Problem: Database transaction issue or repository bug
- Investigation: `file_state_service.py`, `processed_file_repository.py`

## Implementation Considerations

### Potential Challenges

1. **File naming consistency:**
   - Files must have unique names to avoid confusion
   - Suggest: `data-001.json`, `data-002.json`, ..., `data-005.json`

2. **Timing between runs:**
   - Need to ensure first run completes before adding new files
   - Use polling with timeout to guarantee completion

3. **Checkpoint location:**
   - Verify checkpoint directory structure
   - May need to inspect checkpoint contents for debugging

4. **Test isolation:**
   - Use unique table names per test run (already implemented in E2E-01)
   - Clean up lakehouse bucket between tests (fixture already handles this)

### Testing Philosophy Alignment

From `tests/e2e/CLAUDE.md`:
- ✅ **API-only interactions** - No direct database queries for verification
- ✅ **Real systems** - Uses actual MinIO, PostgreSQL, Spark Connect
- ✅ **Single test focus** - One comprehensive test scenario
- ✅ **Outcome validation via API** - All verifications through endpoints or Spark queries

## Success Metrics

This test is successful when:
1. ✅ Test passes consistently (3+ consecutive runs)
2. ✅ Run duration is reasonable (< 5 minutes)
3. ✅ All assertions pass with correct values
4. ✅ No flaky behavior or intermittent failures
5. ✅ Documentation is updated to reflect new test

## Next Steps After E2E-02

Once incremental load is validated:

1. **Error Scenarios (E2E-03):**
   - Invalid credentials
   - Missing bucket
   - Malformed JSON files
   - Empty bucket

2. **Schema Evolution (E2E-04):**
   - Files with additional fields
   - Schema compatibility checking

3. **CSV Format (E2E-05):**
   - Validate format-agnostic design

4. **Scheduled Ingestion (E2E-06):**
   - Test cron-based execution

5. **Unit & Integration Tests:**
   - Service layer tests
   - Repository tests
   - Faster feedback loop

## References

- `tests/README.md` - Test documentation and running instructions
- `tests/e2e/CLAUDE.md` - E2E testing guidelines
- `tests/e2e/test_basic_s3_json_ingestion.py` - Template for implementation
- `docs/batch-processing/batch-processing-implementation-guide.md` - Architecture details
- `app/services/batch_orchestrator.py` - Orchestration logic
- `app/services/file_state_service.py` - File tracking logic

## Approval & Timeline

**Approved By:** [User confirmed priority on 2025-01-05]
**Documentation Phase:** 2025-01-05
**Implementation Phase:** TBD (awaiting user confirmation to proceed with coding)
**Target Test Duration:** 3-5 minutes
**Expected Complexity:** Medium (builds on E2E-01 foundation)

---

**Document Status:** ✅ Complete
**Next Action:** Create detailed test specification document
