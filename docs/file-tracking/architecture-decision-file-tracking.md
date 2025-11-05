# Architectural Decision: File Tracking and State Management

**Status:** Under Discussion
**Date:** 2025-01-03
**Decision Makers:** Engineering Team
**Context:** IOMETE Autoloader MVP - Replacing Databricks Auto Loader with Open Source Spark

---

## Executive Summary

This document analyzes two architectural approaches for implementing file ingestion state management in IOMETE Autoloader. The decision is critical because:

1. **Databricks Auto Loader (`cloudFiles`) is not available** in open source Apache Spark
2. **File-level observability** impacts debugging, auditing, and user experience
3. **Implementation complexity** affects development timeline and maintenance burden
4. **Performance characteristics** impact cost and scalability

**Options under consideration:**
- **Option 1:** Standard Spark Structured Streaming with checkpoint-based state
- **Option 2:** Batch processing with PostgreSQL-based file state tracking
- **Option 1.5:** Hybrid approach combining both

---

## Context: Current Implementation Analysis

### Current Checkpoint Implementation

**Location:** `app/spark/connect_client.py`

**Checkpoint Strategy:**
- **Auto-generated paths:** `{checkpoint_base_path}/{tenant_id}/{ingestion_id}/`
- **Default base:** `s3://iomete-checkpoints/` (configurable via `CHECKPOINT_BASE_PATH`)
- **Schema location:** Sub-directory at `{checkpoint_location}/schema` for Auto Loader schema inference
- **Managed by Spark:** Checkpoints are fully managed by Spark Structured Streaming's cloudFiles source

**Current Implementation:**
```python
# In read_stream():
options = {
    "cloudFiles.format": format_type,
    "cloudFiles.schemaLocation": f"{checkpoint_location}/schema",
    "cloudFiles.inferColumnTypes": "true",
}

# In write_stream():
writer = df.writeStream
    .option("checkpointLocation", checkpoint_location)
    .trigger(Trigger.AvailableNow())  # Batch mode
```

**Key Characteristics:**
- Spark's cloudFiles automatically tracks processed files in checkpoint metadata
- Uses `availableNow` trigger for batch processing (processes all available data, then terminates)
- No manual file tracking - Spark handles incremental state internally
- Checkpoints are persistent across runs and enable exactly-once processing

**Problem:** `cloudFiles` is Databricks-only and will not work with IOMETE's open source Spark.

---

### Database Models Relevant to File Tracking

**Location:** `app/models/domain.py`

#### Ingestion Model (Lines 26-89)
Tracks ingestion configuration and high-level metadata:

```python
class Ingestion(Base):
    # Checkpoint management
    checkpoint_location = Column(String, nullable=False)  # Where Spark stores state

    # Run tracking
    last_run_id = Column(String, nullable=True)
    last_run_time = Column(DateTime, nullable=True)
    next_run_time = Column(DateTime, nullable=True)

    # Schema versioning
    schema_version = Column(Integer, default=1)
    schema_json = Column(JSON, nullable=True)
```

**File Tracking Capability:** None - only tracks checkpoint location as a string

#### Run Model (Lines 91-132)
Tracks execution metrics per run:

```python
class Run(Base):
    # Execution metadata
    id = Column(String, primary_key=True)
    ingestion_id = Column(String, nullable=False, index=True)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    status = Column(SQLEnum(RunStatus))  # RUNNING, SUCCESS, FAILED, PARTIAL
    trigger = Column(String)  # scheduled, manual, retry

    # Metrics (aggregate level)
    files_processed = Column(Integer, default=0)  # COUNT only
    records_ingested = Column(Integer, default=0)
    bytes_read = Column(Integer, default=0)
    bytes_written = Column(Integer, default=0)
    duration_seconds = Column(Integer, nullable=True)
    error_count = Column(Integer, default=0)

    # Error tracking
    errors = Column(JSON, nullable=True)  # Array of error objects

    # Spark metadata
    spark_job_id = Column(String, nullable=True)
    cluster_id = Column(String, nullable=False)
```

**File Tracking Capability:**
- `files_processed`: INTEGER count only (no file names, paths, or timestamps)
- `errors`: JSON array could theoretically store file-level errors, but structure is undefined
- **No individual file tracking mechanism**

#### SchemaVersion Model (Lines 134-151)
Tracks schema evolution:

```python
class SchemaVersion(Base):
    id = Column(String, primary_key=True)
    ingestion_id = Column(String, nullable=False, index=True)
    version = Column(Integer, nullable=False)

    detected_at = Column(DateTime, nullable=False)
    schema_json = Column(JSON, nullable=False)
    affected_files = Column(JSON, nullable=True)  # List of files causing schema change

    # Resolution tracking
    resolution_type = Column(String)  # auto_merge, backfill, ignore, manual
    resolved_at = Column(DateTime, nullable=True)
    resolved_by = Column(String, nullable=True)
```

**File Tracking Capability:**
- `affected_files`: JSON array for files that triggered schema changes
- This is the **ONLY** place where file-level information is stored, but only for schema evolution events

---

### Configuration

**Location:** `app/config.py`

```python
class Settings(BaseSettings):
    # Checkpoint configuration
    checkpoint_base_path: str = "s3://iomete-checkpoints/"

    # Preview limits (for testing)
    max_preview_files: int = 10
    max_preview_rows: int = 100

    # Spark Connect
    spark_session_pool_size: int = 10
    spark_session_idle_timeout: int = 1800  # 30 minutes

    # Scheduler
    scheduler_check_interval: int = 60  # seconds
```

---

### Existing File Tracking or State Management Code

**Analysis:** **NO MANUAL FILE TRACKING EXISTS**

#### What's Handled by Spark:
From `connect_client.py` and `executor.py`:

1. **Incremental Processing:** Spark's cloudFiles automatically tracks:
   - Which files have been processed
   - File metadata (modification time, size)
   - Offset information for each file
   - Schema evolution across files

2. **Metrics Collection** (`executor.py` lines 138-182):
```python
def _monitor_query(self, query) -> Dict[str, Any]:
    metrics = {
        "files_processed": 0,  # Aggregate count only
        "records_ingested": 0,
        "bytes_read": 0,
        "bytes_written": 0,
    }

    for progress in query.recentProgress:
        if progress.sources:
            for source in progress.sources:
                metrics["files_processed"] += source.numInputFiles  # COUNT
                metrics["bytes_read"] += source.inputBytes
```

**What's NOT Tracked:**
- Individual file names/paths
- Processing timestamps per file
- File-level success/failure status
- File-level error messages
- File discovery time vs processing time
- File size per file (only aggregate bytes)

---

### Critical Findings

1. **Zero File-Level Tracking:** The current implementation has NO mechanism to track individual files in the database
2. **Spark Owns State:** All file processing state is inside Spark checkpoints (opaque to application)
3. **Run Model is Aggregate-Only:** `Run.files_processed` is just an integer count
4. **No File History:** Cannot answer "when was file X processed?" or "which files failed?"
5. **Schema Evolution Only:** The only file-level data stored is in `SchemaVersion.affected_files` for schema changes

---

## Option 1: Standard Spark Structured Streaming

### How It Works

```python
# Simplified flow
df = spark.readStream \
    .format("json") \
    .schema(predefined_schema) \
    .load("s3a://bucket/path/")

query = df.writeStream \
    .format("iceberg") \
    .option("checkpointLocation", "s3://checkpoints/ingestion-123/") \
    .trigger(availableNow=True) \
    .start("catalog.db.table")

query.awaitTermination()  # Waits until all available files processed
```

### Architecture Details

**State Management:**
- Spark maintains checkpoint metadata in `s3://checkpoints/ingestion-123/`
- Checkpoint contains:
  - `offsets/`: Which files have been processed (FileStreamSource tracks file paths + mod times)
  - `commits/`: Successfully written batches
  - `metadata`: Query metadata and configuration
  - `sources/`: Source-specific state (file listing cache)
  - `state/`: Stateful operation state (if using aggregations)

**File Discovery:**
- Spark lists files in source directory every trigger interval
- Compares against checkpoint to find new/modified files
- For large directories (10k+ files), listing can be slow
- Uses file modification time + size as change detection

**Exactly-Once Semantics:**
- Checkpoint ensures each file processed exactly once
- Atomic commits to Iceberg table
- If job fails mid-batch, restart from last checkpoint
- No duplicate data (Iceberg's transactional writes)

### Pros

✅ **Minimal Code Complexity**
- ~50 lines of code total
- No custom state management logic
- Spark handles all edge cases (file renames, deletions, partial failures)

✅ **Battle-Tested Reliability**
- Spark Structured Streaming is mature (5+ years in production)
- Handles network failures, partial writes, executor crashes
- Well-documented recovery mechanisms

✅ **Automatic Optimization**
- Spark batches file reads for efficiency
- Adaptive query execution
- Predicate pushdown for Parquet/ORC

✅ **Schema Required Upfront**
- Forces schema validation before ingestion starts
- Your preview/test mode can infer schema once, then save it
- Prevents schema drift surprises mid-ingestion

### Cons

❌ **Zero File-Level Observability**
- Cannot query: "Which files were processed in Run #42?"
- Cannot query: "When was file `data-2024-01-15.json` ingested?"
- Cannot track individual file failures (all-or-nothing per batch)

❌ **Opaque Checkpoint State**
- Checkpoint is binary Parquet/JSON mix, not human-readable
- Debugging requires Spark expertise
- Cannot easily inspect "what will be processed next?"

❌ **Large Directory Performance**
- File listing (S3 ListObjects) can be slow for 100k+ files
- Each run lists entire directory to find new files
- Option: `latestFirst` limits scan, but may miss files

❌ **Limited Error Granularity**
```python
# Your Run model tracks:
run.files_processed = 100  # How many? Which ones?
run.error_count = 5        # Which files failed? Why?
run.errors = [...]         # Batch-level errors only
```

❌ **Schema Must Be Stable**
- `readStream` requires schema upfront (no auto-inference in streaming)
- Schema changes require:
  1. Detect schema mismatch (job fails)
  2. User approves new schema (via your SchemaVersion model)
  3. Update schema in code
  4. Restart job
- Cannot handle mixed schemas in same run

### Performance Characteristics

- **File Discovery:** O(n) where n = total files in directory (every run)
- **State Storage:** ~100KB - 10MB checkpoint data (grows with file count)
- **Memory:** Spark driver memory scales with file count in single batch

### Architectural Implications

**For Option 1:**
- ✅ Already similar to current codebase approach
- ✅ Checkpoints fully managed by Spark (no custom code needed)
- ✅ Exactly-once processing guaranteed by Spark
- ❌ No visibility into individual file processing
- ❌ Cannot query "which files were processed when" from database
- ❌ Debugging failures requires examining Spark checkpoint metadata

---

## Option 2: Batch Processing with PostgreSQL State Tracking

### How It Works

```python
# Simplified flow
from pyspark.sql import SparkSession

# 1. List files from cloud storage
s3_client = boto3.client('s3')
all_files = s3_client.list_objects_v2(Bucket='bucket', Prefix='path/')

# 2. Query database for already-processed files
processed = db.query(ProcessedFile).filter_by(ingestion_id=ingestion_id).all()
processed_paths = {p.file_path for p in processed}

# 3. Find new files
new_files = [f for f in all_files if f not in processed_paths]

# 4. Process each file (or batch)
for file_path in new_files:
    try:
        df = spark.read.format("json").load(f"s3a://bucket/{file_path}")
        df.write.format("iceberg").mode("append").save("catalog.db.table")

        # Track success
        db.add(ProcessedFile(
            ingestion_id=ingestion_id,
            file_path=file_path,
            processed_at=datetime.utcnow(),
            status="SUCCESS",
            records=df.count(),
            bytes=get_file_size(file_path)
        ))
    except Exception as e:
        # Track failure
        db.add(ProcessedFile(
            ingestion_id=ingestion_id,
            file_path=file_path,
            status="FAILED",
            error_message=str(e)
        ))
```

### Required Database Schema

```python
class ProcessedFile(Base):
    __tablename__ = "processed_files"

    id = Column(String, primary_key=True)
    ingestion_id = Column(String, ForeignKey("ingestions.id"), index=True)
    run_id = Column(String, ForeignKey("runs.id"), nullable=True, index=True)

    # File identification
    file_path = Column(String, nullable=False)  # s3://bucket/path/file.json
    file_size_bytes = Column(BigInteger, nullable=True)
    file_modified_at = Column(DateTime, nullable=True)  # From S3 metadata

    # Processing metadata
    discovered_at = Column(DateTime, nullable=False)  # When listed from S3
    processed_at = Column(DateTime, nullable=True)    # When ingestion completed
    status = Column(Enum("PENDING", "SUCCESS", "FAILED", "SKIPPED"))

    # Metrics per file
    records_ingested = Column(Integer, nullable=True)
    bytes_read = Column(BigInteger, nullable=True)
    processing_duration_ms = Column(Integer, nullable=True)

    # Error tracking
    error_message = Column(Text, nullable=True)
    error_type = Column(String, nullable=True)
    retry_count = Column(Integer, default=0)

    # Unique constraint to prevent duplicates
    __table_args__ = (
        Index('idx_ingestion_file', 'ingestion_id', 'file_path', unique=True),
    )
```

### Pros

✅ **Complete File-Level Visibility**
```sql
-- Which files were processed in Run #42?
SELECT file_path, records_ingested, processing_duration_ms
FROM processed_files WHERE run_id = 'run-42';

-- When was this specific file ingested?
SELECT processed_at, status FROM processed_files
WHERE file_path = 's3://bucket/data/2024-01-15.json';

-- Which files failed and why?
SELECT file_path, error_message FROM processed_files
WHERE status = 'FAILED' AND ingestion_id = 'ing-123';

-- Show ingestion progress (useful for long-running jobs)
SELECT status, COUNT(*) FROM processed_files
WHERE run_id = 'run-42' GROUP BY status;
```

✅ **Granular Error Handling**
- Retry individual failed files (not entire batch)
- Skip corrupted files, continue processing others
- Store file-specific error messages
- Implement per-file retry limits

✅ **Flexible Schema Handling**
```python
for file_path in new_files:
    try:
        # Infer schema per file
        df = spark.read.format("json").option("inferSchema", "true").load(file_path)
        current_schema = df.schema

        # Compare with expected schema
        if current_schema != expected_schema:
            # Log schema mismatch, mark file as SKIPPED
            handle_schema_evolution(file_path, current_schema)
        else:
            # Process normally
            process_file(df)
    except Exception:
        # Continue to next file
        pass
```

✅ **Better Debugging**
- Exact file that caused failure
- Timeline of file processing (discovered → processed)
- Can manually re-trigger specific files
- Clear audit trail for compliance

✅ **Performance Insights**
- Track processing time per file
- Identify slow files (large files, complex JSON)
- Optimize based on file patterns

✅ **Incremental Progress UI**
```
Run #42: Processing files...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 847/1000 (85%)
✓ 842 succeeded | ✗ 5 failed | ⏳ 153 pending
```

### Cons

❌ **Implementation Complexity**
Need to implement:
1. **Cloud SDK integration** (boto3, azure-storage-blob, google-cloud-storage)
2. **File listing logic** with pagination (S3 limits to 1000 per request)
3. **Concurrency control** (prevent duplicate processing if multiple workers)
4. **Transaction management** (mark file as processed atomically with Iceberg write)
5. **Retry logic** with exponential backoff
6. **File size limits** (skip huge files that could crash executors)
7. **Partial failure handling** (some files succeed, some fail)

**Estimated:** 300-500 lines of code vs 50 lines for Option 1

❌ **Exactly-Once Guarantees Harder**
```python
# Race condition:
# 1. Worker A reads file, writes to Iceberg
# 2. Worker A crashes before marking file as processed
# 3. Worker B sees file as unprocessed, reads again
# 4. Duplicate data in Iceberg!

# Solution: Use database transactions + SELECT FOR UPDATE
with db.begin():
    file_record = db.query(ProcessedFile) \
        .filter_by(file_path=file_path) \
        .with_for_update(skip_locked=True) \  # Lock row
        .first()

    if not file_record:
        # Process file
        # Write to Iceberg
        # Mark as processed (all in same transaction)
```

**Challenge:** Iceberg writes are separate transactions! Need two-phase commit pattern.

❌ **Database Growth**
- 1 million files/month = 1M rows in `processed_files` table
- Need partitioning strategy (e.g., by year-month)
- Need retention policy (archive old records)
- Queries slow without proper indexes

**Example:** 10 ingestions × 100k files/month × 12 months = **12 million rows/year**

❌ **File Listing Performance**
```python
# S3 ListObjectsV2 costs
# 100,000 files = 100 requests (1000 files/request)
# At $0.005 per 1000 requests = $0.0005 per run
# But: 24 runs/day × 30 days = $0.36/month per ingestion
# 100 ingestions = $36/month just for file listing!

# Optimization: Partition S3 by date
# s3://bucket/year=2024/month=01/day=15/*.json
# List only new partitions based on last_run_time
```

❌ **Schema Inference Overhead**
```python
# Per-file schema inference
for file in new_files:  # 1000 files
    df = spark.read.option("inferSchema", "true").load(file)  # Reads file twice!
    # Once for schema, once for data

# Better: Sample first file for schema, apply to rest
# But: Mixed schemas still problematic
```

### Performance Characteristics

- **File Discovery:** O(n) where n = total files, but you control batching
- **State Storage:** PostgreSQL table grows linearly with total files ever processed
- **Memory:** Constant (process files in batches of N)

### Architectural Implications

**For Option 2:**
- ❌ Requires NEW database model (`ProcessedFile` table)
- ❌ Requires rewriting `read_stream()` to use batch reads with file listing
- ❌ Requires manual exactly-once logic (file locking, idempotency)
- ✅ Full visibility: query processed files by name, timestamp, status
- ✅ Easier debugging and auditing
- ✅ Can implement custom retry logic per file
- ❌ More complex error handling and state management

---

## Option 1.5: Hybrid Approach

### How It Works

Combine Spark Structured Streaming for reliability with database logging for observability:

```python
# Use Spark Structured Streaming for reliability
query = spark.readStream \
    .format("json") \
    .load("s3a://bucket/path/") \
    .writeStream \
    .foreachBatch(custom_writer) \  # Custom writer logs files
    .start()

def custom_writer(batch_df, batch_id):
    # Get file sources from batch metadata
    files_in_batch = batch_df.inputFiles()  # Returns list of file paths

    # Write to Iceberg (transactional)
    batch_df.write.format("iceberg").mode("append").save("catalog.db.table")

    # Log to database (after successful write)
    for file_path in files_in_batch:
        db.add(ProcessedFile(
            ingestion_id=ingestion_id,
            run_id=current_run_id,
            file_path=file_path,
            processed_at=datetime.utcnow(),
            status="SUCCESS"
        ))
```

### Pros

- ✅ Spark manages checkpoints and exactly-once (reliable)
- ✅ Database tracks file-level metadata (observable)
- ✅ Best of both worlds

### Cons

- ❌ Still need open source Spark streaming (no cloudFiles)
- ❌ `inputFiles()` may not give all metadata you want (no per-file record count)
- ❌ Adds complexity to both systems
- ❌ Need to ensure database logging doesn't fail the entire batch

---

## Decision Framework

### Choose **Option 1** if:

- ✅ You trust Spark's black-box checkpoint (battle-tested)
- ✅ Aggregate metrics are sufficient (don't need file-level details)
- ✅ You want minimal code to maintain
- ✅ Your users don't ask "which files were processed?"
- ✅ File counts are moderate (<10k files per directory)

### Choose **Option 2** if:

- ✅ You need file-level audit trail (compliance, debugging)
- ✅ You want granular error handling (retry individual files)
- ✅ You need progress reporting (UI showing 842/1000 files processed)
- ✅ You expect mixed schemas and want to handle per-file
- ✅ Your users will ask "when was this file ingested?"
- ✅ You're okay with 300-500 lines of state management code

---

## Recommendation

Given the use case (AWS/Snyk dumps, daily ingestion, scheduled batches), **Option 2** is recommended with these refinements:

### Implementation Strategy

**Phase 1: Simple File Tracking**
```python
# Track only essential state
class ProcessedFile:
    file_path, ingestion_id, processed_at, status, error_message
```

**Phase 2: Add Metrics**
```python
# Add per-file metrics
records_ingested, bytes_read, processing_duration_ms
```

**Phase 3: Optimize**
```python
# Partition S3 by date, list only new partitions
# Archive old processed_files rows to separate table
```

### Why Option 2 Makes Sense

1. **AWS/Snyk dumps are predictable:** Daily files, bounded size, not 24/7 streaming
2. **Debugging matters:** When AWS schema changes, you need to know which file broke
3. **User visibility:** "Did yesterday's AWS dump get processed?" is a natural question
4. **Retry granularity:** If 1 of 50 files fails, re-process just that file (not all 50)
5. **Cost transparency:** Track exact file → cost mapping
6. **Compliance and auditing:** Full audit trail of what data was ingested when
7. **Progressive enhancement:** Start simple, add metrics and optimizations later

### Migration Path

1. **Remove cloudFiles dependency** from `app/spark/connect_client.py`
2. **Add ProcessedFile model** to `app/models/domain.py`
3. **Create migration** for new table
4. **Implement file listing service** for each cloud provider (S3, Azure, GCS)
5. **Update executor** to use batch processing with state tracking
6. **Add repository methods** for ProcessedFile CRUD operations
7. **Update cost estimator** to use actual file counts from database

---

## Next Steps

If Option 2 is approved:

1. Create detailed implementation plan including:
   - Database schema for `ProcessedFile` model
   - Repository pattern for file state management
   - Boto3/cloud SDK integration for file listing
   - Transaction handling for exactly-once guarantees
   - Batch processing logic with error handling
   - Migration path from current cloudFiles code

2. Create proof-of-concept for:
   - S3 file listing with pagination
   - File state tracking in PostgreSQL
   - Batch Spark read/write with Iceberg
   - Error handling and retry logic

3. Performance testing:
   - Benchmark with 1k, 10k, 100k files
   - Measure database query performance
   - Evaluate S3 listing costs

---

## References

- Original user request: `docs/user-request.md`
- Product requirements: `docs/ingestion-prd-v1.md`
- Spark Connect architecture: `docs/ingestion-prd-using-connect.md`
- Current implementation: `app/spark/connect_client.py`, `app/models/domain.py`
