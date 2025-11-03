# In-Depth Comparison: Option 2 vs Option 3

**Document Purpose:** Detailed technical comparison between two open-source Spark approaches for file ingestion state management

**Options Compared:**
- **Option 2:** Pure Batch Processing with PostgreSQL State Tracking
- **Option 3 (Hybrid):** Spark Structured Streaming + PostgreSQL Logging

**Date:** 2025-01-03

---

## Quick Reference Table

| Aspect | Option 2: Batch + PostgreSQL | Option 3: Hybrid Streaming + PostgreSQL |
|--------|------------------------------|----------------------------------------|
| **Core Approach** | Manual file listing → Batch read → Track in DB | Spark readStream → foreachBatch → Track in DB |
| **State of Record** | PostgreSQL `processed_files` table | Spark checkpoint + PostgreSQL log |
| **Exactly-Once** | Manual (DB locks + idempotency) | Spark checkpoint (automatic) |
| **Code Complexity** | High (300-500 LOC) | Medium (150-200 LOC) |
| **File Discovery** | Cloud SDK (boto3, azure-storage) | Spark FileStreamSource (built-in) |
| **Schema Handling** | Flexible per-file inference | Fixed schema required |
| **Error Granularity** | Per-file | Per-batch (but logged per-file) |
| **Retry Logic** | Custom per-file retry | Spark automatic batch retry |
| **Observability** | Complete (source of truth) | Partial (log after processing) |
| **Database Growth** | All files tracked | All files tracked |
| **Performance** | Controllable batch size | Spark-managed batching |
| **Debugging** | Query DB for any file | Query DB + inspect checkpoints |
| **Operational Complexity** | High (custom orchestration) | Medium (Spark + DB sync) |
| **Failure Modes** | Many edge cases to handle | Fewer (Spark handles most) |

---

## Architecture Deep Dive

### Option 2: Pure Batch Processing with PostgreSQL

#### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     Scheduler (Cron/APScheduler)                │
└────────────────────────────┬────────────────────────────────────┘
                             │ Triggers run at schedule
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Ingestion Executor                         │
│  1. Query PostgreSQL: "What files processed already?"           │
│  2. List cloud storage: "What files exist now?"                 │
│  3. Compute diff: new_files = all_files - processed_files       │
│  4. FOR EACH new_file:                                          │
│     a. Lock file record (SELECT FOR UPDATE SKIP LOCKED)         │
│     b. Read file with Spark batch API                           │
│     c. Write to Iceberg table                                   │
│     d. Mark file as SUCCESS/FAILED in PostgreSQL                │
│     e. Commit transaction                                       │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │   PostgreSQL   │
                    │ processed_files│
                    │ (Source of Truth)
                    └────────────────┘
```

#### Component Breakdown

**1. File Discovery Service**

```python
# app/services/file_discovery_service.py

from typing import List, Dict
from datetime import datetime
import boto3
from azure.storage.blob import BlobServiceClient
from google.cloud import storage

class FileDiscoveryService:
    """Discovers files from cloud storage"""

    def list_files_s3(self, bucket: str, prefix: str,
                      since: datetime = None) -> List[Dict]:
        """
        List files from S3 with pagination handling

        Returns:
            List of dicts with keys: path, size, modified_at, etag
        """
        s3 = boto3.client('s3')
        files = []
        continuation_token = None

        while True:
            # Handle pagination (1000 files per request)
            kwargs = {
                'Bucket': bucket,
                'Prefix': prefix,
                'MaxKeys': 1000
            }
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token

            response = s3.list_objects_v2(**kwargs)

            if 'Contents' not in response:
                break

            for obj in response['Contents']:
                # Skip if older than since date (optimization)
                if since and obj['LastModified'] < since:
                    continue

                files.append({
                    'path': f"s3://{bucket}/{obj['Key']}",
                    'size': obj['Size'],
                    'modified_at': obj['LastModified'],
                    'etag': obj['ETag']
                })

            if not response.get('IsTruncated'):
                break

            continuation_token = response.get('NextContinuationToken')

        return files

    def list_files_azure(self, container: str, prefix: str,
                         connection_string: str) -> List[Dict]:
        """List files from Azure Blob Storage"""
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client(container)

        files = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            files.append({
                'path': f"wasbs://{container}/{blob.name}",
                'size': blob.size,
                'modified_at': blob.last_modified,
                'etag': blob.etag
            })

        return files

    def list_files_gcs(self, bucket: str, prefix: str) -> List[Dict]:
        """List files from Google Cloud Storage"""
        client = storage.Client()
        bucket_obj = client.bucket(bucket)
        blobs = bucket_obj.list_blobs(prefix=prefix)

        files = []
        for blob in blobs:
            files.append({
                'path': f"gs://{bucket}/{blob.name}",
                'size': blob.size,
                'modified_at': blob.updated,
                'etag': blob.etag
            })

        return files
```

**Complexity:** ~150 lines for cloud SDK integration

**2. State Tracking Service**

```python
# app/services/file_state_service.py

from typing import Set, List, Optional
from sqlalchemy.orm import Session
from app.models.domain import ProcessedFile, ProcessedFileStatus
from datetime import datetime

class FileStateService:
    """Manages file processing state in PostgreSQL"""

    def __init__(self, db: Session):
        self.db = db

    def get_processed_files(self, ingestion_id: str) -> Set[str]:
        """
        Get set of already-processed file paths

        Optimized query with index on (ingestion_id, status)
        """
        files = self.db.query(ProcessedFile.file_path) \
            .filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.status.in_(['SUCCESS', 'SKIPPED'])
            ) \
            .all()

        return {f.file_path for f in files}

    def get_failed_files(self, ingestion_id: str,
                         max_retries: int = 3) -> List[ProcessedFile]:
        """Get failed files eligible for retry"""
        return self.db.query(ProcessedFile) \
            .filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.status == 'FAILED',
                ProcessedFile.retry_count < max_retries
            ) \
            .all()

    def lock_file_for_processing(self, ingestion_id: str,
                                  file_path: str) -> Optional[ProcessedFile]:
        """
        Atomically lock a file for processing

        Returns None if already being processed by another worker
        Uses SELECT FOR UPDATE SKIP LOCKED for concurrency safety
        """
        # Try to get existing record
        file_record = self.db.query(ProcessedFile) \
            .filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.file_path == file_path
            ) \
            .with_for_update(skip_locked=True) \
            .first()

        if file_record:
            if file_record.status == 'PROCESSING':
                # Another worker is processing this
                return None

            # Update to PROCESSING
            file_record.status = 'PROCESSING'
            file_record.retry_count += 1
            file_record.processing_started_at = datetime.utcnow()
        else:
            # Create new record
            file_record = ProcessedFile(
                ingestion_id=ingestion_id,
                file_path=file_path,
                status='PROCESSING',
                discovered_at=datetime.utcnow(),
                processing_started_at=datetime.utcnow()
            )
            self.db.add(file_record)

        self.db.commit()
        return file_record

    def mark_file_success(self, file_record: ProcessedFile,
                          records: int, bytes_read: int):
        """Mark file as successfully processed"""
        file_record.status = 'SUCCESS'
        file_record.processed_at = datetime.utcnow()
        file_record.records_ingested = records
        file_record.bytes_read = bytes_read
        file_record.processing_duration_ms = (
            datetime.utcnow() - file_record.processing_started_at
        ).total_seconds() * 1000
        self.db.commit()

    def mark_file_failed(self, file_record: ProcessedFile,
                         error: Exception):
        """Mark file as failed"""
        file_record.status = 'FAILED'
        file_record.error_message = str(error)
        file_record.error_type = type(error).__name__
        file_record.processed_at = datetime.utcnow()
        self.db.commit()
```

**Complexity:** ~100 lines for state management

**3. Batch Processor**

```python
# app/services/batch_processor.py

from typing import List, Dict
from pyspark.sql import SparkSession
from app.spark.connect_client import SparkConnectClient
from app.models.domain import Ingestion, ProcessedFile

class BatchFileProcessor:
    """Processes files in batches with PostgreSQL state tracking"""

    def __init__(self, spark_client: SparkConnectClient,
                 state_service: FileStateService):
        self.spark = spark_client
        self.state = state_service

    def process_new_files(self, ingestion: Ingestion,
                          new_files: List[Dict],
                          run_id: str) -> Dict[str, int]:
        """
        Process list of new files

        Returns metrics: {success: N, failed: N, skipped: N}
        """
        metrics = {'success': 0, 'failed': 0, 'skipped': 0}

        for file_info in new_files:
            file_path = file_info['path']

            # Atomically lock file for processing
            file_record = self.state.lock_file_for_processing(
                ingestion.id, file_path
            )

            if not file_record:
                # Another worker is processing this file
                metrics['skipped'] += 1
                continue

            try:
                # Process file
                result = self._process_single_file(
                    ingestion, file_path, file_info
                )

                # Mark success
                self.state.mark_file_success(
                    file_record,
                    records=result['record_count'],
                    bytes_read=file_info['size']
                )
                metrics['success'] += 1

            except Exception as e:
                # Mark failure
                self.state.mark_file_failed(file_record, e)
                metrics['failed'] += 1

                # Log error but continue processing other files
                logger.error(f"Failed to process {file_path}: {e}")

        return metrics

    def _process_single_file(self, ingestion: Ingestion,
                             file_path: str,
                             file_info: Dict) -> Dict:
        """
        Process a single file with Spark batch API

        Returns: {record_count: N}
        """
        # Read file
        df = self.spark.session.read \
            .format(ingestion.format_type.value) \
            .schema(ingestion.schema_json)  # Use predefined schema

        # Apply format options
        for key, value in ingestion.format_options.items():
            df = df.option(key, value)

        df = df.load(file_path)

        # Get record count before write
        record_count = df.count()

        # Write to Iceberg table
        df.write \
            .format("iceberg") \
            .mode("append") \
            .save(f"{ingestion.catalog}.{ingestion.database}.{ingestion.table}")

        return {'record_count': record_count}
```

**Complexity:** ~150 lines for batch processing

**Total Code:** ~400 lines + error handling, retries, logging

---

### Option 3: Hybrid (Spark Streaming + PostgreSQL)

#### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     Scheduler (Cron/APScheduler)                │
└────────────────────────────┬────────────────────────────────────┘
                             │ Triggers run at schedule
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Spark Structured Streaming Job                  │
│                                                                 │
│  readStream("s3://bucket/path/")                                │
│      ↓                                                          │
│  Spark discovers files (compares with checkpoint)               │
│      ↓                                                          │
│  Batches files for processing                                   │
│      ↓                                                          │
│  foreachBatch(custom_writer)  ← YOUR CUSTOM CODE               │
│      │                                                          │
│      ├─→ Write batch to Iceberg (Spark automatic)              │
│      └─→ Log file metadata to PostgreSQL (best effort)         │
│                                                                 │
│  Checkpoint updated (Spark automatic)                           │
└─────────────────────────────────────────────────────────────────┘
           │                              │
           ▼                              ▼
  ┌────────────────┐            ┌────────────────┐
  │ Spark Checkpoint│            │   PostgreSQL   │
  │  (S3/HDFS)     │            │ processed_files│
  │ Source of Truth│            │  (Log/Audit)   │
  └────────────────┘            └────────────────┘
```

#### Component Breakdown

**1. Streaming Job Orchestrator**

```python
# app/spark/streaming_executor.py

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter
from app.spark.connect_client import SparkConnectClient

class HybridStreamingExecutor:
    """Executes streaming job with PostgreSQL logging"""

    def __init__(self, spark_client: SparkConnectClient,
                 db_session_factory):
        self.spark = spark_client
        self.db_factory = db_session_factory

    def execute_ingestion(self, ingestion: Ingestion, run_id: str):
        """
        Execute streaming ingestion with batch trigger

        Uses availableNow trigger for batch-like execution
        """
        # Create streaming DataFrame
        df = self.spark.session.readStream \
            .format(ingestion.format_type.value) \
            .schema(ingestion.schema_json) \
            .load(ingestion.source_path)

        # Apply transformations if needed
        # df = df.filter(...).select(...)

        # Write with foreachBatch for custom logic
        query = df.writeStream \
            .foreachBatch(lambda batch_df, batch_id:
                self._process_batch(batch_df, batch_id, ingestion, run_id)
            ) \
            .option("checkpointLocation", ingestion.checkpoint_location) \
            .trigger(availableNow=True) \
            .start()

        # Wait for completion (availableNow terminates after processing all files)
        query.awaitTermination()

        return query.lastProgress

    def _process_batch(self, batch_df: DataFrame, batch_id: int,
                       ingestion: Ingestion, run_id: str):
        """
        Process each micro-batch

        Called by Spark for each batch of files
        """
        # Get file paths in this batch
        file_paths = batch_df.inputFiles()

        # Write to Iceberg (Spark manages this transactionally)
        batch_df.write \
            .format("iceberg") \
            .mode("append") \
            .save(f"{ingestion.catalog}.{ingestion.database}.{ingestion.table}")

        # Log to PostgreSQL (best effort, non-transactional with Iceberg)
        try:
            self._log_batch_to_db(
                ingestion_id=ingestion.id,
                run_id=run_id,
                batch_id=batch_id,
                file_paths=file_paths,
                batch_df=batch_df
            )
        except Exception as e:
            # Don't fail the batch if logging fails
            logger.warning(f"Failed to log batch {batch_id} to DB: {e}")
```

**Complexity:** ~80 lines for streaming orchestration

**2. Database Logger**

```python
# app/services/streaming_logger.py

from typing import List
from pyspark.sql import DataFrame
from datetime import datetime
from app.models.domain import ProcessedFile

class StreamingDatabaseLogger:
    """Logs streaming batch metadata to PostgreSQL"""

    def log_batch(self, ingestion_id: str, run_id: str,
                  batch_id: int, file_paths: List[str],
                  batch_df: DataFrame):
        """
        Log batch processing to database

        Note: This is AFTER Spark writes to Iceberg, so it's audit log
        not source of truth for exactly-once
        """
        db = self.db_factory()

        try:
            # Try to get per-file record counts (expensive!)
            # Option A: Log batch-level only (fast)
            self._log_batch_aggregate(
                db, ingestion_id, run_id, batch_id,
                file_paths, batch_df.count()
            )

            # Option B: Log per-file (slow, requires re-reading files)
            # self._log_per_file(db, ingestion_id, run_id, file_paths)

            db.commit()
        except Exception as e:
            db.rollback()
            raise
        finally:
            db.close()

    def _log_batch_aggregate(self, db, ingestion_id: str, run_id: str,
                            batch_id: int, file_paths: List[str],
                            total_records: int):
        """
        Log batch as single aggregate record

        Limitation: Can't track per-file record counts
        """
        for file_path in file_paths:
            # Check if already logged (idempotency for retries)
            existing = db.query(ProcessedFile) \
                .filter_by(
                    ingestion_id=ingestion_id,
                    file_path=file_path
                ) \
                .first()

            if existing:
                # Update
                existing.run_id = run_id
                existing.processed_at = datetime.utcnow()
                existing.status = 'SUCCESS'
            else:
                # Insert
                db.add(ProcessedFile(
                    ingestion_id=ingestion_id,
                    run_id=run_id,
                    file_path=file_path,
                    processed_at=datetime.utcnow(),
                    status='SUCCESS',
                    records_ingested=None,  # Unknown per file
                    batch_id=batch_id
                ))

    def _log_per_file(self, db, ingestion_id: str, run_id: str,
                      file_paths: List[str]):
        """
        Log with per-file record counts

        WARNING: Requires re-reading each file to count records!
        Very expensive for large batches.
        """
        for file_path in file_paths:
            # Re-read file to get count
            file_df = self.spark.session.read \
                .format("json") \
                .load(file_path)

            record_count = file_df.count()

            db.add(ProcessedFile(
                ingestion_id=ingestion_id,
                run_id=run_id,
                file_path=file_path,
                processed_at=datetime.utcnow(),
                status='SUCCESS',
                records_ingested=record_count
            ))
```

**Complexity:** ~70 lines for database logging

**Total Code:** ~150 lines (much less than Option 2)

---

## State Management: Exactly-Once Guarantees

### Option 2: Manual Exactly-Once

**Challenge:** Coordinate PostgreSQL transaction with Iceberg write

```python
# Problem: Two separate transactional systems
# 1. PostgreSQL transaction (file marked as processed)
# 2. Iceberg transaction (data written to table)

# Race condition scenarios:

# Scenario A: DB commit succeeds, Iceberg write fails
with db.begin():
    file_record = mark_processing(file_path)
    df = spark.read.load(file_path)
    df.write.iceberg(table)  # ← FAILS HERE
    mark_success(file_record)  # Never reached
# Result: File stuck in PROCESSING state

# Scenario B: Iceberg write succeeds, DB commit fails
with db.begin():
    file_record = mark_processing(file_path)
    df = spark.read.load(file_path)
    df.write.iceberg(table)  # ← SUCCESS
    mark_success(file_record)
    db.commit()  # ← FAILS HERE
# Result: Data written but file not marked as processed → DUPLICATE on retry

# Scenario C: Process crashes between Iceberg write and DB commit
df.write.iceberg(table)  # ← SUCCESS
# CRASH HERE
mark_success(file_record)  # Never reached
# Result: Data written but file not marked → DUPLICATE on retry
```

**Solution: Idempotent Writes + Upsert Pattern**

```python
def process_file_idempotent(file_path: str, ingestion: Ingestion):
    """
    Idempotent file processing with 2-phase commit simulation

    Key insight: Iceberg supports upsert/merge, so duplicate writes OK
    if we use merge-on-read with unique keys
    """
    db = get_db_session()

    # Phase 1: Mark file as PROCESSING (or get existing record)
    with db.begin():
        file_record = db.query(ProcessedFile) \
            .filter_by(ingestion_id=ingestion.id, file_path=file_path) \
            .with_for_update(skip_locked=True) \
            .first()

        if not file_record:
            file_record = ProcessedFile(
                ingestion_id=ingestion.id,
                file_path=file_path,
                status='PROCESSING'
            )
            db.add(file_record)
        elif file_record.status == 'SUCCESS':
            # Already processed successfully
            return
        elif file_record.status == 'PROCESSING':
            # Check if stale (e.g., > 1 hour old)
            if is_stale(file_record):
                file_record.retry_count += 1
            else:
                # Another worker is processing
                return

    # Phase 2: Process file (outside DB transaction)
    try:
        df = spark.read.format("json").load(file_path)

        # CRITICAL: Use merge/upsert if Iceberg table has unique keys
        # This makes writes idempotent even if retried
        if ingestion.has_unique_key:
            df.writeTo(table).using("iceberg").mergeInto()
        else:
            # Append mode: NOT idempotent, requires deduplication later
            df.write.format("iceberg").mode("append").save(table)

        record_count = df.count()

    except Exception as e:
        # Phase 3a: Mark as FAILED
        with db.begin():
            file_record.status = 'FAILED'
            file_record.error_message = str(e)
        raise

    # Phase 3b: Mark as SUCCESS
    with db.begin():
        file_record.status = 'SUCCESS'
        file_record.records_ingested = record_count
        file_record.processed_at = datetime.utcnow()
```

**Limitations:**
- ❌ Not true exactly-once unless Iceberg table supports merge/upsert
- ❌ Requires careful handling of crash scenarios
- ❌ Stale PROCESSING records need cleanup job
- ✅ Works well if files have unique keys (e.g., event IDs)

---

### Option 3: Spark-Managed Exactly-Once

**Advantage:** Spark Structured Streaming handles this automatically

```python
# Spark Checkpoint contains:
# 1. offsets/0, offsets/1, ... → Which files processed (source of truth)
# 2. commits/0, commits/1, ... → Which batches committed to sink

# Exactly-once flow:
# 1. Spark reads checkpoint: "Last committed batch = 42"
# 2. Spark lists source files, compares with offsets/42
# 3. Spark finds new files, creates batch 43
# 4. Spark writes batch 43 to Iceberg (with checkpoint update atomically)
# 5. Spark commits batch 43 (offsets/43 + commits/43 written)

# If crash happens:
# - Before commit: Batch 43 discarded, retried from checkpoint 42
# - After commit: Batch 43 recorded, moves to batch 44

# PostgreSQL logging is SEPARATE, non-transactional:
query = df.writeStream \
    .foreachBatch(lambda batch_df, batch_id:
        # Spark handles Iceberg write transactionally
        batch_df.write.format("iceberg").mode("append").save(table)

        # Your logging is best-effort, can fail without affecting ingestion
        try:
            log_to_postgres(batch_df.inputFiles())
        except:
            pass  # Don't fail batch if logging fails
    )
```

**PostgreSQL as Audit Log, Not Source of Truth:**

```python
# Query: "Which files were processed?"
# Answer comes from TWO sources:

# 1. Spark Checkpoint (source of truth for exactly-once)
# - Query checkpoint offsets (requires Spark API)
# - Guarantees no duplicates

# 2. PostgreSQL (audit log for visibility)
# - Query processed_files table (SQL)
# - May be incomplete if logging failed
# - Use for debugging, monitoring, UI

# Reconciliation job (periodic):
def reconcile_checkpoint_with_db():
    """Ensure PostgreSQL matches Spark checkpoint"""
    checkpoint_files = read_spark_checkpoint_offsets(checkpoint_location)
    db_files = query_processed_files(ingestion_id)

    missing_in_db = checkpoint_files - db_files
    if missing_in_db:
        # Backfill PostgreSQL from checkpoint
        for file_path in missing_in_db:
            db.add(ProcessedFile(
                file_path=file_path,
                status='SUCCESS',
                records_ingested=None  # Unknown
            ))
```

**Trade-off:**
- ✅ Exactly-once guaranteed by Spark (battle-tested)
- ✅ No complex 2-phase commit logic needed
- ❌ PostgreSQL not source of truth (may be incomplete)
- ❌ Need reconciliation job to keep DB in sync
- ❌ Harder to query "what will be processed next?"

---

## Error Handling

### Option 2: Granular Per-File Error Handling

**File-Level Failures:**

```python
# Process 1000 files
# Files 1-500: SUCCESS
# File 501: FAILED (corrupt JSON)
# Files 502-1000: Continue processing

# Database state after run:
# - 999 files marked SUCCESS
# - 1 file marked FAILED with error message

# Retry strategy:
def retry_failed_files(ingestion_id: str):
    """Retry only failed files, not successful ones"""
    failed = db.query(ProcessedFile) \
        .filter_by(ingestion_id=ingestion_id, status='FAILED') \
        .filter(ProcessedFile.retry_count < 3) \
        .all()

    for file_record in failed:
        try:
            process_file(file_record.file_path)
            mark_success(file_record)
        except Exception as e:
            mark_failed(file_record, e)
```

**Error Scenarios:**

| Scenario | Handling | Result |
|----------|----------|--------|
| Corrupt file | Catch parse exception, mark FAILED, continue | 999 files succeed, 1 fails |
| Schema mismatch | Detect schema diff, mark SKIPPED, continue | 999 files succeed, 1 skipped |
| Network timeout | Retry with exponential backoff, mark FAILED if max retries | Eventual success or failure |
| Out of memory | Skip large files (size check), mark SKIPPED | Process small files successfully |
| Iceberg table lock | Retry later, mark PENDING | Re-attempt in next run |

**Error Recovery UI:**

```python
# User sees detailed error report:
{
    "run_id": "run-123",
    "total_files": 1000,
    "successful": 995,
    "failed": 5,
    "failures": [
        {
            "file": "s3://bucket/data/2024-01-15-corrupt.json",
            "error": "JSONDecodeError: Expecting value: line 42 column 10",
            "retry_count": 3,
            "action": "Manual review required"
        },
        {
            "file": "s3://bucket/data/2024-01-16-large.json",
            "error": "OutOfMemoryError: File size 10GB exceeds limit",
            "retry_count": 0,
            "action": "Increase executor memory or skip file"
        }
    ]
}
```

**Pros:**
- ✅ Surgical retries (only failed files)
- ✅ Detailed error messages per file
- ✅ Can skip problematic files without blocking entire run
- ✅ Clear action items for users

**Cons:**
- ❌ Complex error handling logic (100+ lines)
- ❌ Need to categorize error types (retryable vs permanent)
- ❌ Retry state management (prevent infinite loops)

---

### Option 3: Batch-Level Error Handling

**Batch Failures:**

```python
# Spark processes files in batches
# Batch 1 (files 1-200): SUCCESS
# Batch 2 (files 201-400): FAILED (file 301 corrupt)
# Batch 3: Never attempted (batch 2 failed)

# Spark behavior:
# - Retry entire batch 2 from checkpoint
# - Files 201-300: Re-processed (idempotent)
# - File 301: Fails again
# - Files 302-400: Never processed

# Result: Entire run blocked by single bad file
```

**Handling Bad Files in Streaming:**

```python
# Option A: Filter bad records within batch
def process_batch_with_bad_record_handling(batch_df, batch_id):
    """Handle parse errors within batch"""

    # Add error handling column
    from pyspark.sql.functions import col, when

    # Try to parse, catch errors
    valid_df = batch_df.filter(col("_corrupt_record").isNull())
    invalid_df = batch_df.filter(col("_corrupt_record").isNotNull())

    # Write valid records
    valid_df.write.format("iceberg").save(table)

    # Log invalid records to error table
    invalid_df.write.format("iceberg").save(error_table)

    # Log to PostgreSQL
    log_to_db(
        files=batch_df.inputFiles(),
        valid_records=valid_df.count(),
        invalid_records=invalid_df.count()
    )
```

**Option B: Skip bad files with file-level try-catch**

```python
# NOT POSSIBLE in Spark Structured Streaming
# You can't catch per-file errors in foreachBatch
# All files in batch processed together
```

**Option C: Permissive mode + error table**

```python
# Use Spark's permissive mode
df = spark.readStream \
    .format("json") \
    .option("mode", "PERMISSIVE") \  # Parse errors → null values
    .option("columnNameOfCorruptRecord", "_corrupt") \
    .load(source)

# Split good vs bad records
good_records = df.filter(col("_corrupt").isNull())
bad_records = df.filter(col("_corrupt").isNotNull())

# Write to separate tables
good_records.writeStream.format("iceberg").save(main_table)
bad_records.writeStream.format("iceberg").save(error_table)
```

**Pros:**
- ✅ Spark handles retries automatically
- ✅ Less custom error handling code
- ✅ Permissive mode allows partial success

**Cons:**
- ❌ Batch-level failures (1 bad file blocks batch)
- ❌ Harder to track which specific file caused error
- ❌ Re-processes entire batch on retry (wasteful)
- ❌ Cannot skip individual files easily

---

## Performance Analysis

### File Discovery Performance

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Method** | boto3.list_objects_v2() | Spark FileStreamSource |
| **Optimization** | Custom (list only new partitions) | Spark internal (latestFirst, maxFilesPerTrigger) |
| **Parallelization** | Single-threaded Python | Spark distributed listing |
| **Caching** | Custom (save last_modified in DB) | Spark checkpoint cache |
| **Large dirs (100k+ files)** | Slow (pagination, API rate limits) | Slow (but Spark optimized) |

**Option 2 Optimization Example:**

```python
# Partition-aware listing
def list_files_optimized(bucket: str, prefix: str,
                         last_run_time: datetime):
    """Only list partitions newer than last run"""

    # Assume S3 structure: s3://bucket/year=2024/month=01/day=15/*.json
    # Only list days since last run

    partitions_to_scan = get_partitions_since(last_run_time)
    # ['year=2024/month=01/day=15', 'year=2024/month=01/day=16', ...]

    files = []
    for partition in partitions_to_scan:
        partition_files = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/{partition}"
        )
        files.extend(partition_files)

    return files

# Result: List 2 days (2000 files) instead of entire bucket (100k files)
# Speedup: 50x faster
```

**Option 3 Optimization:**

```python
# Spark built-in options
df = spark.readStream \
    .option("latestFirst", "true") \  # Process newest files first
    .option("maxFilesPerTrigger", 1000) \  # Limit files per batch
    .option("maxFileAge", "7d") \  # Ignore files older than 7 days
    .load(source)
```

---

### Processing Throughput

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Parallelization** | Custom (ThreadPool/ProcessPool) | Spark automatic |
| **Batch size** | Controllable (e.g., 100 files at a time) | Spark dynamic batching |
| **Resource utilization** | Must implement yourself | Spark adaptive execution |
| **Progress tracking** | Per-file in DB | Per-batch in checkpoint |

**Option 2 Parallel Processing:**

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_files_parallel(files: List[str], max_workers: int = 10):
    """Process files in parallel with thread pool"""

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all files
        future_to_file = {
            executor.submit(process_file, f): f
            for f in files
        }

        # Collect results
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                logger.info(f"Processed {file_path}: {result}")
            except Exception as e:
                logger.error(f"Failed {file_path}: {e}")

# Throughput: 10 files simultaneously
# Limitation: Python GIL, I/O bound (Spark connection)
```

**Option 3 Spark Parallelization:**

```python
# Spark automatically parallelizes across executors
# No custom code needed

# Control parallelism via Spark configs:
spark_conf = {
    "spark.executor.instances": 10,
    "spark.executor.cores": 4,
    "spark.sql.shuffle.partitions": 200
}

# Throughput: 40 files simultaneously (10 executors × 4 cores)
# Spark handles scheduling, data locality, retries
```

---

### Memory Usage

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Driver memory** | Low (only file listing) | Medium (query planning) |
| **Executor memory** | Per-file (controllable) | Per-batch (Spark managed) |
| **Checkpoint storage** | None (PostgreSQL only) | Grows with file count |
| **Database memory** | Grows with file count | Grows with file count |

**Option 2 Memory Control:**

```python
# Process files one at a time to limit memory
for file_path in new_files:
    df = spark.read.load(file_path)
    df.write.iceberg(table)
    # df garbage collected after each iteration
    # Memory: O(1) per file

# Or batch N files at a time:
for batch in chunks(new_files, size=100):
    dfs = [spark.read.load(f) for f in batch]
    union_df = reduce(lambda a, b: a.union(b), dfs)
    union_df.write.iceberg(table)
    # Memory: O(N) where N = batch size
```

**Option 3 Memory:**

```python
# Spark manages memory automatically
# Uses maxFilesPerTrigger to limit batch size
# Memory usage opaque, harder to control
```

---

## Observability & Debugging

### What Can You See?

| Question | Option 2 | Option 3 |
|----------|----------|----------|
| Which files were processed? | ✅ Query `processed_files` table | ⚠️ Query `processed_files` (may be incomplete) |
| When was file X ingested? | ✅ `SELECT processed_at FROM processed_files WHERE file_path = 'X'` | ⚠️ Same, but may not exist if logging failed |
| Why did file X fail? | ✅ `SELECT error_message FROM processed_files WHERE file_path = 'X'` | ❌ Need to check Spark logs |
| What will be processed next? | ✅ `new_files - processed_files` (DB query) | ❌ Need to read Spark checkpoint (complex) |
| How long did file X take? | ✅ `SELECT processing_duration_ms` | ❌ Not tracked per-file |
| Progress during run | ✅ `SELECT COUNT(*) GROUP BY status` (real-time) | ⚠️ Query Spark streaming metrics API |
| Audit trail | ✅ Complete in PostgreSQL | ⚠️ Partial (Spark checkpoint + DB) |

### Debugging Scenarios

**Scenario 1: "Run #42 failed, why?"**

**Option 2:**
```sql
-- Find failed files
SELECT file_path, error_message, error_type
FROM processed_files
WHERE run_id = 'run-42' AND status = 'FAILED';

-- Result:
-- file_path                           | error_message                  | error_type
-- s3://bucket/data/2024-01-15.json   | JSONDecodeError: line 42...    | JSONDecodeError
-- s3://bucket/data/2024-01-16.json   | OutOfMemoryError: ...          | OutOfMemoryError

-- Action: Fix corrupt file, retry specific files
```

**Option 3:**
```python
# Check PostgreSQL (may not have errors logged)
SELECT * FROM processed_files WHERE run_id = 'run-42';
# Result: No failed files (logging may have failed before error captured)

# Check Spark logs (need to grep executor logs)
grep "ERROR" /var/log/spark/executor-*.log
# Result: Buried in 100MB of logs, hard to correlate to specific file

# Check Spark streaming metrics
query.lastProgress
# Result: Batch-level error, not file-level
```

**Scenario 2: "Is file X already processed?"**

**Option 2:**
```sql
SELECT status, processed_at
FROM processed_files
WHERE file_path = 's3://bucket/data/2024-01-15.json';

-- Result: Immediate answer (milliseconds)
-- status: SUCCESS | processed_at: 2024-01-15 08:30:00
```

**Option 3:**
```python
# Option A: Query PostgreSQL (may be incomplete)
SELECT * FROM processed_files WHERE file_path = 'X';
# May return no rows even if file was processed (logging failed)

# Option B: Read Spark checkpoint (slow)
import json
checkpoint_files = []
for offset_file in list_files('s3://checkpoints/ingestion-123/offsets/'):
    offsets = json.load(open(offset_file))
    checkpoint_files.extend(offsets['sources'][0]['files'])

# Check if 'X' in checkpoint_files
# Result: Slow (read many JSON files from S3), requires Spark knowledge
```

---

### Monitoring & Alerting

**Option 2: Rich Metrics from PostgreSQL**

```sql
-- Dashboard queries

-- 1. Ingestion health (last 24 hours)
SELECT
    ingestion_id,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') as success_count,
    COUNT(*) FILTER (WHERE status = 'FAILED') as failed_count,
    AVG(processing_duration_ms) as avg_duration_ms,
    MAX(processed_at) as last_processed_at
FROM processed_files
WHERE processed_at > NOW() - INTERVAL '24 hours'
GROUP BY ingestion_id;

-- 2. Slowest files (identify bottlenecks)
SELECT file_path, processing_duration_ms, records_ingested
FROM processed_files
WHERE processed_at > NOW() - INTERVAL '7 days'
ORDER BY processing_duration_ms DESC
LIMIT 10;

-- 3. Error patterns
SELECT error_type, COUNT(*) as count
FROM processed_files
WHERE status = 'FAILED' AND processed_at > NOW() - INTERVAL '30 days'
GROUP BY error_type
ORDER BY count DESC;

-- 4. Daily ingestion volume
SELECT
    DATE(processed_at) as date,
    COUNT(*) as files_processed,
    SUM(records_ingested) as total_records,
    SUM(bytes_read) / 1024 / 1024 / 1024 as total_gb
FROM processed_files
GROUP BY DATE(processed_at)
ORDER BY date DESC;
```

**Option 3: Limited Metrics**

```python
# Spark Streaming metrics (query object)
query.lastProgress
# Returns:
{
    "batchId": 42,
    "numInputRows": 10000,
    "inputRowsPerSecond": 1000,
    "processedRowsPerSecond": 950,
    "durationMs": {
        "addBatch": 5000,
        "getBatch": 500,
        "latestOffset": 100,
        "queryPlanning": 50,
        "walCommit": 200
    },
    "sources": [{
        "description": "FileStreamSource[s3://bucket/path]",
        "startOffset": {"logOffset": 41},
        "endOffset": {"logOffset": 42},
        "numInputRows": 10000,
        "inputRowsPerSecond": 1000,
        "processedRowsPerSecond": 950
    }],
    "sink": {
        "description": "IcebergSink"
    }
}

# Missing:
# - Per-file metrics
# - File paths processed
# - Error details per file
# - Historical trends (only last progress available)

# Need to combine with PostgreSQL queries, but data may be incomplete
```

---

## Database Impact

### Schema Design

**Option 2: Full Tracking Schema**

```sql
CREATE TABLE processed_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_id UUID NOT NULL REFERENCES ingestions(id),
    run_id UUID REFERENCES runs(id),

    -- File identification
    file_path TEXT NOT NULL,
    file_size_bytes BIGINT,
    file_modified_at TIMESTAMP,
    file_etag TEXT,

    -- Processing state
    status VARCHAR(20) NOT NULL,  -- PENDING, PROCESSING, SUCCESS, FAILED, SKIPPED
    discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processing_started_at TIMESTAMP,
    processed_at TIMESTAMP,

    -- Metrics
    records_ingested INTEGER,
    bytes_read BIGINT,
    processing_duration_ms INTEGER,

    -- Error tracking
    error_message TEXT,
    error_type VARCHAR(100),
    retry_count INTEGER DEFAULT 0,

    -- Indexes
    CONSTRAINT unique_ingestion_file UNIQUE (ingestion_id, file_path)
);

CREATE INDEX idx_processed_files_ingestion ON processed_files(ingestion_id, status);
CREATE INDEX idx_processed_files_run ON processed_files(run_id);
CREATE INDEX idx_processed_files_status ON processed_files(status, processed_at);
CREATE INDEX idx_processed_files_path ON processed_files(file_path);  -- For lookups
```

**Option 3: Minimal Logging Schema**

```sql
CREATE TABLE processed_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_id UUID NOT NULL REFERENCES ingestions(id),
    run_id UUID REFERENCES runs(id),
    batch_id INTEGER,  -- Spark batch ID

    -- File identification (minimal)
    file_path TEXT NOT NULL,

    -- Processing state (simple)
    status VARCHAR(20) DEFAULT 'SUCCESS',
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Metrics (aggregate, may be NULL)
    records_ingested INTEGER,  -- Often NULL (unknown per-file)

    CONSTRAINT unique_ingestion_file UNIQUE (ingestion_id, file_path)
);

CREATE INDEX idx_processed_files_ingestion ON processed_files(ingestion_id);
CREATE INDEX idx_processed_files_run ON processed_files(run_id);
```

---

### Database Growth

**10 Ingestions, 1000 files/day each, 1 year:**

| Metric | Option 2 | Option 3 |
|--------|----------|----------|
| **Rows** | 3.65M rows | 3.65M rows (same) |
| **Row size** | ~500 bytes (full metadata) | ~200 bytes (minimal) |
| **Total size** | ~1.8 GB | ~730 MB |
| **Index size** | ~1 GB (4 indexes) | ~400 MB (2 indexes) |
| **Total storage** | ~2.8 GB/year | ~1.1 GB/year |

**Query Performance (at 3.65M rows):**

```sql
-- Query: "Get files for ingestion X"
-- Option 2: ~50ms (with index on ingestion_id, status)
-- Option 3: ~30ms (smaller table, fewer indexes)

-- Query: "Is file Y processed?"
-- Option 2: ~10ms (index on file_path)
-- Option 3: ~10ms (index on file_path)

-- Query: "Failed files in last 30 days"
-- Option 2: ~100ms (index on status, processed_at)
-- Option 3: N/A (no failed files tracked)
```

**Retention & Archiving:**

```python
# Option 2: Archive old records to cold storage
def archive_old_files(retention_days: int = 90):
    """Move old processed files to archive table"""

    # Move to archive
    db.execute("""
        INSERT INTO processed_files_archive
        SELECT * FROM processed_files
        WHERE processed_at < NOW() - INTERVAL '{retention_days} days'
    """)

    # Delete from main table
    db.execute("""
        DELETE FROM processed_files
        WHERE processed_at < NOW() - INTERVAL '{retention_days} days'
    """)

    # Result: Main table stays <500MB, queries stay fast

# Option 3: Less urgency (smaller table size)
# But still recommended for long-term (years)
```

---

## Cost Analysis

### Cloud Storage Costs

**S3 API Calls (file listing):**

| Scenario | Option 2 | Option 3 |
|----------|----------|----------|
| **Method** | boto3 ListObjectsV2 | Spark FileStreamSource |
| **100k files, hourly runs** | 100 requests × 24 runs/day = 2400 requests/day | Same (Spark uses same API) |
| **Cost** | 2400 × $0.005/1000 = $0.012/day | Same |
| **Optimization** | Partition-aware listing (2 requests/day) | maxFilesPerTrigger (limit listing) |
| **Optimized cost** | 2 × $0.005/1000 = $0.00001/day | Depends on Spark config |

**Checkpoint Storage:**

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Checkpoint size** | None (PostgreSQL only) | ~100KB - 10MB (grows with file count) |
| **S3 storage cost** | $0 | ~$0.0002/month (negligible) |
| **S3 request cost** | $0 | ~$0.05/month (PUT requests for checkpoint updates) |

---

### Compute Costs

**Spark Cluster Runtime:**

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Job type** | Batch read/write (short-lived) | Streaming query (slightly longer due to checkpoint overhead) |
| **Overhead** | File listing in Python (5-10 seconds) | Spark checkpoint read/write (10-20 seconds) |
| **1000 files, hourly** | ~10 min/run = 4 hours/day | ~12 min/run = 4.8 hours/day |
| **DBU cost (at $0.40/DBU)** | 4 hours × 2 DBUs × $0.40 = $3.20/day | 4.8 hours × 2 DBUs × $0.40 = $3.84/day |
| **Difference** | Baseline | +20% overhead |

**Database Costs:**

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Write IOPS** | High (INSERT per file) | Same (INSERT per file) |
| **Read IOPS** | High (query processed files) | Low (checkpoint is source of truth) |
| **Storage** | ~2.8 GB/year | ~1.1 GB/year |
| **PostgreSQL (RDS db.t3.small)** | ~$30/month | ~$25/month |

---

### Total Cost of Ownership (TCO)

**Scenario: 10 ingestions, 1000 files/day each, hourly runs**

| Cost Component | Option 2 | Option 3 |
|----------------|----------|----------|
| **Compute (Spark)** | $96/month | $115/month (+20%) |
| **Database (RDS)** | $30/month | $25/month |
| **S3 storage** | $0.50/month | $0.52/month |
| **S3 API calls** | $0.36/month | $0.36/month |
| **Total** | **$127/month** | **$141/month** |
| **Difference** | Baseline | +11% more expensive |

**TCO Conclusion:** Option 3 is slightly more expensive due to Spark checkpoint overhead, but difference is small (~$14/month).

---

## Edge Cases

### Scenario 1: File Modified After Initial Processing

**Problem:** File `data.json` processed on Jan 1. File modified on Jan 5. Should it be re-processed?

**Option 2:**
```python
# Compare file metadata
def should_reprocess(file_info: Dict, processed_record: ProcessedFile) -> bool:
    """Check if file changed since last processing"""

    # Option A: Use ETag (hash of content)
    if file_info['etag'] != processed_record.file_etag:
        return True

    # Option B: Use modification time
    if file_info['modified_at'] > processed_record.file_modified_at:
        return True

    return False

# Re-process changed files
for file_info in all_files:
    existing = db.query(ProcessedFile).filter_by(file_path=file_info['path']).first()

    if not existing or should_reprocess(file_info, existing):
        process_file(file_info)
```

**Option 3:**
```python
# Spark uses modification time by default
# If file modified, Spark re-processes automatically
# BUT: May cause duplicates in Iceberg table!

# Solution: Use Iceberg MERGE instead of APPEND
df.writeTo(table).using("iceberg").overwritePartitions()
```

---

### Scenario 2: Late-Arriving Files

**Problem:** Files arrive out of order. Jan 15 file arrives on Jan 20. Should it be processed?

**Option 2:**
```python
# Full control - you decide
def list_files_with_lookback(bucket: str, lookback_days: int = 7):
    """List files from last N days to catch late arrivals"""

    since = datetime.utcnow() - timedelta(days=lookback_days)
    return list_files_s3(bucket, prefix, since=since)

# Process late files
files = list_files_with_lookback(bucket, lookback_days=7)
# Will find Jan 15 file on Jan 20
```

**Option 3:**
```python
# Spark option: maxFileAge
df = spark.readStream \
    .option("maxFileAge", "7d") \  # Look back 7 days
    .load(source)

# Spark will find Jan 15 file on Jan 20
```

---

### Scenario 3: Duplicate Files (Same Name, Different Location)

**Problem:**
- `s3://bucket-A/data.json`
- `s3://bucket-B/data.json`

**Option 2:**
```python
# File path includes bucket, so no collision
# processed_files.file_path = 's3://bucket-A/data.json' (unique)
# processed_files.file_path = 's3://bucket-B/data.json' (unique)
# No issue
```

**Option 3:**
```python
# Same - file paths are fully qualified
# No issue
```

---

### Scenario 4: Very Large File (10GB+)

**Problem:** Single file too large for Spark executor memory

**Option 2:**
```python
# Pre-process large files
def process_file_with_size_check(file_info: Dict, max_size_gb: int = 5):
    """Skip or split large files"""

    file_size_gb = file_info['size'] / (1024**3)

    if file_size_gb > max_size_gb:
        # Option A: Skip and alert
        mark_skipped(file_info['path'], reason="File too large")
        send_alert(f"Large file detected: {file_size_gb:.2f} GB")
        return

        # Option B: Process in streaming mode (line by line)
        process_large_file_streaming(file_info['path'])

        # Option C: Split file into chunks
        chunk_files = split_s3_file(file_info['path'], chunk_size_gb=1)
        for chunk in chunk_files:
            process_file(chunk)

# Full control over large file handling
```

**Option 3:**
```python
# Spark handles automatically via partitioning
# But may still OOM if file not splittable (e.g., single-line JSON)

# Solution: Use foreachBatch to check file sizes
def process_batch_with_size_filter(batch_df, batch_id):
    file_paths = batch_df.inputFiles()

    # Check sizes (requires re-querying S3)
    for file_path in file_paths:
        size = get_file_size(file_path)
        if size > 5 * 1024**3:  # 5GB
            log_skipped(file_path, "Too large")
            # But: Can't remove from batch! Entire batch fails
```

---

### Scenario 5: Schema Evolution Mid-Run

**Problem:** First 500 files have schema A. Files 501-1000 have schema B (new column).

**Option 2:**
```python
# Detect schema change per file
for file_path in new_files:
    df = spark.read.option("inferSchema", "true").load(file_path)
    file_schema = df.schema

    if file_schema != expected_schema:
        # Log schema change
        db.add(SchemaVersion(
            ingestion_id=ingestion_id,
            version=next_version,
            schema_json=file_schema.json(),
            affected_files=[file_path]
        ))

        # Options:
        # A) Skip file, await user approval
        mark_skipped(file_path, reason="Schema mismatch")

        # B) Auto-merge schemas
        merged_schema = merge_schemas(expected_schema, file_schema)
        update_ingestion_schema(ingestion_id, merged_schema)
        process_file(file_path, schema=merged_schema)

        # C) Process to separate table
        process_file_to_staging(file_path, schema=file_schema)

# Result: 500 files processed, 500 skipped pending approval
# User approves new schema → retry skipped files
```

**Option 3:**
```python
# Spark requires fixed schema for readStream
# Schema change causes job failure

# Behavior:
# - Files 1-500: Processed successfully (batch 1-5)
# - File 501: Schema mismatch → BATCH FAILS
# - Files 502-1000: Not processed (job stopped)

# Manual intervention:
# 1. User updates schema in ingestion config
# 2. Job restarted with new schema
# 3. Checkpoint ensures files 1-500 not re-processed
# 4. Files 501-1000 processed with new schema

# Limitation: Cannot auto-merge schemas during run
```

---

## Testing Strategy

### Option 2: What to Test

**Unit Tests:**
```python
def test_file_discovery_pagination():
    """Test S3 pagination handles >1000 files"""
    # Mock boto3 to return 3 pages
    # Assert all files collected

def test_state_service_lock():
    """Test concurrent file locking"""
    # Simulate 2 workers trying to process same file
    # Assert only 1 succeeds, other gets None

def test_idempotent_processing():
    """Test file processed twice doesn't duplicate data"""
    # Process file → Check DB + Iceberg
    # Process same file again → Check no duplicates

def test_error_handling():
    """Test failed file doesn't block others"""
    # Process [good.json, bad.json, good2.json]
    # Assert 2 success, 1 failed in DB
```

**Integration Tests:**
```python
def test_end_to_end_ingestion():
    """Test full ingestion flow"""
    # 1. Upload files to test S3 bucket
    # 2. Trigger ingestion
    # 3. Assert all files in processed_files table
    # 4. Assert data in Iceberg table
    # 5. Retry failed files
    # 6. Assert retry count updated

def test_schema_evolution():
    """Test schema change detection"""
    # Upload files with schema A
    # Process → SUCCESS
    # Upload files with schema B
    # Process → SKIPPED (schema mismatch)
```

---

### Option 3: What to Test

**Unit Tests:**
```python
def test_foreachBatch_writer():
    """Test custom batch writer"""
    # Create test DataFrame
    # Call foreachBatch handler
    # Assert data written to Iceberg
    # Assert file paths logged to DB

def test_checkpoint_recovery():
    """Test job recovery from checkpoint"""
    # Process batch 1-5
    # Simulate crash
    # Restart job
    # Assert batch 6 processed (no duplicates from 1-5)
```

**Integration Tests:**
```python
def test_streaming_end_to_end():
    """Test Spark Structured Streaming flow"""
    # 1. Upload files to test S3
    # 2. Start streaming query
    # 3. Wait for availableNow completion
    # 4. Assert data in Iceberg
    # 5. Assert checkpoint created
    # 6. Upload more files
    # 7. Restart job
    # 8. Assert only new files processed

def test_bad_record_handling():
    """Test permissive mode with corrupt records"""
    # Upload mix of good + corrupt files
    # Process with permissive mode
    # Assert good records in main table
    # Assert corrupt records in error table
```

---

## Migration Complexity

### From Current Code (cloudFiles) to Option 2

**Steps:**

1. **Remove Spark Structured Streaming code** (~50 lines deleted)
   - Delete `read_stream()` in `connect_client.py`
   - Delete `write_stream()` in `connect_client.py`

2. **Add cloud SDK dependencies** (`requirements.txt`)
   ```
   boto3==1.34.34
   azure-storage-blob==12.19.0
   google-cloud-storage==2.14.0
   ```

3. **Create ProcessedFile model** (`app/models/domain.py`)
   - Add ~50 lines for model definition

4. **Create database migration** (Alembic)
   ```bash
   alembic revision -m "add_processed_files_table"
   # Edit migration file with CREATE TABLE + indexes
   alembic upgrade head
   ```

5. **Implement FileDiscoveryService** (~150 lines)
6. **Implement FileStateService** (~100 lines)
7. **Implement BatchFileProcessor** (~150 lines)
8. **Update IngestionExecutor** (~50 lines)
9. **Add error handling + retry logic** (~50 lines)
10. **Add tests** (~200 lines)

**Total effort:** ~750 lines of new code, 2-3 weeks for 1 developer

---

### From Current Code (cloudFiles) to Option 3

**Steps:**

1. **Replace cloudFiles with standard readStream** (~20 lines changed)
   ```python
   # Before:
   df = spark.readStream.format("cloudFiles") \
       .option("cloudFiles.format", "json") \
       .load(source)

   # After:
   df = spark.readStream.format("json") \
       .schema(predefined_schema) \
       .load(source)
   ```

2. **Add foreachBatch writer** (~80 lines new)
   - Implement custom writer function
   - Add database logging logic

3. **Create ProcessedFile model** (same as Option 2, ~50 lines)

4. **Create database migration** (same as Option 2)

5. **Implement StreamingDatabaseLogger** (~70 lines)

6. **Update IngestionExecutor to use foreachBatch** (~30 lines changed)

7. **Add tests** (~150 lines)

**Total effort:** ~250 lines of new code, 1 week for 1 developer

**Migration complexity:** Option 3 is **3x easier** to implement than Option 2

---

## Recommendation Matrix

| Your Priority | Choose Option 2 | Choose Option 3 |
|---------------|-----------------|-----------------|
| **Minimize implementation effort** | ❌ | ✅ (3x less code) |
| **Maximize file-level observability** | ✅ (PostgreSQL is source of truth) | ⚠️ (PostgreSQL may be incomplete) |
| **Granular error handling (per-file retry)** | ✅ | ❌ (batch-level only) |
| **Battle-tested reliability** | ❌ (custom code has bugs) | ✅ (Spark proven) |
| **Easy debugging** | ✅ (query DB) | ⚠️ (query DB + checkpoint) |
| **Schema flexibility** | ✅ (per-file inference) | ❌ (fixed schema) |
| **Progress reporting UI** | ✅ (real-time from DB) | ⚠️ (Spark metrics) |
| **Audit compliance** | ✅ (complete trail in DB) | ⚠️ (partial trail) |
| **Minimize operational complexity** | ❌ | ✅ (Spark handles most) |
| **Control over file discovery** | ✅ (custom optimization) | ⚠️ (Spark options) |
| **Cost optimization** | ✅ (11% cheaper) | ⚠️ (11% more expensive) |
| **Testing complexity** | ❌ (more edge cases) | ✅ (fewer edge cases) |

---

## Final Recommendation

### For MVP (Phase 1): **Choose Option 3 (Hybrid)**

**Rationale:**
1. **Faster to market:** 1 week vs 3 weeks implementation
2. **Lower risk:** Leverage Spark's battle-tested streaming
3. **Good enough observability:** PostgreSQL logging covers 80% of use cases
4. **Easy to enhance:** Can add more detailed logging later

**MVP Implementation:**
```python
# Phase 1: Basic hybrid
# - Spark readStream for reliability
# - PostgreSQL logging for visibility (file path + timestamp only)
# - Permissive mode for bad records
# - Simple error handling (log to error table)

# Phase 2: Enhanced logging (later)
# - Add per-file record counts (via re-reading or approximation)
# - Add reconciliation job (sync checkpoint → DB)
# - Add detailed error categorization
```

---

### For Production (Phase 2+): **Migrate to Option 2**

**When to migrate:**
- User feedback requests better error handling
- Debugging becomes difficult with partial logs
- Compliance requires complete audit trail
- Schema evolution becomes frequent

**Rationale:**
1. **Full control:** Handle all edge cases precisely
2. **Complete observability:** Every question answerable via SQL
3. **Granular retries:** Don't waste compute re-processing successful files
4. **Schema flexibility:** Handle mixed schemas in same run

**Migration path:**
```python
# Run Option 3 and Option 2 in parallel for 1 month
# - Option 3: Production traffic
# - Option 2: Shadow mode (process same files, compare results)
# - Validate Option 2 correctness
# - Switch traffic to Option 2
# - Deprecate Option 3
```

---

## Conclusion

**TL;DR:**

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Best for** | Production, compliance, debugging | MVP, speed to market |
| **Implementation** | 750 LOC, 3 weeks | 250 LOC, 1 week |
| **Observability** | Complete | Partial |
| **Reliability** | Custom (riskier) | Spark (proven) |
| **Cost** | $127/month | $141/month |
| **Complexity** | High | Medium |

**Recommendation:** Start with **Option 3** for MVP, migrate to **Option 2** for production when observability becomes critical.

---

## Next Steps

1. **Approve approach** (Option 2, Option 3, or custom hybrid)
2. **Create implementation plan** with task breakdown
3. **Set up test environment** (test S3 bucket, test database)
4. **Implement proof of concept** (100 LOC, 2 days)
5. **Validate with real AWS/Snyk data** (sample 1000 files)
6. **Iterate based on findings**

---

**Document Version:** 1.0
**Last Updated:** 2025-01-03
**Authors:** Engineering Team
**Status:** Awaiting decision
