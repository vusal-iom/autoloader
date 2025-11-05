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
| **File Discovery** | Cloud SDK (boto3, azure-storage) OR Event-driven (SQS) | Spark FileStreamSource (built-in) |
| **Schema Handling** | ✅ Flexible per-file inference | ❌ Fixed schema required |
| **Event-Driven Support** | ✅ Native (SQS consumer) | ❌ Requires custom source |
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

## Schema Evolution: Deep Dive

### Why Schema Evolution is Critical

For AWS/Snyk data dumps and similar use cases:
- **APIs change**: Vendors add new fields to their output
- **Mixed deployments**: Old and new API versions coexist
- **Late discovery**: Schema changes aren't announced in advance
- **No downtime tolerance**: Can't pause ingestion to update schemas

**User requirement:** System must handle schema changes gracefully without manual intervention.

---

### Option 2: Flexible Per-File Schema Handling

#### Architecture

```python
# Schema evolution workflow

class SchemaEvolutionHandler:
    """Handles schema changes on a per-file basis"""

    def __init__(self, ingestion: Ingestion):
        self.ingestion = ingestion
        self.current_schema = self.load_current_schema()

    def load_current_schema(self) -> StructType:
        """Load current schema from ingestion config"""
        if self.ingestion.schema_json:
            return StructType.fromJson(json.loads(self.ingestion.schema_json))
        return None

    def process_file_with_schema_detection(self, file_path: str) -> ProcessResult:
        """Process file with automatic schema evolution detection"""

        # Step 1: Infer schema from file
        file_df = spark.read.format("json") \
            .option("inferSchema", "true") \
            .load(file_path)

        detected_schema = file_df.schema

        # Step 2: Compare with current schema
        if self.current_schema is None:
            # First file - establish baseline schema
            self.save_schema(detected_schema, version=1)
            return self.process_with_schema(file_path, detected_schema)

        # Step 3: Check for schema changes
        schema_diff = self.compare_schemas(self.current_schema, detected_schema)

        if schema_diff.is_compatible:
            # Compatible change (new optional fields, wider types)
            if schema_diff.has_changes:
                self.handle_compatible_change(file_path, detected_schema, schema_diff)
            return self.process_with_schema(file_path, detected_schema)

        elif schema_diff.is_breaking:
            # Breaking change (removed fields, narrower types, type conflicts)
            return self.handle_breaking_change(file_path, detected_schema, schema_diff)

        else:
            # No changes
            return self.process_with_schema(file_path, self.current_schema)
```

#### Schema Comparison Logic

```python
from dataclasses import dataclass
from typing import List, Set

@dataclass
class SchemaField:
    name: str
    data_type: str
    nullable: bool

@dataclass
class SchemaDiff:
    is_compatible: bool
    is_breaking: bool
    has_changes: bool

    added_fields: List[SchemaField]      # New fields in detected schema
    removed_fields: List[SchemaField]    # Fields missing from detected schema
    type_changes: List[tuple]            # (field_name, old_type, new_type)
    nullability_changes: List[tuple]     # (field_name, old_nullable, new_nullable)

class SchemaComparator:
    """Compare two Spark schemas"""

    def compare(self, current: StructType, detected: StructType) -> SchemaDiff:
        """Compare schemas and classify changes"""

        current_fields = {f.name: f for f in current.fields}
        detected_fields = {f.name: f for f in detected.fields}

        # Identify changes
        added = set(detected_fields.keys()) - set(current_fields.keys())
        removed = set(current_fields.keys()) - set(detected_fields.keys())
        common = set(current_fields.keys()) & set(detected_fields.keys())

        type_changes = []
        nullability_changes = []

        for field_name in common:
            curr_field = current_fields[field_name]
            det_field = detected_fields[field_name]

            # Check type changes
            if curr_field.dataType != det_field.dataType:
                type_changes.append((field_name, curr_field.dataType, det_field.dataType))

            # Check nullability changes
            if curr_field.nullable != det_field.nullable:
                nullability_changes.append((field_name, curr_field.nullable, det_field.nullable))

        # Classify compatibility
        is_breaking = (
            len(removed) > 0 or                           # Removed fields
            any(self.is_type_narrowing(old, new) for _, old, new in type_changes) or  # Narrowing type
            any(old and not new for _, old, new in nullability_changes)  # Made non-nullable
        )

        is_compatible = not is_breaking
        has_changes = len(added) > 0 or len(type_changes) > 0 or len(nullability_changes) > 0

        return SchemaDiff(
            is_compatible=is_compatible,
            is_breaking=is_breaking,
            has_changes=has_changes,
            added_fields=[SchemaField(name, str(detected_fields[name].dataType), detected_fields[name].nullable) for name in added],
            removed_fields=[SchemaField(name, str(current_fields[name].dataType), current_fields[name].nullable) for name in removed],
            type_changes=type_changes,
            nullability_changes=nullability_changes
        )

    def is_type_narrowing(self, old_type, new_type) -> bool:
        """Check if type change is narrowing (breaking)"""

        # Examples of narrowing (breaking):
        # - LongType → IntegerType (potential data loss)
        # - StringType → IntegerType (parse errors)
        # - StructType → StringType (loss of structure)

        type_hierarchy = {
            'string': ['int', 'long', 'double', 'boolean', 'timestamp'],
            'double': ['int', 'long'],
            'long': ['int'],
        }

        old_type_name = str(old_type).lower()
        new_type_name = str(new_type).lower()

        # Check if new type is narrower
        for wider, narrower_types in type_hierarchy.items():
            if wider in old_type_name and any(n in new_type_name for n in narrower_types):
                return True

        return old_type != new_type  # Default: any type change is considered narrowing
```

#### Handling Compatible Changes (Auto-Merge)

```python
def handle_compatible_change(self, file_path: str, new_schema: StructType, diff: SchemaDiff):
    """Automatically handle compatible schema changes"""

    # Merge schemas (union of all fields)
    merged_schema = self.merge_schemas(self.current_schema, new_schema)

    # Record schema evolution event
    schema_version = self.create_schema_version(
        ingestion_id=self.ingestion.id,
        version=self.ingestion.schema_version + 1,
        schema_json=merged_schema.json(),
        affected_files=[file_path],
        resolution_type='auto_merge',
        changes_summary=f"Added fields: {[f.name for f in diff.added_fields]}"
    )

    # Update ingestion with new schema
    self.ingestion.schema_json = merged_schema.json()
    self.ingestion.schema_version += 1
    self.db.commit()

    logger.info(f"Auto-merged schema for {file_path}. New fields: {diff.added_fields}")

    return merged_schema

def merge_schemas(self, schema1: StructType, schema2: StructType) -> StructType:
    """Merge two schemas (union of fields, handle conflicts)"""

    fields_dict = {}

    # Add all fields from schema1
    for field in schema1.fields:
        fields_dict[field.name] = field

    # Add new fields from schema2, handle conflicts
    for field in schema2.fields:
        if field.name in fields_dict:
            existing = fields_dict[field.name]

            # If types differ, use common supertype or string
            if existing.dataType != field.dataType:
                # Promote to string if types incompatible
                fields_dict[field.name] = StructField(
                    field.name,
                    StringType(),  # Safe common type
                    nullable=True   # Make nullable to be safe
                )
            else:
                # If types match, make nullable if either is nullable
                fields_dict[field.name] = StructField(
                    field.name,
                    field.dataType,
                    nullable=(existing.nullable or field.nullable)
                )
        else:
            # New field - add as nullable
            fields_dict[field.name] = StructField(
                field.name,
                field.dataType,
                nullable=True  # New fields must be nullable for backward compat
            )

    return StructType(list(fields_dict.values()))
```

#### Handling Breaking Changes (Manual Approval)

```python
def handle_breaking_change(self, file_path: str, new_schema: StructType, diff: SchemaDiff) -> ProcessResult:
    """Handle breaking schema changes (requires user approval)"""

    # Record schema evolution event
    schema_version = self.create_schema_version(
        ingestion_id=self.ingestion.id,
        version=self.ingestion.schema_version + 1,
        schema_json=new_schema.json(),
        affected_files=[file_path],
        resolution_type='pending_approval',
        changes_summary=self.format_breaking_changes(diff)
    )

    # Mark file as SKIPPED with schema mismatch reason
    self.db.add(ProcessedFile(
        ingestion_id=self.ingestion.id,
        file_path=file_path,
        status='SKIPPED',
        discovered_at=datetime.utcnow(),
        error_message=f"Breaking schema change detected: {diff.removed_fields or diff.type_changes}",
        error_type='SchemaEvolutionError'
    ))
    self.db.commit()

    # Send alert to user
    self.send_schema_change_alert(
        ingestion_id=self.ingestion.id,
        schema_version_id=schema_version.id,
        changes=diff,
        action_required=True
    )

    logger.warning(f"Breaking schema change in {file_path}. File skipped, awaiting user approval.")

    return ProcessResult(status='SKIPPED', reason='breaking_schema_change')

def format_breaking_changes(self, diff: SchemaDiff) -> str:
    """Format breaking changes for user notification"""

    changes = []

    if diff.removed_fields:
        changes.append(f"Removed fields: {[f.name for f in diff.removed_fields]}")

    if diff.type_changes:
        changes.append(f"Type changes: {[(name, str(old), str(new)) for name, old, new in diff.type_changes]}")

    if diff.nullability_changes:
        non_nullable = [(name, old, new) for name, old, new in diff.nullability_changes if old and not new]
        if non_nullable:
            changes.append(f"Made non-nullable: {[name for name, _, _ in non_nullable]}")

    return "; ".join(changes)
```

#### User Approval Workflow

```python
# API endpoint for user to approve schema changes
@app.post("/api/v1/ingestions/{ingestion_id}/schema-versions/{version_id}/approve")
def approve_schema_change(ingestion_id: str, version_id: str, action: str):
    """
    User approves or rejects schema change

    Actions:
    - 'accept': Accept new schema, reprocess skipped files
    - 'merge': Merge schemas (make removed fields optional), reprocess
    - 'reject': Reject change, permanently skip affected files
    """

    schema_version = db.query(SchemaVersion).get(version_id)
    ingestion = db.query(Ingestion).get(ingestion_id)

    if action == 'accept':
        # Update ingestion schema to new version
        ingestion.schema_json = schema_version.schema_json
        ingestion.schema_version = schema_version.version

        # Mark schema version as resolved
        schema_version.resolution_type = 'accepted'
        schema_version.resolved_at = datetime.utcnow()
        schema_version.resolved_by = current_user.id

        # Reprocess skipped files with new schema
        skipped_files = db.query(ProcessedFile) \
            .filter_by(
                ingestion_id=ingestion_id,
                status='SKIPPED',
                error_type='SchemaEvolutionError'
            ) \
            .all()

        for file in skipped_files:
            file.status = 'PENDING'  # Will be retried in next run

        db.commit()

        return {"status": "accepted", "files_to_reprocess": len(skipped_files)}

    elif action == 'merge':
        # Merge old and new schemas
        old_schema = StructType.fromJson(json.loads(ingestion.schema_json))
        new_schema = StructType.fromJson(json.loads(schema_version.schema_json))
        merged = merge_schemas(old_schema, new_schema)

        ingestion.schema_json = merged.json()
        ingestion.schema_version = schema_version.version

        schema_version.resolution_type = 'merged'
        schema_version.resolved_at = datetime.utcnow()

        # Reprocess skipped files
        # ... (same as 'accept')

    elif action == 'reject':
        # Permanently skip affected files
        schema_version.resolution_type = 'rejected'
        schema_version.resolved_at = datetime.utcnow()

        affected_files = db.query(ProcessedFile) \
            .filter_by(ingestion_id=ingestion_id) \
            .filter(ProcessedFile.file_path.in_(schema_version.affected_files)) \
            .all()

        for file in affected_files:
            file.status = 'REJECTED'

        db.commit()

        return {"status": "rejected", "files_skipped": len(affected_files)}
```

---

### Option 3: Fixed Schema with Manual Updates

#### Problem: Spark Streaming Requires Fixed Schema

```python
# Spark readStream REQUIRES schema upfront
df = spark.readStream \
    .format("json") \
    .schema(predefined_schema) \  # MUST provide schema
    .load("s3://bucket/path/")

# Schema inference NOT supported in streaming
# df = spark.readStream.option("inferSchema", "true")  # ❌ DOES NOT WORK
```

#### Behavior When Schema Changes

```python
# Scenario: AWS API adds new field "region" to output

# Files 1-1000: Schema A (no "region" field)
# Files 1001+: Schema B (includes "region" field)

# What happens:

# Option 3.1: Schema evolution OFF (default)
df = spark.readStream.format("json").schema(schema_A).load(source)

# Processing:
# - Files 1-1000: ✅ SUCCESS (schema matches)
# - File 1001: ✅ SUCCESS (but "region" field silently dropped!)
# - Files 1002+: ✅ SUCCESS (but "region" field silently dropped!)

# Result: Data loss! New field ignored.
```

```python
# Option 3.2: Schema evolution ON (mergeSchema)
df = spark.readStream \
    .format("json") \
    .schema(schema_A) \
    .option("mergeSchema", "true") \  # Attempt to merge schemas
    .load(source)

# Processing:
# - Files 1-1000: ✅ SUCCESS
# - File 1001: ❌ BATCH FAILS (schema mismatch in streaming mode)
# - Files 1002+: ⏸️ Not processed (batch 1001 blocking)

# Error message:
# "pyspark.sql.utils.AnalysisException: A schema mismatch detected
# when restoring from checkpoint location."

# Result: Entire ingestion blocked!
```

#### Manual Recovery Process

```bash
# Step 1: User notices ingestion stopped
# Step 2: Check Spark logs, find schema mismatch error
# Step 3: Manually update schema in ingestion config

# Update schema via API
curl -X PUT /api/v1/ingestions/123 \
  -d '{
    "schema_json": "{ ... new schema with region field ... }"
  }'

# Step 4: Restart ingestion job
# Step 5: Checkpoint ensures files 1-1000 not re-processed
# Step 6: Files 1001+ processed with new schema

# Downtime: Hours to days (depending on detection time)
```

#### Attempted Workarounds (All Have Issues)

**Workaround 1: Use permissive mode**
```python
# Read with lax schema
df = spark.readStream \
    .format("json") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt") \
    .schema(schema_A) \
    .load(source)

# Result:
# - Files with extra fields: Fields stored in _corrupt column as raw JSON string
# - Lost: Type safety, query ability
# - User must manually parse _corrupt column (defeats purpose of schema)
```

**Workaround 2: Over-specified schema**
```python
# Pre-define schema with ALL possible future fields
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("region", StringType(), True),  # Not in API yet, but might be added
    StructField("department", StringType(), True),  # Not in API yet
    StructField("manager", StringType(), True),  # Not in API yet
    # ... 50 more "just in case" fields
])

# Problems:
# - Can't predict all future fields
# - Cluttered schema with mostly-null columns
# - Doesn't handle type changes
```

**Workaround 3: Batch schema inference per-batch**
```python
# In foreachBatch, re-infer schema
def process_with_schema_detection(batch_df, batch_id):
    # ❌ batch_df already read with fixed schema!
    # Can't re-infer at this point, data already parsed

    # Would need to:
    # 1. Get raw file paths from batch
    # 2. Re-read files with inferSchema=true
    # 3. Compare schemas
    # 4. Handle mismatches

    # This defeats the purpose of streaming - you're doing batch processing anyway!
```

---

### Schema Evolution Comparison Summary

| Aspect | Option 2 | Option 3 |
|--------|----------|----------|
| **Schema flexibility** | ✅ Per-file inference | ❌ Fixed schema required |
| **Auto-detection** | ✅ Detect changes per file | ❌ Fails on change |
| **Compatible changes** | ✅ Auto-merge (new optional fields) | ❌ Silently drops new fields OR fails |
| **Breaking changes** | ✅ Skip file, alert user, await approval | ❌ Entire job stops |
| **Downtime on change** | ⏱️ 0 (files skipped, not blocked) | ⏱️ Hours to days (manual intervention) |
| **User control** | ✅ Approve/reject/merge per change | ❌ Only option: update schema manually |
| **Data loss risk** | ✅ None (changes logged, files skipped) | ⚠️ High (new fields silently dropped) |
| **Audit trail** | ✅ SchemaVersion table tracks all changes | ❌ No automatic tracking |
| **Testing required** | ⚠️ Moderate (schema comparison logic) | ⚠️ High (manual schema management) |

---

### Recommendation for Schema Evolution

Given that **schema change handling is very important** for AWS/Snyk data dumps:

**Option 2 is strongly recommended** because:

1. **Automatic detection**: No manual monitoring required
2. **Graceful degradation**: Breaking changes skip files, not entire runs
3. **User empowerment**: Approve/reject/merge changes via UI
4. **No data loss**: All changes logged and auditable
5. **Zero downtime**: Compatible changes handled automatically

Option 3 is **not viable** for environments with frequent schema changes.

---

## Event-Driven File Discovery: S3 Notifications

### The Problem with Polling (Current Approach)

**Current approach** (both options use this):
```python
# Every hour, list all files in S3
while True:
    all_files = s3.list_objects_v2(Bucket='bucket', Prefix='data/')
    # 100,000 files × 24 runs/day = 2.4M API calls/day
    # Cost: $12/day just for file listing!

    new_files = [f for f in all_files if not processed(f)]
    process(new_files)

    sleep(3600)  # Wait 1 hour
```

**Problems:**
- **High cost**: List operations on large buckets are expensive
- **High latency**: Files sit unprocessed until next poll (up to 1 hour wait)
- **Inefficient**: Scanning millions of files to find handful of new ones
- **S3 rate limits**: Can hit throttling on frequent ListObjects calls

---

### Event-Driven Architecture with S3 Notifications

**New approach:**
```python
# S3 sends notification when file arrives
# System processes file immediately (seconds, not hours)
# No polling needed!

┌─────────────┐
│   AWS S3    │  File uploaded: data.json
│   Bucket    │
└──────┬──────┘
       │ S3 Event Notification
       ▼
┌─────────────┐
│  SNS Topic  │  (optional fanout)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  SQS Queue  │  Queue: {bucket, key, size, timestamp}
└──────┬──────┘
       │ Poll queue (every 5-10 seconds)
       ▼
┌─────────────────────────────────┐
│  IOMETE Autoloader Worker      │
│  1. Read message from SQS       │
│  2. Check if already processed  │
│  3. Process file with Spark     │
│  4. Mark as processed in DB     │
│  5. Delete message from SQS     │
└─────────────────────────────────┘
```

---

### Implementation: Option 2 with SQS Integration

#### Step 1: Configure S3 Event Notifications

```json
{
  "LambdaFunctionConfigurations": [],
  "TopicConfigurations": [],
  "QueueConfigurations": [
    {
      "Id": "iomete-autoloader-file-events",
      "QueueArn": "arn:aws:sqs:us-east-1:123456789:iomete-file-events",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "data/aws-dumps/"
            },
            {
              "Name": "suffix",
              "Value": ".json"
            }
          ]
        }
      }
    }
  ]
}
```

#### Step 2: SQS Message Consumer

```python
# app/services/sqs_file_consumer.py

import boto3
import json
from typing import List, Dict
from datetime import datetime

class SQSFileConsumer:
    """Consumes S3 file events from SQS queue"""

    def __init__(self, queue_url: str, ingestion_id: str):
        self.sqs = boto3.client('sqs')
        self.queue_url = queue_url
        self.ingestion_id = ingestion_id

    def poll_messages(self, max_messages: int = 10) -> List[Dict]:
        """
        Poll SQS queue for file events

        Returns list of file events with metadata
        """
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,  # Up to 10
            WaitTimeSeconds=10,  # Long polling (reduce API calls)
            MessageAttributeNames=['All']
        )

        if 'Messages' not in response:
            return []

        file_events = []
        for message in response['Messages']:
            try:
                event = self.parse_s3_event(message['Body'])
                file_events.append({
                    'file_path': event['file_path'],
                    'bucket': event['bucket'],
                    'key': event['key'],
                    'size': event['size'],
                    'timestamp': event['timestamp'],
                    'etag': event['etag'],
                    'receipt_handle': message['ReceiptHandle']  # For deletion
                })
            except Exception as e:
                logger.error(f"Failed to parse SQS message: {e}")

        return file_events

    def parse_s3_event(self, message_body: str) -> Dict:
        """Parse S3 event notification message"""

        body = json.loads(message_body)

        # S3 event structure:
        # {
        #   "Records": [{
        #     "s3": {
        #       "bucket": {"name": "my-bucket"},
        #       "object": {"key": "data/file.json", "size": 1024, "eTag": "abc123"}
        #     },
        #     "eventTime": "2024-01-15T10:30:00Z"
        #   }]
        # }

        record = body['Records'][0]
        s3_info = record['s3']

        bucket = s3_info['bucket']['name']
        key = s3_info['object']['key']

        return {
            'file_path': f"s3://{bucket}/{key}",
            'bucket': bucket,
            'key': key,
            'size': s3_info['object']['size'],
            'timestamp': record['eventTime'],
            'etag': s3_info['object'].get('eTag')
        }

    def delete_message(self, receipt_handle: str):
        """Delete message from queue after successful processing"""

        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )

    def return_message_to_queue(self, receipt_handle: str, visibility_timeout: int = 300):
        """
        Return message to queue if processing failed

        Message will become visible again after timeout for retry
        """
        self.sqs.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout  # 5 minutes
        )
```

#### Step 3: Event-Driven Executor

```python
# app/services/event_driven_executor.py

class EventDrivenIngestionExecutor:
    """
    Processes files from SQS events instead of polling S3

    Advantages over polling:
    - Near real-time processing (seconds vs hours)
    - No expensive ListObjects calls
    - Natural backpressure (SQS queue size)
    - Built-in retry (SQS visibility timeout)
    """

    def __init__(self, ingestion: Ingestion):
        self.ingestion = ingestion
        self.sqs_consumer = SQSFileConsumer(
            queue_url=ingestion.sqs_queue_url,
            ingestion_id=ingestion.id
        )
        self.file_processor = BatchFileProcessor(...)
        self.file_state = FileStateService(db)

    def run_continuous(self):
        """
        Continuously poll SQS queue and process files

        This runs as a long-lived process (systemd service, K8s pod, etc.)
        """
        logger.info(f"Starting event-driven ingestion for {self.ingestion.id}")

        while True:
            try:
                # Poll SQS queue
                file_events = self.sqs_consumer.poll_messages(max_messages=10)

                if not file_events:
                    # No messages, sleep briefly then retry
                    time.sleep(1)
                    continue

                # Process events
                for event in file_events:
                    self.process_file_event(event)

            except Exception as e:
                logger.error(f"Error in event loop: {e}")
                time.sleep(5)  # Back off on error

    def run_batch(self, max_messages: int = 100):
        """
        Process batch of messages (for scheduled runs)

        Useful for hybrid approach:
        - Continuous worker for real-time processing
        - Scheduled batch for cleanup (in case worker was down)
        """
        file_events = self.sqs_consumer.poll_messages(max_messages=max_messages)

        for event in file_events:
            self.process_file_event(event)

        return len(file_events)

    def process_file_event(self, event: Dict):
        """Process single file from SQS event"""

        file_path = event['file_path']
        receipt_handle = event['receipt_handle']

        try:
            # Check if already processed (idempotency)
            existing = self.file_state.get_processed_file(
                ingestion_id=self.ingestion.id,
                file_path=file_path
            )

            if existing and existing.status == 'SUCCESS':
                logger.info(f"File {file_path} already processed, skipping")
                self.sqs_consumer.delete_message(receipt_handle)
                return

            # Lock file for processing (prevent duplicate processing)
            file_record = self.file_state.lock_file_for_processing(
                ingestion_id=self.ingestion.id,
                file_path=file_path
            )

            if not file_record:
                # Another worker is processing this
                logger.info(f"File {file_path} locked by another worker")
                return

            # Process file with Spark
            result = self.file_processor.process_single_file(
                ingestion=self.ingestion,
                file_path=file_path,
                file_info=event
            )

            # Mark as successful
            self.file_state.mark_file_success(
                file_record,
                records=result['record_count'],
                bytes_read=event['size']
            )

            # Delete message from queue (success)
            self.sqs_consumer.delete_message(receipt_handle)

            logger.info(f"Successfully processed {file_path}")

        except Exception as e:
            # Mark as failed
            self.file_state.mark_file_failed(file_record, e)

            # Return message to queue for retry (after 5 min visibility timeout)
            self.sqs_consumer.return_message_to_queue(receipt_handle)

            logger.error(f"Failed to process {file_path}: {e}")
```

#### Step 4: Deployment Models

**Model 1: Continuous Worker (Recommended)**
```yaml
# Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: autoloader-worker
spec:
  replicas: 3  # Scale based on queue depth
  template:
    spec:
      containers:
      - name: worker
        image: iomete/autoloader:latest
        command: ["python", "-m", "app.workers.event_driven_worker"]
        env:
        - name: INGESTION_ID
          value: "ingestion-123"
        - name: SQS_QUEUE_URL
          value: "https://sqs.us-east-1.amazonaws.com/123/iomete-files"
```

**Model 2: Scheduled Batch (Hybrid)**
```python
# Cron job runs every 5 minutes
# Processes up to 100 messages per run
# Use for backfill and cleanup

@app.post("/api/v1/ingestions/{id}/process-events")
def process_events(id: str, max_messages: int = 100):
    ingestion = db.query(Ingestion).get(id)
    executor = EventDrivenIngestionExecutor(ingestion)

    processed = executor.run_batch(max_messages=max_messages)

    return {"processed": processed}
```

---

### Option 3: Cannot Easily Integrate with SQS

**Problem:** Spark Structured Streaming is designed for directory-based sources

```python
# Spark readStream works with:
# - File directories (polls for new files)
# - Kafka topics (streaming source)
# - Socket streams
# - NOT SQS queues (no built-in connector)

# Would need custom source:
class SQSStreamingSource(StreamingSource):
    """Custom Spark streaming source for SQS"""

    def __init__(self, queue_url):
        # Implement Spark streaming source interface
        # - latestOffset()
        # - getBatch()
        # - commit()
        # Very complex (200+ lines)
        pass

# Then use:
df = spark.readStream \
    .format("sqs") \  # Custom source
    .option("queueUrl", queue_url) \
    .load()

# Problems:
# - Spark streaming designed for continuous data (logs, events)
# - Files are discrete units (batch semantics better fit)
# - Complex integration, lots of edge cases
# - Defeats purpose of using Spark streaming (simpler to do batch)
```

**Workaround:** Hybrid approach (SQS → trigger Spark batch job)

```python
# SQS consumer triggers Spark job per file
# But this is essentially Option 2! (batch processing)

def sqs_consumer():
    while True:
        messages = sqs.receive_message(...)

        for message in messages:
            file_path = parse_event(message)

            # Trigger Spark batch job (not streaming)
            spark.read.format("json").load(file_path)
            # Write to Iceberg
            # Delete SQS message
```

**Conclusion:** Option 3 doesn't naturally fit event-driven architecture. Would require custom source or batch processing (defeating the purpose).

---

### Event-Driven Benefits Summary

| Aspect | Polling (Current) | Event-Driven (SQS) |
|--------|-------------------|-------------------|
| **Latency** | Minutes to hours (depends on poll frequency) | Seconds (near real-time) |
| **S3 API Calls** | List entire bucket every run (100k+ calls/day) | Zero (S3 sends events) |
| **Cost** | High ($12/day for 100k files) | Low ($0.02/day for SQS) |
| **Scalability** | Limited (rate limits on ListObjects) | High (SQS queues scale infinitely) |
| **Backpressure** | Manual (limit batch size) | Automatic (queue depth) |
| **Retry** | Manual (track failed files) | Automatic (SQS visibility timeout) |
| **Exactly-once** | Complex (DB locking) | Simpler (SQS + idempotency) |
| **Worker scaling** | Complex (coordinate polling) | Easy (multiple consumers read same queue) |
| **Late files** | Caught on next poll | Caught immediately (event sent) |

---

### Recommendation for Event-Driven

For AWS S3 use cases, **event-driven is strongly recommended** because:

1. **10-100x cost reduction**: No more expensive ListObjects calls
2. **Sub-minute latency**: Files processed within seconds of arrival
3. **Better scalability**: Scale workers based on queue depth
4. **Built-in retry**: SQS handles visibility timeout and dead-letter queue
5. **Industry standard**: How AWS recommends processing S3 files

**Option 2 integrates naturally** with SQS:
- SQS consumer is 50 lines of code
- Fits batch processing model perfectly
- Full control over file-level state

**Option 3 requires workarounds**:
- Would need custom Spark streaming source (200+ lines)
- Or revert to batch processing (defeating purpose)

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
| **Schema flexibility (CRITICAL)** | ✅ Per-file inference + auto-merge | ❌ Fixed schema, fails on change |
| **Schema evolution handling** | ✅ Auto-detect, skip, alert, approve | ❌ Manual intervention required |
| **Event-driven support (S3 notifications)** | ✅ Native SQS integration (~50 LOC) | ❌ Custom source required (~200 LOC) |
| **Progress reporting UI** | ✅ (real-time from DB) | ⚠️ (Spark metrics) |
| **Audit compliance** | ✅ (complete trail in DB) | ⚠️ (partial trail) |
| **Minimize operational complexity** | ❌ | ✅ (Spark handles most) |
| **Control over file discovery** | ✅ (custom optimization + SQS) | ⚠️ (Spark options only) |
| **Cost optimization** | ✅ 11% cheaper + SQS savings | ⚠️ (11% more expensive) |
| **Testing complexity** | ❌ (more edge cases) | ✅ (fewer edge cases) |

---

## Final Recommendation

### **REVISED RECOMMENDATION: Option 2 (Batch + PostgreSQL)**

Given the two **critical requirements** you've identified:

1. **Schema change handling is very important**
2. **Event-driven file discovery (S3 notifications) is desired**

**Option 3 is NOT viable** for your use case. Here's why:

---

### Why Option 3 Fails Your Requirements

#### 1. Schema Evolution: Deal-Breaker

**Option 3 Behavior:**
- Requires fixed schema upfront (no inference in streaming)
- When AWS/Snyk API adds new field → **Entire ingestion stops** OR **New fields silently dropped**
- Manual intervention required (hours to days of downtime)
- No automatic detection or user approval workflow

**Your Requirement:** Graceful handling of schema changes without manual intervention

**Verdict:** ❌ **Option 3 cannot meet this requirement**

#### 2. Event-Driven: Poor Fit

**Option 3 Behavior:**
- Spark Structured Streaming designed for directory polling, not SQS events
- Would need custom streaming source implementation (200+ lines, complex)
- Or revert to batch processing (defeats purpose of using streaming)

**Your Requirement:** Integrate with S3 Event Notifications (SQS)

**Verdict:** ❌ **Option 3 makes event-driven architecture unnecessarily complex**

---

### Why Option 2 Excels for Your Requirements

#### 1. Schema Evolution: Built-In

✅ **Per-file schema inference** - Detect changes automatically
✅ **Auto-merge compatible changes** - New optional fields handled without intervention
✅ **Skip breaking changes** - Files with breaking changes skipped, not blocked
✅ **User approval workflow** - API endpoint for approve/reject/merge decisions
✅ **Zero downtime** - Compatible changes auto-merged, breaking changes queued for review
✅ **Complete audit trail** - SchemaVersion table tracks all changes

**Scenario Example:**
```
AWS API adds "region" field to output:
- Option 2: Detects new field, auto-merges schema, continues processing ✅
- Option 3: Job fails OR new field dropped ❌
```

#### 2. Event-Driven: Natural Fit

✅ **Native SQS integration** - 50 lines of code (SQSFileConsumer)
✅ **Idempotent processing** - DB tracks processed files, handles duplicates
✅ **10-100x cost reduction** - No more expensive S3 ListObjects calls
✅ **Sub-minute latency** - Files processed within seconds of upload
✅ **Auto-scaling** - Multiple workers consume same queue
✅ **Built-in retry** - SQS visibility timeout handles failures

**Architecture:**
```
S3 Event → SQS Queue → Worker polls queue → Process file → Mark in DB → Delete from queue
```

---

### Implementation Plan for Option 2

#### Phase 1: Core Implementation (Week 1-2)

```python
# Week 1: Database & File Processing
1. Create ProcessedFile model + migration
2. Implement FileStateService (DB locking, status tracking)
3. Implement BatchFileProcessor (Spark batch read/write)
4. Implement SchemaEvolutionHandler (detect, compare, merge)
5. Add basic error handling

# Week 2: Event-Driven Integration
6. Implement SQSFileConsumer (poll queue, parse events)
7. Implement EventDrivenIngestionExecutor (process events loop)
8. Configure S3 Event Notifications → SQS
9. Deploy worker (K8s deployment or systemd service)
10. Add monitoring (queue depth, processing rate)
```

**Estimated effort:** ~800 lines of code, 2 weeks for 1 developer

#### Phase 2: Schema Evolution UI (Week 3)

```python
11. Add API endpoint: POST /ingestions/{id}/schema-versions/{version_id}/approve
12. Implement schema approval workflow (accept/merge/reject)
13. Add schema change alerts (email/Slack)
14. Create UI for reviewing schema changes
15. Add testing for schema evolution scenarios
```

#### Phase 3: Production Hardening (Week 4)

```python
16. Add comprehensive error handling (network, permissions, OOM)
17. Implement retry logic (exponential backoff, max retries)
18. Add metrics collection (Prometheus)
19. Create dashboards (Grafana: files/sec, errors, queue depth)
20. Load testing (1000+ files/sec)
```

---

### Cost Benefit Analysis

| Aspect | Option 2 (Event-Driven) | Option 3 (Streaming) |
|--------|-------------------------|----------------------|
| **S3 API Calls** | $0.02/day (SQS only) | $12/day (ListObjects) |
| **Latency** | Seconds | Hours (polling) |
| **Schema Change Downtime** | 0 hours | Hours to days |
| **Development Time** | 3-4 weeks | 1 week (but doesn't work!) |
| **Maintenance** | Moderate | Low (Spark handles) |
| **Observability** | Complete | Partial |

**Monthly Savings:**
- S3 API costs: **$360/month** savings (vs polling)
- Downtime prevention: **Priceless** (vs Option 3 manual intervention)

---

### Migration from Current Code (cloudFiles)

**Step-by-step:**

1. **Remove cloudFiles dependency** (~20 lines deleted)
2. **Add ProcessedFile model** (~50 lines)
3. **Create Alembic migration** (CREATE TABLE + indexes)
4. **Implement core services** (~600 lines):
   - FileDiscoveryService (cloud SDKs)
   - FileStateService (DB operations)
   - BatchFileProcessor (Spark batch)
   - SchemaEvolutionHandler (detect/merge)
5. **Add SQS integration** (~150 lines):
   - SQSFileConsumer
   - EventDrivenIngestionExecutor
6. **Configure S3 → SQS** (AWS Console or Terraform)
7. **Deploy worker** (K8s/Docker)
8. **Add monitoring** (Prometheus/Grafana)

**Total effort:** ~800 lines, 3-4 weeks

---

### Decision Summary

**For your specific requirements (schema evolution + event-driven):**

## **Choose Option 2: Batch Processing + PostgreSQL + SQS**

**Key Benefits:**
1. ✅ **Schema changes handled gracefully** (auto-detect, auto-merge, zero downtime)
2. ✅ **Event-driven architecture** (SQS integration, sub-minute latency)
3. ✅ **10-100x cost reduction** (vs polling S3)
4. ✅ **Complete observability** (every file tracked in DB)
5. ✅ **Granular error handling** (retry individual files)

**Trade-offs:**
- ⚠️ More code to write (~800 lines vs 250 for Option 3)
- ⚠️ More complex testing (schema evolution, error scenarios)
- ⚠️ Custom exactly-once logic (vs Spark automatic)

**Why It's Worth It:**
- Schema evolution is **mandatory** for AWS/Snyk use case
- Event-driven is **industry best practice** for S3 file processing
- Option 3 simply **cannot deliver** on these requirements

---

## Conclusion

**TL;DR:**

| Aspect | Option 2 (RECOMMENDED) | Option 3 (Not Viable) |
|--------|------------------------|----------------------|
| **Best for** | Schema evolution + event-driven | Simple streaming (no schema changes) |
| **Implementation** | 800 LOC, 3-4 weeks | 250 LOC, 1 week |
| **Schema Evolution** | ✅ Auto-detect, auto-merge, zero downtime | ❌ Fixed schema, fails on change |
| **Event-Driven** | ✅ Native SQS integration | ❌ Complex custom source |
| **Observability** | ✅ Complete (every file tracked) | ⚠️ Partial (checkpoint + logs) |
| **Cost (with SQS)** | $127/month - $360/month = ❌$233/month saving | $141/month + $360 polling = $501/month |
| **Reliability** | ⚠️ Custom exactly-once | ✅ Spark automatic |
| **Complexity** | High (but necessary) | Low (but insufficient) |

**FINAL VERDICT:**

## **Use Option 2: Batch Processing + PostgreSQL + Event-Driven SQS**

**Reasons:**
1. ✅ **Schema evolution is mandatory** - Option 3 cannot handle gracefully
2. ✅ **Event-driven saves $360/month** - Industry best practice for S3
3. ✅ **Complete observability** - Critical for debugging AWS/Snyk data issues
4. ✅ **Zero downtime on schema changes** - Compatible changes auto-merged
5. ✅ **Per-file retry** - Don't waste compute on already-processed files

**Trade-off Accepted:**
- More code to write (800 vs 250 lines)
- More complex testing
- Custom exactly-once logic

**Why It's Worth It:**
Your requirements (schema evolution + event-driven) make Option 2 the **ONLY viable choice**. Option 3 simply cannot deliver on these critical needs.

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
