# Batch Processing with PostgreSQL State Tracking - Implementation Guide

**Document Purpose:** Complete implementation guide for building a batch-based file ingestion system with PostgreSQL state management

**Approach:** Pure Batch Processing with PostgreSQL as Source of Truth

**Date:** 2025-01-04

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [State Management](#state-management)
5. [Error Handling](#error-handling)
6. [Schema Evolution](#schema-evolution)
7. [Performance Optimization](#performance-optimization)
8. [Observability & Monitoring](#observability--monitoring)
9. [Database Design](#database-design)
10. [Edge Cases & Solutions](#edge-cases--solutions)
11. [Implementation Roadmap](#implementation-roadmap)

---

## Overview

### What is This Approach?

A scheduled batch processing system that:
- Lists files from cloud storage (S3/Azure/GCS) using cloud SDKs
- Tracks processing state in PostgreSQL as the source of truth
- Processes files individually or in batches using Spark
- Writes data to Apache Iceberg tables
- Provides complete visibility and control over every file

### Key Characteristics

**Architecture Pattern:**
```
Scheduler → File Discovery → State Check → Batch Processing → State Update
```

**State of Record:** PostgreSQL `processed_files` table (complete source of truth)

**Exactly-Once Guarantee:** Manual implementation using database locks + idempotency patterns

**Code Complexity:** ~300-500 lines (higher than streaming, but with full control)

### Why Choose This Approach?

✅ **Complete Control:**
- Full visibility into every file's state
- Granular per-file error handling
- Flexible schema evolution per file
- Custom retry logic and filtering

✅ **Flexible Processing:**
- Per-file schema inference
- Support for event-driven ingestion (SQS, webhooks)
- Custom file filtering and validation
- Size-based processing strategies

✅ **Superior Observability:**
- PostgreSQL as queryable source of truth
- Rich metadata tracking (size, duration, errors)
- Easy debugging and auditing
- Real-time progress tracking

✅ **Production-Ready Features:**
- Surgical retries (only failed files)
- Detailed error messages per file
- Skip problematic files without blocking
- Complete audit trail

❌ **Trade-offs:**
- More code to write and maintain
- Manual exactly-once implementation
- Complex edge case handling
- Higher operational complexity

---

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Scheduler (Cron/APScheduler)                │
│                  Triggers at configured intervals                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Ingestion Orchestrator                      │
│  1. Load ingestion configuration from database                  │
│  2. Initialize cloud storage client (S3/Azure/GCS)              │
│  3. Query PostgreSQL: "What files are already processed?"       │
│  4. List files from cloud storage                               │
│  5. Compute diff: new_files = all_files - processed_files       │
│  6. Pass new_files to Batch Processor                           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Batch File Processor                       │
│  FOR EACH file in new_files:                                    │
│    1. Lock file record (SELECT FOR UPDATE SKIP LOCKED)          │
│    2. Read file with Spark (batch API)                          │
│    3. Optionally infer/validate schema                          │
│    4. Write to Iceberg table                                    │
│    5. Mark file as SUCCESS/FAILED in PostgreSQL                 │
│    6. Commit transaction                                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │   PostgreSQL   │
                    │ processed_files│
                    │ Source of Truth│
                    └────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │ Apache Iceberg │
                    │  Data Tables   │
                    └────────────────┘
```

### Data Flow

1. **Discovery Phase:**
   - Scheduler triggers ingestion run
   - File discovery service queries cloud storage
   - Returns list of all files matching pattern

2. **State Reconciliation:**
   - Query PostgreSQL for already-processed files
   - Compute set difference: `new_files = discovered - processed`
   - Filter by size, date, or custom criteria

3. **Processing Phase:**
   - For each new file:
     - Acquire database lock (concurrency-safe)
     - Process with Spark (read → transform → write)
     - Track metrics (duration, records, bytes)
     - Update state (SUCCESS/FAILED)

4. **Completion:**
   - Aggregate run metrics
   - Store run summary in `runs` table
   - Release resources

---

## Core Components

### Component 1: File Discovery Service

**Purpose:** List files from cloud storage with pagination and filtering

**Implementation:**

```python
# app/services/file_discovery_service.py

from typing import List, Dict, Optional
from datetime import datetime
import boto3
from azure.storage.blob import BlobServiceClient
from google.cloud import storage

class FileDiscoveryService:
    """Discovers files from cloud storage across multiple providers"""

    def __init__(self, source_type: str, credentials: Dict):
        self.source_type = source_type
        self.credentials = credentials
        self._client = None

    def list_files(self, bucket: str, prefix: str,
                   since: Optional[datetime] = None,
                   max_files: Optional[int] = None) -> List[Dict]:
        """
        List files from cloud storage

        Args:
            bucket: Bucket/container name
            prefix: Path prefix to filter files
            since: Only return files modified after this date (optimization)
            max_files: Maximum number of files to return

        Returns:
            List of file metadata dicts with keys:
            - path: Full path (e.g., s3://bucket/path/file.json)
            - size: File size in bytes
            - modified_at: Last modified timestamp
            - etag: Entity tag (hash) for change detection
        """
        if self.source_type == 'S3':
            return self._list_files_s3(bucket, prefix, since, max_files)
        elif self.source_type == 'AZURE':
            return self._list_files_azure(bucket, prefix, since, max_files)
        elif self.source_type == 'GCS':
            return self._list_files_gcs(bucket, prefix, since, max_files)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

    def _list_files_s3(self, bucket: str, prefix: str,
                       since: Optional[datetime] = None,
                       max_files: Optional[int] = None) -> List[Dict]:
        """List files from AWS S3 with pagination handling"""
        s3 = boto3.client('s3', **self._get_s3_credentials())
        files = []
        continuation_token = None

        while True:
            # Handle pagination (1000 files per request max)
            kwargs = {
                'Bucket': bucket,
                'Prefix': prefix,
                'MaxKeys': min(1000, max_files - len(files)) if max_files else 1000
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
                    'etag': obj['ETag'].strip('"')  # Remove quotes
                })

                # Stop if we've reached max_files
                if max_files and len(files) >= max_files:
                    return files

            # Check if there are more results
            if not response.get('IsTruncated'):
                break

            continuation_token = response.get('NextContinuationToken')

        return files

    def _list_files_azure(self, container: str, prefix: str,
                          since: Optional[datetime] = None,
                          max_files: Optional[int] = None) -> List[Dict]:
        """List files from Azure Blob Storage"""
        connection_string = self._get_azure_credentials()
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client(container)

        files = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            # Filter by date
            if since and blob.last_modified < since:
                continue

            files.append({
                'path': f"wasbs://{container}/{blob.name}",
                'size': blob.size,
                'modified_at': blob.last_modified,
                'etag': blob.etag.strip('"')
            })

            # Stop if we've reached max_files
            if max_files and len(files) >= max_files:
                break

        return files

    def _list_files_gcs(self, bucket: str, prefix: str,
                        since: Optional[datetime] = None,
                        max_files: Optional[int] = None) -> List[Dict]:
        """List files from Google Cloud Storage"""
        credentials = self._get_gcs_credentials()
        client = storage.Client(credentials=credentials)
        bucket_obj = client.bucket(bucket)
        blobs = bucket_obj.list_blobs(prefix=prefix)

        files = []
        for blob in blobs:
            # Filter by date
            if since and blob.updated < since:
                continue

            files.append({
                'path': f"gs://{bucket}/{blob.name}",
                'size': blob.size,
                'modified_at': blob.updated,
                'etag': blob.etag.strip('"')
            })

            # Stop if we've reached max_files
            if max_files and len(files) >= max_files:
                break

        return files

    def _get_s3_credentials(self) -> Dict:
        """Extract S3 credentials from stored credentials"""
        return {
            'aws_access_key_id': self.credentials.get('aws_access_key_id'),
            'aws_secret_access_key': self.credentials.get('aws_secret_access_key'),
            'region_name': self.credentials.get('aws_region', 'us-east-1')
        }

    def _get_azure_credentials(self) -> str:
        """Extract Azure connection string from stored credentials"""
        return self.credentials.get('azure_connection_string')

    def _get_gcs_credentials(self):
        """Extract GCS credentials from stored credentials"""
        from google.oauth2 import service_account
        credentials_json = self.credentials.get('gcs_service_account_json')
        return service_account.Credentials.from_service_account_info(credentials_json)
```

**Key Features:**
- Multi-cloud support (S3, Azure, GCS)
- Pagination handling for large directories
- Date filtering for performance optimization
- Entity tag (ETag) for change detection
- Configurable max file limits

**Optimization: Partition-Aware Listing**

```python
def list_files_optimized(self, bucket: str, base_prefix: str,
                         last_run_time: datetime) -> List[Dict]:
    """
    Only list partitions modified since last run

    Example: For S3 structure s3://bucket/year=2024/month=01/day=15/*.json
    Only scan partitions created/modified since last_run_time

    Result: 50x faster for large buckets with date-based partitioning
    """
    # Determine partitions to scan based on last_run_time
    partitions_to_scan = self._get_partitions_since(last_run_time)
    # e.g., ['year=2024/month=01/day=15', 'year=2024/month=01/day=16']

    all_files = []
    for partition in partitions_to_scan:
        partition_prefix = f"{base_prefix}/{partition}"
        files = self.list_files(bucket, partition_prefix)
        all_files.extend(files)

    return all_files

def _get_partitions_since(self, since: datetime) -> List[str]:
    """Generate list of partitions to scan based on date"""
    from datetime import timedelta

    partitions = []
    current_date = since.date()
    today = datetime.utcnow().date()

    while current_date <= today:
        partition = f"year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}"
        partitions.append(partition)
        current_date += timedelta(days=1)

    return partitions
```

---

### Component 2: State Tracking Service

**Purpose:** Manage file processing state in PostgreSQL with concurrency safety

**Implementation:**

```python
# app/services/file_state_service.py

from typing import Set, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from app.models.domain import ProcessedFile, ProcessedFileStatus
from datetime import datetime, timedelta

class FileStateService:
    """Manages file processing state in PostgreSQL"""

    def __init__(self, db: Session):
        self.db = db

    def get_processed_files(self, ingestion_id: str,
                           statuses: Optional[List[str]] = None) -> Set[str]:
        """
        Get set of processed file paths for an ingestion

        Args:
            ingestion_id: ID of the ingestion
            statuses: Filter by statuses (default: SUCCESS, SKIPPED)

        Returns:
            Set of file paths that have been processed

        Optimized with index on (ingestion_id, status)
        """
        if statuses is None:
            statuses = ['SUCCESS', 'SKIPPED']

        files = self.db.query(ProcessedFile.file_path) \
            .filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.status.in_(statuses)
            ) \
            .all()

        return {f.file_path for f in files}

    def get_failed_files(self, ingestion_id: str,
                        max_retries: int = 3) -> List[ProcessedFile]:
        """
        Get failed files eligible for retry

        Returns files that:
        - Have status FAILED
        - Have retry_count < max_retries
        """
        return self.db.query(ProcessedFile) \
            .filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.status == 'FAILED',
                ProcessedFile.retry_count < max_retries
            ) \
            .all()

    def get_stale_processing_files(self, ingestion_id: str,
                                   stale_threshold_hours: int = 1) -> List[ProcessedFile]:
        """
        Get files stuck in PROCESSING state (likely crashed worker)

        Args:
            ingestion_id: ID of the ingestion
            stale_threshold_hours: Hours before considering a file stale

        Returns:
            List of files that have been PROCESSING for too long
        """
        threshold = datetime.utcnow() - timedelta(hours=stale_threshold_hours)

        return self.db.query(ProcessedFile) \
            .filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.status == 'PROCESSING',
                ProcessedFile.processing_started_at < threshold
            ) \
            .all()

    def lock_file_for_processing(self, ingestion_id: str,
                                 file_path: str,
                                 file_metadata: Optional[Dict] = None) -> Optional[ProcessedFile]:
        """
        Atomically lock a file for processing

        Uses SELECT FOR UPDATE SKIP LOCKED for concurrency safety.
        Multiple workers can safely call this method simultaneously.

        Args:
            ingestion_id: ID of the ingestion
            file_path: Full path to the file
            file_metadata: Optional metadata (size, modified_at, etag)

        Returns:
            ProcessedFile record if lock acquired, None if already locked
        """
        # Try to get existing record with row-level lock
        file_record = self.db.query(ProcessedFile) \
            .filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.file_path == file_path
            ) \
            .with_for_update(skip_locked=True) \
            .first()

        if file_record:
            # Record exists - check status
            if file_record.status == 'PROCESSING':
                # Another worker is processing this
                return None

            if file_record.status == 'SUCCESS':
                # Already successfully processed - check if file changed
                if file_metadata and self._file_changed(file_record, file_metadata):
                    # File modified since last processing - re-process
                    file_record.status = 'PROCESSING'
                    file_record.retry_count += 1
                    file_record.processing_started_at = datetime.utcnow()
                    self._update_file_metadata(file_record, file_metadata)
                else:
                    # No changes, skip
                    return None
            else:
                # FAILED or PENDING - retry
                file_record.status = 'PROCESSING'
                file_record.retry_count += 1
                file_record.processing_started_at = datetime.utcnow()
                if file_metadata:
                    self._update_file_metadata(file_record, file_metadata)
        else:
            # Create new record
            file_record = ProcessedFile(
                ingestion_id=ingestion_id,
                file_path=file_path,
                status='PROCESSING',
                discovered_at=datetime.utcnow(),
                processing_started_at=datetime.utcnow(),
                retry_count=0
            )
            if file_metadata:
                self._update_file_metadata(file_record, file_metadata)

            self.db.add(file_record)

        self.db.commit()
        return file_record

    def mark_file_success(self, file_record: ProcessedFile,
                         records_ingested: int,
                         bytes_read: int):
        """Mark file as successfully processed"""
        file_record.status = 'SUCCESS'
        file_record.processed_at = datetime.utcnow()
        file_record.records_ingested = records_ingested
        file_record.bytes_read = bytes_read
        file_record.processing_duration_ms = int(
            (datetime.utcnow() - file_record.processing_started_at).total_seconds() * 1000
        )
        file_record.error_message = None
        file_record.error_type = None
        self.db.commit()

    def mark_file_failed(self, file_record: ProcessedFile,
                        error: Exception):
        """Mark file as failed with error details"""
        file_record.status = 'FAILED'
        file_record.error_message = str(error)
        file_record.error_type = type(error).__name__
        file_record.processed_at = datetime.utcnow()
        if file_record.processing_started_at:
            file_record.processing_duration_ms = int(
                (datetime.utcnow() - file_record.processing_started_at).total_seconds() * 1000
            )
        self.db.commit()

    def mark_file_skipped(self, file_record: ProcessedFile,
                         reason: str):
        """Mark file as skipped (e.g., too large, schema mismatch)"""
        file_record.status = 'SKIPPED'
        file_record.error_message = reason
        file_record.processed_at = datetime.utcnow()
        self.db.commit()

    def _file_changed(self, record: ProcessedFile, metadata: Dict) -> bool:
        """Check if file has been modified since last processing"""
        # Compare ETag (most reliable)
        if metadata.get('etag') and record.file_etag:
            return metadata['etag'] != record.file_etag

        # Fallback to modification time
        if metadata.get('modified_at') and record.file_modified_at:
            return metadata['modified_at'] > record.file_modified_at

        # If no metadata to compare, assume not changed
        return False

    def _update_file_metadata(self, record: ProcessedFile, metadata: Dict):
        """Update file record with metadata from cloud storage"""
        if 'size' in metadata:
            record.file_size_bytes = metadata['size']
        if 'modified_at' in metadata:
            record.file_modified_at = metadata['modified_at']
        if 'etag' in metadata:
            record.file_etag = metadata['etag']
```

**Key Features:**
- Atomic locking with `SELECT FOR UPDATE SKIP LOCKED`
- Concurrency-safe for multiple workers
- Stale lock detection and recovery
- File change detection (ETag, modification time)
- Detailed state tracking (SUCCESS, FAILED, PROCESSING, SKIPPED)

---

### Component 3: Batch File Processor

**Purpose:** Process files with Spark and manage state transitions

**Implementation:**

```python
# app/services/batch_processor.py

from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from app.spark.connect_client import SparkConnectClient
from app.services.file_state_service import FileStateService
from app.models.domain import Ingestion, ProcessedFile
import logging

logger = logging.getLogger(__name__)

class BatchFileProcessor:
    """Processes files in batches with PostgreSQL state tracking"""

    def __init__(self,
                 spark_client: SparkConnectClient,
                 state_service: FileStateService,
                 ingestion: Ingestion):
        self.spark = spark_client
        self.state = state_service
        self.ingestion = ingestion

    def process_new_files(self, new_files: List[Dict], run_id: str) -> Dict[str, int]:
        """
        Process list of new files

        Args:
            new_files: List of file metadata dicts from FileDiscoveryService
            run_id: ID of the current run for tracking

        Returns:
            Metrics dict: {success: N, failed: N, skipped: N}
        """
        metrics = {'success': 0, 'failed': 0, 'skipped': 0}

        logger.info(f"Processing {len(new_files)} files for ingestion {self.ingestion.id}")

        for file_info in new_files:
            file_path = file_info['path']

            # Atomically lock file for processing
            file_record = self.state.lock_file_for_processing(
                ingestion_id=self.ingestion.id,
                file_path=file_path,
                file_metadata=file_info
            )

            if not file_record:
                # Another worker is processing or file already done
                logger.debug(f"Skipping {file_path} (locked or completed)")
                metrics['skipped'] += 1
                continue

            try:
                # Process single file
                logger.info(f"Processing file: {file_path}")
                result = self._process_single_file(file_path, file_info)

                # Mark success
                self.state.mark_file_success(
                    file_record,
                    records_ingested=result['record_count'],
                    bytes_read=file_info.get('size', 0)
                )
                metrics['success'] += 1
                logger.info(f"Successfully processed {file_path}: {result['record_count']} records")

            except Exception as e:
                # Mark failure
                self.state.mark_file_failed(file_record, e)
                metrics['failed'] += 1
                logger.error(f"Failed to process {file_path}: {e}", exc_info=True)

        logger.info(f"Batch complete: {metrics}")
        return metrics

    def _process_single_file(self, file_path: str, file_info: Dict) -> Dict:
        """
        Process a single file with Spark batch API

        Returns:
            Dict with processing results: {record_count: N}
        """
        # Determine if we should infer schema or use predefined
        if self.ingestion.schema_json:
            # Use predefined schema
            df = self._read_file_with_schema(file_path)
        else:
            # Infer schema from file
            df = self._read_file_infer_schema(file_path)

            # Optionally handle schema evolution
            if self.ingestion.enable_schema_evolution:
                self._handle_schema_evolution(df.schema, file_path)

        # Get record count before write (for metrics)
        record_count = df.count()

        if record_count == 0:
            logger.warning(f"File {file_path} is empty, skipping write")
            return {'record_count': 0}

        # Write to Iceberg table
        self._write_to_iceberg(df)

        return {'record_count': record_count}

    def _read_file_with_schema(self, file_path: str) -> DataFrame:
        """Read file with predefined schema"""
        reader = self.spark.session.read \
            .format(self.ingestion.format_type.value)

        # Apply schema if available
        if self.ingestion.schema_json:
            from pyspark.sql.types import StructType
            import json
            schema = StructType.fromJson(json.loads(self.ingestion.schema_json))
            reader = reader.schema(schema)

        # Apply format options
        for key, value in self.ingestion.format_options.items():
            reader = reader.option(key, value)

        return reader.load(file_path)

    def _read_file_infer_schema(self, file_path: str) -> DataFrame:
        """Read file with schema inference"""
        reader = self.spark.session.read \
            .format(self.ingestion.format_type.value) \
            .option("inferSchema", "true")

        # Apply format options
        for key, value in self.ingestion.format_options.items():
            reader = reader.option(key, value)

        return reader.load(file_path)

    def _write_to_iceberg(self, df: DataFrame):
        """Write DataFrame to Iceberg table"""
        table_identifier = f"{self.ingestion.catalog}.{self.ingestion.database}.{self.ingestion.table}"

        writer = df.write.format("iceberg")

        # Apply write mode
        if self.ingestion.write_mode:
            writer = writer.mode(self.ingestion.write_mode.value)
        else:
            writer = writer.mode("append")  # Default

        # Apply partitioning if configured
        if self.ingestion.partition_columns:
            writer = writer.partitionBy(*self.ingestion.partition_columns)

        # Apply table properties
        if self.ingestion.table_properties:
            for key, value in self.ingestion.table_properties.items():
                writer = writer.option(key, value)

        # Write
        writer.save(table_identifier)

    def _handle_schema_evolution(self, detected_schema, file_path: str):
        """
        Handle schema evolution if enabled

        This is a placeholder - full implementation in Schema Evolution section
        """
        # TODO: Implement schema comparison and evolution logic
        pass
```

**Key Features:**
- Per-file processing with state tracking
- Automatic locking and concurrency handling
- Schema inference or predefined schema
- Flexible write modes (append, overwrite, upsert)
- Comprehensive error handling and metrics

---

## State Management

### Exactly-Once Guarantees

**Challenge:** Coordinate two separate transactional systems:
1. PostgreSQL transaction (file marked as processed)
2. Iceberg transaction (data written to table)

**Problem Scenarios:**

```python
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
# Result: Data written but file not marked → DUPLICATE on retry

# Scenario C: Process crashes between Iceberg write and DB commit
df.write.iceberg(table)  # ← SUCCESS
# CRASH HERE
mark_success(file_record)  # Never reached
# Result: Data written but file not marked → DUPLICATE on retry
```

### Solution: Idempotent Writes + 2-Phase Commit Simulation

```python
def process_file_idempotent(file_path: str, file_metadata: Dict,
                           ingestion: Ingestion, state_service: FileStateService):
    """
    Idempotent file processing with crash recovery

    Key insight: Iceberg supports merge/upsert, making duplicate writes safe
    if data has unique keys
    """
    db = state_service.db

    # Phase 1: Mark file as PROCESSING (or get existing record)
    # This happens in a separate transaction
    file_record = state_service.lock_file_for_processing(
        ingestion.id, file_path, file_metadata
    )

    if not file_record:
        # Already processed or locked by another worker
        return

    # Phase 2: Process file (OUTSIDE database transaction)
    # If this crashes, file stays in PROCESSING state and can be retried
    try:
        df = spark.read.format(ingestion.format_type.value).load(file_path)

        # CRITICAL: Use merge/upsert if Iceberg table has unique keys
        # This makes writes idempotent even if retried after crash
        table = f"{ingestion.catalog}.{ingestion.database}.{ingestion.table}"

        if ingestion.merge_keys:
            # Idempotent upsert mode
            df.writeTo(table) \
                .using("iceberg") \
                .tableProperty("write.upsert.enabled", "true") \
                .option("merge-schema", "true") \
                .append()
        else:
            # Append mode: NOT fully idempotent, but acceptable for many use cases
            # Duplicates can be deduplicated later using file_path metadata
            df.write.format("iceberg").mode("append").save(table)

        record_count = df.count()

    except Exception as e:
        # Phase 3a: Mark as FAILED (separate transaction)
        state_service.mark_file_failed(file_record, e)
        raise

    # Phase 3b: Mark as SUCCESS (separate transaction)
    state_service.mark_file_success(
        file_record,
        records_ingested=record_count,
        bytes_read=file_metadata.get('size', 0)
    )
```

**Guarantees:**

✅ **No Lost Data:** If Iceberg write succeeds, retry will either:
- Skip file (if DB updated successfully)
- Re-write same data (idempotent if using merge/upsert)

✅ **No Stuck Files:** Stale PROCESSING records can be detected and retried

✅ **Crash Recovery:** System recovers gracefully from crashes at any point

❌ **Limitations:**
- Not true exactly-once unless Iceberg table supports merge/upsert
- Requires cleanup job for stale PROCESSING records
- Works best with data that has natural unique keys (event IDs, timestamps)

### Stale Lock Cleanup Job

```python
def cleanup_stale_locks(state_service: FileStateService,
                        ingestion_id: str,
                        stale_threshold_hours: int = 2):
    """
    Periodic job to clean up stale PROCESSING records

    Run this every hour to recover from crashed workers
    """
    stale_files = state_service.get_stale_processing_files(
        ingestion_id, stale_threshold_hours
    )

    logger.warning(f"Found {len(stale_files)} stale PROCESSING records")

    for file_record in stale_files:
        # Reset to PENDING for retry
        file_record.status = 'PENDING'
        file_record.error_message = f"Reset from stale PROCESSING state (started {file_record.processing_started_at})"

    state_service.db.commit()

    logger.info(f"Reset {len(stale_files)} stale files to PENDING")
```

---

## Error Handling

### Per-File Error Handling Strategy

**Advantage:** One corrupt file doesn't block 999 good files

**Implementation:**

```python
def process_files_with_granular_errors(files: List[Dict],
                                       processor: BatchFileProcessor,
                                       run_id: str) -> Dict:
    """
    Process files with per-file error isolation

    Result:
    - 999 files marked SUCCESS
    - 1 file marked FAILED with specific error
    - User can retry only failed files
    """
    results = {
        'total': len(files),
        'success': 0,
        'failed': 0,
        'skipped': 0,
        'errors': []
    }

    for file_info in files:
        file_path = file_info['path']

        try:
            # Attempt to process file
            file_record = processor.state.lock_file_for_processing(
                processor.ingestion.id, file_path, file_info
            )

            if not file_record:
                results['skipped'] += 1
                continue

            # Apply pre-processing filters
            if not processor._should_process_file(file_info):
                processor.state.mark_file_skipped(
                    file_record,
                    reason="Failed pre-processing checks"
                )
                results['skipped'] += 1
                continue

            # Process file
            result = processor._process_single_file(file_path, file_info)
            processor.state.mark_file_success(
                file_record,
                records_ingested=result['record_count'],
                bytes_read=file_info.get('size', 0)
            )
            results['success'] += 1

        except CorruptFileException as e:
            # Corrupt file - permanent failure, don't retry
            processor.state.mark_file_failed(file_record, e)
            results['failed'] += 1
            results['errors'].append({
                'file': file_path,
                'error_type': 'corrupt',
                'error': str(e),
                'retryable': False
            })
            logger.error(f"Corrupt file {file_path}: {e}")

        except SchemaException as e:
            # Schema mismatch - skip and alert user
            processor.state.mark_file_skipped(
                file_record,
                reason=f"Schema mismatch: {e}"
            )
            results['skipped'] += 1
            results['errors'].append({
                'file': file_path,
                'error_type': 'schema_mismatch',
                'error': str(e),
                'retryable': False,
                'action_required': True
            })
            logger.warning(f"Schema mismatch {file_path}: {e}")

        except TransientException as e:
            # Network timeout, temporary Iceberg lock, etc. - retry
            processor.state.mark_file_failed(file_record, e)
            results['failed'] += 1
            results['errors'].append({
                'file': file_path,
                'error_type': 'transient',
                'error': str(e),
                'retryable': True
            })
            logger.warning(f"Transient error {file_path}: {e}")

        except Exception as e:
            # Unknown error - mark failed and retry
            processor.state.mark_file_failed(file_record, e)
            results['failed'] += 1
            results['errors'].append({
                'file': file_path,
                'error_type': 'unknown',
                'error': str(e),
                'retryable': True
            })
            logger.error(f"Unexpected error {file_path}: {e}", exc_info=True)

    return results
```

### Pre-Processing Filters

```python
class BatchFileProcessor:
    """Extended with pre-processing filters"""

    def _should_process_file(self, file_info: Dict) -> bool:
        """
        Apply filters before processing

        Returns False if file should be skipped
        """
        # Check 1: File size limits
        if self.ingestion.max_file_size_gb:
            file_size_gb = file_info.get('size', 0) / (1024**3)
            if file_size_gb > self.ingestion.max_file_size_gb:
                logger.warning(
                    f"File {file_info['path']} too large: "
                    f"{file_size_gb:.2f}GB > {self.ingestion.max_file_size_gb}GB"
                )
                return False

        # Check 2: File name pattern
        if self.ingestion.exclude_pattern:
            import re
            if re.search(self.ingestion.exclude_pattern, file_info['path']):
                logger.debug(f"File {file_info['path']} matches exclude pattern")
                return False

        # Check 3: File age
        if self.ingestion.max_file_age_days:
            from datetime import datetime, timedelta
            max_age = datetime.utcnow() - timedelta(days=self.ingestion.max_file_age_days)
            if file_info.get('modified_at') and file_info['modified_at'] < max_age:
                logger.debug(f"File {file_info['path']} too old")
                return False

        return True
```

### Retry Strategy

```python
def retry_failed_files(ingestion_id: str,
                       state_service: FileStateService,
                       processor: BatchFileProcessor,
                       max_retries: int = 3) -> Dict:
    """
    Retry only failed files (surgical retry)

    Called manually by user or automatically on next scheduled run
    """
    failed_files = state_service.get_failed_files(ingestion_id, max_retries)

    if not failed_files:
        logger.info(f"No failed files to retry for ingestion {ingestion_id}")
        return {'retried': 0}

    logger.info(f"Retrying {len(failed_files)} failed files for ingestion {ingestion_id}")

    # Convert ProcessedFile records to file_info dicts
    files_to_retry = [
        {
            'path': f.file_path,
            'size': f.file_size_bytes,
            'modified_at': f.file_modified_at,
            'etag': f.file_etag
        }
        for f in failed_files
    ]

    # Process with exponential backoff
    return processor.process_new_files(files_to_retry, run_id=f"retry-{datetime.utcnow().isoformat()}")
```

---

## Schema Evolution

### Per-File Schema Detection

**Why This Matters:**
- API vendors add new fields without notice
- Mixed API versions produce different schemas
- Need to handle schema changes without downtime

**Architecture:**

```python
# app/services/schema_evolution_handler.py

from pyspark.sql.types import StructType, StructField, DataType
from typing import List, Tuple
from dataclasses import dataclass
import json

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

    added_fields: List[SchemaField]
    removed_fields: List[SchemaField]
    type_changes: List[Tuple[str, str, str]]  # (field_name, old_type, new_type)
    nullability_changes: List[Tuple[str, bool, bool]]  # (field_name, old_nullable, new_nullable)

class SchemaEvolutionHandler:
    """Handles schema changes on a per-file basis"""

    def __init__(self, ingestion: Ingestion, db: Session):
        self.ingestion = ingestion
        self.db = db
        self.current_schema = self._load_current_schema()

    def _load_current_schema(self) -> Optional[StructType]:
        """Load current schema from ingestion config"""
        if self.ingestion.schema_json:
            return StructType.fromJson(json.loads(self.ingestion.schema_json))
        return None

    def process_file_with_schema_detection(self,
                                           spark: SparkSession,
                                           file_path: str) -> Tuple[DataFrame, bool]:
        """
        Process file with automatic schema evolution detection

        Returns:
            (DataFrame, schema_changed: bool)
        """
        # Step 1: Infer schema from file
        file_df = spark.read.format("json") \
            .option("inferSchema", "true") \
            .load(file_path)

        detected_schema = file_df.schema

        # Step 2: First file - establish baseline
        if self.current_schema is None:
            self._save_schema(detected_schema, version=1)
            self.current_schema = detected_schema
            return file_df, True

        # Step 3: Compare schemas
        diff = self._compare_schemas(self.current_schema, detected_schema)

        # Step 4: Handle based on compatibility
        if not diff.has_changes:
            # No changes - use current schema
            return file_df, False

        elif diff.is_compatible:
            # Compatible change (new optional fields)
            merged_schema = self._handle_compatible_change(file_path, detected_schema, diff)
            self.current_schema = merged_schema

            # Re-read file with merged schema
            file_df = spark.read.schema(merged_schema).format("json").load(file_path)
            return file_df, True

        else:
            # Breaking change - requires manual approval
            self._handle_breaking_change(file_path, detected_schema, diff)
            raise SchemaEvolutionError(
                f"Breaking schema change detected in {file_path}. "
                f"File skipped, awaiting user approval."
            )

    def _compare_schemas(self, current: StructType, detected: StructType) -> SchemaDiff:
        """Compare two schemas and classify changes"""
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
            if str(curr_field.dataType) != str(det_field.dataType):
                type_changes.append((
                    field_name,
                    str(curr_field.dataType),
                    str(det_field.dataType)
                ))

            # Check nullability changes
            if curr_field.nullable != det_field.nullable:
                nullability_changes.append((
                    field_name,
                    curr_field.nullable,
                    det_field.nullable
                ))

        # Classify compatibility
        is_breaking = (
            len(removed) > 0 or  # Removed fields
            any(self._is_type_narrowing(old, new) for _, old, new in type_changes) or  # Narrowing type
            any(old and not new for _, old, new in nullability_changes)  # Made non-nullable
        )

        is_compatible = not is_breaking
        has_changes = len(added) > 0 or len(type_changes) > 0 or len(nullability_changes) > 0

        return SchemaDiff(
            is_compatible=is_compatible,
            is_breaking=is_breaking,
            has_changes=has_changes,
            added_fields=[
                SchemaField(name, str(detected_fields[name].dataType), detected_fields[name].nullable)
                for name in added
            ],
            removed_fields=[
                SchemaField(name, str(current_fields[name].dataType), current_fields[name].nullable)
                for name in removed
            ],
            type_changes=type_changes,
            nullability_changes=nullability_changes
        )

    def _is_type_narrowing(self, old_type: str, new_type: str) -> bool:
        """Check if type change is narrowing (breaking)"""
        # Type hierarchy: string > double > long > int
        type_hierarchy = {
            'string': ['double', 'long', 'int', 'boolean', 'timestamp'],
            'double': ['long', 'int'],
            'long': ['int'],
        }

        old_lower = old_type.lower()
        new_lower = new_type.lower()

        for wider, narrower_types in type_hierarchy.items():
            if wider in old_lower and any(n in new_lower for n in narrower_types):
                return True

        return False

    def _handle_compatible_change(self, file_path: str,
                                  new_schema: StructType,
                                  diff: SchemaDiff) -> StructType:
        """Automatically merge compatible schema changes"""
        # Merge schemas (union of all fields)
        merged_schema = self._merge_schemas(self.current_schema, new_schema)

        # Record schema evolution event
        from app.models.domain import SchemaVersion
        schema_version = SchemaVersion(
            ingestion_id=self.ingestion.id,
            version=self.ingestion.schema_version + 1,
            schema_json=merged_schema.json(),
            detected_at=datetime.utcnow(),
            affected_files=[file_path],
            resolution_type='auto_merge',
            changes_summary=f"Added fields: {[f.name for f in diff.added_fields]}"
        )
        self.db.add(schema_version)

        # Update ingestion
        self.ingestion.schema_json = merged_schema.json()
        self.ingestion.schema_version += 1
        self.db.commit()

        logger.info(f"Auto-merged schema for {file_path}. New fields: {[f.name for f in diff.added_fields]}")

        return merged_schema

    def _merge_schemas(self, schema1: StructType, schema2: StructType) -> StructType:
        """Merge two schemas (union of fields)"""
        fields_dict = {}

        # Add all fields from schema1
        for field in schema1.fields:
            fields_dict[field.name] = field

        # Add/merge fields from schema2
        for field in schema2.fields:
            if field.name in fields_dict:
                existing = fields_dict[field.name]

                # If types differ, promote to string (safe common type)
                if str(existing.dataType) != str(field.dataType):
                    from pyspark.sql.types import StringType
                    fields_dict[field.name] = StructField(
                        field.name,
                        StringType(),
                        nullable=True
                    )
                else:
                    # Same type - make nullable if either is nullable
                    fields_dict[field.name] = StructField(
                        field.name,
                        field.dataType,
                        nullable=(existing.nullable or field.nullable)
                    )
            else:
                # New field - add as nullable (backward compatibility)
                from pyspark.sql.types import StructField
                fields_dict[field.name] = StructField(
                    field.name,
                    field.dataType,
                    nullable=True
                )

        return StructType(list(fields_dict.values()))

    def _handle_breaking_change(self, file_path: str,
                               new_schema: StructType,
                               diff: SchemaDiff):
        """Handle breaking schema changes (requires user approval)"""
        from app.models.domain import SchemaVersion

        # Record schema version awaiting approval
        schema_version = SchemaVersion(
            ingestion_id=self.ingestion.id,
            version=self.ingestion.schema_version + 1,
            schema_json=new_schema.json(),
            detected_at=datetime.utcnow(),
            affected_files=[file_path],
            resolution_type='pending_approval',
            changes_summary=self._format_breaking_changes(diff)
        )
        self.db.add(schema_version)
        self.db.commit()

        # Send alert (implement based on notification system)
        self._send_schema_change_alert(schema_version.id, diff)

        logger.warning(f"Breaking schema change in {file_path}. Awaiting user approval.")

    def _format_breaking_changes(self, diff: SchemaDiff) -> str:
        """Format breaking changes for user notification"""
        changes = []

        if diff.removed_fields:
            changes.append(f"Removed fields: {[f.name for f in diff.removed_fields]}")

        if diff.type_changes:
            changes.append(f"Type changes: {diff.type_changes}")

        if diff.nullability_changes:
            non_nullable = [(name, old, new) for name, old, new in diff.nullability_changes if old and not new]
            if non_nullable:
                changes.append(f"Made non-nullable: {[name for name, _, _ in non_nullable]}")

        return "; ".join(changes)

    def _save_schema(self, schema: StructType, version: int):
        """Save schema to ingestion config"""
        from app.models.domain import SchemaVersion

        schema_version = SchemaVersion(
            ingestion_id=self.ingestion.id,
            version=version,
            schema_json=schema.json(),
            detected_at=datetime.utcnow(),
            resolution_type='accepted'
        )
        self.db.add(schema_version)

        self.ingestion.schema_json = schema.json()
        self.ingestion.schema_version = version
        self.db.commit()
```

### User Approval Workflow

```python
# app/api/v1/schema_versions.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.domain import SchemaVersion, Ingestion, ProcessedFile

router = APIRouter()

@router.post("/ingestions/{ingestion_id}/schema-versions/{version_id}/approve")
def approve_schema_change(
    ingestion_id: str,
    version_id: str,
    action: str,  # 'accept', 'merge', or 'reject'
    db: Session = Depends(get_db)
):
    """
    User approves or rejects schema change

    Actions:
    - 'accept': Use new schema, reprocess skipped files
    - 'merge': Merge old and new schemas, reprocess skipped files
    - 'reject': Reject change, permanently skip affected files
    """
    schema_version = db.query(SchemaVersion).filter_by(id=version_id).first()
    if not schema_version:
        raise HTTPException(status_code=404, detail="Schema version not found")

    ingestion = db.query(Ingestion).filter_by(id=ingestion_id).first()
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    if action == 'accept':
        # Accept new schema
        ingestion.schema_json = schema_version.schema_json
        ingestion.schema_version = schema_version.version

        schema_version.resolution_type = 'accepted'
        schema_version.resolved_at = datetime.utcnow()

        # Reset skipped files for reprocessing
        skipped_count = db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status == 'SKIPPED',
            ProcessedFile.error_type == 'SchemaEvolutionError'
        ).update({'status': 'PENDING'})

        db.commit()

        return {
            "status": "accepted",
            "files_to_reprocess": skipped_count
        }

    elif action == 'merge':
        # Merge schemas
        from pyspark.sql.types import StructType
        old_schema = StructType.fromJson(json.loads(ingestion.schema_json))
        new_schema = StructType.fromJson(json.loads(schema_version.schema_json))

        handler = SchemaEvolutionHandler(ingestion, db)
        merged = handler._merge_schemas(old_schema, new_schema)

        ingestion.schema_json = merged.json()
        ingestion.schema_version = schema_version.version

        schema_version.resolution_type = 'merged'
        schema_version.resolved_at = datetime.utcnow()

        # Reset skipped files
        skipped_count = db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status == 'SKIPPED',
            ProcessedFile.error_type == 'SchemaEvolutionError'
        ).update({'status': 'PENDING'})

        db.commit()

        return {
            "status": "merged",
            "merged_schema": merged.json(),
            "files_to_reprocess": skipped_count
        }

    elif action == 'reject':
        # Reject change, permanently skip files
        schema_version.resolution_type = 'rejected'
        schema_version.resolved_at = datetime.utcnow()

        # Mark skipped files as permanently skipped
        db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status == 'SKIPPED',
            ProcessedFile.error_type == 'SchemaEvolutionError'
        ).update({
            'status': 'PERMANENTLY_SKIPPED',
            'error_message': 'Schema change rejected by user'
        })

        db.commit()

        return {"status": "rejected"}

    else:
        raise HTTPException(status_code=400, detail=f"Invalid action: {action}")
```

---

## Performance Optimization

### Partition-Aware File Listing

**Scenario:** Listing 100k files from S3 every hour is slow and expensive

**Solution:** Only scan partitions modified since last run

```python
def list_files_optimized(self, bucket: str, base_prefix: str,
                        last_run_time: datetime) -> List[Dict]:
    """
    Partition-aware listing for date-partitioned data

    Example structure: s3://bucket/year=2024/month=01/day=15/*.json

    Instead of listing entire bucket (100k files):
    - Calculate which partitions to scan based on last_run_time
    - Only list those specific partitions (e.g., 2 days = 2k files)

    Result: 50x speedup
    """
    partitions_to_scan = self._get_partitions_since(last_run_time)

    all_files = []
    for partition in partitions_to_scan:
        partition_prefix = f"{base_prefix}/{partition}"
        files = self.list_files(bucket, partition_prefix)
        all_files.extend(files)

    logger.info(
        f"Scanned {len(partitions_to_scan)} partitions, "
        f"found {len(all_files)} files"
    )

    return all_files

def _get_partitions_since(self, since: datetime) -> List[str]:
    """Generate list of date partitions to scan"""
    from datetime import timedelta

    partitions = []
    current_date = since.date()
    today = datetime.utcnow().date()

    while current_date <= today:
        # Adjust partition format based on your structure
        partition = (
            f"year={current_date.year}/"
            f"month={current_date.month:02d}/"
            f"day={current_date.day:02d}"
        )
        partitions.append(partition)
        current_date += timedelta(days=1)

    return partitions
```

### Parallel Processing with ThreadPool

**Scenario:** Processing files sequentially is slow

**Solution:** Use ThreadPoolExecutor for parallel processing

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

def process_files_parallel(files: List[Dict],
                          processor: BatchFileProcessor,
                          run_id: str,
                          max_workers: int = 10) -> Dict:
    """
    Process files in parallel with thread pool

    Args:
        files: List of file metadata dicts
        processor: BatchFileProcessor instance
        run_id: Run ID for tracking
        max_workers: Number of parallel workers (default 10)

    Returns:
        Aggregated metrics
    """
    metrics = {'success': 0, 'failed': 0, 'skipped': 0}

    def process_single(file_info):
        """Worker function for processing one file"""
        file_path = file_info['path']

        try:
            file_record = processor.state.lock_file_for_processing(
                processor.ingestion.id, file_path, file_info
            )

            if not file_record:
                return 'skipped', None

            result = processor._process_single_file(file_path, file_info)
            processor.state.mark_file_success(
                file_record,
                records_ingested=result['record_count'],
                bytes_read=file_info.get('size', 0)
            )
            return 'success', None

        except Exception as e:
            if file_record:
                processor.state.mark_file_failed(file_record, e)
            return 'failed', str(e)

    # Submit all files to thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single, f): f
            for f in files
        }

        # Collect results as they complete
        for future in as_completed(future_to_file):
            file_info = future_to_file[future]
            try:
                status, error = future.result()
                metrics[status] += 1

                if error:
                    logger.error(f"Error processing {file_info['path']}: {error}")

            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                metrics['failed'] += 1

    logger.info(f"Parallel processing complete: {metrics}")
    return metrics
```

**Performance:**
- 10 files processed simultaneously
- Throughput: ~10x faster than sequential
- Limitation: Python GIL, mostly I/O bound

### Batching Files for Efficiency

**Scenario:** Reading 1000 small files individually is inefficient

**Solution:** Batch multiple files into single Spark read

```python
def process_files_in_batches(files: List[Dict],
                             processor: BatchFileProcessor,
                             batch_size: int = 100) -> Dict:
    """
    Process multiple files per Spark read operation

    Optimization for many small files:
    - Read 100 files at once
    - Union DataFrames
    - Single write to Iceberg
    - Mark all files atomically
    """
    from functools import reduce

    metrics = {'success': 0, 'failed': 0, 'skipped': 0}

    # Split files into batches
    for i in range(0, len(files), batch_size):
        batch = files[i:i+batch_size]
        batch_paths = [f['path'] for f in batch]

        try:
            # Read all files in batch
            dfs = []
            for file_info in batch:
                df = processor.spark.session.read \
                    .format(processor.ingestion.format_type.value) \
                    .load(file_info['path'])

                # Add file_path column for tracking
                from pyspark.sql.functions import lit
                df = df.withColumn('_source_file', lit(file_info['path']))
                dfs.append(df)

            # Union all DataFrames
            if dfs:
                batch_df = reduce(lambda a, b: a.union(b), dfs)

                # Write batch to Iceberg
                processor._write_to_iceberg(batch_df)

                # Mark all files as success
                for file_info in batch:
                    file_record = processor.state.lock_file_for_processing(
                        processor.ingestion.id, file_info['path'], file_info
                    )
                    if file_record:
                        processor.state.mark_file_success(
                            file_record,
                            records_ingested=0,  # Unknown per-file
                            bytes_read=file_info.get('size', 0)
                        )
                        metrics['success'] += 1

        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            # Fall back to processing files individually
            for file_info in batch:
                try:
                    processor._process_single_file(file_info['path'], file_info)
                    metrics['success'] += 1
                except Exception as file_error:
                    logger.error(f"File {file_info['path']} failed: {file_error}")
                    metrics['failed'] += 1

    return metrics
```

---

## Observability & Monitoring

### Queryable State in PostgreSQL

**Advantage:** Rich metrics directly from SQL queries

**Dashboard Queries:**

```sql
-- 1. Ingestion Health (Last 24 Hours)
SELECT
    ingestion_id,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') as success_count,
    COUNT(*) FILTER (WHERE status = 'FAILED') as failed_count,
    COUNT(*) FILTER (WHERE status = 'SKIPPED') as skipped_count,
    AVG(processing_duration_ms) as avg_duration_ms,
    SUM(records_ingested) as total_records,
    SUM(bytes_read) / 1024 / 1024 / 1024 as total_gb,
    MAX(processed_at) as last_processed_at
FROM processed_files
WHERE processed_at > NOW() - INTERVAL '24 hours'
GROUP BY ingestion_id
ORDER BY last_processed_at DESC;

-- 2. Slowest Files (Bottleneck Analysis)
SELECT
    file_path,
    processing_duration_ms / 1000.0 as duration_seconds,
    records_ingested,
    bytes_read / 1024 / 1024 as size_mb,
    (records_ingested::float / (processing_duration_ms / 1000.0)) as records_per_second
FROM processed_files
WHERE
    processed_at > NOW() - INTERVAL '7 days'
    AND status = 'SUCCESS'
ORDER BY processing_duration_ms DESC
LIMIT 20;

-- 3. Error Patterns
SELECT
    error_type,
    COUNT(*) as error_count,
    COUNT(DISTINCT ingestion_id) as affected_ingestions,
    array_agg(DISTINCT LEFT(error_message, 100)) as sample_messages
FROM processed_files
WHERE
    status = 'FAILED'
    AND processed_at > NOW() - INTERVAL '30 days'
GROUP BY error_type
ORDER BY error_count DESC;

-- 4. Daily Ingestion Volume
SELECT
    DATE(processed_at) as date,
    COUNT(*) as files_processed,
    SUM(records_ingested) as total_records,
    SUM(bytes_read) / 1024 / 1024 / 1024 as total_gb,
    AVG(processing_duration_ms) / 1000.0 as avg_duration_seconds
FROM processed_files
WHERE status = 'SUCCESS'
GROUP BY DATE(processed_at)
ORDER BY date DESC
LIMIT 30;

-- 5. Files Pending Processing
SELECT
    i.name as ingestion_name,
    COUNT(pf.id) as pending_count,
    MIN(pf.discovered_at) as oldest_discovered
FROM processed_files pf
JOIN ingestions i ON i.id = pf.ingestion_id
WHERE pf.status = 'PENDING'
GROUP BY i.id, i.name
ORDER BY pending_count DESC;

-- 6. Retry Analysis
SELECT
    ingestion_id,
    retry_count,
    COUNT(*) as file_count,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') as eventually_succeeded,
    COUNT(*) FILTER (WHERE status = 'FAILED') as still_failing
FROM processed_files
WHERE retry_count > 0
GROUP BY ingestion_id, retry_count
ORDER BY ingestion_id, retry_count;
```

### Real-Time Progress Tracking

```python
def get_run_progress(run_id: str, db: Session) -> Dict:
    """
    Get real-time progress of a running ingestion

    Returns:
        Progress metrics updated in real-time
    """
    from sqlalchemy import func

    # Query current status
    progress = db.query(
        func.count(ProcessedFile.id).label('total'),
        func.count(ProcessedFile.id).filter(ProcessedFile.status == 'SUCCESS').label('success'),
        func.count(ProcessedFile.id).filter(ProcessedFile.status == 'FAILED').label('failed'),
        func.count(ProcessedFile.id).filter(ProcessedFile.status == 'PROCESSING').label('processing'),
        func.count(ProcessedFile.id).filter(ProcessedFile.status == 'PENDING').label('pending'),
        func.sum(ProcessedFile.records_ingested).label('total_records'),
        func.sum(ProcessedFile.bytes_read).label('total_bytes')
    ).filter(
        ProcessedFile.run_id == run_id
    ).first()

    return {
        'run_id': run_id,
        'total_files': progress.total or 0,
        'success': progress.success or 0,
        'failed': progress.failed or 0,
        'processing': progress.processing or 0,
        'pending': progress.pending or 0,
        'total_records': progress.total_records or 0,
        'total_bytes': progress.total_bytes or 0,
        'completion_percentage': (
            (progress.success + progress.failed) / progress.total * 100
            if progress.total > 0 else 0
        )
    }
```

### Debugging Specific Files

```sql
-- Query: "Why did file X fail?"
SELECT
    file_path,
    status,
    error_type,
    error_message,
    retry_count,
    processing_started_at,
    processed_at,
    processing_duration_ms / 1000.0 as duration_seconds
FROM processed_files
WHERE file_path = 's3://bucket/data/2024-01-15.json';

-- Result (immediate answer):
-- status: FAILED
-- error_type: JSONDecodeError
-- error_message: Expecting value: line 42 column 10
-- retry_count: 3
-- Action: Fix corrupt JSON at line 42
```

```sql
-- Query: "Is file X already processed?"
SELECT
    status,
    processed_at,
    records_ingested,
    bytes_read
FROM processed_files
WHERE file_path = 's3://bucket/data/2024-01-15.json';

-- Result (milliseconds):
-- status: SUCCESS
-- processed_at: 2024-01-15 08:30:00
-- records_ingested: 10000
-- bytes_read: 5242880
```

### Alerting and Notifications

```python
# app/services/alert_service.py

from typing import Dict, List
import logging

class AlertService:
    """Send alerts for ingestion issues"""

    def __init__(self, config):
        self.config = config

    def check_and_alert(self, ingestion_id: str, run_id: str,
                       metrics: Dict, db: Session):
        """
        Check metrics against thresholds and send alerts

        Alert conditions:
        - Failure rate > threshold
        - Processing time > expected
        - Schema evolution detected
        - Stale files in PROCESSING state
        """
        from app.models.domain import Ingestion

        ingestion = db.query(Ingestion).get(ingestion_id)

        # Check failure rate
        if metrics['failed'] > 0:
            failure_rate = metrics['failed'] / (metrics['success'] + metrics['failed'])
            if failure_rate > ingestion.max_failure_rate:
                self._send_alert(
                    ingestion_id,
                    severity='warning',
                    message=f"High failure rate: {failure_rate:.1%} ({metrics['failed']} failed files)",
                    details=metrics
                )

        # Check for schema evolution
        from app.models.domain import SchemaVersion
        pending_schemas = db.query(SchemaVersion).filter(
            SchemaVersion.ingestion_id == ingestion_id,
            SchemaVersion.resolution_type == 'pending_approval'
        ).count()

        if pending_schemas > 0:
            self._send_alert(
                ingestion_id,
                severity='info',
                message=f"{pending_schemas} schema changes awaiting approval",
                action_required=True
            )

    def _send_alert(self, ingestion_id: str, severity: str,
                   message: str, **kwargs):
        """
        Send alert via configured channels

        Channels: Email, Slack, PagerDuty, etc.
        """
        # TODO: Implement based on notification system
        logger.warning(f"[ALERT] {severity.upper()}: {message}")
```

---

## Database Design

### Complete Schema

```sql
-- Core table: Processed files with full tracking
CREATE TABLE processed_files (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Foreign keys
    ingestion_id UUID NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
    run_id UUID REFERENCES runs(id) ON DELETE SET NULL,

    -- File identification
    file_path TEXT NOT NULL,
    file_size_bytes BIGINT,
    file_modified_at TIMESTAMP WITH TIME ZONE,
    file_etag TEXT,

    -- Processing state
    status VARCHAR(20) NOT NULL,  -- PENDING, PROCESSING, SUCCESS, FAILED, SKIPPED, PERMANENTLY_SKIPPED
    discovered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processing_started_at TIMESTAMP WITH TIME ZONE,
    processed_at TIMESTAMP WITH TIME ZONE,

    -- Metrics
    records_ingested INTEGER,
    bytes_read BIGINT,
    processing_duration_ms INTEGER,

    -- Error tracking
    error_message TEXT,
    error_type VARCHAR(100),
    retry_count INTEGER DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT unique_ingestion_file UNIQUE (ingestion_id, file_path),
    CONSTRAINT valid_status CHECK (status IN (
        'PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'SKIPPED', 'PERMANENTLY_SKIPPED'
    ))
);

-- Indexes for performance
CREATE INDEX idx_processed_files_ingestion_status
    ON processed_files(ingestion_id, status);

CREATE INDEX idx_processed_files_run
    ON processed_files(run_id);

CREATE INDEX idx_processed_files_status_date
    ON processed_files(status, processed_at DESC);

CREATE INDEX idx_processed_files_path
    ON processed_files(file_path);  -- For fast lookups

CREATE INDEX idx_processed_files_stale
    ON processed_files(ingestion_id, status, processing_started_at)
    WHERE status = 'PROCESSING';  -- Partial index for stale detection

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_processed_files_updated_at
    BEFORE UPDATE ON processed_files
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
```

### Database Growth and Retention

**Growth Projection:**

Scenario: 10 ingestions, 1000 files/day each, 365 days

```
Total rows: 10 × 1000 × 365 = 3,650,000 rows
Row size: ~500 bytes (full metadata)
Data size: 3.65M × 500 bytes = ~1.8 GB
Index size: ~1 GB (4 indexes)
Total storage: ~2.8 GB/year
```

**Retention Strategy:**

```sql
-- Archive old records to cold storage
CREATE TABLE processed_files_archive (
    LIKE processed_files INCLUDING ALL
);

-- Archive records older than 90 days
CREATE OR REPLACE FUNCTION archive_old_processed_files(retention_days INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    -- Move to archive
    WITH moved AS (
        INSERT INTO processed_files_archive
        SELECT * FROM processed_files
        WHERE processed_at < NOW() - INTERVAL '1 day' * retention_days
        RETURNING id
    )
    SELECT COUNT(*) INTO archived_count FROM moved;

    -- Delete from main table
    DELETE FROM processed_files
    WHERE processed_at < NOW() - INTERVAL '1 day' * retention_days;

    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- Run monthly via cron
-- SELECT archive_old_processed_files(90);
```

---

## Edge Cases & Solutions

### 1. File Modified After Processing

**Problem:** File `data.json` processed on Jan 1, modified on Jan 5

**Solution:**

```python
def should_reprocess_file(file_info: Dict, existing_record: ProcessedFile) -> bool:
    """
    Determine if file should be reprocessed based on changes

    Uses ETag (content hash) or modification time
    """
    # Method 1: ETag (most reliable)
    if file_info.get('etag') and existing_record.file_etag:
        if file_info['etag'] != existing_record.file_etag:
            logger.info(f"File {file_info['path']} changed (ETag mismatch)")
            return True

    # Method 2: Modification time
    if file_info.get('modified_at') and existing_record.file_modified_at:
        if file_info['modified_at'] > existing_record.file_modified_at:
            logger.info(f"File {file_info['path']} changed (newer modification time)")
            return True

    # No changes detected
    return False

# Integrate into lock_file_for_processing (already implemented above)
```

### 2. Late-Arriving Files

**Problem:** Jan 15 file arrives on Jan 20 (out of order)

**Solution: Lookback Window**

```python
def list_files_with_lookback(discovery_service: FileDiscoveryService,
                             bucket: str, prefix: str,
                             lookback_days: int = 7) -> List[Dict]:
    """
    List files with lookback window to catch late arrivals

    Example: If today is Jan 20, scan files from Jan 13-20
    """
    from datetime import timedelta
    since = datetime.utcnow() - timedelta(days=lookback_days)

    files = discovery_service.list_files(bucket, prefix, since=since)

    logger.info(f"Found {len(files)} files with {lookback_days}-day lookback")
    return files
```

### 3. Very Large Files (10GB+)

**Problem:** Single file too large for executor memory

**Solution: Size-Based Filtering**

```python
def handle_large_files(file_info: Dict,
                      processor: BatchFileProcessor,
                      max_size_gb: int = 5) -> str:
    """
    Handle files that exceed size limits

    Strategies:
    1. Skip and alert
    2. Process in streaming mode
    3. Split file into chunks (if possible)
    """
    file_size_gb = file_info['size'] / (1024**3)

    if file_size_gb <= max_size_gb:
        # Normal processing
        return 'process'

    # Option 1: Skip and alert
    logger.warning(f"File {file_info['path']} too large: {file_size_gb:.2f}GB")

    file_record = processor.state.lock_file_for_processing(
        processor.ingestion.id, file_info['path'], file_info
    )

    if file_record:
        processor.state.mark_file_skipped(
            file_record,
            reason=f"File too large: {file_size_gb:.2f}GB > {max_size_gb}GB limit"
        )

    # Send alert for manual handling
    send_alert(
        title="Large File Detected",
        message=f"File {file_info['path']} ({file_size_gb:.2f}GB) exceeds limit",
        action_required=True
    )

    return 'skipped'

# Option 2: Process large file in streaming mode (if format supports)
def process_large_file_streaming(file_path: str):
    """Process large file line-by-line"""
    # For formats like JSON Lines, CSV
    # Read and process in chunks
    pass
```

### 4. Duplicate File Names (Different Locations)

**Problem:**
- `s3://bucket-A/data.json`
- `s3://bucket-B/data.json`

**Solution:** Full path as unique identifier (already handled)

```python
# File paths are fully qualified, so no collision
# processed_files.file_path = 's3://bucket-A/data.json' (unique)
# processed_files.file_path = 's3://bucket-B/data.json' (unique)
# Constraint: UNIQUE (ingestion_id, file_path) prevents duplicates
```

### 5. Schema Evolution Mid-Run

**Problem:** First 500 files have schema A, files 501-1000 have schema B

**Solution: Per-File Schema Detection** (implemented in Schema Evolution section)

```python
# Process files with schema detection
for file_info in new_files:
    try:
        # Detect schema per file
        df, schema_changed = schema_handler.process_file_with_schema_detection(
            spark, file_info['path']
        )

        # If compatible change, auto-merged
        # If breaking change, file skipped and user alerted

        processor._write_to_iceberg(df)

    except SchemaEvolutionError:
        # File skipped, awaiting user approval
        # Processing continues with other files
        continue
```

---

## Implementation Roadmap

### Phase 1: Core Foundation (Week 1-2)

**Tasks:**
1. Implement `FileDiscoveryService` for S3, Azure, GCS
2. Implement `FileStateService` with locking
3. Create `ProcessedFile` database table with indexes
4. Implement basic `BatchFileProcessor`
5. Add unit tests for core components

**Deliverable:** Can list files and track state in PostgreSQL

---

### Phase 2: Processing Pipeline (Week 3-4)

**Tasks:**
1. Implement Spark Connect integration for file reading
2. Add Iceberg write functionality
3. Implement per-file error handling
4. Add retry logic for failed files
5. Implement stale lock cleanup job
6. Add integration tests

**Deliverable:** Can process files end-to-end with error handling

---

### Phase 3: Schema Evolution (Week 5-6)

**Tasks:**
1. Implement `SchemaEvolutionHandler`
2. Add schema comparison logic
3. Create `SchemaVersion` table
4. Implement auto-merge for compatible changes
5. Add user approval API endpoints
6. Add schema change notifications

**Deliverable:** Handles schema changes gracefully

---

### Phase 4: Performance & Observability (Week 7-8)

**Tasks:**
1. Implement partition-aware file listing
2. Add parallel processing with ThreadPool
3. Implement file batching for small files
4. Create monitoring dashboard queries
5. Add real-time progress tracking
6. Implement alert service
7. Add performance benchmarks

**Deliverable:** Production-ready performance and monitoring

---

### Phase 5: Production Hardening (Week 9-10)

**Tasks:**
1. Add comprehensive error recovery
2. Implement database archival strategy
3. Add metrics collection (Prometheus)
4. Implement graceful shutdown
5. Add operational runbooks
6. Load testing and tuning
7. Security audit and hardening

**Deliverable:** Production-ready system

---

## Summary

This batch processing approach with PostgreSQL state tracking provides:

✅ **Complete Control & Visibility**
- Every file tracked in queryable database
- Rich metadata and metrics
- Easy debugging and auditing

✅ **Flexibility**
- Per-file schema inference
- Granular error handling
- Custom filtering and validation
- Event-driven support

✅ **Production Features**
- Surgical retries
- Comprehensive monitoring
- Graceful schema evolution
- Crash recovery

❌ **Trade-offs**
- More code to write (~500 lines)
- Manual exactly-once implementation
- Higher operational complexity

**Best For:**
- Scheduled batch ingestion
- AWS/Snyk data dumps with evolving schemas
- Use cases requiring complete audit trail
- Scenarios where per-file control is critical

**Cost:** Similar to streaming (~$127/month for reference scenario), with better observability

---

**Next Steps:**
1. Review architecture with team
2. Begin Phase 1 implementation
3. Set up development environment
4. Create test data sets for validation
