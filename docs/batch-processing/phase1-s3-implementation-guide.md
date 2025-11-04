# Phase 1: S3 Batch Processing Implementation Guide

**Document Purpose:** Complete implementation guide for Phase 1 - Core Foundation with S3 batch ingestion and PostgreSQL state tracking

**Scope:** AWS S3 only (Azure and GCS deferred to later phases)

**Timeline:** Week 1-2

**Date:** 2025-01-04

---

## Table of Contents

1. [Phase 1 Overview](#phase-1-overview)
2. [Architecture](#architecture)
3. [Database Schema Changes](#database-schema-changes)
4. [Component 1: File Discovery Service](#component-1-file-discovery-service)
5. [Component 2: File State Service](#component-2-file-state-service)
6. [Component 3: Batch File Processor](#component-3-batch-file-processor)
7. [Component 4: Batch Orchestrator](#component-4-batch-orchestrator)
8. [API Integration](#api-integration)
9. [Testing Strategy](#testing-strategy)
10. [Implementation Checklist](#implementation-checklist)
11. [Migration from Current Code](#migration-from-current-code)

---

## Phase 1 Overview

### Objectives

Build the core foundation for batch-based file ingestion with PostgreSQL state management:

1. **List files from S3** using boto3 SDK
2. **Track processing state in PostgreSQL** as the source of truth
3. **Process files individually or in batches** using Spark batch API (not streaming)
4. **Write data to Apache Iceberg tables**
5. **Provide complete visibility** into every file's state

### Deliverables

By end of Phase 1, the system must:

- ✅ Discover new files from S3 buckets based on ingestion configuration
- ✅ Track each file's state in PostgreSQL (DISCOVERED, PROCESSING, SUCCESS, FAILED, SKIPPED)
- ✅ Process files using Spark DataFrame batch API
- ✅ Write data to Iceberg tables
- ✅ Handle per-file errors gracefully (one bad file doesn't block others)
- ✅ Support manual "Run Now" trigger via API
- ✅ Provide queryable state for debugging ("Is file X processed?")
- ✅ Pass unit and integration tests

### Success Criteria

- [ ] Can create an S3 ingestion configuration via API
- [ ] Can discover 1000+ files from S3 bucket
- [ ] Can process files and track each in `processed_files` table
- [ ] Failed files don't block successful files
- [ ] Can query file state via PostgreSQL
- [ ] Can re-run failed files without reprocessing successes
- [ ] All tests pass

### Out of Scope for Phase 1

- ❌ Scheduler integration (manual trigger only)
- ❌ Schema evolution detection (will use inferred or fixed schema)
- ❌ Azure Blob / GCS support
- ❌ Parallel processing with ThreadPool
- ❌ Advanced optimizations (partition-aware listing, batching)
- ❌ Authentication/authorization
- ❌ Email notifications

### Timeline

- **Days 1-3:** Database schema + migrations + ProcessedFileRepository
- **Days 4-6:** File Discovery Service (S3)
- **Days 7-9:** Batch File Processor (Spark refactor)
- **Days 10-12:** Batch Orchestrator + API integration
- **Days 13-14:** Testing and bug fixes

---

## Architecture

### High-Level Flow

```
┌────────────────────────────────────────────────────────────┐
│                     User / API Trigger                      │
│              POST /api/v1/ingestions/{id}/run              │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────┐
│                  IngestionService.trigger_manual_run()     │
│               (app/services/ingestion_service.py)          │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────┐
│                    BatchOrchestrator.run()                 │
│                (app/services/batch_orchestrator.py)        │
│                                                             │
│  1. Load ingestion config from DB                          │
│  2. Create new Run record                                  │
│  3. Call FileDiscoveryService to list S3 files             │
│  4. Query ProcessedFile table for already-processed files  │
│  5. Compute diff: new_files = all_files - processed_files  │
│  6. Call BatchFileProcessor for each new file              │
│  7. Update Run record with metrics                         │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────┐
│                  FileDiscoveryService.list_files()         │
│             (app/services/file_discovery_service.py)       │
│                                                             │
│  - Initialize boto3 S3 client with credentials             │
│  - List objects matching pattern (paginated)               │
│  - Collect metadata: path, size, etag, modified_at         │
│  - Return List[FileMetadata]                               │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────┐
│               FileStateService.get_processed_files()       │
│              (app/services/file_state_service.py)          │
│                                                             │
│  - Query processed_files table for ingestion_id            │
│  - Filter by status (SUCCESS, SKIPPED)                     │
│  - Return Set[file_path]                                   │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────┐
│            BatchFileProcessor.process_files()              │
│              (app/services/batch_file_processor.py)        │
│                                                             │
│  FOR EACH file in new_files:                               │
│    1. Lock file (FileStateService.lock_file)               │
│    2. Read file with Spark (batch API)                     │
│    3. Infer or apply schema                                │
│    4. Write to Iceberg table                               │
│    5. Mark as SUCCESS/FAILED (FileStateService)            │
│    6. Commit transaction                                   │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ▼
                ┌─────────────────┐
                │   PostgreSQL    │
                │ processed_files │
                │ Source of Truth │
                └─────────────────┘
                          │
                          ▼
                ┌─────────────────┐
                │ Apache Iceberg  │
                │  Data Tables    │
                └─────────────────┘
```

### Component Interactions

```
┌───────────────────────────────────────────────────────────────┐
│                        API Layer                              │
│  /api/v1/ingestions/{id}/run (POST)                          │
└───────────────────────┬───────────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────┐
│                     Service Layer                             │
│  ┌─────────────────┐  ┌──────────────────┐                   │
│  │IngestionService │  │BatchOrchestrator │                   │
│  └─────────────────┘  └──────────────────┘                   │
│  ┌──────────────────┐ ┌──────────────────┐                   │
│  │FileDiscoveryServ.│ │FileStateService  │                   │
│  └──────────────────┘ └──────────────────┘                   │
│  ┌──────────────────┐                                         │
│  │BatchFileProcessor│                                         │
│  └──────────────────┘                                         │
└───────────────────────┬───────────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────┐
│                   Repository Layer                            │
│  ┌─────────────────────┐  ┌──────────────────────┐           │
│  │IngestionRepository  │  │RunRepository         │           │
│  └─────────────────────┘  └──────────────────────┘           │
│  ┌─────────────────────┐                                      │
│  │ProcessedFileRepo    │ (NEW)                                │
│  └─────────────────────┘                                      │
└───────────────────────┬───────────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────┐
│                     Database Layer                            │
│  PostgreSQL: ingestions, runs, processed_files                │
└───────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────┐
│                    Spark Connect (Parallel)                   │
│  SparkConnectClient → Remote Spark → Iceberg                 │
└───────────────────────────────────────────────────────────────┘
```

### Data Flow

**Phase 1: Discovery**
1. API receives POST `/api/v1/ingestions/{id}/run`
2. IngestionService loads ingestion config from DB
3. BatchOrchestrator creates new Run record with status=RUNNING
4. FileDiscoveryService lists S3 objects with pagination
5. Returns List[FileMetadata] with path, size, etag, modified_at

**Phase 2: State Reconciliation**
1. FileStateService queries `processed_files` table for ingestion_id
2. Filters by status IN (SUCCESS, SKIPPED)
3. Returns Set[file_path] of already-processed files
4. BatchOrchestrator computes: `new_files = discovered_files - processed_files`

**Phase 3: Processing**
1. For each file in new_files:
   - FileStateService.lock_file() creates/updates ProcessedFile with status=PROCESSING
   - Uses SELECT FOR UPDATE SKIP LOCKED for concurrency safety
   - BatchFileProcessor reads file with Spark DataFrame API
   - Writes to Iceberg table
   - FileStateService.mark_success() or mark_failed()
2. Errors are isolated per-file (one failure doesn't stop others)

**Phase 4: Completion**
1. BatchOrchestrator aggregates metrics (success_count, failed_count)
2. Updates Run record with final status and metrics
3. Returns response to API caller

---

## Database Schema Changes

### New Model: ProcessedFile

**Purpose:** Track individual file processing state with full audit trail

**Location:** Add to `app/models/domain.py`

**SQLAlchemy Model:**

```python
from sqlalchemy import Column, String, BigInteger, Integer, DateTime, Text, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
import uuid

class ProcessedFile(Base):
    """
    Tracks processing state of individual files.

    Each file discovered from cloud storage gets a record here.
    This is the source of truth for "has this file been processed?"
    """
    __tablename__ = "processed_files"

    # Primary key
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # Foreign keys
    ingestion_id = Column(String(36), ForeignKey("ingestions.id", ondelete="CASCADE"), nullable=False)
    run_id = Column(String(36), ForeignKey("runs.id", ondelete="SET NULL"), nullable=True)

    # File identification
    file_path = Column(Text, nullable=False)  # Full path: s3://bucket/path/to/file.json
    file_size_bytes = Column(BigInteger, nullable=True)
    file_modified_at = Column(DateTime(timezone=True), nullable=True)
    file_etag = Column(String(255), nullable=True)  # S3 ETag for change detection

    # Processing state
    status = Column(
        String(20),
        nullable=False,
        default="DISCOVERED"
    )  # DISCOVERED, PROCESSING, SUCCESS, FAILED, SKIPPED

    discovered_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    processing_started_at = Column(DateTime(timezone=True), nullable=True)
    processed_at = Column(DateTime(timezone=True), nullable=True)

    # Metrics
    records_ingested = Column(Integer, nullable=True)
    bytes_read = Column(BigInteger, nullable=True)
    processing_duration_ms = Column(Integer, nullable=True)

    # Error tracking
    error_message = Column(Text, nullable=True)
    error_type = Column(String(100), nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    ingestion = relationship("Ingestion", back_populates="processed_files")
    run = relationship("Run", back_populates="processed_files")

    # Constraints
    __table_args__ = (
        Index("idx_processed_files_ingestion_status", "ingestion_id", "status"),
        Index("idx_processed_files_run", "run_id"),
        Index("idx_processed_files_path", "file_path"),
        Index("idx_processed_files_status_date", "status", "processed_at"),
        Index(
            "idx_processed_files_stale",
            "ingestion_id", "status", "processing_started_at",
            postgresql_where=(Column("status") == "PROCESSING")  # Partial index
        ),
        # Unique constraint: one record per (ingestion, file)
        # Multiple runs can reference the same file record
        # We update the record if file changes (etag mismatch)
    )

    def __repr__(self):
        return f"<ProcessedFile(id={self.id}, file_path={self.file_path}, status={self.status})>"
```

### Update Existing Models

**Ingestion Model** - Add relationship:

```python
class Ingestion(Base):
    # ... existing fields ...

    # Add this relationship
    processed_files = relationship("ProcessedFile", back_populates="ingestion", cascade="all, delete-orphan")
```

**Run Model** - Add relationship:

```python
class Run(Base):
    # ... existing fields ...

    # Add this relationship
    processed_files = relationship("ProcessedFile", back_populates="run")
```

### Status Enum

**Location:** Add to `app/models/domain.py`

```python
import enum

class ProcessedFileStatus(str, enum.Enum):
    """Status of a processed file"""
    DISCOVERED = "DISCOVERED"      # File found, not yet processed
    PROCESSING = "PROCESSING"      # Currently being processed
    SUCCESS = "SUCCESS"            # Successfully processed
    FAILED = "FAILED"              # Processing failed (retryable)
    SKIPPED = "SKIPPED"            # Intentionally skipped (e.g., too large, already processed with no changes)
```

### Alembic Migration

**Step 1: Generate migration**

```bash
# From project root
alembic revision --autogenerate -m "add_processed_files_table"
```

**Step 2: Review generated migration in `alembic/versions/XXXX_add_processed_files_table.py`**

**Step 3: Ensure migration includes:**
- `processed_files` table creation
- All indexes (especially partial index for stale detection)
- Foreign key constraints to `ingestions` and `runs`
- Unique constraint on (ingestion_id, file_path) if desired

**Step 4: Apply migration**

```bash
alembic upgrade head
```

### Database Growth Estimation

**Scenario:** 1 ingestion, 1000 files/day, 30 days

```
Total rows: 1 × 1000 × 30 = 30,000 rows
Row size: ~500 bytes (with metadata)
Data size: 30,000 × 500 = ~15 MB
Index size: ~10 MB (5 indexes)
Total: ~25 MB/month per ingestion
```

**For 10 ingestions:** ~250 MB/month
**For 100 ingestions:** ~2.5 GB/month

PostgreSQL can easily handle millions of rows. No partitioning needed for Phase 1.

---

## Component 1: File Discovery Service

### Purpose

Discover files from AWS S3 bucket based on ingestion configuration.

### Responsibilities

1. Initialize boto3 S3 client with credentials from ingestion config
2. List objects in bucket matching path pattern
3. Handle pagination (S3 returns max 1000 objects per request)
4. Collect file metadata: path, size, etag, modified_at
5. Return structured list of file metadata

### File Structure

**Location:** `app/services/file_discovery_service.py`

### Implementation

```python
"""
File Discovery Service - AWS S3 Implementation

Lists files from S3 bucket with pagination and metadata collection.
"""

from typing import List, Optional, Dict
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)


@dataclass
class FileMetadata:
    """Metadata for a discovered file"""
    path: str  # Full S3 path: s3://bucket/path/to/file.json
    size: int  # Size in bytes
    modified_at: datetime  # Last modified timestamp
    etag: str  # S3 ETag (hash) for change detection

    def to_dict(self) -> Dict:
        """Convert to dict for JSON serialization"""
        return {
            "path": self.path,
            "size": self.size,
            "modified_at": self.modified_at.isoformat(),
            "etag": self.etag
        }


class FileDiscoveryService:
    """
    Discovers files from AWS S3 bucket.

    Phase 1: S3 only
    Future phases: Azure Blob, GCS
    """

    def __init__(self, source_type: str, credentials: Dict, region: Optional[str] = None):
        """
        Initialize file discovery service.

        Args:
            source_type: Must be "S3" in Phase 1
            credentials: Dict with aws_access_key_id, aws_secret_access_key
            region: AWS region (default: us-east-1)
        """
        if source_type != "S3":
            raise ValueError(f"Unsupported source type for Phase 1: {source_type}. Only S3 is supported.")

        self.source_type = source_type
        self.credentials = credentials
        self.region = region or "us-east-1"
        self._client = None

    def list_files(
        self,
        bucket: str,
        prefix: str,
        pattern: Optional[str] = None,
        since: Optional[datetime] = None,
        max_files: Optional[int] = None
    ) -> List[FileMetadata]:
        """
        List files from S3 bucket.

        Args:
            bucket: S3 bucket name (e.g., "my-data-bucket")
            prefix: Path prefix (e.g., "data/events/")
            pattern: File pattern for filtering (e.g., "*.json") - NOT IMPLEMENTED IN PHASE 1
            since: Only return files modified after this date (optimization)
            max_files: Maximum number of files to return

        Returns:
            List of FileMetadata objects

        Raises:
            ClientError: If S3 API call fails
            NoCredentialsError: If credentials are invalid
        """
        logger.info(f"Listing files from S3: bucket={bucket}, prefix={prefix}")

        # Initialize S3 client lazily
        if not self._client:
            self._client = self._init_s3_client()

        files = []
        continuation_token = None

        try:
            while True:
                # Build request parameters
                kwargs = {
                    'Bucket': bucket,
                    'Prefix': prefix,
                    'MaxKeys': min(1000, max_files - len(files)) if max_files else 1000
                }

                if continuation_token:
                    kwargs['ContinuationToken'] = continuation_token

                # Call S3 list_objects_v2
                response = self._client.list_objects_v2(**kwargs)

                # Check if any objects returned
                if 'Contents' not in response:
                    logger.info(f"No objects found in s3://{bucket}/{prefix}")
                    break

                # Process each object
                for obj in response['Contents']:
                    # Apply date filter
                    if since and obj['LastModified'] < since:
                        continue

                    # Apply pattern filter (Phase 1: simple suffix check)
                    # TODO Phase 2: Support glob patterns
                    if pattern and not obj['Key'].endswith(pattern.replace("*", "")):
                        continue

                    # Build full S3 path
                    file_path = f"s3://{bucket}/{obj['Key']}"

                    # Create FileMetadata
                    file_metadata = FileMetadata(
                        path=file_path,
                        size=obj['Size'],
                        modified_at=obj['LastModified'],
                        etag=obj['ETag'].strip('"')  # Remove quotes from ETag
                    )

                    files.append(file_metadata)

                    # Stop if we've reached max_files
                    if max_files and len(files) >= max_files:
                        logger.info(f"Reached max_files limit: {max_files}")
                        return files

                # Check if there are more results
                if not response.get('IsTruncated'):
                    break

                continuation_token = response.get('NextContinuationToken')

            logger.info(f"Discovered {len(files)} files from s3://{bucket}/{prefix}")
            return files

        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"S3 ClientError: {error_code} - {error_message}")
            raise

        except NoCredentialsError:
            logger.error("AWS credentials not found or invalid")
            raise

        except Exception as e:
            logger.error(f"Unexpected error listing S3 files: {e}", exc_info=True)
            raise

    def _init_s3_client(self):
        """Initialize boto3 S3 client with credentials"""
        try:
            # Extract credentials
            aws_access_key_id = self.credentials.get('aws_access_key_id')
            aws_secret_access_key = self.credentials.get('aws_secret_access_key')

            if not aws_access_key_id or not aws_secret_access_key:
                raise ValueError("Missing AWS credentials: aws_access_key_id and aws_secret_access_key required")

            # Create S3 client
            client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=self.region
            )

            logger.info(f"Initialized S3 client for region: {self.region}")
            return client

        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def test_connection(self, bucket: str) -> bool:
        """
        Test S3 connection by checking if bucket is accessible.

        Args:
            bucket: S3 bucket name

        Returns:
            True if connection successful

        Raises:
            ClientError if connection fails
        """
        try:
            if not self._client:
                self._client = self._init_s3_client()

            # Simple head_bucket call to test connectivity
            self._client.head_bucket(Bucket=bucket)
            logger.info(f"Successfully connected to S3 bucket: {bucket}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket not found: {bucket}")
            elif error_code == '403':
                logger.error(f"Access denied to bucket: {bucket}")
            else:
                logger.error(f"S3 connection test failed: {error_code}")
            raise
```

### Usage Example

```python
from app.services.file_discovery_service import FileDiscoveryService, FileMetadata
from app.models.domain import Ingestion
import json

# Load ingestion config
ingestion = db.query(Ingestion).get(ingestion_id)

# Parse credentials
credentials = json.loads(ingestion.source_credentials)

# Initialize service
discovery_service = FileDiscoveryService(
    source_type=ingestion.source_type,  # "S3"
    credentials=credentials,
    region=credentials.get('aws_region', 'us-east-1')
)

# Parse bucket and prefix from source_path
# Example: s3://my-bucket/data/events/ → bucket=my-bucket, prefix=data/events/
source_path = ingestion.source_path  # "s3://my-bucket/data/events/"
bucket = source_path.split('/')[2]  # "my-bucket"
prefix = '/'.join(source_path.split('/')[3:])  # "data/events/"

# List files
files = discovery_service.list_files(
    bucket=bucket,
    prefix=prefix,
    pattern=ingestion.source_file_pattern,  # e.g., "*.json"
    max_files=None  # No limit
)

# files is List[FileMetadata]
for file in files:
    print(f"Discovered: {file.path} ({file.size} bytes)")
```

### Error Handling

**Common S3 Errors:**

1. **NoCredentialsError:** Invalid or missing AWS credentials
   - Log error, raise to caller
   - User needs to fix credentials in ingestion config

2. **ClientError 403 (Access Denied):** Insufficient IAM permissions
   - Log error, raise to caller
   - User needs to grant s3:ListBucket permission

3. **ClientError 404 (Not Found):** Bucket doesn't exist
   - Log error, raise to caller
   - User needs to fix bucket name

4. **ClientError (Throttling):** Too many requests
   - Log warning
   - Retry with exponential backoff (TODO Phase 2)

### Testing

**Unit Tests:**

```python
# tests/services/test_file_discovery_service.py

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from app.services.file_discovery_service import FileDiscoveryService, FileMetadata

def test_list_files_success(mock_s3_client):
    """Test successful file listing"""
    # Mock S3 response
    mock_s3_client.list_objects_v2.return_value = {
        'Contents': [
            {
                'Key': 'data/file1.json',
                'Size': 1024,
                'LastModified': datetime(2024, 1, 1),
                'ETag': '"abc123"'
            }
        ],
        'IsTruncated': False
    }

    service = FileDiscoveryService("S3", {"aws_access_key_id": "test", "aws_secret_access_key": "test"})
    service._client = mock_s3_client

    files = service.list_files(bucket="test-bucket", prefix="data/")

    assert len(files) == 1
    assert files[0].path == "s3://test-bucket/data/file1.json"
    assert files[0].size == 1024

def test_list_files_pagination(mock_s3_client):
    """Test pagination handling"""
    # Mock paginated responses
    mock_s3_client.list_objects_v2.side_effect = [
        {
            'Contents': [{'Key': f'file{i}.json', 'Size': 100, 'LastModified': datetime.now(), 'ETag': f'"{i}"'} for i in range(1000)],
            'IsTruncated': True,
            'NextContinuationToken': 'token1'
        },
        {
            'Contents': [{'Key': f'file{i}.json', 'Size': 100, 'LastModified': datetime.now(), 'ETag': f'"{i}"'} for i in range(1000, 1500)],
            'IsTruncated': False
        }
    ]

    service = FileDiscoveryService("S3", {"aws_access_key_id": "test", "aws_secret_access_key": "test"})
    service._client = mock_s3_client

    files = service.list_files(bucket="test-bucket", prefix="data/")

    assert len(files) == 1500
    assert mock_s3_client.list_objects_v2.call_count == 2

def test_list_files_no_credentials():
    """Test error when credentials missing"""
    with pytest.raises(ValueError, match="Missing AWS credentials"):
        service = FileDiscoveryService("S3", {})
        service.list_files(bucket="test", prefix="data/")
```

---

## Component 2: File State Service

### Purpose

Manage file processing state in PostgreSQL with concurrency safety.

### Responsibilities

1. Query processed files from database
2. Atomically lock files for processing (prevent duplicate work)
3. Update file status (DISCOVERED → PROCESSING → SUCCESS/FAILED)
4. Detect file changes (ETag comparison)
5. Support retry logic for failed files
6. Detect stale locks (crashed workers)

### File Structure

**Location:** `app/services/file_state_service.py`
**Repository:** `app/repositories/processed_file_repository.py`

### Repository Implementation

```python
"""
ProcessedFile Repository - Data access for file state tracking
"""

from typing import List, Set, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models.domain import ProcessedFile, ProcessedFileStatus
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class ProcessedFileRepository:
    """Repository for ProcessedFile data access"""

    def __init__(self, db: Session):
        self.db = db

    def create(self, processed_file: ProcessedFile) -> ProcessedFile:
        """
        Create new ProcessedFile record.

        Args:
            processed_file: ProcessedFile instance to create

        Returns:
            Created ProcessedFile with ID
        """
        self.db.add(processed_file)
        self.db.commit()
        self.db.refresh(processed_file)
        logger.debug(f"Created ProcessedFile: {processed_file.id}")
        return processed_file

    def get_by_id(self, file_id: str) -> Optional[ProcessedFile]:
        """Get ProcessedFile by ID"""
        return self.db.query(ProcessedFile).filter(ProcessedFile.id == file_id).first()

    def get_by_file_path(self, ingestion_id: str, file_path: str) -> Optional[ProcessedFile]:
        """
        Get ProcessedFile by ingestion_id and file_path.

        Args:
            ingestion_id: Ingestion ID
            file_path: Full file path (e.g., s3://bucket/path/file.json)

        Returns:
            ProcessedFile if exists, None otherwise
        """
        return self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.file_path == file_path
        ).first()

    def get_processed_file_paths(
        self,
        ingestion_id: str,
        statuses: Optional[List[str]] = None
    ) -> Set[str]:
        """
        Get set of file paths that have been processed.

        Args:
            ingestion_id: Ingestion ID
            statuses: List of statuses to include (default: SUCCESS, SKIPPED)

        Returns:
            Set of file paths
        """
        if statuses is None:
            statuses = [ProcessedFileStatus.SUCCESS.value, ProcessedFileStatus.SKIPPED.value]

        files = self.db.query(ProcessedFile.file_path).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status.in_(statuses)
        ).all()

        return {f.file_path for f in files}

    def get_failed_files(self, ingestion_id: str, max_retries: int = 3) -> List[ProcessedFile]:
        """
        Get failed files eligible for retry.

        Args:
            ingestion_id: Ingestion ID
            max_retries: Maximum retry count

        Returns:
            List of ProcessedFile records with status=FAILED and retry_count < max_retries
        """
        return self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status == ProcessedFileStatus.FAILED.value,
            ProcessedFile.retry_count < max_retries
        ).all()

    def get_stale_processing_files(
        self,
        ingestion_id: str,
        stale_threshold_hours: int = 1
    ) -> List[ProcessedFile]:
        """
        Get files stuck in PROCESSING state (likely crashed worker).

        Args:
            ingestion_id: Ingestion ID
            stale_threshold_hours: Hours before considering file stale

        Returns:
            List of ProcessedFile records stuck in PROCESSING
        """
        threshold = datetime.utcnow() - timedelta(hours=stale_threshold_hours)

        return self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status == ProcessedFileStatus.PROCESSING.value,
            ProcessedFile.processing_started_at < threshold
        ).all()

    def lock_file_for_processing(
        self,
        ingestion_id: str,
        file_path: str,
        run_id: str,
        file_metadata: Optional[dict] = None
    ) -> Optional[ProcessedFile]:
        """
        Atomically lock a file for processing using SELECT FOR UPDATE SKIP LOCKED.

        This method is safe for concurrent execution by multiple workers.

        Args:
            ingestion_id: Ingestion ID
            file_path: Full file path
            run_id: Current run ID
            file_metadata: Optional metadata (size, modified_at, etag)

        Returns:
            ProcessedFile record if lock acquired, None if already locked
        """
        # Try to get existing record with row-level lock
        file_record = self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.file_path == file_path
        ).with_for_update(skip_locked=True).first()

        if file_record:
            # Record exists - check status
            if file_record.status == ProcessedFileStatus.PROCESSING.value:
                # Another worker is processing this file
                logger.debug(f"File already being processed: {file_path}")
                return None

            if file_record.status == ProcessedFileStatus.SUCCESS.value:
                # Already successfully processed - check if file changed
                if file_metadata and self._file_changed(file_record, file_metadata):
                    # File modified since last processing - re-process
                    logger.info(f"File changed, re-processing: {file_path}")
                    file_record.status = ProcessedFileStatus.PROCESSING.value
                    file_record.retry_count += 1
                    file_record.run_id = run_id
                    file_record.processing_started_at = datetime.utcnow()
                    self._update_file_metadata(file_record, file_metadata)
                else:
                    # No changes, skip
                    logger.debug(f"File unchanged, skipping: {file_path}")
                    return None
            else:
                # FAILED or DISCOVERED - process/retry
                logger.info(f"Processing file (retry={file_record.retry_count}): {file_path}")
                file_record.status = ProcessedFileStatus.PROCESSING.value
                file_record.retry_count += 1
                file_record.run_id = run_id
                file_record.processing_started_at = datetime.utcnow()
                if file_metadata:
                    self._update_file_metadata(file_record, file_metadata)
        else:
            # Create new record
            logger.info(f"Discovered new file: {file_path}")
            file_record = ProcessedFile(
                ingestion_id=ingestion_id,
                run_id=run_id,
                file_path=file_path,
                status=ProcessedFileStatus.PROCESSING.value,
                discovered_at=datetime.utcnow(),
                processing_started_at=datetime.utcnow(),
                retry_count=0
            )
            if file_metadata:
                self._update_file_metadata(file_record, file_metadata)

            self.db.add(file_record)

        self.db.commit()
        self.db.refresh(file_record)
        return file_record

    def mark_success(
        self,
        file_record: ProcessedFile,
        records_ingested: int,
        bytes_read: int
    ):
        """
        Mark file as successfully processed.

        Args:
            file_record: ProcessedFile instance
            records_ingested: Number of records ingested
            bytes_read: Bytes read from file
        """
        file_record.status = ProcessedFileStatus.SUCCESS.value
        file_record.processed_at = datetime.utcnow()
        file_record.records_ingested = records_ingested
        file_record.bytes_read = bytes_read

        if file_record.processing_started_at:
            duration = (datetime.utcnow() - file_record.processing_started_at).total_seconds()
            file_record.processing_duration_ms = int(duration * 1000)

        file_record.error_message = None
        file_record.error_type = None

        self.db.commit()
        logger.info(f"Marked file SUCCESS: {file_record.file_path} ({records_ingested} records)")

    def mark_failed(self, file_record: ProcessedFile, error: Exception):
        """
        Mark file as failed with error details.

        Args:
            file_record: ProcessedFile instance
            error: Exception that caused failure
        """
        file_record.status = ProcessedFileStatus.FAILED.value
        file_record.error_message = str(error)
        file_record.error_type = type(error).__name__
        file_record.processed_at = datetime.utcnow()

        if file_record.processing_started_at:
            duration = (datetime.utcnow() - file_record.processing_started_at).total_seconds()
            file_record.processing_duration_ms = int(duration * 1000)

        self.db.commit()
        logger.error(f"Marked file FAILED: {file_record.file_path} - {error}")

    def mark_skipped(self, file_record: ProcessedFile, reason: str):
        """
        Mark file as skipped (e.g., too large, schema mismatch).

        Args:
            file_record: ProcessedFile instance
            reason: Reason for skipping
        """
        file_record.status = ProcessedFileStatus.SKIPPED.value
        file_record.error_message = reason
        file_record.processed_at = datetime.utcnow()

        self.db.commit()
        logger.warning(f"Marked file SKIPPED: {file_record.file_path} - {reason}")

    def update(self, file_record: ProcessedFile) -> ProcessedFile:
        """Update ProcessedFile record"""
        self.db.commit()
        self.db.refresh(file_record)
        return file_record

    def _file_changed(self, record: ProcessedFile, metadata: dict) -> bool:
        """
        Check if file has been modified since last processing.

        Args:
            record: Existing ProcessedFile record
            metadata: New metadata from file discovery

        Returns:
            True if file changed, False otherwise
        """
        # Compare ETag (most reliable)
        if metadata.get('etag') and record.file_etag:
            return metadata['etag'] != record.file_etag

        # Fallback to modification time
        if metadata.get('modified_at') and record.file_modified_at:
            return metadata['modified_at'] > record.file_modified_at

        # If no metadata to compare, assume not changed
        return False

    def _update_file_metadata(self, record: ProcessedFile, metadata: dict):
        """
        Update file record with metadata from cloud storage.

        Args:
            record: ProcessedFile instance
            metadata: Metadata dict with size, modified_at, etag
        """
        if 'size' in metadata:
            record.file_size_bytes = metadata['size']
        if 'modified_at' in metadata:
            record.file_modified_at = metadata['modified_at']
        if 'etag' in metadata:
            record.file_etag = metadata['etag']
```

### Service Implementation

```python
"""
File State Service - Business logic for file state management
"""

from typing import Set, List, Optional
from app.repositories.processed_file_repository import ProcessedFileRepository
from app.models.domain import ProcessedFile
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)


class FileStateService:
    """Service for managing file processing state"""

    def __init__(self, db: Session):
        self.db = db
        self.repo = ProcessedFileRepository(db)

    def get_processed_files(
        self,
        ingestion_id: str,
        statuses: Optional[List[str]] = None
    ) -> Set[str]:
        """
        Get set of processed file paths for an ingestion.

        Args:
            ingestion_id: Ingestion ID
            statuses: Filter by statuses (default: SUCCESS, SKIPPED)

        Returns:
            Set of file paths that have been processed
        """
        return self.repo.get_processed_file_paths(ingestion_id, statuses)

    def get_failed_files(self, ingestion_id: str, max_retries: int = 3) -> List[ProcessedFile]:
        """
        Get failed files eligible for retry.

        Args:
            ingestion_id: Ingestion ID
            max_retries: Maximum retry count

        Returns:
            List of ProcessedFile records
        """
        return self.repo.get_failed_files(ingestion_id, max_retries)

    def lock_file_for_processing(
        self,
        ingestion_id: str,
        file_path: str,
        run_id: str,
        file_metadata: Optional[dict] = None
    ) -> Optional[ProcessedFile]:
        """
        Atomically lock a file for processing.

        Safe for concurrent execution by multiple workers.

        Args:
            ingestion_id: Ingestion ID
            file_path: Full file path
            run_id: Current run ID
            file_metadata: Optional metadata (size, modified_at, etag)

        Returns:
            ProcessedFile if lock acquired, None if already locked
        """
        return self.repo.lock_file_for_processing(
            ingestion_id, file_path, run_id, file_metadata
        )

    def mark_file_success(
        self,
        file_record: ProcessedFile,
        records_ingested: int,
        bytes_read: int
    ):
        """Mark file as successfully processed"""
        self.repo.mark_success(file_record, records_ingested, bytes_read)

    def mark_file_failed(self, file_record: ProcessedFile, error: Exception):
        """Mark file as failed with error details"""
        self.repo.mark_failed(file_record, error)

    def mark_file_skipped(self, file_record: ProcessedFile, reason: str):
        """Mark file as skipped (e.g., too large, schema mismatch)"""
        self.repo.mark_skipped(file_record, reason)
```

### Concurrency Safety

**Key Feature:** Uses PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED`

**How it works:**

```sql
-- Worker 1 tries to lock file1.json
SELECT * FROM processed_files
WHERE ingestion_id = '...' AND file_path = 's3://.../file1.json'
FOR UPDATE SKIP LOCKED;

-- Returns row, locks it

-- Worker 2 tries to lock same file
SELECT * FROM processed_files
WHERE ingestion_id = '...' AND file_path = 's3://.../file1.json'
FOR UPDATE SKIP LOCKED;

-- Returns nothing (row locked by Worker 1)
-- Worker 2 skips this file and tries next
```

**Benefits:**
- No deadlocks
- No duplicate work
- Safe for parallel execution
- Automatic lock release on transaction commit

### Usage Example

```python
from app.services.file_state_service import FileStateService
from sqlalchemy.orm import Session

# Initialize service
state_service = FileStateService(db)

# Get already-processed files
processed_files = state_service.get_processed_files(
    ingestion_id="123",
    statuses=["SUCCESS", "SKIPPED"]
)
# Returns: {"s3://bucket/file1.json", "s3://bucket/file2.json", ...}

# Try to lock file for processing
file_record = state_service.lock_file_for_processing(
    ingestion_id="123",
    file_path="s3://bucket/file3.json",
    run_id="run-456",
    file_metadata={
        "size": 1024,
        "modified_at": datetime.utcnow(),
        "etag": "abc123"
    }
)

if file_record:
    # Lock acquired, process file
    try:
        # ... process file ...
        state_service.mark_file_success(file_record, records_ingested=100, bytes_read=1024)
    except Exception as e:
        state_service.mark_file_failed(file_record, e)
else:
    # Another worker is processing, or file already done
    pass
```

---

## Component 3: Batch File Processor

### Purpose

Process files using Spark DataFrame batch API (not streaming) with per-file state tracking.

### Responsibilities

1. Read individual files with Spark DataFrame API
2. Infer or apply schema
3. Write to Iceberg table
4. Track metrics (records, bytes, duration)
5. Handle errors per file (isolation)
6. Update file state via FileStateService

### Refactoring Strategy

**Current Code:** `app/spark/executor.py` uses Auto Loader (cloudFiles) - Databricks-only streaming API

**Phase 1 Changes:**
- Replace `spark.readStream` with `spark.read` (batch API)
- Remove `cloudFiles` format
- Process files one-by-one or in small batches
- Remove `availableNow` trigger (not needed for batch)
- Add per-file error handling

### File Structure

**Location:** `app/services/batch_file_processor.py`

### Implementation

```python
"""
Batch File Processor - Processes files with Spark batch API

Replaces streaming Auto Loader with explicit batch processing.
"""

from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from app.spark.connect_client import SparkConnectClient
from app.services.file_state_service import FileStateService
from app.models.domain import Ingestion, ProcessedFile
import logging

logger = logging.getLogger(__name__)


class BatchFileProcessor:
    """
    Processes files in batches with PostgreSQL state tracking.

    Phase 1: Sequential processing
    Phase 4: Parallel processing with ThreadPool
    """

    def __init__(
        self,
        spark_client: SparkConnectClient,
        state_service: FileStateService,
        ingestion: Ingestion
    ):
        self.spark = spark_client
        self.state = state_service
        self.ingestion = ingestion

    def process_files(
        self,
        files: List[Dict],
        run_id: str
    ) -> Dict[str, int]:
        """
        Process list of files.

        Args:
            files: List of file metadata dicts from FileDiscoveryService
            run_id: ID of the current run for tracking

        Returns:
            Metrics dict: {success: N, failed: N, skipped: N}
        """
        metrics = {'success': 0, 'failed': 0, 'skipped': 0}

        logger.info(f"Processing {len(files)} files for ingestion {self.ingestion.id}")

        for file_info in files:
            file_path = file_info['path']

            # Atomically lock file for processing
            file_record = self.state.lock_file_for_processing(
                ingestion_id=self.ingestion.id,
                file_path=file_path,
                run_id=run_id,
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
        Process a single file with Spark batch API.

        Args:
            file_path: Full S3 path (e.g., s3://bucket/path/file.json)
            file_info: File metadata dict

        Returns:
            Dict with processing results: {record_count: N}
        """
        # Step 1: Read file with Spark DataFrame batch API
        if self.ingestion.schema_json:
            # Use predefined schema
            df = self._read_file_with_schema(file_path)
        else:
            # Infer schema from file
            df = self._read_file_infer_schema(file_path)

        # Step 2: Get record count (for metrics)
        record_count = df.count()

        if record_count == 0:
            logger.warning(f"File {file_path} is empty, skipping write")
            return {'record_count': 0}

        # Step 3: Write to Iceberg table
        self._write_to_iceberg(df)

        return {'record_count': record_count}

    def _read_file_with_schema(self, file_path: str) -> DataFrame:
        """
        Read file with predefined schema.

        Args:
            file_path: Full S3 path

        Returns:
            Spark DataFrame
        """
        reader = self.spark.session.read.format(self.ingestion.format_type.value)

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
        """
        Read file with schema inference.

        Args:
            file_path: Full S3 path

        Returns:
            Spark DataFrame
        """
        reader = self.spark.session.read \
            .format(self.ingestion.format_type.value) \
            .option("inferSchema", "true")

        # Apply format options
        for key, value in self.ingestion.format_options.items():
            reader = reader.option(key, value)

        return reader.load(file_path)

    def _write_to_iceberg(self, df: DataFrame):
        """
        Write DataFrame to Iceberg table.

        Args:
            df: Spark DataFrame to write
        """
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

        # Apply table properties (e.g., z-ordering)
        if self.ingestion.table_properties:
            for key, value in self.ingestion.table_properties.items():
                writer = writer.option(key, value)

        # Write
        writer.save(table_identifier)
        logger.debug(f"Wrote DataFrame to {table_identifier}")
```

### Comparison: Old vs New

**Old Code (Streaming):**

```python
# app/spark/executor.py (current)
def execute(self, ingestion: Ingestion):
    # Uses Databricks Auto Loader (cloudFiles)
    df = self.spark.read_stream(
        format="cloudFiles",  # Databricks-only
        path=ingestion.source_path,
        ...
    )

    query = self.spark.write_stream(
        df=df,
        trigger="availableNow",  # Batch mode
        ...
    )

    query.awaitTermination()
```

**New Code (Batch):**

```python
# app/services/batch_file_processor.py (Phase 1)
def _process_single_file(self, file_path: str):
    # Uses standard Spark DataFrame API
    df = self.spark.session.read \
        .format(self.ingestion.format_type.value) \
        .option("inferSchema", "true") \
        .load(file_path)  # Single file

    df.write \
        .format("iceberg") \
        .mode("append") \
        .save(table_identifier)  # Direct write, no streaming
```

### Error Handling

**Per-File Isolation:** One corrupt file doesn't block others

```python
for file_info in files:
    try:
        # Try to process file
        result = self._process_single_file(file_path, file_info)
        self.state.mark_file_success(...)
        metrics['success'] += 1
    except Exception as e:
        # Catch error, mark file failed
        self.state.mark_file_failed(file_record, e)
        metrics['failed'] += 1
        # Continue to next file
```

**Result:**
- 999 files succeed
- 1 file fails with error
- User can retry only the failed file

### Testing

```python
# tests/services/test_batch_file_processor.py

import pytest
from unittest.mock import Mock
from app.services.batch_file_processor import BatchFileProcessor

def test_process_files_success(mock_spark, mock_state_service, mock_ingestion):
    """Test successful file processing"""
    processor = BatchFileProcessor(mock_spark, mock_state_service, mock_ingestion)

    files = [
        {"path": "s3://bucket/file1.json", "size": 1024},
        {"path": "s3://bucket/file2.json", "size": 2048}
    ]

    # Mock lock returns file records
    mock_state_service.lock_file_for_processing.return_value = Mock()

    # Mock Spark returns DataFrame
    mock_df = Mock()
    mock_df.count.return_value = 100
    mock_spark.session.read.format().option().load.return_value = mock_df

    metrics = processor.process_files(files, run_id="test-run")

    assert metrics['success'] == 2
    assert metrics['failed'] == 0
    assert mock_state_service.mark_file_success.call_count == 2

def test_process_files_with_failure(mock_spark, mock_state_service, mock_ingestion):
    """Test that one failure doesn't block others"""
    processor = BatchFileProcessor(mock_spark, mock_state_service, mock_ingestion)

    files = [
        {"path": "s3://bucket/file1.json", "size": 1024},
        {"path": "s3://bucket/file2.json", "size": 2048},  # Will fail
        {"path": "s3://bucket/file3.json", "size": 512}
    ]

    mock_state_service.lock_file_for_processing.return_value = Mock()

    # Mock Spark: file2 raises error
    def mock_read(file_path):
        if "file2" in file_path:
            raise Exception("Corrupt file")
        mock_df = Mock()
        mock_df.count.return_value = 100
        return mock_df

    mock_spark.session.read.format().option().load.side_effect = mock_read

    metrics = processor.process_files(files, run_id="test-run")

    assert metrics['success'] == 2  # file1 and file3
    assert metrics['failed'] == 1   # file2
    assert mock_state_service.mark_file_success.call_count == 2
    assert mock_state_service.mark_file_failed.call_count == 1
```

---

## Component 4: Batch Orchestrator

### Purpose

Orchestrate the entire batch processing flow: discovery → state check → processing.

### Responsibilities

1. Load ingestion configuration
2. Create Run record (status=RUNNING)
3. Call FileDiscoveryService to list files
4. Call FileStateService to get processed files
5. Compute diff (new files)
6. Call BatchFileProcessor to process new files
7. Update Run record with metrics (status=SUCCESS/FAILED)

### File Structure

**Location:** `app/services/batch_orchestrator.py`

### Implementation

```python
"""
Batch Orchestrator - Coordinates batch processing workflow

Entry point for all batch ingestion runs.
"""

from typing import Dict
from sqlalchemy.orm import Session
from app.models.domain import Ingestion, Run, RunStatus
from app.services.file_discovery_service import FileDiscoveryService
from app.services.file_state_service import FileStateService
from app.services.batch_file_processor import BatchFileProcessor
from app.spark.connect_client import SparkConnectClient
from app.repositories.run_repository import RunRepository
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


class BatchOrchestrator:
    """
    Orchestrates batch processing workflow.

    Flow:
    1. Create Run record
    2. Discover files from S3
    3. Check processed files in PostgreSQL
    4. Compute new files
    5. Process files
    6. Update Run record
    """

    def __init__(self, db: Session):
        self.db = db
        self.run_repo = RunRepository(db)

    def run_ingestion(self, ingestion: Ingestion) -> Run:
        """
        Execute batch ingestion for an ingestion configuration.

        Args:
            ingestion: Ingestion configuration

        Returns:
            Run record with metrics
        """
        logger.info(f"Starting batch ingestion run for: {ingestion.name} ({ingestion.id})")

        # Step 1: Create Run record
        run = self._create_run(ingestion)

        try:
            # Step 2: Initialize services
            discovery_service = self._init_discovery_service(ingestion)
            state_service = FileStateService(self.db)
            spark_client = self._init_spark_client(ingestion)
            processor = BatchFileProcessor(spark_client, state_service, ingestion)

            # Step 3: Discover files from S3
            logger.info(f"Discovering files from {ingestion.source_path}")
            discovered_files = self._discover_files(discovery_service, ingestion)
            logger.info(f"Discovered {len(discovered_files)} files")

            # Step 4: Get already-processed files
            processed_file_paths = state_service.get_processed_files(
                ingestion_id=ingestion.id,
                statuses=["SUCCESS", "SKIPPED"]
            )
            logger.info(f"Already processed: {len(processed_file_paths)} files")

            # Step 5: Compute new files
            new_files = [
                f for f in discovered_files
                if f.path not in processed_file_paths
            ]
            logger.info(f"New files to process: {len(new_files)}")

            if len(new_files) == 0:
                logger.info("No new files to process")
                self._complete_run(run, metrics={'success': 0, 'failed': 0, 'skipped': 0})
                return run

            # Step 6: Process files
            # Convert FileMetadata to dict
            file_dicts = [f.to_dict() for f in new_files]
            metrics = processor.process_files(file_dicts, run_id=run.id)

            # Step 7: Update Run record
            self._complete_run(run, metrics)

            logger.info(f"Batch ingestion run complete: {run.id} - {metrics}")
            return run

        except Exception as e:
            logger.error(f"Batch ingestion run failed: {e}", exc_info=True)
            self._fail_run(run, str(e))
            raise

    def _create_run(self, ingestion: Ingestion) -> Run:
        """Create new Run record with status=RUNNING"""
        run = Run(
            ingestion_id=ingestion.id,
            tenant_id=ingestion.tenant_id,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
            trigger="manual",  # Phase 1: manual only
            cluster_id=ingestion.cluster_id
        )
        return self.run_repo.create(run)

    def _init_discovery_service(self, ingestion: Ingestion) -> FileDiscoveryService:
        """Initialize FileDiscoveryService from ingestion config"""
        credentials = json.loads(ingestion.source_credentials)

        return FileDiscoveryService(
            source_type=ingestion.source_type.value,
            credentials=credentials,
            region=credentials.get('aws_region', 'us-east-1')
        )

    def _init_spark_client(self, ingestion: Ingestion) -> SparkConnectClient:
        """Initialize Spark Connect client"""
        credentials = json.loads(ingestion.source_credentials)

        return SparkConnectClient(
            spark_connect_url=ingestion.spark_connect_url,
            spark_connect_token=ingestion.spark_connect_token,
            source_type=ingestion.source_type.value,
            credentials=credentials
        )

    def _discover_files(
        self,
        discovery_service: FileDiscoveryService,
        ingestion: Ingestion
    ) -> list:
        """
        Discover files from source.

        Args:
            discovery_service: FileDiscoveryService instance
            ingestion: Ingestion configuration

        Returns:
            List of FileMetadata objects
        """
        # Parse bucket and prefix from source_path
        # Example: s3://my-bucket/data/events/ → bucket=my-bucket, prefix=data/events/
        source_path = ingestion.source_path

        if source_path.startswith("s3://"):
            parts = source_path.replace("s3://", "").split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
        else:
            raise ValueError(f"Invalid source_path format: {source_path}")

        # List files
        files = discovery_service.list_files(
            bucket=bucket,
            prefix=prefix,
            pattern=ingestion.source_file_pattern,  # e.g., "*.json"
            max_files=None  # No limit in Phase 1
        )

        return files

    def _complete_run(self, run: Run, metrics: Dict):
        """
        Complete run with success status and metrics.

        Args:
            run: Run record
            metrics: Metrics dict from BatchFileProcessor
        """
        run.status = RunStatus.SUCCESS.value
        run.ended_at = datetime.utcnow()
        run.files_processed = metrics['success'] + metrics['failed']

        # Duration
        if run.started_at:
            duration = (run.ended_at - run.started_at).total_seconds()
            run.duration_seconds = int(duration)

        # Note: records_ingested, bytes_read will be aggregated from processed_files table
        # For Phase 1, we can set them to 0 or aggregate in a separate query

        self.run_repo.update(run)
        logger.info(f"Run completed: {run.id}")

    def _fail_run(self, run: Run, error_message: str):
        """
        Mark run as failed with error message.

        Args:
            run: Run record
            error_message: Error message
        """
        run.status = RunStatus.FAILED.value
        run.ended_at = datetime.utcnow()
        run.errors = [{"message": error_message, "timestamp": datetime.utcnow().isoformat()}]

        if run.started_at:
            duration = (run.ended_at - run.started_at).total_seconds()
            run.duration_seconds = int(duration)

        self.run_repo.update(run)
        logger.error(f"Run failed: {run.id} - {error_message}")
```

### Usage Example

```python
from app.services.batch_orchestrator import BatchOrchestrator
from app.repositories.ingestion_repository import IngestionRepository
from sqlalchemy.orm import Session

# Load ingestion
ingestion_repo = IngestionRepository(db)
ingestion = ingestion_repo.get_by_id(ingestion_id)

# Run batch ingestion
orchestrator = BatchOrchestrator(db)
run = orchestrator.run_ingestion(ingestion)

# Check results
print(f"Run {run.id}: {run.status}")
print(f"Files processed: {run.files_processed}")
print(f"Duration: {run.duration_seconds}s")
```

### Sequence Diagram

```
User            API              IngestionService    BatchOrchestrator    FileDiscovery    FileState    BatchProcessor    Spark
 |               |                     |                    |                   |              |              |              |
 |--POST /run -->|                     |                    |                   |              |              |              |
 |               |--trigger_manual---->|                    |                   |              |              |              |
 |               |                     |--run_ingestion---->|                   |              |              |              |
 |               |                     |                    |--list_files------>|              |              |              |
 |               |                     |                    |<--List[File]------|              |              |              |
 |               |                     |                    |--get_processed--->|              |              |              |
 |               |                     |                    |<--Set[paths]------|              |              |              |
 |               |                     |                    |--process_files------------------->|              |              |
 |               |                     |                    |                   |              |--lock_file-->|              |
 |               |                     |                    |                   |              |<-FileRecord--|              |
 |               |                     |                    |                   |              |              |--read------->|
 |               |                     |                    |                   |              |              |<--DataFrame--|
 |               |                     |                    |                   |              |              |--write------>|
 |               |                     |                    |                   |              |--mark_success|              |
 |               |                     |                    |<--metrics-----------------------------------|              |
 |               |                     |<--Run record-------|                   |              |              |              |
 |<--202 ACCEPTED|                     |                    |                   |              |              |              |
```

---

## API Integration

### Existing Endpoint

**Route:** `POST /api/v1/ingestions/{id}/run`

**Current Implementation:** `app/api/v1/ingestions.py`

```python
@router.post("/{ingestion_id}/run", response_model=RunResponse, status_code=202)
async def trigger_ingestion_run(
    ingestion_id: str,
    db: Session = Depends(get_db)
):
    """Trigger manual ingestion run"""
    ingestion_service = IngestionService(db)
    run = ingestion_service.trigger_manual_run(ingestion_id)
    return run
```

### Updated Implementation

**File:** `app/services/ingestion_service.py`

**Method:** `trigger_manual_run()`

```python
def trigger_manual_run(self, ingestion_id: str) -> Run:
    """
    Trigger manual ingestion run.

    Args:
        ingestion_id: Ingestion ID

    Returns:
        Run record

    Raises:
        ValueError: If ingestion not found or not active
    """
    # Load ingestion
    ingestion = self.ingestion_repo.get_by_id(ingestion_id)
    if not ingestion:
        raise ValueError(f"Ingestion not found: {ingestion_id}")

    if ingestion.status != IngestionStatus.ACTIVE.value:
        raise ValueError(f"Ingestion not active: {ingestion.status}")

    # Run batch ingestion via orchestrator
    orchestrator = BatchOrchestrator(self.db)
    run = orchestrator.run_ingestion(ingestion)

    return run
```

### Response Format

**Success (202 ACCEPTED):**

```json
{
  "id": "run-123",
  "ingestion_id": "ing-456",
  "status": "RUNNING",
  "started_at": "2024-01-15T10:30:00Z",
  "trigger": "manual",
  "files_processed": 0
}
```

**After Completion (GET /api/v1/ingestions/{id}/runs/{run_id}):**

```json
{
  "id": "run-123",
  "ingestion_id": "ing-456",
  "status": "SUCCESS",
  "started_at": "2024-01-15T10:30:00Z",
  "ended_at": "2024-01-15T10:35:00Z",
  "duration_seconds": 300,
  "trigger": "manual",
  "files_processed": 100,
  "records_ingested": 10000,
  "bytes_read": 52428800,
  "errors": []
}
```

---

## Testing Strategy

### Unit Tests

**Structure:**

```
tests/
├── services/
│   ├── test_file_discovery_service.py
│   ├── test_file_state_service.py
│   ├── test_batch_file_processor.py
│   └── test_batch_orchestrator.py
├── repositories/
│   └── test_processed_file_repository.py
├── models/
│   └── test_processed_file.py
└── fixtures/
    ├── mock_s3.py
    └── test_data/
        ├── sample.json
        ├── sample.csv
        └── corrupt.json
```

### Test Coverage Requirements

- [ ] FileDiscoveryService: list_files(), pagination, filtering
- [ ] ProcessedFileRepository: all CRUD operations, locking
- [ ] FileStateService: state transitions, concurrency
- [ ] BatchFileProcessor: file processing, error isolation
- [ ] BatchOrchestrator: full workflow integration

### Integration Tests

**Scenario 1: End-to-End Batch Processing**

```python
def test_e2e_batch_processing(db_session, mock_s3, mock_spark):
    """
    Test complete batch processing workflow.

    Setup:
    - Create ingestion config
    - Mock S3 with 10 files
    - Mock Spark responses

    Execute:
    - Trigger manual run

    Verify:
    - 10 ProcessedFile records created
    - All marked SUCCESS
    - Run record has correct metrics
    """
    pass
```

**Scenario 2: Failed File Doesn't Block Others**

```python
def test_failed_file_isolation(db_session, mock_s3, mock_spark):
    """
    Test that one corrupt file doesn't stop processing.

    Setup:
    - Mock S3 with 5 files (1 corrupt)
    - Mock Spark: file3 raises exception

    Execute:
    - Run batch processing

    Verify:
    - 4 files marked SUCCESS
    - 1 file marked FAILED
    - Run status = SUCCESS (partial)
    """
    pass
```

**Scenario 3: Duplicate Run Doesn't Reprocess**

```python
def test_duplicate_run_skip(db_session, mock_s3, mock_spark):
    """
    Test that already-processed files are skipped.

    Setup:
    - Run batch processing (10 files)
    - All marked SUCCESS

    Execute:
    - Trigger second run (same files)

    Verify:
    - 0 new files processed
    - Run completes immediately
    """
    pass
```

**Scenario 4: File Changed - Reprocess**

```python
def test_file_changed_reprocess(db_session, mock_s3, mock_spark):
    """
    Test that modified files are reprocessed.

    Setup:
    - Process file1.json (ETag: abc123)
    - Mark SUCCESS

    Execute:
    - Modify file1.json (ETag: xyz789)
    - Trigger second run

    Verify:
    - file1.json reprocessed
    - New SUCCESS record or updated record
    """
    pass
```

### Mock S3 Setup

**Using moto library:**

```python
# tests/fixtures/mock_s3.py

import pytest
from moto import mock_s3
import boto3

@pytest.fixture
def mock_s3_bucket():
    """Create mock S3 bucket with test files"""
    with mock_s3():
        # Create S3 resource
        s3 = boto3.client('s3', region_name='us-east-1')

        # Create bucket
        s3.create_bucket(Bucket='test-bucket')

        # Upload test files
        for i in range(10):
            s3.put_object(
                Bucket='test-bucket',
                Key=f'data/file{i}.json',
                Body=f'{{"id": {i}, "value": "test"}}'
            )

        yield s3
```

### Test Data

**Sample JSON file:**

```json
{
  "id": 1,
  "timestamp": "2024-01-15T10:30:00Z",
  "event": "user_login",
  "user_id": "user-123",
  "metadata": {
    "ip": "192.168.1.1",
    "user_agent": "Mozilla/5.0"
  }
}
```

**Sample CSV file:**

```csv
id,timestamp,event,user_id
1,2024-01-15T10:30:00Z,user_login,user-123
2,2024-01-15T10:31:00Z,page_view,user-456
```

---

## Implementation Checklist

### Day 1-3: Database Schema

- [ ] Add ProcessedFile model to `app/models/domain.py`
- [ ] Add ProcessedFileStatus enum
- [ ] Add relationships to Ingestion and Run models
- [ ] Create Alembic migration: `alembic revision --autogenerate -m "add_processed_files"`
- [ ] Review and edit migration file
- [ ] Apply migration: `alembic upgrade head`
- [ ] Verify table created in PostgreSQL
- [ ] Create ProcessedFileRepository in `app/repositories/`
- [ ] Implement all repository methods
- [ ] Write unit tests for ProcessedFileRepository
- [ ] Run tests: `pytest tests/repositories/test_processed_file_repository.py`

### Day 4-6: File Discovery Service

- [ ] Create `app/services/file_discovery_service.py`
- [ ] Implement FileMetadata dataclass
- [ ] Implement FileDiscoveryService class
- [ ] Implement S3 client initialization
- [ ] Implement list_files() with pagination
- [ ] Implement test_connection()
- [ ] Add error handling for S3 errors
- [ ] Create unit tests with mocked boto3
- [ ] Test pagination with 2000+ files
- [ ] Test error scenarios (403, 404, credentials)
- [ ] Run tests: `pytest tests/services/test_file_discovery_service.py`

### Day 7-9: Batch File Processor

- [ ] Create `app/services/batch_file_processor.py`
- [ ] Create `app/services/file_state_service.py`
- [ ] Implement FileStateService wrapper
- [ ] Implement BatchFileProcessor class
- [ ] Implement process_files() method
- [ ] Implement _process_single_file() method
- [ ] Implement _read_file_with_schema() method
- [ ] Implement _read_file_infer_schema() method
- [ ] Implement _write_to_iceberg() method
- [ ] Add per-file error handling
- [ ] Create unit tests with mocked Spark
- [ ] Test successful processing
- [ ] Test error isolation (one failure doesn't block others)
- [ ] Test empty files
- [ ] Run tests: `pytest tests/services/test_batch_file_processor.py`

### Day 10-12: Batch Orchestrator + API Integration

- [ ] Create `app/services/batch_orchestrator.py`
- [ ] Implement BatchOrchestrator class
- [ ] Implement run_ingestion() method
- [ ] Implement _create_run() method
- [ ] Implement _discover_files() method
- [ ] Implement _complete_run() method
- [ ] Implement _fail_run() method
- [ ] Update `app/services/ingestion_service.py`
- [ ] Implement trigger_manual_run() to call orchestrator
- [ ] Test API endpoint: POST /api/v1/ingestions/{id}/run
- [ ] Create integration tests
- [ ] Test end-to-end workflow
- [ ] Test duplicate run (skip already-processed)
- [ ] Test file change detection
- [ ] Run tests: `pytest tests/services/test_batch_orchestrator.py`

### Day 13-14: Testing and Bug Fixes

- [ ] Run full test suite: `pytest`
- [ ] Check test coverage: `pytest --cov=app`
- [ ] Fix any failing tests
- [ ] Perform manual testing with real S3 bucket
- [ ] Test with 1000+ files
- [ ] Test with large files (100MB+)
- [ ] Test with corrupt files
- [ ] Test concurrent runs (2 workers)
- [ ] Verify database constraints
- [ ] Verify concurrency safety (SELECT FOR UPDATE SKIP LOCKED)
- [ ] Document any known issues
- [ ] Update README with Phase 1 status

### Verification Steps

**After Day 3:**
```bash
# Verify database schema
psql -d autoloader_db -c "\d processed_files"
psql -d autoloader_db -c "\d+ processed_files"  # Check indexes
```

**After Day 6:**
```python
# Test file discovery
from app.services.file_discovery_service import FileDiscoveryService

service = FileDiscoveryService("S3", {"aws_access_key_id": "...", "aws_secret_access_key": "..."})
files = service.list_files(bucket="test-bucket", prefix="data/")
print(f"Discovered {len(files)} files")
```

**After Day 9:**
```python
# Test batch processing (with mocks)
pytest tests/services/test_batch_file_processor.py -v
```

**After Day 12:**
```bash
# Test API endpoint
curl -X POST http://localhost:8000/api/v1/ingestions/{id}/run
```

---

## Migration from Current Code

### Code to Keep (Reuse)

✅ **Database Models:**
- `app/models/domain.py`: Ingestion, Run (add relationship only)
- `app/database.py`: Database setup, session management

✅ **Repositories:**
- `app/repositories/ingestion_repository.py`: Fully reusable
- `app/repositories/run_repository.py`: Fully reusable

✅ **API Layer:**
- `app/api/v1/ingestions.py`: Keep all routes (update trigger_manual_run)
- `app/api/v1/runs.py`: Keep all routes

✅ **Configuration:**
- `app/config.py`: Keep all settings
- `app/main.py`: Keep FastAPI app setup

✅ **Spark Connect Client:**
- `app/spark/connect_client.py`: Keep most methods
- Keep: `connect()`, `test_connection()`, credential setup
- Refactor: Remove `read_stream()`, keep batch read methods

### Code to Refactor

⚠️ **Spark Executor:**

**Current:** `app/spark/executor.py` (uses cloudFiles streaming)

**Changes Needed:**
1. Remove `read_stream()` usage
2. Remove `cloudFiles` format
3. Remove `write_stream()` and `availableNow` trigger
4. Replace with batch DataFrame API (moved to BatchFileProcessor)
5. Keep session management logic

**Option:** Keep file for future streaming mode, create new BatchFileProcessor for Phase 1

⚠️ **Ingestion Service:**

**Current:** `app/services/ingestion_service.py`

**Changes Needed:**
1. Update `trigger_manual_run()` to call BatchOrchestrator
2. Keep all other methods (create, update, delete, preview, etc.)

### Code to Add (New)

➕ **New Files:**
1. `app/models/domain.py`: Add ProcessedFile model
2. `app/repositories/processed_file_repository.py`: New repository
3. `app/services/file_discovery_service.py`: New service
4. `app/services/file_state_service.py`: New service
5. `app/services/batch_file_processor.py`: New service
6. `app/services/batch_orchestrator.py`: New service
7. `alembic/versions/XXX_add_processed_files.py`: New migration

➕ **Tests:**
1. `tests/repositories/test_processed_file_repository.py`
2. `tests/services/test_file_discovery_service.py`
3. `tests/services/test_file_state_service.py`
4. `tests/services/test_batch_file_processor.py`
5. `tests/services/test_batch_orchestrator.py`

### Migration Strategy

**Step 1: Parallel Development**
- Keep existing code working
- Add new Phase 1 code alongside
- Don't delete streaming code yet (future use)

**Step 2: Route Traffic**
- Update `trigger_manual_run()` to use BatchOrchestrator
- Old streaming code becomes dead code (but keep for reference)

**Step 3: Testing**
- Test new batch processing path thoroughly
- Compare metrics with streaming approach (if available)

**Step 4: Documentation**
- Update README with Phase 1 approach
- Document differences from streaming

### Code Organization

**Before Phase 1:**
```
app/
├── services/
│   ├── ingestion_service.py (uses streaming)
│   └── spark_service.py
└── spark/
    ├── executor.py (cloudFiles streaming)
    └── connect_client.py
```

**After Phase 1:**
```
app/
├── services/
│   ├── ingestion_service.py (updated to use batch)
│   ├── spark_service.py
│   ├── file_discovery_service.py (NEW)
│   ├── file_state_service.py (NEW)
│   ├── batch_file_processor.py (NEW)
│   └── batch_orchestrator.py (NEW)
├── spark/
│   ├── executor.py (keep for future, not used in Phase 1)
│   └── connect_client.py (updated for batch)
└── repositories/
    └── processed_file_repository.py (NEW)
```

---

## Summary

### What We're Building

Phase 1 delivers a **production-ready batch ingestion system** for AWS S3 with PostgreSQL state tracking.

### Key Features

- ✅ S3 file discovery with pagination
- ✅ Per-file state tracking in PostgreSQL
- ✅ Batch processing with Spark DataFrame API
- ✅ Per-file error isolation
- ✅ Concurrency-safe with row-level locking
- ✅ File change detection (ETag)
- ✅ Manual "Run Now" trigger
- ✅ Complete audit trail

### Success Metrics

- [ ] Can process 1000+ files from S3
- [ ] Failed files don't block successful files
- [ ] Duplicate runs skip already-processed files
- [ ] Modified files are detected and reprocessed
- [ ] All database queries use indexes
- [ ] Test coverage > 80%
- [ ] API responds in < 500ms
- [ ] Processing throughput > 100 files/minute

### Next Steps After Phase 1

**Phase 2: Processing Pipeline (Week 3-4)**
- Scheduler integration (APScheduler)
- Retry logic for failed files
- Stale lock cleanup job
- Advanced error handling

**Phase 3: Schema Evolution (Week 5-6)**
- Schema detection and evolution
- User approval workflow
- Schema version tracking

**Phase 4: Performance & Observability (Week 7-8)**
- Parallel processing with ThreadPool
- Partition-aware listing
- Monitoring dashboard
- Alerting

**Phase 5: Production Hardening (Week 9-10)**
- Security audit
- Load testing
- Operational runbooks
- Multi-cloud support (Azure, GCS)

---

## Questions & Clarifications

**For Discussion Before Implementation:**

1. **Unique Constraint:** Should `(ingestion_id, file_path)` be unique in processed_files table?
   - Option A: Unique (one record per file, update on reprocess)
   - Option B: Non-unique (new record on reprocess, track history)
   - **Recommendation:** Unique (simpler queries, less storage)

2. **File Change Detection:** Use ETag or modification time?
   - ETag is more reliable but requires extra S3 API call
   - Modification time is free but less reliable
   - **Recommendation:** ETag (S3 provides it in list_objects response)

3. **Max File Size:** What's the limit for single-file processing?
   - Spark executor memory constraint
   - **Recommendation:** 5GB default, configurable per ingestion

4. **Concurrency:** How many workers can process same ingestion?
   - PostgreSQL row-level locking supports unlimited
   - **Recommendation:** Start with 1 worker, scale to 10+ in Phase 4

5. **Checkpoint Location:** Use separate checkpoint per run or shared?
   - **Phase 1:** No checkpoints needed (using batch API, not streaming)
   - **Future:** If we re-introduce streaming, use shared checkpoint

---

**End of Phase 1 Implementation Guide**

This document provides all the details needed to implement Phase 1. Refer back to this during development for specifications, code examples, and testing strategies.
