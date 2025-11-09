# Transaction and Commit Management Analysis

**Date:** 2025-11-09
**Status:** Critical Issues Identified
**Priority:** High - Requires Immediate Action

---

## Executive Summary

This document provides a comprehensive analysis of transaction and commit handling across the IOMETE Autoloader application, with specific consideration for Prefect distributed execution on worker nodes.

### Critical Findings

**🔴 CRITICAL ISSUES:**
1. Repositories commit directly, violating transaction boundaries
2. Service layer cannot control transaction scope
3. Potential for orphaned records on partial failures
4. Redundant commits causing confusion

**🟢 CORRECT PATTERNS:**
1. Prefect tasks creating separate database sessions (required for distributed execution)
2. Row-level locking with `SELECT FOR UPDATE SKIP LOCKED`
3. Individual file commits in batch processing for partial progress

---

## Architecture Context

### Distributed Execution Model

The application uses **Prefect** for workflow orchestration, where:

```
┌─────────────────────────────────────────────────────────┐
│                   Prefect Server                        │
└─────────────────────────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼───┐      ┌────▼───┐     ┌────▼───┐
    │Worker A│      │Worker B│     │Worker C│
    │        │      │        │     │        │
    │Task 1  │      │Task 2  │     │Task 3  │
    └────┬───┘      └────┬───┘     └────┬───┘
         │               │               │
         └───────────────┼───────────────┘
                         │
                    ┌────▼────┐
                    │PostgreSQL│
                    └─────────┘
```

**Key Characteristics:**
- Each Prefect task runs on potentially different machines
- Each task creates its own database session
- No shared session state between tasks
- Tasks must be atomic and idempotent

---

## Detailed Analysis

### 1. Repository Layer - Direct Commits (CRITICAL)

**Problem:** All repositories commit immediately after every operation, breaking transaction boundaries.

#### Examples Found

**File: app/repositories/ingestion_repository.py**

```python
# Lines 15-20
def create(self, ingestion: Ingestion) -> Ingestion:
    """Create a new ingestion."""
    self.db.add(ingestion)
    self.db.commit()  # ❌ PROBLEM: Commits immediately
    self.db.refresh(ingestion)
    return ingestion

# Lines 38-43
def update(self, ingestion: Ingestion) -> Ingestion:
    """Update an ingestion."""
    ingestion.updated_at = datetime.utcnow()
    self.db.commit()  # ❌ PROBLEM: Commits immediately
    self.db.refresh(ingestion)
    return ingestion

# Lines 45-52
def delete(self, ingestion_id: str) -> bool:
    """Delete an ingestion."""
    ingestion = self.get_by_id(ingestion_id)
    if ingestion:
        self.db.delete(ingestion)
        self.db.commit()  # ❌ PROBLEM: Commits immediately
        return True
    return False
```

**File: app/repositories/run_repository.py**

```python
# Lines 15-20
def create(self, run: Run) -> Run:
    """Create a new run."""
    self.db.add(run)
    self.db.commit()  # ❌ Line 18
    self.db.refresh(run)
    return run

# Lines 39-43
def update(self, run: Run) -> Run:
    """Update a run."""
    self.db.commit()  # ❌ Line 41
    self.db.refresh(run)
    return run
```

**File: app/repositories/processed_file_repository.py**

```python
# Lines 21-35
def create(self, processed_file: ProcessedFile) -> ProcessedFile:
    self.db.add(processed_file)
    self.db.commit()  # ❌ Line 32
    self.db.refresh(processed_file)
    return processed_file

# Lines 122-198
def lock_file_for_processing(...) -> Optional[ProcessedFile]:
    # ... locking logic ...
    self.db.commit()  # ❌ Line 196
    self.db.refresh(file_record)
    return file_record

# Lines 200-228
def mark_success(self, file_record: ProcessedFile, ...):
    # ... update logic ...
    self.db.commit()  # ❌ Line 227

# Lines 230-249
def mark_failed(self, file_record: ProcessedFile, error: Exception):
    # ... update logic ...
    self.db.commit()  # ❌ Line 248

# Lines 251-264
def mark_skipped(self, file_record: ProcessedFile, reason: str):
    # ... update logic ...
    self.db.commit()  # ❌ Line 263
```

#### Why This Is Wrong

1. **Violates Single Responsibility Principle**
   - Repositories should handle data access, NOT transaction management
   - Transaction boundaries are a service/application layer concern

2. **Breaks Atomicity**
   - Cannot group multiple repository operations into a single transaction
   - Service layer cannot rollback partial operations

3. **Prevents Complex Workflows**
   - Multi-step operations can't be atomic
   - Partial failures leave inconsistent state

4. **Confusing Ownership**
   - Unclear who is responsible for transaction management

---

### 2. Service Layer - Lost Transaction Control

**Problem:** Services cannot control transaction boundaries when repositories commit internally.

#### Critical Example: Ingestion Creation

**File: app/services/ingestion_service.py:104-125**

```python
async def create_ingestion(self, data: IngestionCreate, user_id: str):
    # ... build ingestion object ...

    # Step 1: Save to database
    ingestion = self.ingestion_repo.create(ingestion)  # ❌ Commits at line 104

    # Step 2: Create Prefect deployment (required for execution)
    if ingestion.schedule_frequency:
        prefect = await get_prefect_service()

        if not prefect.initialized:
            # Rollback ingestion creation
            self.ingestion_repo.delete(ingestion_id)  # ❌ Tries to rollback
            raise HTTPException(
                status_code=503,
                detail="Cannot create ingestion: Orchestration service unavailable"
            )

        deployment_id = await prefect.create_deployment(ingestion)

        # Store deployment ID
        ingestion.prefect_deployment_id = deployment_id
        self.db.commit()  # ❌ Yet another commit at line 123
        self.db.refresh(ingestion)
```

**What Happens:**

```
┌─────────────────────────────────────────────────────────┐
│ Timeline: Ingestion Creation with Prefect Failure      │
└─────────────────────────────────────────────────────────┘

t=0: service.create_ingestion() called
  │
t=1: ingestion_repo.create(ingestion)
  │   ├─ INSERT INTO ingestion ...
  │   └─ COMMIT ✓  ← Ingestion now in database
  │
t=2: Check Prefect service
  │   └─ prefect.initialized = False  ← Problem detected!
  │
t=3: Try to rollback
  │   └─ ingestion_repo.delete(ingestion_id)
  │       ├─ DELETE FROM ingestion ...
  │       └─ COMMIT ✓  ← Delete committed
  │
t=4: Raise HTTPException

Result: Two database transactions (create + delete) instead of one rollback
Risk: If delete fails, orphaned ingestion record remains
```

#### Another Example: Refresh Service

**File: app/services/refresh_service.py:119-190**

```python
async def refresh(self, ingestion_id: str, ...):
    # Step 1: Drop table
    try:
        table_result = self.spark.drop_table(...)  # External operation
        operations.append({"operation": "table_dropped", "status": "success"})
    except Exception as e:
        operations.append({"operation": "table_dropped", "status": "failed"})
        return self._build_error_response(...)

    # Step 2: Clear processed files (only if mode="full")
    if is_full_refresh:
        try:
            files_cleared = self.file_state.clear_processed_files(ingestion_id)
            # ❌ This commits internally via repository
            operations.append({"operation": "processed_files_cleared", "status": "success"})
        except Exception as e:
            # ❌ Table is already dropped, can't rollback!
            operations.append({"operation": "processed_files_cleared", "status": "failed"})
            return self._build_error_response(...)
```

**Problem:** If file clearing fails, the table is already dropped. No way to rollback the entire operation.

---

### 3. Prefect Tasks - Session Management

**Current Implementation:**

**File: app/prefect/tasks/run_management.py:21-60**

```python
@task(name="create_run_record", retries=1, tags=["database"])
def create_run_record_task(ingestion_id: str, trigger: str = "scheduled") -> str:
    logger = get_run_logger()

    db = SessionLocal()  # ✅ New session per task
    try:
        ingestion_repo = IngestionRepository(db)
        ingestion = ingestion_repo.get_by_id(ingestion_id)

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        run = Run(
            id=str(uuid.uuid4()),
            ingestion_id=ingestion_id,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
            trigger=trigger,
            cluster_id=ingestion.cluster_id
        )

        run_repo = RunRepository(db)
        created_run = run_repo.create(run)  # Commits internally
        db.commit()  # ❌ Redundant commit

        logger.info(f"Created run record: {created_run.id}")
        return created_run.id

    finally:
        db.close()  # ✅ Cleanup
```

**File: app/prefect/tasks/processing.py:15-67**

```python
@task(name="process_files", retries=2, timeout_seconds=3600, tags=["processing"])
def process_files_task(ingestion_id: str, run_id: str, files_to_process: List[Dict]):
    logger = get_run_logger()

    db = SessionLocal()  # ✅ New session per task
    try:
        repo = IngestionRepository(db)
        ingestion = repo.get_by_id(ingestion_id)

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Initialize services
        spark_url, spark_token = get_spark_connect_credentials(ingestion.cluster_id)
        spark_client = SparkConnectClient(connect_url=spark_url, token=spark_token)

        state_service = FileStateService(db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion)

        # Process files (commits happen per file internally)
        metrics = processor.process_files(files_to_process, run_id=run_id)

        logger.info(f"Processing complete: {metrics}")
        return metrics

    finally:
        db.close()  # ✅ Cleanup
```

#### Analysis

**✅ What's Correct:**
- Each Prefect task creates its own database session
- Sessions are properly closed in `finally` blocks
- This is **REQUIRED** for distributed execution across worker nodes
- Each worker needs its own connection to the database

**❌ What's Wrong:**
- Redundant commits (e.g., line 54 in run_management.py after repository already committed)
- Lack of explicit rollback on errors
- Tasks are not idempotent (retrying creates duplicate records)

**Why Separate Sessions Are Required:**

```
Worker Node A (California)          Worker Node B (Virginia)
┌───────────────────────┐          ┌───────────────────────┐
│ create_run_record()   │          │ process_files()       │
│                       │          │                       │
│ db = SessionLocal()   │          │ db = SessionLocal()   │
│  ├─ conn_1 ──────────────┐       │  ├─ conn_2 ──────────────┐
│  └─ Transaction 1     │  │       │  └─ Transaction 2     │  │
└───────────────────────┘  │       └───────────────────────┘  │
                           │                                  │
                           └──────────┬───────────────────────┘
                                      │
                              ┌───────▼──────┐
                              │  PostgreSQL  │
                              │  Database    │
                              └──────────────┘
```

**Cannot share sessions across network boundaries!**

---

### 4. Batch File Processing - Intentional Partial Commits

**File: app/services/batch_file_processor.py:36-93**

```python
def process_files(self, files: List[Dict], run_id: str) -> Dict[str, int]:
    metrics = {'success': 0, 'failed': 0, 'skipped': 0}

    for file_info in files:
        file_path = file_info['path']

        # Atomically lock file for processing
        file_record = self.state.lock_file_for_processing(
            ingestion_id=self.ingestion.id,
            file_path=file_path,
            run_id=run_id,
            file_metadata=file_info
        )  # ✅ Commits internally to release lock immediately

        if not file_record:
            # Another worker is processing or file already done
            metrics['skipped'] += 1
            continue

        try:
            # Process single file
            result = self._process_single_file(file_path, file_info)

            # Mark success
            self.state.mark_file_success(
                file_record,
                records_ingested=result['record_count'],
                bytes_read=file_info.get('size', 0)
            )  # ✅ Commits internally
            metrics['success'] += 1

        except Exception as e:
            # Mark failure
            self.state.mark_file_failed(file_record, e)  # ✅ Commits internally
            metrics['failed'] += 1
```

#### Analysis

**✅ Correct Design Decisions:**

1. **Per-File Commits**
   - Each file commits separately
   - Partial progress is saved
   - If worker crashes, completed files are preserved
   - Other workers can pick up remaining files

2. **Row-Level Locking**
   - `lock_file_for_processing()` uses `SELECT FOR UPDATE SKIP LOCKED`
   - Prevents duplicate processing by concurrent workers
   - Lock is released immediately via commit

**⚠️ Trade-offs:**

- No atomicity across multiple files
- Run record updates are separate from file processing
- Requires reconciliation logic in `complete_run_record_task`

**Example Scenario:**

```
┌─────────────────────────────────────────────────────────┐
│ Worker crashes while processing 100 files              │
└─────────────────────────────────────────────────────────┘

Files 1-50:  ✅ Committed as SUCCESS
Files 51-75: ✅ Committed as FAILED
File 76:     🔄 Locked (PROCESSING) but worker crashed
Files 77-100: ⏳ Not started

Recovery:
- Restart triggers get_stale_processing_files()
- File 76 detected as stale (PROCESSING for > 1 hour)
- Retry logic re-processes file 76 and files 77-100
```

This is **intentional and correct** for this use case.

---

### 5. Row-Level Locking Implementation

**File: app/repositories/processed_file_repository.py:122-198**

```python
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
    """
    # Try to get existing record with row-level lock
    file_record = self.db.query(ProcessedFile).filter(
        ProcessedFile.ingestion_id == ingestion_id,
        ProcessedFile.file_path == file_path
    ).with_for_update(skip_locked=True).first()  # ✅ PostgreSQL row lock

    if file_record:
        # Record exists - check status
        if file_record.status == ProcessedFileStatus.PROCESSING.value:
            # Another worker is processing this file
            return None  # Skip

        if file_record.status == ProcessedFileStatus.SUCCESS.value:
            # Check if file changed
            if file_metadata and self._file_changed(file_record, file_metadata):
                # Re-process
                file_record.status = ProcessedFileStatus.PROCESSING.value
                file_record.retry_count += 1
            else:
                return None  # Skip unchanged file
        else:
            # FAILED or DISCOVERED - retry
            file_record.status = ProcessedFileStatus.PROCESSING.value
            file_record.retry_count += 1
    else:
        # Create new record
        file_record = ProcessedFile(
            ingestion_id=ingestion_id,
            run_id=run_id,
            file_path=file_path,
            status=ProcessedFileStatus.PROCESSING.value,
            discovered_at=datetime.now(timezone.utc),
            processing_started_at=datetime.now(timezone.utc),
            retry_count=0
        )
        self.db.add(file_record)

    self.db.commit()  # ✅ Must commit to release lock
    self.db.refresh(file_record)
    return file_record
```

#### Why Immediate Commit is Required

**PostgreSQL Row Locking:**

```sql
-- What happens internally:

-- Worker A executes:
BEGIN;
SELECT * FROM processed_files
WHERE file_path = 'file1.json'
FOR UPDATE SKIP LOCKED;
-- Row is locked

-- Worker B executes (concurrent):
BEGIN;
SELECT * FROM processed_files
WHERE file_path = 'file1.json'
FOR UPDATE SKIP LOCKED;
-- Returns empty (lock is held by Worker A)

-- Worker A must commit to release lock:
UPDATE processed_files SET status = 'PROCESSING' WHERE ...;
COMMIT;  -- Lock released
```

**✅ This pattern is correct for distributed file processing.**

---

## Summary of Issues

### Critical Issues (Priority 1)

| Issue | Location | Impact | Line References |
|-------|----------|--------|-----------------|
| Repositories commit directly | All repositories | Breaks transaction boundaries | ingestion_repository.py:18,42,50<br>run_repository.py:18,41<br>processed_file_repository.py:32,196,227,248,263,288 |
| No rollback capability in services | Service layer | Orphaned records on failures | ingestion_service.py:104-125<br>refresh_service.py:119-190 |
| Redundant commits | Prefect tasks | Confusing code | run_management.py:54,108,149 |
| Non-idempotent tasks | Prefect tasks | Duplicate records on retries | run_management.py:21-60 |

### Correct Patterns (To Preserve)

| Pattern | Location | Reason |
|---------|----------|--------|
| Separate sessions per Prefect task | All Prefect tasks | Required for distributed execution |
| Row-level locking with immediate commit | processed_file_repository.py:122-198 | Required to release PostgreSQL locks |
| Per-file commits in batch processing | batch_file_processor.py:36-93 | Saves partial progress |

---

## Recommendations

### Phase 1: Critical Fixes (Immediate)

#### 1.1 Remove Commits from Repositories

**All repository files should change from:**

```python
def create(self, entity: Entity) -> Entity:
    self.db.add(entity)
    self.db.commit()  # ❌ Remove this
    self.db.refresh(entity)
    return entity
```

**To:**

```python
def create(self, entity: Entity) -> Entity:
    """
    Create entity (does not commit).
    Caller is responsible for transaction management.
    """
    self.db.add(entity)
    self.db.flush()  # ✅ Get ID without committing
    self.db.refresh(entity)
    return entity
```

**Exception:** `ProcessedFileRepository.lock_file_for_processing()` should keep its commit (required for lock release).

**Files to modify:**
- `app/repositories/ingestion_repository.py` - remove commits from lines 18, 42, 50
- `app/repositories/run_repository.py` - remove commits from lines 18, 41
- `app/repositories/processed_file_repository.py` - remove commits from lines 32, 227, 248, 263, 288 (keep line 196)

#### 1.2 Add Transaction Control to Services

**Pattern for service methods:**

```python
async def create_ingestion(self, data: IngestionCreate, user_id: str) -> IngestionResponse:
    """
    Create ingestion with proper transaction management.
    All operations commit together or rollback together.
    """
    try:
        # Step 1: Create ingestion (no commit yet)
        ingestion = Ingestion(...)
        ingestion = self.ingestion_repo.create(ingestion)

        # Step 2: Estimate cost
        cost = self.cost_estimator.estimate(data)
        ingestion.estimated_monthly_cost = cost.total_monthly

        # Step 3: Create Prefect deployment
        if ingestion.schedule_frequency:
            prefect = await get_prefect_service()

            if not prefect.initialized:
                # Will trigger rollback below
                raise HTTPException(
                    status_code=503,
                    detail="Cannot create ingestion: Orchestration service unavailable"
                )

            deployment_id = await prefect.create_deployment(ingestion)
            ingestion.prefect_deployment_id = deployment_id

        # Step 4: ONLY commit if ALL operations succeeded
        self.db.commit()
        self.db.refresh(ingestion)

        logger.info(f"✅ Created ingestion {ingestion.id}")
        return self._to_response(ingestion)

    except Exception as e:
        # Rollback ALL database changes
        self.db.rollback()
        logger.error(f"❌ Failed to create ingestion: {e}")
        raise
```

**Files to modify:**
- `app/services/ingestion_service.py` - all async methods
- `app/services/refresh_service.py` - refresh() method

#### 1.3 Fix Prefect Tasks

**Remove redundant commits and add proper error handling:**

```python
@task(name="create_run_record", retries=1, tags=["database"])
def create_run_record_task(ingestion_id: str, trigger: str = "scheduled") -> str:
    """
    Create Run record with status=RUNNING.
    Idempotent: Can be safely retried.
    """
    db = SessionLocal()
    try:
        # Check if run already exists (for idempotency)
        # ... idempotency logic ...

        ingestion_repo = IngestionRepository(db)
        ingestion = ingestion_repo.get_by_id(ingestion_id)

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        run = Run(
            id=str(uuid.uuid4()),
            ingestion_id=ingestion_id,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
            trigger=trigger,
            cluster_id=ingestion.cluster_id
        )

        run_repo = RunRepository(db)
        created_run = run_repo.create(run)  # No longer commits

        # ✅ Single commit at task boundary
        db.commit()

        logger.info(f"Created run record: {created_run.id}")
        return created_run.id

    except Exception as e:
        # ✅ Explicit rollback
        db.rollback()
        logger.error(f"Failed to create run record: {e}")
        raise

    finally:
        db.close()
```

**Files to modify:**
- `app/prefect/tasks/run_management.py` - all tasks
- `app/prefect/tasks/discovery.py` - all tasks
- `app/prefect/tasks/state.py` - all tasks

---

### Phase 2: Add Safeguards (Short Term)

#### 2.1 Transaction Context Manager

**Create reusable transaction context:**

**File: app/database.py (add this)**

```python
from contextlib import contextmanager
from typing import Generator

@contextmanager
def transaction_scope(session_factory=SessionLocal) -> Generator[Session, None, None]:
    """
    Provide a transactional scope around a series of operations.

    Usage:
        with transaction_scope() as session:
            repo = SomeRepository(session)
            repo.create(entity)
            # ... more operations ...
        # Automatically commits on success, rolls back on exception
    """
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
```

**Usage in services:**

```python
from app.database import transaction_scope

class SomeService:
    def complex_operation(self):
        with transaction_scope() as session:
            repo1 = Repository1(session)
            repo2 = Repository2(session)

            repo1.create(entity1)
            repo2.update(entity2)
            # Auto-commit on success, auto-rollback on exception
```

#### 2.2 Make Prefect Tasks Idempotent

**Pattern for idempotent task:**

```python
@task(name="create_run_record", retries=1, tags=["database"])
def create_run_record_task(
    ingestion_id: str,
    trigger: str = "scheduled",
    run_id: Optional[str] = None  # ✅ Allow passing existing run_id
) -> str:
    """
    Create or retrieve run record (idempotent).

    If run_id is provided and exists, returns existing run.
    Otherwise creates new run.
    """
    db = SessionLocal()
    try:
        run_repo = RunRepository(db)

        # Check if run already exists
        if run_id:
            existing_run = run_repo.get_run(run_id)
            if existing_run:
                logger.info(f"Run {run_id} already exists, skipping creation")
                return existing_run.id

        # Create new run
        ingestion_repo = IngestionRepository(db)
        ingestion = ingestion_repo.get_by_id(ingestion_id)

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        run = Run(
            id=run_id or str(uuid.uuid4()),  # Use provided ID or generate
            ingestion_id=ingestion_id,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
            trigger=trigger,
            cluster_id=ingestion.cluster_id
        )

        created_run = run_repo.create(run)
        db.commit()

        logger.info(f"Created run record: {created_run.id}")
        return created_run.id

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create run record: {e}")
        raise

    finally:
        db.close()
```

---

### Phase 3: Long-Term Improvements

#### 3.1 Unit of Work Pattern

**Implement for complex multi-repository operations:**

```python
# app/database.py

class UnitOfWork:
    """
    Coordinates multiple repository operations in a single transaction.
    """

    def __init__(self, session_factory=SessionLocal):
        self.session_factory = session_factory
        self._session: Optional[Session] = None

    def __enter__(self):
        self._session = self.session_factory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self._session.close()

    @property
    def session(self) -> Session:
        if self._session is None:
            raise RuntimeError("UnitOfWork not entered")
        return self._session

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    # Lazy-load repositories
    @property
    def ingestions(self) -> IngestionRepository:
        return IngestionRepository(self.session)

    @property
    def runs(self) -> RunRepository:
        return RunRepository(self.session)

    @property
    def processed_files(self) -> ProcessedFileRepository:
        return ProcessedFileRepository(self.session)
```

**Usage:**

```python
class IngestionService:
    async def create_ingestion(self, data: IngestionCreate, user_id: str):
        with UnitOfWork() as uow:
            # Create ingestion
            ingestion = Ingestion(...)
            ingestion = uow.ingestions.create(ingestion)

            # Create Prefect deployment
            if ingestion.schedule_frequency:
                deployment_id = await self._create_deployment(ingestion)
                ingestion.prefect_deployment_id = deployment_id

            # Single commit for all operations
            # (happens automatically on __exit__)
```

#### 3.2 Saga Pattern for Distributed Transactions

**For operations spanning multiple services (DB + Prefect):**

```python
class IngestionCreationSaga:
    """
    Coordinates ingestion creation across database and Prefect.
    Provides compensating transactions for rollback.
    """

    def __init__(self, db: Session):
        self.db = db
        self.completed_steps = []

    async def execute(self, data: IngestionCreate, user_id: str):
        try:
            # Step 1: Create ingestion in DB
            ingestion = await self._create_ingestion_in_db(data, user_id)
            self.completed_steps.append(('db_create', ingestion.id))

            # Step 2: Create Prefect deployment
            if ingestion.schedule_frequency:
                deployment_id = await self._create_prefect_deployment(ingestion)
                self.completed_steps.append(('prefect_create', deployment_id))

                # Step 3: Link deployment to ingestion
                await self._link_deployment(ingestion, deployment_id)
                self.completed_steps.append(('link', (ingestion.id, deployment_id)))

            # All steps succeeded
            self.db.commit()
            return ingestion

        except Exception as e:
            # Compensating transactions (in reverse order)
            await self._rollback()
            raise

    async def _rollback(self):
        """Execute compensating transactions for completed steps."""
        for step_type, data in reversed(self.completed_steps):
            try:
                if step_type == 'prefect_create':
                    await self._delete_prefect_deployment(data)
                elif step_type == 'db_create':
                    self._delete_ingestion_from_db(data)
                # ... more compensating actions ...
            except Exception as e:
                logger.error(f"Compensating transaction failed: {e}")

        self.db.rollback()
```

---

## Migration Strategy

### Step 1: Preparation (No Code Changes)

1. **Create comprehensive test suite:**
   - Test multi-step service operations
   - Test Prefect task retries
   - Test concurrent file processing
   - Test partial failure scenarios

2. **Document current behavior:**
   - Record expected outcomes for test scenarios
   - Identify areas where current behavior is buggy

### Step 2: Repository Layer (Breaking Change)

1. **Remove all commits from repositories** (except lock_file_for_processing)
2. **Add `flush()` calls** where needed to get generated IDs
3. **Update repository docstrings** to indicate they don't commit
4. **Run test suite** - many tests will fail (expected)

### Step 3: Service Layer

1. **Add explicit commits** to all service methods
2. **Add try/except/rollback** blocks
3. **Update test suite** to verify new behavior
4. **All tests should pass**

### Step 4: Prefect Tasks

1. **Remove redundant commits**
2. **Add explicit error handling**
3. **Make tasks idempotent**
4. **Test retry scenarios**

### Step 5: Advanced Patterns (Optional)

1. **Implement transaction_scope context manager**
2. **Refactor services to use it**
3. **Implement UnitOfWork pattern** for complex operations
4. **Consider Saga pattern** for multi-service operations

---

## Testing Strategy

### Unit Tests

```python
# tests/repositories/test_ingestion_repository.py

def test_repository_does_not_commit(db_session):
    """Verify repository operations don't auto-commit."""
    repo = IngestionRepository(db_session)

    ingestion = Ingestion(...)
    repo.create(ingestion)

    # Rollback should undo the create
    db_session.rollback()

    # Ingestion should not exist
    result = repo.get_by_id(ingestion.id)
    assert result is None
```

### Integration Tests

```python
# tests/services/test_ingestion_service.py

@pytest.mark.asyncio
async def test_create_ingestion_rolls_back_on_prefect_failure(db_session, mock_prefect_unavailable):
    """
    Verify that ingestion creation is rolled back if Prefect deployment fails.
    """
    service = IngestionService(db_session)

    data = IngestionCreate(
        name="Test Ingestion",
        schedule=ScheduleConfig(frequency="daily", ...),
        # ... other required fields ...
    )

    # Should raise exception due to Prefect being unavailable
    with pytest.raises(HTTPException) as exc_info:
        await service.create_ingestion(data, user_id="test_user")

    assert exc_info.value.status_code == 503

    # Verify NO ingestion was created in database
    all_ingestions = db_session.query(Ingestion).all()
    assert len(all_ingestions) == 0
```

### Distributed Tests

```python
# tests/prefect/test_distributed_processing.py

@pytest.mark.asyncio
async def test_concurrent_file_processing_no_duplicates():
    """
    Verify that multiple workers processing same ingestion don't duplicate work.
    """
    # Create ingestion with 10 files
    ingestion_id = create_test_ingestion()

    # Simulate 3 concurrent workers
    tasks = [
        asyncio.create_task(process_files_task(ingestion_id, run_id, files)),
        asyncio.create_task(process_files_task(ingestion_id, run_id, files)),
        asyncio.create_task(process_files_task(ingestion_id, run_id, files)),
    ]

    await asyncio.gather(*tasks)

    # Verify each file was processed exactly once
    processed_files = db.query(ProcessedFile).filter(
        ProcessedFile.ingestion_id == ingestion_id
    ).all()

    assert len(processed_files) == 10
    assert all(f.status == 'SUCCESS' for f in processed_files)
```

---

## Checklist for Implementation

### Phase 1: Critical Fixes

- [ ] Remove commits from `IngestionRepository` (create, update, delete)
- [ ] Remove commits from `RunRepository` (create, update)
- [ ] Remove commits from `ProcessedFileRepository` (create, mark_success, mark_failed, mark_skipped, delete_by_ingestion)
- [ ] Keep commit in `ProcessedFileRepository.lock_file_for_processing()` (required)
- [ ] Add explicit commit/rollback to `IngestionService.create_ingestion()`
- [ ] Add explicit commit/rollback to `IngestionService.update_ingestion()`
- [ ] Add explicit commit/rollback to `IngestionService.delete_ingestion()`
- [ ] Add explicit commit/rollback to `RefreshService.refresh()`
- [ ] Remove redundant commits from `create_run_record_task`
- [ ] Remove redundant commits from `complete_run_record_task`
- [ ] Remove redundant commits from `fail_run_record_task`
- [ ] Add explicit rollback to all Prefect tasks

### Phase 2: Safeguards

- [ ] Implement `transaction_scope()` context manager
- [ ] Make `create_run_record_task` idempotent
- [ ] Make `complete_run_record_task` idempotent
- [ ] Make `fail_run_record_task` idempotent
- [ ] Add comprehensive integration tests

### Phase 3: Advanced Patterns

- [ ] Implement `UnitOfWork` pattern
- [ ] Refactor services to use `UnitOfWork`
- [ ] Implement `Saga` pattern for multi-service operations
- [ ] Add distributed transaction monitoring

---

## Conclusion

The current implementation has **critical transaction management issues** that stem from repositories committing directly. This violates separation of concerns and prevents the service layer from controlling transaction boundaries.

**Key Takeaways:**

1. **Repositories should NOT commit** - Transaction management belongs to the service layer
2. **Prefect tasks need separate sessions** - This is correct for distributed execution
3. **Row-level locking requires immediate commits** - This is correct for concurrency control
4. **Per-file commits are intentional** - This enables partial progress in batch processing

**Priority Actions:**

1. Remove commits from repositories (except lock operations)
2. Add explicit transaction management to services
3. Make Prefect tasks idempotent for safe retries

**Timeline:**

- **Week 1:** Phase 1 (critical fixes) - 3-5 days
- **Week 2:** Phase 2 (safeguards) - 3-5 days
- **Week 3-4:** Phase 3 (advanced patterns) - optional, ongoing

---

**Document Version:** 1.0
**Last Updated:** 2025-11-09
**Reviewed By:** Claude Code Analysis
**Next Review:** After Phase 1 implementation
