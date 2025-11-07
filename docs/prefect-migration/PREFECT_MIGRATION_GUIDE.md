# IOMETE Autoloader: Prefect Migration Guide

**Document Version:** 2.0
**Created:** 2025-11-07
**Status:** Comprehensive Implementation Guide
**Estimated Effort:** 2-3 weeks
**Related Documents:**
- `prefect-orchestration-guide.md` - Why Prefect (already exists)
- `prefect-architecture-explained.md` - Prefect concepts (already exists)
- `scheduler-implementation-guide.md` - Original APScheduler plan

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current State Analysis](#current-state-analysis)
3. [Target Architecture](#target-architecture)
4. [Migration Strategy](#migration-strategy)
5. [Component Mapping](#component-mapping)
6. [Implementation Plan](#implementation-plan)
7. [Code Changes Required](#code-changes-required)
8. [Database Schema Changes](#database-schema-changes)
9. [Deployment Architecture](#deployment-architecture)
10. [Testing Strategy](#testing-strategy)
11. [Migration Execution Plan](#migration-execution-plan)
12. [Rollback Plan](#rollback-plan)
13. [Post-Migration Validation](#post-migration-validation)
14. [Operational Considerations](#operational-considerations)

---

## 1. Executive Summary

### 1.1 Purpose

This document provides a comprehensive guide for migrating IOMETE Autoloader from a **manual-trigger-only** system to a **Prefect-based orchestration** system with full scheduling capabilities.

### 1.2 Current Pain Points

**Today (Manual System):**
- ❌ No automated scheduling - all runs must be triggered manually
- ❌ No scheduler implemented (TODOs in code)
- ❌ Difficult to scale horizontally
- ❌ Limited observability (basic logging only)
- ❌ No retry/failure recovery built-in
- ❌ Would require building distributed APScheduler from scratch

**Tomorrow (Prefect System):**
- ✅ Automated scheduling with cron expressions
- ✅ Horizontal scaling out-of-the-box
- ✅ Rich observability (UI + API)
- ✅ Built-in retry and failure recovery
- ✅ Production-proven orchestration platform
- ✅ Already in your stack (zero new dependencies)

### 1.3 Migration Scope

**What Changes:**
- Add Prefect flows and tasks
- Replace manual triggering with Prefect deployments
- Add Prefect server and workers to infrastructure
- Migrate BatchOrchestrator to Prefect flow pattern
- Update API endpoints to interact with Prefect

**What Stays the Same:**
- FastAPI application structure
- Database schema (minimal changes)
- Business logic in services
- Spark Connect integration
- File discovery and processing logic
- Repository pattern

### 1.4 Benefits

| Capability | Current (Manual) | After Migration (Prefect) |
|------------|------------------|---------------------------|
| **Scheduling** | None (manual only) | Full cron support |
| **Horizontal Scaling** | N/A | Built-in workers |
| **Observability** | Basic logs | Rich UI + API + metrics |
| **Retry Logic** | Manual | Automatic with backoff |
| **Cost** | N/A | Lower than DIY scheduler |
| **Development Time** | N/A | 62% faster than DIY |
| **Operational Burden** | N/A | 70% less than DIY |

---

## 2. Current State Analysis

### 2.1 Current Architecture

```
┌───────────────────────────────────────────────────────────┐
│                   FastAPI Application                     │
│                                                           │
│  ┌────────────────┐         ┌─────────────────┐          │
│  │  API Endpoints │────────▶│ IngestionService│          │
│  │ /ingestions    │         │                 │          │
│  │ /runs          │         └────────┬────────┘          │
│  └────────────────┘                  │                    │
│                                      ▼                     │
│                          ┌───────────────────┐            │
│                          │BatchOrchestrator  │            │
│                          │  .run_ingestion() │            │
│                          └─────────┬─────────┘            │
│                                    │                       │
│                  ┌─────────────────┼─────────────────┐    │
│                  ▼                 ▼                 ▼    │
│          ┌──────────────┐  ┌─────────────┐  ┌──────────┐│
│          │FileDiscovery │  │FileProcessor│  │FileState ││
│          │Service       │  │             │  │Service   ││
│          └──────────────┘  └─────────────┘  └──────────┘│
│                                    │                       │
│                                    ▼                       │
│                          ┌───────────────────┐            │
│                          │ Spark Connect     │            │
│                          │ (Remote Execution)│            │
│                          └───────────────────┘            │
└───────────────────────────────────────────────────────────┘
```

**Trigger Mechanism:** `POST /api/v1/ingestions/{id}/run` → Manual only

### 2.2 Current File Structure

```
app/
├── main.py                           # FastAPI entry point
├── config.py                         # Settings (Pydantic)
├── database.py                       # SQLAlchemy setup
│
├── api/v1/
│   ├── ingestions.py                 # Ingestion CRUD + manual run
│   ├── runs.py                       # Run history
│   ├── clusters.py                   # Cluster management
│   └── refresh.py                    # Refresh operations
│
├── models/
│   ├── domain.py                     # SQLAlchemy ORM (Ingestion, Run, etc.)
│   └── schemas.py                    # Pydantic schemas
│
├── services/
│   ├── ingestion_service.py          # Ingestion management
│   ├── batch_orchestrator.py         # 🔴 Main orchestration logic
│   ├── batch_file_processor.py       # File processing with Spark
│   ├── file_discovery_service.py     # Cloud storage file listing
│   ├── file_state_service.py         # File state tracking
│   └── spark_service.py              # Spark operations wrapper
│
├── repositories/
│   ├── ingestion_repository.py       # Ingestion data access
│   ├── run_repository.py             # Run history data access
│   └── processed_file_repository.py  # File state data access
│
└── spark/
    ├── connect_client.py             # Spark Connect client
    └── session_manager.py            # Session pooling
```

### 2.3 Current Execution Flow

```
User Request: POST /api/v1/ingestions/{id}/run
    ↓
API Endpoint: trigger_run()
    ↓
IngestionService.trigger_manual_run()
    ↓
BatchOrchestrator.run_ingestion(ingestion)
    ↓
1. Create Run record (status=RUNNING)
2. FileDiscoveryService → Discover files from S3
3. FileStateService → Get already-processed files
4. Compute new_files = discovered - processed
5. BatchFileProcessor.process_files(new_files)
   ├─ For each file:
   │  ├─ Lock file (atomic with SELECT FOR UPDATE SKIP LOCKED)
   │  ├─ Read with Spark
   │  ├─ Write to Iceberg
   │  └─ Mark success/failure
   └─ Return metrics
6. Update Run record (status=SUCCESS/FAILED)
    ↓
Return Run to user
```

### 2.4 What's Missing

| Component | Status | Impact |
|-----------|--------|--------|
| **Scheduler** | ❌ Not implemented (TODOs in code) | No automated runs |
| **Distributed Execution** | ❌ Single-process only | Can't scale horizontally |
| **Retry Logic** | ❌ Manual retry via API only | No automatic recovery |
| **Observability** | 🟡 Basic logging | Limited debugging |
| **Work Distribution** | ❌ None | All runs on single instance |
| **Priority Queues** | ❌ None | No workload prioritization |

---

## 3. Target Architecture

### 3.1 Prefect-Based Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    FastAPI Application                      │
│                    (Stateless, Scalable)                    │
│                                                            │
│  ┌────────────────┐         ┌──────────────────┐          │
│  │  API Endpoints │────────▶│ IngestionService │          │
│  │ /ingestions    │         │ + PrefectService │          │
│  └────────────────┘         └────────┬─────────┘          │
│                                       │                     │
│                              Creates/Updates                │
│                              Prefect Deployments            │
└───────────────────────────────────────┼────────────────────┘
                                        │
                                        ▼
┌────────────────────────────────────────────────────────────┐
│                    Prefect Server                          │
│                    (Orchestration Hub)                     │
│                                                            │
│  Responsibilities:                                         │
│  - Schedule management (cron triggers)                     │
│  - Work queue distribution                                 │
│  - State management (flow runs)                            │
│  - Run history and logs                                    │
│  - Worker coordination                                     │
│                                                            │
│  Database: PostgreSQL (can share with Autoloader)          │
└───────────────────────┬────────────────────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Work Queue:  │ │ Work Queue:  │ │ Work Queue:  │
│  default     │ │  high-memory │ │  priority    │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
   ┌───┴───┐        ┌───┴───┐        ┌───┴───┐
   ▼       ▼        ▼       ▼        ▼       ▼
┌───────┐┌───────┐┌───────┐┌───────┐┌───────┐┌───────┐
│Worker││Worker││Worker││Worker││Worker││Worker│
│  1   ││  2   ││  3   ││  4   ││  5   ││  6   │
└───┬───┘└───┬───┘└───┬───┘└───┬───┘└───┬───┘└───┬───┘
    │        │        │        │        │        │
    └────────┴────────┴────────┴────────┴────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │   Execute Prefect Flow:     │
        │   run_ingestion_flow()      │
        │                             │
        │   Tasks:                    │
        │   1. discover_files_task    │
        │   2. check_state_task       │
        │   3. process_files_task     │
        │   4. update_metrics_task    │
        └─────────────┬───────────────┘
                      │
                      ▼
            ┌───────────────────┐
            │  Spark Connect    │
            │  (IOMETE Cluster) │
            └───────────────────┘
```

### 3.2 Component Responsibilities

| Component | Current Role | Prefect Role |
|-----------|--------------|--------------|
| **FastAPI API** | CRUD + Manual trigger | CRUD + Deployment management |
| **IngestionService** | Business logic + Manual runs | Business logic only |
| **BatchOrchestrator** | Execution orchestration | → **Prefect Flow** |
| **File Services** | Discovery, processing, state | → **Prefect Tasks** |
| **Scheduler** | ❌ Not implemented | ✅ Prefect Server |
| **Workers** | ❌ None | ✅ Prefect Workers (Kubernetes pods) |

---

## 4. Migration Strategy

### 4.1 Migration Approach: **Incremental Migration**

We'll use a **3-phase incremental migration** to minimize risk:

**Phase 1: Foundation (Week 1)**
- Deploy Prefect infrastructure (server + workers)
- Create Prefect flows (without removing current code)
- Run in parallel with existing manual trigger system
- Validate flows work correctly

**Phase 2: Integration (Week 2)**
- Update API to create Prefect deployments
- Keep manual trigger as fallback
- Migrate subset of ingestions to Prefect
- Monitor and validate

**Phase 3: Migration & Cleanup (Week 3)**
- Migrate all ingestions to Prefect
- Remove manual trigger code (optional)
- Remove APScheduler-related code/TODOs
- Production validation

### 4.2 Risk Mitigation

| Risk | Mitigation |
|------|------------|
| **Prefect infrastructure failure** | Keep manual trigger as backup |
| **Flow execution errors** | Extensive testing before migration |
| **Performance degradation** | Load testing with realistic workloads |
| **Data inconsistency** | Validate state tracking works correctly |
| **Deployment issues** | Blue-green deployment strategy |

### 4.3 Success Criteria

**Must Have:**
- ✅ All existing ingestions can be scheduled
- ✅ Flows execute successfully with same results
- ✅ State tracking (ProcessedFile) works correctly
- ✅ Run history preserved
- ✅ No data loss or duplication
- ✅ <1% flow failure rate

**Should Have:**
- ✅ Improved observability (Prefect UI shows runs)
- ✅ Automatic retries work
- ✅ Worker auto-scaling operational
- ✅ Monitoring dashboards configured

---

## 5. Component Mapping

### 5.1 Current → Prefect Mapping

| Current Component | Prefect Equivalent | Notes |
|-------------------|-------------------|-------|
| `BatchOrchestrator.run_ingestion()` | `@flow run_ingestion_flow()` | Main orchestration → Prefect Flow |
| Manual API trigger | Prefect deployment trigger | `POST /deployments/{id}/create_flow_run` |
| N/A (no scheduler) | Prefect Server scheduler | Built-in cron scheduling |
| `FileDiscoveryService.discover_files()` | `@task discover_files_task()` | Wrapped as Prefect task |
| `BatchFileProcessor.process_files()` | `@task process_files_task()` | Wrapped as Prefect task |
| `FileStateService` methods | `@task` functions | State operations as tasks |
| `Run` (domain model) | Prefect `FlowRun` + local `Run` | Dual tracking (Prefect + DB) |
| Logging | Prefect logging + existing logs | Enhanced observability |

### 5.2 New Components Required

| Component | Purpose | Location |
|-----------|---------|----------|
| `app/prefect/flows/run_ingestion.py` | Main ingestion flow | NEW |
| `app/prefect/tasks/discovery.py` | File discovery task | NEW |
| `app/prefect/tasks/processing.py` | File processing task | NEW |
| `app/prefect/tasks/state.py` | State management tasks | NEW |
| `app/services/prefect_service.py` | Prefect API client wrapper | NEW |
| `kubernetes/prefect-server.yaml` | Prefect server deployment | NEW |
| `kubernetes/prefect-worker.yaml` | Worker deployment | NEW |

---

## 6. Implementation Plan

### 6.1 Phase 1: Foundation (Week 1 - Days 1-5)

**Day 1-2: Infrastructure Setup**
- [ ] Add Prefect to `requirements.txt` (`prefect>=2.14.0`)
- [ ] Deploy Prefect server on Kubernetes
- [ ] Create PostgreSQL database for Prefect (or reuse existing)
- [ ] Configure Prefect API URL in settings
- [ ] Deploy test worker pod

**Day 3-4: Flow Implementation**
- [ ] Create `app/prefect/` directory structure
- [ ] Implement `run_ingestion_flow()` (main flow)
- [ ] Implement tasks: `discover_files_task`, `process_files_task`
- [ ] Add error handling and logging
- [ ] Local testing with `prefect server start`

**Day 5: Service Integration**
- [ ] Create `PrefectService` (deployment CRUD)
- [ ] Add configuration settings for Prefect
- [ ] Update database schema (add `prefect_deployment_id`)
- [ ] Write unit tests for flows and tasks

### 6.2 Phase 2: Integration (Week 2 - Days 6-10)

**Day 6-7: API Integration**
- [ ] Update `IngestionService` to create Prefect deployments
- [ ] Modify `create_ingestion()` to create deployment
- [ ] Modify `update_ingestion()` to update deployment
- [ ] Modify `delete_ingestion()` to delete deployment
- [ ] Keep manual trigger endpoint as fallback

**Day 8-9: Deployment & Testing**
- [ ] Deploy workers to Kubernetes (3-5 pods)
- [ ] Configure work queues (default, high-memory, priority)
- [ ] Migrate 5-10 test ingestions to Prefect
- [ ] Run E2E tests with Prefect flows
- [ ] Monitor Prefect UI for successful execution

**Day 10: Validation**
- [ ] Validate state tracking works correctly
- [ ] Verify no duplicate processing
- [ ] Check run history matches
- [ ] Load testing (50+ concurrent ingestions)
- [ ] Fix any issues discovered

### 6.3 Phase 3: Migration & Cleanup (Week 3 - Days 11-15)

**Day 11-12: Full Migration**
- [ ] Write migration script to convert all ingestions
- [ ] Migrate all remaining ingestions to Prefect
- [ ] Monitor for 24 hours
- [ ] Validate all schedules executing correctly

**Day 13-14: Cleanup**
- [ ] Remove APScheduler TODOs from code
- [ ] Optional: Remove manual trigger code (or keep as backup)
- [ ] Update documentation
- [ ] Create operational runbooks

**Day 15: Production Validation**
- [ ] Run full regression tests
- [ ] Performance validation
- [ ] Monitoring dashboards configured
- [ ] Team training on Prefect UI
- [ ] Go/no-go decision for final cutover

---

## 7. Code Changes Required

### 7.1 New Dependencies

Add to `requirements.txt`:

```python
# Prefect
prefect>=2.14.0
prefect-kubernetes>=0.3.0  # For Kubernetes worker
```

### 7.2 New Directory Structure

```
app/
├── prefect/                          # NEW: Prefect-specific code
│   ├── __init__.py
│   ├── flows/
│   │   ├── __init__.py
│   │   └── run_ingestion.py          # Main ingestion flow
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── discovery.py              # File discovery task
│   │   ├── processing.py             # File processing task
│   │   └── state.py                  # State management tasks
│   └── config.py                     # Prefect configuration
│
├── services/
│   └── prefect_service.py            # NEW: Prefect API client
```

### 7.3 Main Flow Implementation

**File:** `app/prefect/flows/run_ingestion.py`

```python
"""
Main Prefect flow for ingestion execution.

This replaces BatchOrchestrator as the entry point for scheduled runs.
"""
from prefect import flow, task, get_run_logger
from typing import Dict, List
from uuid import UUID
import httpx

from app.database import SessionLocal
from app.repositories.ingestion_repository import IngestionRepository
from app.services.file_discovery_service import FileDiscoveryService
from app.services.file_state_service import FileStateService
from app.services.batch_file_processor import BatchFileProcessor
from app.spark.connect_client import SparkConnectClient
from app.config import get_spark_connect_credentials


@task(
    name="discover_files",
    retries=2,
    retry_delay_seconds=60,
    tags=["discovery"]
)
def discover_files_task(ingestion_id: str) -> List[Dict]:
    """
    Discover new files from cloud storage.

    Args:
        ingestion_id: UUID of the ingestion

    Returns:
        List of file metadata dicts
    """
    logger = get_run_logger()
    logger.info(f"Discovering files for ingestion {ingestion_id}")

    db = SessionLocal()
    try:
        # Get ingestion config
        repo = IngestionRepository(db)
        ingestion = repo.get_by_id(UUID(ingestion_id))

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Initialize discovery service
        import json
        credentials = (
            json.loads(ingestion.source_credentials)
            if isinstance(ingestion.source_credentials, str)
            else ingestion.source_credentials
        )

        discovery = FileDiscoveryService(
            source_type=ingestion.source_type,
            credentials=credentials,
            region=credentials.get('aws_region', 'us-east-1')
        )

        # Discover files
        files = discovery.discover_files_from_path(
            source_path=ingestion.source_path,
            pattern=ingestion.source_file_pattern,
            max_files=None
        )

        logger.info(f"Discovered {len(files)} files")
        return [f.to_dict() for f in files]

    finally:
        db.close()


@task(
    name="check_file_state",
    retries=1,
    retry_delay_seconds=30,
    tags=["state"]
)
def check_file_state_task(ingestion_id: str) -> List[str]:
    """
    Get list of already-processed file paths.

    Args:
        ingestion_id: UUID of the ingestion

    Returns:
        Set of processed file paths
    """
    logger = get_run_logger()

    db = SessionLocal()
    try:
        state_service = FileStateService(db)
        processed = state_service.get_processed_files(
            ingestion_id=UUID(ingestion_id),
            statuses=["SUCCESS", "SKIPPED"]
        )

        logger.info(f"Found {len(processed)} already-processed files")
        return list(processed)

    finally:
        db.close()


@task(
    name="process_files",
    retries=2,
    retry_delay_seconds=120,
    timeout_seconds=3600,
    tags=["processing"]
)
def process_files_task(
    ingestion_id: str,
    run_id: str,
    files_to_process: List[Dict]
) -> Dict[str, int]:
    """
    Process files via Spark Connect.

    Args:
        ingestion_id: UUID of the ingestion
        run_id: UUID of the run
        files_to_process: List of file metadata dicts

    Returns:
        Metrics dict: {success: N, failed: N, skipped: N}
    """
    logger = get_run_logger()
    logger.info(f"Processing {len(files_to_process)} files")

    db = SessionLocal()
    try:
        # Get ingestion config
        repo = IngestionRepository(db)
        ingestion = repo.get_by_id(UUID(ingestion_id))

        # Initialize services
        spark_url, spark_token = get_spark_connect_credentials(ingestion.cluster_id)
        spark_client = SparkConnectClient(
            connect_url=spark_url,
            token=spark_token
        )

        state_service = FileStateService(db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion)

        # Process files
        metrics = processor.process_files(files_to_process, run_id=run_id)

        logger.info(f"Processing complete: {metrics}")
        return metrics

    finally:
        db.close()


@task(
    name="create_run_record",
    retries=1,
    tags=["database"]
)
def create_run_record_task(ingestion_id: str) -> str:
    """
    Create Run record with status=RUNNING.

    Args:
        ingestion_id: UUID of the ingestion

    Returns:
        Run ID (UUID string)
    """
    from app.repositories.run_repository import RunRepository
    from app.models.domain import Run, RunStatus
    from datetime import datetime
    import uuid

    db = SessionLocal()
    try:
        run = Run(
            id=str(uuid.uuid4()),
            ingestion_id=ingestion_id,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
            trigger="scheduled",  # or "manual" for manual triggers
            cluster_id=ingestion_id  # TODO: Get from ingestion
        )

        run_repo = RunRepository(db)
        created_run = run_repo.create(run)
        db.commit()

        return created_run.id

    finally:
        db.close()


@task(
    name="complete_run_record",
    retries=2,
    tags=["database"]
)
def complete_run_record_task(run_id: str, metrics: Dict[str, int]):
    """
    Update Run record with final status and metrics.

    Args:
        run_id: UUID of the run
        metrics: Metrics dict from processing
    """
    from app.repositories.run_repository import RunRepository
    from app.models.domain import RunStatus, ProcessedFile
    from datetime import datetime
    from sqlalchemy import func

    logger = get_run_logger()

    db = SessionLocal()
    try:
        run_repo = RunRepository(db)
        run = run_repo.get_run(run_id)

        if not run:
            raise ValueError(f"Run not found: {run_id}")

        # Update status
        run.status = RunStatus.SUCCESS.value
        run.ended_at = datetime.utcnow()
        run.files_processed = metrics['success'] + metrics['failed']

        # Calculate duration
        if run.started_at:
            duration = (run.ended_at - run.started_at).total_seconds()
            run.duration_seconds = int(duration)

        # Aggregate metrics from ProcessedFile table
        result = db.query(
            func.sum(ProcessedFile.records_ingested).label('total_records'),
            func.sum(ProcessedFile.bytes_read).label('total_bytes')
        ).filter(
            ProcessedFile.run_id == run_id,
            ProcessedFile.status == 'SUCCESS'
        ).first()

        run.records_ingested = int(result.total_records) if result.total_records else 0
        run.bytes_read = int(result.total_bytes) if result.total_bytes else 0

        db.commit()

        logger.info(f"Run completed: {run_id} - {run.records_ingested} records")

    finally:
        db.close()


@flow(
    name="run_ingestion",
    log_prints=True,
    retries=1,
    retry_delay_seconds=300,
)
def run_ingestion_flow(ingestion_id: str):
    """
    Main Prefect flow for ingestion execution.

    This flow:
    1. Creates a Run record
    2. Discovers new files from cloud storage
    3. Checks which files are already processed
    4. Processes new files via Spark Connect
    5. Updates Run record with final status

    Args:
        ingestion_id: UUID of the ingestion to run (as string)
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting ingestion flow for: {ingestion_id}")

    try:
        # Step 1: Create Run record
        run_id = create_run_record_task(ingestion_id)
        logger.info(f"Created run: {run_id}")

        # Step 2: Discover files
        discovered_files = discover_files_task(ingestion_id)

        # Step 3: Check already-processed files
        processed_file_paths = check_file_state_task(ingestion_id)

        # Step 4: Compute new files
        new_files = [
            f for f in discovered_files
            if f['path'] not in processed_file_paths
        ]

        logger.info(
            f"Discovered: {len(discovered_files)}, "
            f"Already processed: {len(processed_file_paths)}, "
            f"New files: {len(new_files)}"
        )

        if len(new_files) == 0:
            logger.info("No new files to process")
            complete_run_record_task(run_id, {'success': 0, 'failed': 0, 'skipped': 0})
            return {
                "status": "NO_NEW_FILES",
                "run_id": run_id,
                "files_processed": 0
            }

        # Step 5: Process files
        metrics = process_files_task(ingestion_id, run_id, new_files)

        # Step 6: Complete run
        complete_run_record_task(run_id, metrics)

        logger.info(f"✅ Ingestion flow completed: {run_id}")

        return {
            "status": "SUCCESS",
            "run_id": run_id,
            "files_processed": metrics['success'] + metrics['failed'],
            "metrics": metrics
        }

    except Exception as e:
        logger.error(f"❌ Ingestion flow failed: {e}", exc_info=True)
        raise
```

### 7.4 Prefect Service

**File:** `app/services/prefect_service.py`

```python
"""
Prefect Service - Manages Prefect deployments and flow runs.

This service wraps the Prefect API client and provides methods for:
- Creating/updating/deleting deployments
- Triggering flow runs
- Querying run history
- Managing schedules
"""
from prefect.client import get_client
from prefect.server.schemas.schedules import CronSchedule
from typing import Optional, List, Dict
from uuid import UUID
from datetime import datetime
import logging

from app.config import get_settings
from app.models.domain import Ingestion

logger = logging.getLogger(__name__)
settings = get_settings()


class PrefectService:
    """Service for managing Prefect deployments and flow runs."""

    def __init__(self):
        self.api_url = settings.PREFECT_API_URL
        # Flow ID will be set during initialization
        self.ingestion_flow_id: Optional[str] = None

    async def initialize(self):
        """
        Initialize service by registering flows.
        Call this during application startup.
        """
        from app.prefect.flows.run_ingestion import run_ingestion_flow

        async with get_client() as client:
            # Register the ingestion flow
            flows = await client.read_flows()

            # Find or create flow
            flow = next(
                (f for f in flows if f.name == "run_ingestion"),
                None
            )

            if flow:
                self.ingestion_flow_id = str(flow.id)
                logger.info(f"Found existing flow: {self.ingestion_flow_id}")
            else:
                logger.info("Flow will be created on first deployment")

    async def create_deployment(
        self,
        ingestion: Ingestion,
    ) -> str:
        """
        Create a Prefect deployment for an ingestion.

        Args:
            ingestion: Ingestion domain model

        Returns:
            Deployment ID (UUID string)
        """
        async with get_client() as client:
            # Determine work queue
            work_queue = self._get_work_queue(ingestion)

            # Build cron expression
            cron_expr = self._build_cron_expression(ingestion)

            # Create deployment
            from prefect.deployments import Deployment
            from app.prefect.flows.run_ingestion import run_ingestion_flow

            deployment = await Deployment.build_from_flow(
                flow=run_ingestion_flow,
                name=f"ingestion-{ingestion.id}",
                parameters={"ingestion_id": str(ingestion.id)},
                schedule=(
                    CronSchedule(cron=cron_expr, timezone=ingestion.schedule_timezone)
                    if cron_expr else None
                ),
                work_queue_name=work_queue,
                tags=[
                    "autoloader",
                    f"tenant-{ingestion.tenant_id}",
                    f"source-{ingestion.source_type}",
                ],
                description=f"Ingestion: {ingestion.name}",
            )

            deployment_id = await deployment.apply()

            logger.info(
                f"Created deployment {deployment_id} for ingestion {ingestion.id}"
            )

            return str(deployment_id)

    async def update_deployment_schedule(
        self,
        deployment_id: str,
        schedule_cron: str,
        timezone: str = "UTC"
    ):
        """
        Update deployment schedule.

        Args:
            deployment_id: Deployment UUID
            schedule_cron: Cron expression
            timezone: IANA timezone
        """
        async with get_client() as client:
            await client.update_deployment(
                deployment_id=deployment_id,
                schedule=CronSchedule(cron=schedule_cron, timezone=timezone),
            )

            logger.info(f"Updated schedule for deployment {deployment_id}")

    async def pause_deployment(self, deployment_id: str):
        """
        Pause deployment (disable schedule).

        Args:
            deployment_id: Deployment UUID
        """
        async with get_client() as client:
            await client.set_deployment_paused_state(
                deployment_id=deployment_id,
                paused=True,
            )

            logger.info(f"Paused deployment {deployment_id}")

    async def resume_deployment(self, deployment_id: str):
        """
        Resume deployment (enable schedule).

        Args:
            deployment_id: Deployment UUID
        """
        async with get_client() as client:
            await client.set_deployment_paused_state(
                deployment_id=deployment_id,
                paused=False,
            )

            logger.info(f"Resumed deployment {deployment_id}")

    async def delete_deployment(self, deployment_id: str):
        """
        Delete deployment.

        Args:
            deployment_id: Deployment UUID
        """
        async with get_client() as client:
            await client.delete_deployment(deployment_id)

            logger.info(f"Deleted deployment {deployment_id}")

    async def trigger_deployment(
        self,
        deployment_id: str,
        parameters: Optional[Dict] = None,
    ) -> str:
        """
        Manually trigger a deployment (create flow run).

        Args:
            deployment_id: Deployment UUID
            parameters: Optional override parameters

        Returns:
            Flow run ID (UUID string)
        """
        async with get_client() as client:
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters=parameters or {},
            )

            logger.info(f"Triggered deployment {deployment_id}, run: {flow_run.id}")

            return str(flow_run.id)

    async def get_flow_runs(
        self,
        deployment_id: str,
        limit: int = 50,
    ) -> List[Dict]:
        """
        Get flow runs for a deployment.

        Args:
            deployment_id: Deployment UUID
            limit: Maximum number of runs to return

        Returns:
            List of flow run dicts
        """
        async with get_client() as client:
            flow_runs = await client.read_flow_runs(
                deployment_filter={"id": {"any_": [deployment_id]}},
                limit=limit,
                sort="START_TIME_DESC",
            )

            return [
                {
                    "id": str(run.id),
                    "state": run.state.type.value,
                    "state_name": run.state.name,
                    "start_time": run.start_time,
                    "end_time": run.end_time,
                    "total_run_time": run.total_run_time,
                    "parameters": run.parameters,
                }
                for run in flow_runs
            ]

    def _get_work_queue(self, ingestion: Ingestion) -> str:
        """
        Determine work queue based on ingestion characteristics.

        Args:
            ingestion: Ingestion configuration

        Returns:
            Work queue name
        """
        # For now, use default queue
        # TODO: Implement resource-based routing
        return settings.PREFECT_DEFAULT_WORK_QUEUE

    def _build_cron_expression(self, ingestion: Ingestion) -> Optional[str]:
        """
        Build cron expression from ingestion schedule configuration.

        Args:
            ingestion: Ingestion configuration

        Returns:
            Cron expression string or None
        """
        if not ingestion.schedule_frequency:
            return None

        if ingestion.schedule_frequency == "custom":
            return ingestion.schedule_cron

        if ingestion.schedule_frequency == "hourly":
            return "0 * * * *"

        if ingestion.schedule_frequency == "daily":
            hour, minute = self._parse_schedule_time(ingestion.schedule_time)
            return f"{minute} {hour} * * *"

        if ingestion.schedule_frequency == "weekly":
            hour, minute = self._parse_schedule_time(ingestion.schedule_time)
            return f"{minute} {hour} * * 0"  # Sunday

        return None

    def _parse_schedule_time(self, time_str: Optional[str]) -> tuple[int, int]:
        """
        Parse schedule_time string (HH:MM) into (hour, minute).

        Args:
            time_str: Time string in HH:MM format

        Returns:
            Tuple of (hour, minute)
        """
        if not time_str:
            return (0, 0)

        parts = time_str.split(":")
        return (int(parts[0]), int(parts[1]))


# Global singleton instance
_prefect_service: Optional[PrefectService] = None


async def get_prefect_service() -> PrefectService:
    """
    Get singleton Prefect service instance.

    Returns:
        PrefectService instance
    """
    global _prefect_service

    if _prefect_service is None:
        _prefect_service = PrefectService()
        await _prefect_service.initialize()

    return _prefect_service
```

### 7.5 Configuration Updates

**File:** `app/config.py`

Add Prefect configuration:

```python
class Settings(BaseSettings):
    # ... existing settings ...

    # Prefect Configuration
    PREFECT_API_URL: str = "http://prefect-server:4200/api"
    PREFECT_API_KEY: Optional[str] = None  # For Prefect Cloud (optional)

    # Work Queues
    PREFECT_DEFAULT_WORK_QUEUE: str = "autoloader-default"
    PREFECT_HIGH_MEMORY_WORK_QUEUE: str = "autoloader-high-memory"
    PREFECT_PRIORITY_WORK_QUEUE: str = "autoloader-priority"

    # Flow Configuration
    PREFECT_FLOW_RUN_TIMEOUT_SECONDS: int = 3600  # 1 hour
    PREFECT_TASK_RETRY_DELAY_SECONDS: int = 60
    PREFECT_MAX_TASK_RETRIES: int = 3

    # Resource Thresholds
    HIGH_MEMORY_THRESHOLD_GB: int = 10  # Files > 10GB use high-memory queue
```

### 7.6 Database Schema Changes

**Migration:** `alembic/versions/XXX_add_prefect_fields.py`

```python
"""Add Prefect integration fields

Revision ID: XXX
"""
from alembic import op
import sqlalchemy as sa


def upgrade():
    # Add Prefect deployment tracking
    op.add_column('ingestions',
        sa.Column('prefect_deployment_id', sa.String(255), nullable=True)
    )
    op.add_column('ingestions',
        sa.Column('prefect_flow_id', sa.String(255), nullable=True)
    )

    # Add index for faster lookups
    op.create_index(
        'ix_ingestions_prefect_deployment_id',
        'ingestions',
        ['prefect_deployment_id']
    )


def downgrade():
    op.drop_index('ix_ingestions_prefect_deployment_id', 'ingestions')
    op.drop_column('ingestions', 'prefect_flow_id')
    op.drop_column('ingestions', 'prefect_deployment_id')
```

### 7.7 Updated Ingestion Service

**File:** `app/services/ingestion_service.py`

Update to use Prefect:

```python
from app.services.prefect_service import get_prefect_service

class IngestionService:
    def __init__(self, db: Session):
        self.db = db
        self.ingestion_repo = IngestionRepository(db)
        self.run_repo = RunRepository(db)
        # Remove: self.scheduler = get_scheduler_service()

    async def create_ingestion(
        self,
        data: IngestionCreate,
        user_id: str
    ) -> IngestionResponse:
        """Create ingestion and Prefect deployment."""

        # ... existing validation and creation logic ...

        # Create ingestion in database
        ingestion = self.ingestion_repo.create(ingestion)

        # Create Prefect deployment if scheduling enabled
        if ingestion.schedule_frequency:
            prefect = await get_prefect_service()
            deployment_id = await prefect.create_deployment(ingestion)

            # Store deployment ID
            ingestion.prefect_deployment_id = deployment_id
            self.db.commit()

        return self._to_response(ingestion)

    async def trigger_manual_run(self, ingestion_id: str) -> Run:
        """
        Trigger manual run via Prefect.

        Args:
            ingestion_id: Ingestion UUID

        Returns:
            Run record (local database)
        """
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Trigger via Prefect
        if ingestion.prefect_deployment_id:
            prefect = await get_prefect_service()
            flow_run_id = await prefect.trigger_deployment(
                ingestion.prefect_deployment_id
            )

            # Return a placeholder Run (actual run created by flow)
            # TODO: Link flow_run_id to local Run record
            return {"flow_run_id": flow_run_id, "status": "TRIGGERED"}

        # Fallback: Use BatchOrchestrator directly
        orchestrator = BatchOrchestrator(self.db)
        run = orchestrator.run_ingestion(ingestion)
        return run
```

---

## 8. Database Schema Changes

### 8.1 Ingestion Table Changes

**Add fields to `Ingestion` model:**

```python
# app/models/domain.py

class Ingestion(Base):
    __tablename__ = "ingestions"

    # ... existing fields ...

    # Prefect integration (NEW)
    prefect_deployment_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    prefect_flow_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
```

### 8.2 Prefect Database Tables

Prefect will create its own tables in PostgreSQL:

```sql
-- Created automatically by Prefect
CREATE TABLE prefect_deployment (...);
CREATE TABLE prefect_flow (...);
CREATE TABLE prefect_flow_run (...);
CREATE TABLE prefect_task_run (...);
CREATE TABLE prefect_log (...);
-- ... and more
```

**Option:** Use same database as Autoloader or separate database.

---

## 9. Deployment Architecture

### 9.1 Kubernetes Components

**New Components:**

```
┌────────────────────────────────────────────────────────┐
│  Kubernetes Namespace: iomete                          │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Deployment: autoloader-api                      │ │
│  │  - Replicas: 3                                   │ │
│  │  - No changes (stateless API)                    │ │
│  └──────────────────────────────────────────────────┘ │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Deployment: prefect-server (NEW)                │ │
│  │  - Replicas: 2 (HA)                              │ │
│  │  - Image: prefecthq/prefect:2.14-python3.11      │ │
│  │  - Port: 4200 (API)                              │ │
│  │  - Service: prefect-server:4200                  │ │
│  └──────────────────────────────────────────────────┘ │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Deployment: prefect-worker-default (NEW)        │ │
│  │  - Replicas: 3-5 (HPA enabled)                   │ │
│  │  - Image: iomete/autoloader-worker:latest        │ │
│  │  - Work Queue: autoloader-default                │ │
│  │  - Resources: 2 CPU, 4GB RAM                     │ │
│  └──────────────────────────────────────────────────┘ │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Deployment: prefect-worker-high-memory (NEW)    │ │
│  │  - Replicas: 2 (HPA enabled)                     │ │
│  │  - Image: iomete/autoloader-worker:latest        │ │
│  │  - Work Queue: autoloader-high-memory            │ │
│  │  - Resources: 4 CPU, 16GB RAM                    │ │
│  └──────────────────────────────────────────────────┘ │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  StatefulSet: postgresql                         │ │
│  │  - Database for Prefect (can share with app)     │ │
│  └──────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
```

### 9.2 Kubernetes Manifests

**Prefect Server Deployment:**

**File:** `kubernetes/prefect-server.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-server
  namespace: iomete
spec:
  replicas: 2
  selector:
    matchLabels:
      app: prefect-server
  template:
    metadata:
      labels:
        app: prefect-server
    spec:
      containers:
      - name: prefect-server
        image: prefecthq/prefect:2.14-python3.11
        command:
          - prefect
          - server
          - start
          - --host
          - 0.0.0.0
        env:
        - name: PREFECT_SERVER_API_HOST
          value: "0.0.0.0"
        - name: PREFECT_SERVER_API_PORT
          value: "4200"
        - name: PREFECT_API_DATABASE_CONNECTION_URL
          valueFrom:
            secretKeyRef:
              name: prefect-db-secret
              key: connection-url
        ports:
        - containerPort: 4200
          name: http
        resources:
          requests:
            cpu: 1
            memory: 2Gi
          limits:
            cpu: 2
            memory: 4Gi
---
apiVersion: v1
kind: Service
metadata:
  name: prefect-server
  namespace: iomete
spec:
  selector:
    app: prefect-server
  ports:
  - port: 4200
    targetPort: 4200
  type: ClusterIP
```

**Prefect Worker Deployment:**

**File:** `kubernetes/prefect-worker.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-default
  namespace: iomete
spec:
  replicas: 3
  selector:
    matchLabels:
      app: prefect-worker
      queue: default
  template:
    metadata:
      labels:
        app: prefect-worker
        queue: default
    spec:
      containers:
      - name: worker
        image: iomete/autoloader-worker:latest
        command:
          - prefect
          - worker
          - start
          - --pool
          - autoloader-pool
          - --work-queue
          - autoloader-default
          - --type
          - process
        env:
        - name: PREFECT_API_URL
          value: "http://prefect-server:4200/api"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: autoloader-db-secret
              key: connection-url
        - name: SPARK_CONNECT_URL
          value: "sc://spark-connect-service:15002"
        resources:
          requests:
            cpu: 2
            memory: 4Gi
          limits:
            cpu: 4
            memory: 8Gi
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: prefect-worker-default-hpa
  namespace: iomete
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: prefect-worker-default
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

### 9.3 Worker Docker Image

**File:** `Dockerfile.worker`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Set Python path
ENV PYTHONPATH=/app

# Worker will be started via Kubernetes command
# (allows different workers to poll different queues)
```

**Build and push:**

```bash
docker build -f Dockerfile.worker -t iomete/autoloader-worker:latest .
docker push iomete/autoloader-worker:latest
```

---

## 10. Testing Strategy

### 10.1 Unit Tests

**Test Prefect flows and tasks independently:**

```python
# tests/unit/test_prefect_flows.py

import pytest
from app.prefect.flows.run_ingestion import (
    run_ingestion_flow,
    discover_files_task,
    process_files_task
)


def test_discover_files_task(test_ingestion):
    """Test file discovery task."""
    files = discover_files_task.fn(str(test_ingestion.id))
    assert isinstance(files, list)


def test_process_files_task(test_ingestion, test_run):
    """Test file processing task."""
    files = [{"path": "s3a://bucket/file.json", "size": 1024}]
    metrics = process_files_task.fn(
        str(test_ingestion.id),
        str(test_run.id),
        files
    )
    assert 'success' in metrics
    assert 'failed' in metrics
```

### 10.2 Integration Tests

**Test Prefect integration:**

```python
# tests/integration/test_prefect_integration.py

import pytest
from app.services.prefect_service import PrefectService


@pytest.mark.asyncio
async def test_create_deployment(test_ingestion):
    """Test creating Prefect deployment."""
    prefect = PrefectService()
    await prefect.initialize()

    deployment_id = await prefect.create_deployment(test_ingestion)
    assert deployment_id is not None

    # Cleanup
    await prefect.delete_deployment(deployment_id)


@pytest.mark.asyncio
async def test_trigger_flow_run(test_ingestion):
    """Test triggering flow run."""
    prefect = PrefectService()
    await prefect.initialize()

    deployment_id = await prefect.create_deployment(test_ingestion)
    flow_run_id = await prefect.trigger_deployment(deployment_id)

    assert flow_run_id is not None

    # Cleanup
    await prefect.delete_deployment(deployment_id)
```

### 10.3 E2E Tests

**Test complete flow execution:**

```python
# tests/e2e/test_prefect_e2e.py

import pytest
import asyncio
from app.prefect.flows.run_ingestion import run_ingestion_flow


@pytest.mark.e2e
@pytest.mark.prefect
def test_prefect_flow_execution_e2e(
    test_ingestion,
    minio_client,
    spark_session
):
    """
    Test complete Prefect flow execution end-to-end.

    Steps:
    1. Upload test files to MinIO
    2. Execute flow directly (not via Prefect server)
    3. Verify data in Iceberg table
    4. Verify run record created
    """
    # 1. Upload test files
    bucket = "test-prefect-bucket"
    minio_client.create_bucket(bucket)
    upload_json_file(minio_client, bucket, "data.json", [{"id": 1}])

    # 2. Execute flow directly
    result = run_ingestion_flow(str(test_ingestion.id))

    # 3. Verify result
    assert result["status"] == "SUCCESS"
    assert result["files_processed"] > 0

    # 4. Verify data in Iceberg
    df = spark_session.table(test_ingestion.destination_table)
    assert df.count() == 1
```

---

## 11. Migration Execution Plan

### 11.1 Pre-Migration Checklist

**Infrastructure:**
- [ ] PostgreSQL database ready for Prefect
- [ ] Kubernetes cluster configured
- [ ] Docker registry accessible
- [ ] Secrets created (DB credentials, Spark Connect tokens)
- [ ] Network policies configured

**Code:**
- [ ] All Prefect code merged to `prefect` branch
- [ ] Unit tests passing (>90% coverage)
- [ ] Integration tests passing
- [ ] E2E tests passing
- [ ] Code review completed

**Documentation:**
- [ ] Runbooks created for Prefect operations
- [ ] Monitoring dashboards configured
- [ ] Alert rules defined
- [ ] Team training completed

### 11.2 Migration Steps

**Week 1: Infrastructure Setup**

```bash
# Day 1: Deploy Prefect Server
kubectl apply -f kubernetes/prefect-server.yaml

# Verify
kubectl get pods -l app=prefect-server
kubectl logs -l app=prefect-server

# Access Prefect UI
kubectl port-forward svc/prefect-server 4200:4200
# Open: http://localhost:4200

# Day 2: Build and deploy worker image
docker build -f Dockerfile.worker -t iomete/autoloader-worker:v1.0.0 .
docker push iomete/autoloader-worker:v1.0.0

# Day 3: Deploy workers
kubectl apply -f kubernetes/prefect-worker.yaml

# Verify workers are polling
kubectl logs -l app=prefect-worker --tail=50

# Day 4: Create work pools and queues
prefect work-pool create autoloader-pool --type process
prefect work-queue create autoloader-default --pool autoloader-pool
prefect work-queue create autoloader-high-memory --pool autoloader-pool

# Day 5: Test with dummy flow
python scripts/test_prefect_flow.py
```

**Week 2: Parallel Deployment**

```bash
# Day 6: Deploy updated API (with Prefect integration)
kubectl set image deployment/autoloader-api \
  api=iomete/autoloader-api:v2.0.0-prefect

# Day 7: Create test ingestions with Prefect deployments
# Use API to create 5-10 test ingestions
curl -X POST http://api/api/v1/ingestions -d '{...}'

# Day 8: Validate Prefect deployments created
prefect deployment ls

# Day 9: Trigger manual runs via Prefect
# Validate flows execute successfully
curl -X POST http://api/api/v1/ingestions/{id}/run

# Day 10: Monitor for 24 hours
# Check Prefect UI for successful executions
# Validate data correctness
```

**Week 3: Full Migration**

```bash
# Day 11: Migration script - convert all ingestions
python scripts/migrate_to_prefect.py --dry-run
python scripts/migrate_to_prefect.py --execute

# Day 12: Validate all deployments
prefect deployment ls | wc -l  # Should match ingestion count

# Day 13: Monitor scheduled executions
# Wait for scheduled runs to trigger
# Validate all execute successfully

# Day 14: Performance validation
# Run load tests
# Verify worker auto-scaling

# Day 15: Go/No-Go decision
# Review metrics, logs, errors
# Final approval for production cutover
```

### 11.3 Migration Script

**File:** `scripts/migrate_to_prefect.py`

```python
"""
Migration script to convert all ingestions to Prefect deployments.

Usage:
  python scripts/migrate_to_prefect.py --dry-run  # Preview
  python scripts/migrate_to_prefect.py --execute  # Migrate
"""
import asyncio
import argparse
from app.database import SessionLocal
from app.repositories.ingestion_repository import IngestionRepository
from app.services.prefect_service import get_prefect_service
from app.models.domain import Ingestion


async def migrate_ingestion(ingestion: Ingestion, prefect_service, dry_run=False):
    """Migrate single ingestion to Prefect."""
    print(f"[{ingestion.id}] {ingestion.name}")

    # Skip if already migrated
    if ingestion.prefect_deployment_id:
        print(f"  ⏭️  Already migrated (deployment: {ingestion.prefect_deployment_id})")
        return

    # Skip if no schedule
    if not ingestion.schedule_frequency:
        print(f"  ⏭️  No schedule configured, skipping")
        return

    if dry_run:
        print(f"  🔍 Would create Prefect deployment")
        print(f"     Schedule: {ingestion.schedule_frequency} {ingestion.schedule_time}")
        print(f"     Timezone: {ingestion.schedule_timezone}")
        return

    try:
        # Create Prefect deployment
        deployment_id = await prefect_service.create_deployment(ingestion)

        # Update database
        ingestion.prefect_deployment_id = deployment_id

        print(f"  ✅ Migrated → Deployment: {deployment_id}")

    except Exception as e:
        print(f"  ❌ Failed: {e}")
        raise


async def main(dry_run=False):
    """Main migration function."""
    print("=" * 60)
    print("IOMETE Autoloader → Prefect Migration")
    print("=" * 60)

    if dry_run:
        print("\n🔍 DRY RUN MODE - No changes will be made\n")
    else:
        print("\n⚠️  EXECUTION MODE - Ingestions will be migrated\n")

    db = SessionLocal()
    try:
        # Get all ingestions
        repo = IngestionRepository(db)
        ingestions = repo.get_all()

        print(f"Found {len(ingestions)} ingestions\n")

        # Initialize Prefect service
        prefect = await get_prefect_service()

        # Migrate each ingestion
        migrated = 0
        skipped = 0
        failed = 0

        for ingestion in ingestions:
            try:
                await migrate_ingestion(ingestion, prefect, dry_run=dry_run)
                migrated += 1
            except Exception as e:
                failed += 1
                continue

        if not dry_run:
            db.commit()

        print("\n" + "=" * 60)
        print("MIGRATION SUMMARY")
        print("=" * 60)
        print(f"Total:    {len(ingestions)}")
        print(f"Migrated: {migrated}")
        print(f"Skipped:  {skipped}")
        print(f"Failed:   {failed}")
        print("=" * 60)

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview migration without making changes"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute migration"
    )

    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("Error: Must specify --dry-run or --execute")
        exit(1)

    asyncio.run(main(dry_run=args.dry_run))
```

---

## 12. Rollback Plan

### 12.1 Rollback Triggers

Rollback if:
- ❌ >5% flow failure rate
- ❌ Data corruption detected
- ❌ Prefect server unavailable for >15 minutes
- ❌ Worker pods crashlooping
- ❌ Performance degradation >50%

### 12.2 Rollback Procedure

```bash
# Step 1: Pause all Prefect deployments
prefect deployment pause --all

# Step 2: Revert API to previous version
kubectl rollout undo deployment/autoloader-api

# Step 3: Re-enable manual trigger endpoints
# (if they were removed)

# Step 4: Scale down Prefect workers
kubectl scale deployment prefect-worker-default --replicas=0

# Step 5: Validate manual triggers work
curl -X POST http://api/api/v1/ingestions/{id}/run

# Step 6: Monitor for 1 hour
# Ensure manual triggers work correctly

# Step 7: Post-mortem analysis
# Investigate root cause of issues
```

### 12.3 Data Recovery

If data corruption detected:

```sql
-- Restore from backup
-- (Autoloader DB only, Prefect DB can be rebuilt)
pg_restore -d autoloader backup_YYYYMMDD.dump

-- Verify ingestions intact
SELECT COUNT(*) FROM ingestions;

-- Verify runs intact
SELECT COUNT(*) FROM runs;

-- Verify processed files intact
SELECT COUNT(*) FROM processed_files;
```

---

## 13. Post-Migration Validation

### 13.1 Validation Checklist

**Day 1 Post-Migration:**
- [ ] All ingestions have Prefect deployments
- [ ] Scheduled runs executing on time
- [ ] Flow success rate >99%
- [ ] No data duplication
- [ ] No missed files
- [ ] Prefect UI accessible
- [ ] Monitoring dashboards showing data

**Week 1 Post-Migration:**
- [ ] Zero critical incidents
- [ ] Worker auto-scaling working
- [ ] Retry logic functioning
- [ ] Performance meets SLAs
- [ ] Team comfortable with Prefect UI

**Month 1 Post-Migration:**
- [ ] Cost analysis complete
- [ ] Operational burden reduced
- [ ] User satisfaction high
- [ ] Documentation updated
- [ ] Lessons learned documented

### 13.2 Success Metrics

| Metric | Target | How to Measure |
|--------|--------|----------------|
| **Flow Success Rate** | >99% | Prefect UI → Runs → Success % |
| **Schedule Adherence** | >99% | Runs start within 1 min of schedule |
| **Data Correctness** | 100% | Validate row counts match expectations |
| **Worker Utilization** | 60-80% | Kubernetes metrics |
| **P95 Flow Duration** | <5 min | Prefect UI → Analytics |
| **Operational Incidents** | <1 per week | Incident tracking |

---

## 14. Operational Considerations

### 14.1 Monitoring

**Key Dashboards:**

1. **Prefect Overview Dashboard**
   - Total deployments
   - Active flow runs
   - Flow success rate
   - Worker health

2. **Worker Performance Dashboard**
   - CPU/Memory usage per worker
   - Task execution time
   - Queue depth
   - Auto-scaling events

3. **Business Metrics Dashboard**
   - Files processed per hour
   - Records ingested per hour
   - Failed ingestions
   - Cost per ingestion

**Alerts:**

```yaml
# Example Prometheus alerts
- alert: PrefectServerDown
  expr: up{job="prefect-server"} == 0
  for: 1m

- alert: HighFlowFailureRate
  expr: rate(prefect_flow_runs_failed[5m]) > 0.05
  for: 5m

- alert: WorkerPodCrashing
  expr: kube_pod_container_status_restarts_total{pod=~"prefect-worker.*"} > 5
  for: 10m
```

### 14.2 Troubleshooting Guide

**Common Issues:**

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Flow stuck in PENDING** | Run not starting | Check if workers are running: `kubectl get pods -l app=prefect-worker` |
| **Worker OOM** | Pod restarting | Increase memory limits in deployment |
| **Slow execution** | Flows taking too long | Check Spark Connect connectivity, increase worker concurrency |
| **Deployment not found** | 404 error | Check `prefect_deployment_id` in database matches Prefect |
| **Schedule not triggering** | No runs at scheduled time | Check deployment is not paused in Prefect UI |

**Debugging Commands:**

```bash
# Check Prefect server logs
kubectl logs -l app=prefect-server --tail=100

# Check worker logs
kubectl logs -l app=prefect-worker --tail=100

# Check specific flow run
prefect flow-run inspect <flow-run-id>

# List all deployments
prefect deployment ls

# Check work queue status
prefect work-queue ls --pool autoloader-pool
```

### 14.3 Operational Runbooks

**Runbook 1: Scale Workers**

```bash
# Scale up workers during high load
kubectl scale deployment prefect-worker-default --replicas=10

# Scale down during low load
kubectl scale deployment prefect-worker-default --replicas=3
```

**Runbook 2: Restart Prefect Server**

```bash
# Graceful restart
kubectl rollout restart deployment prefect-server

# Verify server is healthy
kubectl get pods -l app=prefect-server
prefect server ls  # Should return server info
```

**Runbook 3: Emergency Pause All Ingestions**

```bash
# Pause all deployments
prefect deployment pause --all

# Verify
prefect deployment ls | grep PAUSED
```

---

## 15. Appendix

### 15.1 Glossary

| Term | Definition |
|------|------------|
| **Prefect Flow** | A containerized workflow (like a function) that orchestrates tasks |
| **Prefect Task** | A unit of work within a flow (retriable, composable) |
| **Prefect Deployment** | A flow + schedule + infrastructure configuration |
| **Prefect Worker** | A process that pulls work from queues and executes flows |
| **Work Pool** | A logical grouping of work queues (by infrastructure type) |
| **Work Queue** | A named queue that holds pending flow runs |
| **Flow Run** | A single execution instance of a flow |

### 15.2 Reference Links

- **Prefect Documentation:** https://docs.prefect.io/
- **Prefect API Reference:** https://docs.prefect.io/api-ref/
- **Kubernetes Documentation:** https://kubernetes.io/docs/
- **IOMETE Autoloader Docs:** `docs/batch-processing/`
- **Existing Prefect Research:** `docs/scheduler/prefect-*.md`

### 15.3 Decision Records

**Why Prefect over APScheduler?**
- ✅ Already in your stack (zero new dependencies)
- ✅ Horizontal scaling out-of-the-box
- ✅ 62% lower TCO than DIY distributed APScheduler
- ✅ Production-proven at scale
- ✅ Rich observability (UI + API)
- ✅ Faster time-to-market (2-3 weeks vs 4+ weeks)

**Why not keep manual triggering only?**
- ❌ Doesn't meet user requirements (automated scheduling)
- ❌ Manual intervention doesn't scale
- ❌ Poor user experience for scheduled ingestion use case

**Why incremental migration?**
- ✅ Reduces risk of production outage
- ✅ Allows rollback at any point
- ✅ Validates each step before proceeding
- ✅ Minimizes impact on end users

---

**End of Migration Guide**

**Next Steps:**
1. Review this document with the team
2. Get approval for migration timeline
3. Create Jira tickets for each phase
4. Begin Phase 1: Infrastructure Setup

**Questions?**
- Technical: Contact DevOps team
- Business: Contact Product Owner
- Escalation: Contact Engineering Manager

---

**Document History:**
- v1.0 - Initial draft (2025-11-07)
- v2.0 - Comprehensive migration guide (2025-11-07)

**Maintained By:** IOMETE Engineering Team
**Last Updated:** 2025-11-07
