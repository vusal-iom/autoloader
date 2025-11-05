# Scheduler Implementation Guide

**Document Version:** 1.0
**Created:** 2025-11-05
**Status:** Implementation Specification
**Estimated Effort:** 4-6 hours

---

## Table of Contents

1. [Overview](#overview)
2. [Current State](#current-state)
3. [Requirements](#requirements)
4. [Architecture Design](#architecture-design)
5. [Implementation Plan](#implementation-plan)
6. [Database Schema](#database-schema)
7. [API Changes](#api-changes)
8. [Service Layer Implementation](#service-layer-implementation)
9. [Configuration](#configuration)
10. [Error Handling & Recovery](#error-handling--recovery)
11. [Testing Strategy](#testing-strategy)
12. [Deployment Considerations](#deployment-considerations)
13. [Examples](#examples)
14. [References](#references)

---

## 1. Overview

### 1.1 Purpose

Implement automated scheduled execution of ingestion jobs, eliminating the need for manual triggering or external schedulers. This enables true "set it and forget it" data ingestion workflows.

### 1.2 Goals

- **Automated Execution:** Run ingestions on user-defined schedules without manual intervention
- **Flexible Scheduling:** Support common patterns (hourly, daily, weekly) and custom cron expressions
- **Timezone Support:** Respect user-specified timezones for schedule execution
- **High Availability:** Handle service restarts and ensure schedules persist
- **Observability:** Provide clear feedback on next scheduled run times
- **Resource Efficiency:** Only run when needed, not continuous streaming

### 1.3 Non-Goals (Phase 1)

- Distributed scheduling across multiple instances (single instance only)
- Advanced scheduling features (skip holidays, backpressure control)
- Dynamic schedule optimization based on data patterns
- Integration with external orchestration systems (Airflow, Dagster)

---

## 2. Current State

### 2.1 What Exists

**Database Fields** (already in `Ingestion` model):
```python
schedule_frequency = Column(String, nullable=True)  # "daily", "hourly", "weekly", "custom"
schedule_time = Column(String, nullable=True)       # "14:30", "02:00", etc.
schedule_timezone = Column(String, default="UTC")   # IANA timezone
schedule_cron = Column(String, nullable=True)       # "0 2 * * *"
```

**Dependencies** (already in requirements.txt):
```python
croniter==2.0.1  # Cron expression parsing and iteration
```

**Manual Trigger** (already implemented):
- `POST /api/v1/ingestions/{id}/run` - Manually trigger a run
- `IngestionService.trigger_manual_run()` - Service method
- `BatchOrchestrator.run()` - Execution logic

### 2.2 What's Missing

1. **SchedulerService** - Core scheduling logic
2. **APScheduler Integration** - Job scheduling library
3. **Lifecycle Management** - Start/stop with application
4. **Schedule Validation** - Validate cron expressions and configurations
5. **Next Run Calculation** - Show users when next run will occur
6. **Schedule Management API** - Enable/disable/update schedules
7. **Job State Persistence** - Survive application restarts
8. **Error Handling** - Handle failed scheduled runs

---

## 3. Requirements

### 3.1 Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1 | Support preset schedules: hourly, daily, weekly | MUST |
| FR-2 | Support custom cron expressions | MUST |
| FR-3 | Respect user-specified timezones | MUST |
| FR-4 | Calculate and display next scheduled run time | MUST |
| FR-5 | Enable/disable schedules without deleting ingestion | MUST |
| FR-6 | Handle overlapping runs (skip if previous still running) | MUST |
| FR-7 | Persist schedule state across restarts | MUST |
| FR-8 | Log all scheduled executions | MUST |
| FR-9 | Support schedule updates without service restart | SHOULD |
| FR-10 | Provide schedule validation before saving | SHOULD |

### 3.2 Non-Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-1 | Scheduler must be lightweight (minimal CPU when idle) | MUST |
| NFR-2 | Schedule precision within 1 minute | MUST |
| NFR-3 | Support at least 1000 concurrent scheduled ingestions | SHOULD |
| NFR-4 | Graceful shutdown (wait for running jobs) | MUST |
| NFR-5 | Observability through logs and metrics | SHOULD |

### 3.3 User Stories

**Story 1: Daily Ingestion**
```
As a data engineer
I want to schedule my S3 ingestion to run daily at 2 AM UTC
So that I can process yesterday's data automatically
```

**Story 2: Hourly Ingestion**
```
As an analyst
I want to schedule my ingestion to run every hour
So that my dashboard always has fresh data
```

**Story 3: Custom Schedule**
```
As a DevOps engineer
I want to schedule my ingestion using a custom cron expression "0 */6 * * *"
So that I can run every 6 hours to match my data source's update pattern
```

**Story 4: Timezone Awareness**
```
As a business user in London
I want to schedule my ingestion at 9 AM London time
So that data is ready when I start work, regardless of UTC offset
```

---

## 4. Architecture Design

### 4.1 Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Application                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐         ┌──────────────────┐         │
│  │   API Endpoint   │────────▶│ IngestionService │         │
│  │  /ingestions     │         │  create/update   │         │
│  └──────────────────┘         └────────┬─────────┘         │
│                                         │                    │
│                                         ▼                    │
│                              ┌──────────────────┐            │
│                              │ SchedulerService │            │
│                              │  - add_job()     │            │
│                              │  - remove_job()  │            │
│                              │  - update_job()  │            │
│                              └────────┬─────────┘            │
│                                       │                      │
│                                       ▼                      │
│                          ┌─────────────────────┐             │
│                          │   APScheduler       │             │
│                          │   BackgroundScheduler│            │
│                          │   - Job store       │             │
│                          │   - Executor pool   │             │
│                          └──────────┬──────────┘             │
│                                     │                        │
│                                     │ trigger at schedule    │
│                                     ▼                        │
│                          ┌─────────────────────┐             │
│                          │ BatchOrchestrator   │             │
│                          │   .run()            │             │
│                          └─────────────────────┘             │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 4.2 Scheduler Choice: APScheduler

**Why APScheduler?**
- ✅ Python-native, well-maintained
- ✅ Supports cron expressions, intervals, and one-time jobs
- ✅ Built-in persistence (SQLAlchemyJobStore)
- ✅ Timezone-aware scheduling
- ✅ Thread-safe and process-safe
- ✅ 10M+ downloads/month, battle-tested

**Alternatives Considered:**
- ❌ **Celery Beat:** Too heavy, requires Redis/RabbitMQ
- ❌ **Airflow:** Separate service, overkill for simple scheduling
- ❌ **Cron:** External dependency, no Python integration
- ❌ **asyncio scheduling:** No persistence, no cron support

### 4.3 Job Store Strategy

**SQLAlchemyJobStore** backed by PostgreSQL:
- Jobs persist across restarts
- Shares database with application
- Supports concurrent access
- Automatic cleanup of old jobs

**Configuration:**
```python
jobstores = {
    'default': SQLAlchemyJobStore(url=DATABASE_URL)
}
executors = {
    'default': ThreadPoolExecutor(10)  # Max 10 concurrent jobs
}
```

### 4.4 Scheduling Flow

```
1. User creates/updates ingestion with schedule
   ↓
2. IngestionService validates schedule configuration
   ↓
3. IngestionService calls SchedulerService.schedule_ingestion()
   ↓
4. SchedulerService creates APScheduler job with:
   - job_id: f"ingestion_{ingestion_id}"
   - trigger: CronTrigger (from cron expression)
   - func: BatchOrchestrator.run
   - args: [ingestion_id, tenant_id]
   - timezone: user's timezone
   ↓
5. APScheduler persists job to database
   ↓
6. At scheduled time, APScheduler executes job
   ↓
7. BatchOrchestrator.run() executes ingestion
   ↓
8. Run history recorded in database
```

---

## 5. Implementation Plan

### 5.1 Phase 1: Core Scheduler (2-3 hours)

**Tasks:**
1. Add APScheduler to requirements.txt
2. Create `app/services/scheduler_service.py`
3. Implement job scheduling methods
4. Add lifecycle hooks to `app/main.py`
5. Create database table for APScheduler jobs

**Deliverables:**
- SchedulerService with CRUD operations
- Application startup/shutdown integration
- Basic logging

### 5.2 Phase 2: API Integration (1-2 hours)

**Tasks:**
1. Update `IngestionService` to call SchedulerService
2. Add schedule validation logic
3. Implement next_run_time calculation
4. Update API response schemas

**Deliverables:**
- Automatic scheduling on ingestion create/update
- Schedule enable/disable functionality
- Next run time in API responses

### 5.3 Phase 3: Testing & Documentation (1 hour)

**Tasks:**
1. Write unit tests for SchedulerService
2. Write E2E test for scheduled execution
3. Update API documentation
4. Add configuration documentation

**Deliverables:**
- Test coverage >80%
- User-facing documentation
- Operations guide

---

## 6. Database Schema

### 6.1 Existing Schema (No Changes Needed)

```python
class Ingestion(Base):
    __tablename__ = "ingestions"

    # ... existing fields ...

    # Schedule configuration (already exists)
    schedule_frequency = Column(String, nullable=True)  # "daily", "hourly", "weekly", "custom"
    schedule_time = Column(String, nullable=True)       # "14:30"
    schedule_timezone = Column(String, default="UTC")   # "America/New_York"
    schedule_cron = Column(String, nullable=True)       # "0 2 * * *"

    # Additional fields (if needed)
    schedule_enabled = Column(Boolean, default=False)   # Master on/off switch
    next_run_time = Column(DateTime, nullable=True)     # Calculated field
    last_scheduled_run = Column(DateTime, nullable=True) # Track last execution
```

### 6.2 APScheduler Tables (Auto-Created)

APScheduler will create its own table:
```sql
CREATE TABLE apscheduler_jobs (
    id VARCHAR(191) PRIMARY KEY,
    next_run_time FLOAT,
    job_state BLOB  -- Pickled job configuration
);
```

**Note:** We'll configure APScheduler to use a separate table prefix: `autoloader_apscheduler_jobs`

### 6.3 Migration Required

```python
# alembic/versions/XXX_add_schedule_enabled.py

def upgrade():
    op.add_column('ingestions', sa.Column('schedule_enabled', sa.Boolean(), default=False))
    op.add_column('ingestions', sa.Column('next_run_time', sa.DateTime(), nullable=True))
    op.add_column('ingestions', sa.Column('last_scheduled_run', sa.DateTime(), nullable=True))

def downgrade():
    op.drop_column('ingestions', 'last_scheduled_run')
    op.drop_column('ingestions', 'next_run_time')
    op.drop_column('ingestions', 'schedule_enabled')
```

---

## 7. API Changes

### 7.1 Create/Update Ingestion Request

**Existing Schema (`IngestionCreate` in `app/models/schemas.py`):**
```python
class IngestionCreate(BaseModel):
    name: str
    description: Optional[str] = None

    # ... source/destination config ...

    # Schedule configuration (already exists in schema)
    schedule_frequency: Optional[str] = None  # "daily", "hourly", "weekly", "custom"
    schedule_time: Optional[str] = None       # "14:30"
    schedule_timezone: str = "UTC"            # IANA timezone
    schedule_cron: Optional[str] = None       # "0 2 * * *"
```

**New Fields to Add:**
```python
class IngestionCreate(BaseModel):
    # ... existing fields ...

    schedule_enabled: bool = False  # Master on/off switch
```

**Validation Rules:**
```python
@validator('schedule_cron')
def validate_cron(cls, v, values):
    if v:
        # Validate cron expression using croniter
        try:
            croniter(v)
        except Exception as e:
            raise ValueError(f"Invalid cron expression: {e}")
    return v

@validator('schedule_frequency')
def validate_frequency(cls, v):
    if v and v not in ["hourly", "daily", "weekly", "custom"]:
        raise ValueError("schedule_frequency must be: hourly, daily, weekly, or custom")
    return v

@validator('schedule_time')
def validate_time(cls, v):
    if v:
        # Validate HH:MM format
        if not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', v):
            raise ValueError("schedule_time must be in HH:MM format (e.g., '14:30')")
    return v

@validator('schedule_timezone')
def validate_timezone(cls, v):
    try:
        pytz.timezone(v)
    except Exception:
        raise ValueError(f"Invalid timezone: {v}")
    return v
```

### 7.2 Response Schema Updates

**Ingestion Response:**
```python
class IngestionResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    name: str
    status: IngestionStatus

    # ... existing fields ...

    # Schedule information
    schedule_frequency: Optional[str] = None
    schedule_time: Optional[str] = None
    schedule_timezone: str = "UTC"
    schedule_cron: Optional[str] = None
    schedule_enabled: bool = False

    # NEW: Runtime schedule information
    next_run_time: Optional[datetime] = None  # When will next run execute
    last_scheduled_run: Optional[datetime] = None  # When was last scheduled run

    created_at: datetime
    updated_at: datetime
```

### 7.3 New Endpoints

**Enable/Disable Schedule:**
```python
@router.post("/{ingestion_id}/schedule/enable")
async def enable_schedule(
    ingestion_id: UUID,
    db: Session = Depends(get_db)
) -> IngestionResponse:
    """Enable scheduled execution for this ingestion."""
    pass

@router.post("/{ingestion_id}/schedule/disable")
async def disable_schedule(
    ingestion_id: UUID,
    db: Session = Depends(get_db)
) -> IngestionResponse:
    """Disable scheduled execution (does not delete ingestion)."""
    pass
```

**Get Schedule Info:**
```python
@router.get("/{ingestion_id}/schedule")
async def get_schedule_info(
    ingestion_id: UUID,
    db: Session = Depends(get_db)
) -> ScheduleInfoResponse:
    """Get detailed schedule information including next 5 run times."""
    pass

class ScheduleInfoResponse(BaseModel):
    enabled: bool
    frequency: Optional[str]
    cron_expression: Optional[str]
    timezone: str
    next_run_time: Optional[datetime]
    upcoming_runs: List[datetime]  # Next 5 scheduled times
```

---

## 8. Service Layer Implementation

### 8.1 SchedulerService

**File:** `app/services/scheduler_service.py`

```python
from typing import Optional
from uuid import UUID
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from croniter import croniter
import pytz
import logging

from app.config import settings
from app.models.domain import Ingestion
from app.services.batch_orchestrator import BatchOrchestrator

logger = logging.getLogger(__name__)


class SchedulerService:
    """
    Manages scheduled execution of ingestion jobs using APScheduler.

    Responsibilities:
    - Add/remove/update scheduled jobs
    - Calculate next run times
    - Handle schedule validation
    - Provide schedule lifecycle management
    """

    def __init__(self):
        """Initialize the scheduler with persistent job store."""

        jobstores = {
            'default': SQLAlchemyJobStore(
                url=settings.DATABASE_URL,
                tablename='autoloader_apscheduler_jobs'
            )
        }

        executors = {
            'default': ThreadPoolExecutor(max_workers=settings.SCHEDULER_MAX_WORKERS)
        }

        job_defaults = {
            'coalesce': True,  # Combine multiple missed runs into one
            'max_instances': 1,  # Only one instance of a job at a time
            'misfire_grace_time': 300  # 5 minutes grace period
        }

        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=pytz.UTC
        )

        logger.info("SchedulerService initialized")

    def start(self):
        """Start the scheduler. Call this during application startup."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")

    def shutdown(self, wait: bool = True):
        """
        Shutdown the scheduler gracefully.

        Args:
            wait: If True, wait for all running jobs to complete
        """
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info(f"Scheduler stopped (wait={wait})")

    def schedule_ingestion(
        self,
        ingestion: Ingestion,
        db_session
    ) -> Optional[datetime]:
        """
        Schedule an ingestion job.

        Args:
            ingestion: Ingestion domain model
            db_session: Database session for orchestrator

        Returns:
            Next run time if scheduled, None otherwise
        """

        if not ingestion.schedule_enabled:
            logger.info(f"Schedule disabled for ingestion {ingestion.id}")
            return None

        # Remove existing job if any
        self.remove_ingestion(ingestion.id)

        # Build cron expression
        cron_expr = self._build_cron_expression(ingestion)
        if not cron_expr:
            logger.warning(f"Could not build cron expression for ingestion {ingestion.id}")
            return None

        # Create trigger with user's timezone
        tz = pytz.timezone(ingestion.schedule_timezone)
        trigger = CronTrigger.from_crontab(cron_expr, timezone=tz)

        # Schedule the job
        job = self.scheduler.add_job(
            func=self._execute_scheduled_run,
            trigger=trigger,
            args=[str(ingestion.id), str(ingestion.tenant_id)],
            id=self._job_id(ingestion.id),
            name=f"Ingestion: {ingestion.name}",
            replace_existing=True
        )

        logger.info(
            f"Scheduled ingestion {ingestion.id} with cron '{cron_expr}' "
            f"in timezone {ingestion.schedule_timezone}. "
            f"Next run: {job.next_run_time}"
        )

        return job.next_run_time

    def remove_ingestion(self, ingestion_id: UUID) -> bool:
        """
        Remove a scheduled ingestion job.

        Args:
            ingestion_id: Ingestion UUID

        Returns:
            True if job was removed, False if not found
        """
        job_id = self._job_id(ingestion_id)
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed scheduled job for ingestion {ingestion_id}")
            return True
        except Exception as e:
            logger.debug(f"Job {job_id} not found: {e}")
            return False

    def get_next_run_time(self, ingestion_id: UUID) -> Optional[datetime]:
        """
        Get the next scheduled run time for an ingestion.

        Args:
            ingestion_id: Ingestion UUID

        Returns:
            Next run time or None if not scheduled
        """
        job_id = self._job_id(ingestion_id)
        job = self.scheduler.get_job(job_id)
        return job.next_run_time if job else None

    def get_upcoming_runs(self, ingestion: Ingestion, count: int = 5) -> list[datetime]:
        """
        Calculate the next N scheduled run times.

        Args:
            ingestion: Ingestion configuration
            count: Number of upcoming runs to calculate

        Returns:
            List of upcoming run times
        """
        cron_expr = self._build_cron_expression(ingestion)
        if not cron_expr:
            return []

        tz = pytz.timezone(ingestion.schedule_timezone)
        iter_obj = croniter(cron_expr, datetime.now(tz))

        return [iter_obj.get_next(datetime) for _ in range(count)]

    def _build_cron_expression(self, ingestion: Ingestion) -> Optional[str]:
        """
        Build a cron expression from ingestion schedule configuration.

        Supports:
        - Custom: Use schedule_cron directly
        - Hourly: "0 * * * *"
        - Daily: "0 {HH} * * *" (from schedule_time)
        - Weekly: "0 {HH} * * 0" (Sunday, from schedule_time)

        Returns:
            Cron expression string or None if invalid
        """

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

        Defaults to 00:00 if not provided.
        """
        if not time_str:
            return (0, 0)

        parts = time_str.split(":")
        return (int(parts[0]), int(parts[1]))

    def _job_id(self, ingestion_id: UUID) -> str:
        """Generate APScheduler job ID from ingestion ID."""
        return f"ingestion_{ingestion_id}"

    def _execute_scheduled_run(self, ingestion_id_str: str, tenant_id_str: str):
        """
        Execute a scheduled ingestion run.

        This is called by APScheduler at the scheduled time.
        Wraps BatchOrchestrator with error handling and logging.

        Args:
            ingestion_id_str: Ingestion UUID as string
            tenant_id_str: Tenant UUID as string
        """
        from uuid import UUID
        from sqlalchemy.orm import Session
        from app.database import SessionLocal

        ingestion_id = UUID(ingestion_id_str)
        tenant_id = UUID(tenant_id_str)

        logger.info(f"Executing scheduled run for ingestion {ingestion_id}")

        db: Session = SessionLocal()
        try:
            orchestrator = BatchOrchestrator(db)
            run = orchestrator.run(
                ingestion_id=ingestion_id,
                tenant_id=tenant_id,
                triggered_by="scheduler"
            )

            logger.info(
                f"Scheduled run completed for ingestion {ingestion_id}. "
                f"Run ID: {run.id}, Status: {run.status}"
            )

            # Update last_scheduled_run timestamp
            from app.repositories.ingestion_repository import IngestionRepository
            repo = IngestionRepository(db)
            ingestion = repo.get_by_id(tenant_id, ingestion_id)
            if ingestion:
                ingestion.last_scheduled_run = datetime.utcnow()
                db.commit()

        except Exception as e:
            logger.error(
                f"Scheduled run failed for ingestion {ingestion_id}: {e}",
                exc_info=True
            )
            db.rollback()
        finally:
            db.close()


# Global singleton instance
_scheduler_service: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    """Get the global SchedulerService instance."""
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
```

### 8.2 Configuration Updates

**File:** `app/config.py`

```python
class Settings(BaseSettings):
    # ... existing settings ...

    # Scheduler configuration
    SCHEDULER_MAX_WORKERS: int = 10  # Max concurrent scheduled jobs
    SCHEDULER_MISFIRE_GRACE_TIME: int = 300  # 5 minutes
    SCHEDULER_COALESCE: bool = True  # Combine missed runs

    class Config:
        env_file = ".env"
```

### 8.3 IngestionService Updates

**File:** `app/services/ingestion_service.py`

Add scheduler integration:

```python
from app.services.scheduler_service import get_scheduler_service

class IngestionService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = IngestionRepository(db)
        self.scheduler = get_scheduler_service()

    def create_ingestion(
        self,
        tenant_id: UUID,
        ingestion_data: IngestionCreate
    ) -> Ingestion:
        """Create a new ingestion configuration."""

        # Validate schedule configuration
        self._validate_schedule(ingestion_data)

        # Create ingestion
        ingestion = self.repository.create(tenant_id, ingestion_data)

        # Schedule if enabled
        if ingestion.schedule_enabled:
            next_run = self.scheduler.schedule_ingestion(ingestion, self.db)
            ingestion.next_run_time = next_run
            self.db.commit()

        return ingestion

    def update_ingestion(
        self,
        tenant_id: UUID,
        ingestion_id: UUID,
        update_data: IngestionUpdate
    ) -> Ingestion:
        """Update an existing ingestion configuration."""

        # Validate schedule if provided
        if any(k.startswith('schedule_') for k in update_data.dict(exclude_unset=True)):
            self._validate_schedule(update_data)

        # Update ingestion
        ingestion = self.repository.update(tenant_id, ingestion_id, update_data)

        # Update schedule
        if ingestion.schedule_enabled:
            next_run = self.scheduler.schedule_ingestion(ingestion, self.db)
            ingestion.next_run_time = next_run
        else:
            self.scheduler.remove_ingestion(ingestion_id)
            ingestion.next_run_time = None

        self.db.commit()
        return ingestion

    def delete_ingestion(self, tenant_id: UUID, ingestion_id: UUID) -> bool:
        """Delete an ingestion and remove its schedule."""

        # Remove schedule first
        self.scheduler.remove_ingestion(ingestion_id)

        # Delete ingestion
        return self.repository.delete(tenant_id, ingestion_id)

    def enable_schedule(self, tenant_id: UUID, ingestion_id: UUID) -> Ingestion:
        """Enable scheduled execution."""
        ingestion = self.repository.get_by_id(tenant_id, ingestion_id)
        if not ingestion:
            raise ValueError("Ingestion not found")

        ingestion.schedule_enabled = True
        next_run = self.scheduler.schedule_ingestion(ingestion, self.db)
        ingestion.next_run_time = next_run

        self.db.commit()
        return ingestion

    def disable_schedule(self, tenant_id: UUID, ingestion_id: UUID) -> Ingestion:
        """Disable scheduled execution."""
        ingestion = self.repository.get_by_id(tenant_id, ingestion_id)
        if not ingestion:
            raise ValueError("Ingestion not found")

        ingestion.schedule_enabled = False
        ingestion.next_run_time = None
        self.scheduler.remove_ingestion(ingestion_id)

        self.db.commit()
        return ingestion

    def _validate_schedule(self, data) -> None:
        """Validate schedule configuration."""

        # If schedule_enabled, must have frequency
        if getattr(data, 'schedule_enabled', False):
            if not getattr(data, 'schedule_frequency', None):
                raise ValueError("schedule_frequency is required when schedule_enabled=True")

            # If custom, must have cron expression
            if data.schedule_frequency == "custom" and not getattr(data, 'schedule_cron', None):
                raise ValueError("schedule_cron is required for custom frequency")

            # If not custom, validate schedule_time if provided
            if data.schedule_frequency in ["daily", "weekly"]:
                if not getattr(data, 'schedule_time', None):
                    raise ValueError(f"schedule_time is required for {data.schedule_frequency} frequency")
```

---

## 9. Configuration

### 9.1 Environment Variables

Add to `.env`:

```bash
# Scheduler Configuration
SCHEDULER_MAX_WORKERS=10           # Max concurrent scheduled jobs
SCHEDULER_MISFIRE_GRACE_TIME=300   # 5 minutes grace period
SCHEDULER_COALESCE=true            # Combine missed runs
```

### 9.2 Application Lifecycle

**File:** `app/main.py`

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.services.scheduler_service import get_scheduler_service
from app.database import SessionLocal
import logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""

    # Startup
    logger.info("Starting IOMETE Autoloader...")

    # Initialize scheduler
    scheduler = get_scheduler_service()
    scheduler.start()

    # Load existing schedules from database
    await _load_existing_schedules()

    logger.info("Scheduler started and schedules loaded")

    yield

    # Shutdown
    logger.info("Shutting down IOMETE Autoloader...")

    # Stop scheduler (wait for running jobs)
    scheduler.shutdown(wait=True)

    logger.info("Scheduler stopped")


async def _load_existing_schedules():
    """
    Load all enabled schedules from database on startup.
    This ensures schedules persist across application restarts.
    """
    from app.repositories.ingestion_repository import IngestionRepository
    from app.models.domain import IngestionStatus

    db = SessionLocal()
    try:
        repo = IngestionRepository(db)
        scheduler = get_scheduler_service()

        # Find all active ingestions with schedules enabled
        ingestions = db.query(Ingestion).filter(
            Ingestion.schedule_enabled == True,
            Ingestion.status == IngestionStatus.ACTIVE
        ).all()

        logger.info(f"Loading {len(ingestions)} scheduled ingestions...")

        for ingestion in ingestions:
            try:
                next_run = scheduler.schedule_ingestion(ingestion, db)
                ingestion.next_run_time = next_run
                logger.info(
                    f"Loaded schedule for ingestion {ingestion.id} "
                    f"(next run: {next_run})"
                )
            except Exception as e:
                logger.error(
                    f"Failed to load schedule for ingestion {ingestion.id}: {e}"
                )

        db.commit()
        logger.info(f"Successfully loaded {len(ingestions)} schedules")

    except Exception as e:
        logger.error(f"Failed to load schedules: {e}")
        db.rollback()
    finally:
        db.close()


# Create app with lifespan
app = FastAPI(
    title="IOMETE Autoloader",
    version="1.0.0",
    lifespan=lifespan
)
```

---

## 10. Error Handling & Recovery

### 10.1 Misfire Handling

**Scenario:** Application was down during scheduled time.

**APScheduler Behavior:**
- If misfire is within grace period (5 min): Execute immediately
- If beyond grace period: Skip and wait for next scheduled time
- If `coalesce=True`: Combine multiple missed runs into one

**Configuration:**
```python
job_defaults = {
    'coalesce': True,  # Combine missed runs
    'max_instances': 1,  # Prevent overlapping executions
    'misfire_grace_time': 300  # 5 minutes
}
```

### 10.2 Overlapping Runs

**Scenario:** Previous run still executing when next schedule arrives.

**Prevention:**
- `max_instances=1` ensures only one instance runs at a time
- APScheduler will skip the new execution
- Log warning for monitoring

**Implementation:**
```python
# In SchedulerService._execute_scheduled_run
if self.scheduler.get_job(job_id).next_run_time < datetime.now():
    logger.warning(
        f"Ingestion {ingestion_id} still running, skipping scheduled execution"
    )
```

### 10.3 Failed Run Recovery

**Scenario:** Scheduled run fails with exception.

**Behavior:**
- Exception is logged
- Run record saved with ERROR status
- Schedule continues (next run unaffected)
- User can retry manually via API

**No automatic retry** to prevent infinite failure loops. User must:
- Fix underlying issue (credentials, data format, etc.)
- Manually retry via API or wait for next schedule

### 10.4 Database Connection Loss

**Scenario:** Database becomes unavailable during execution.

**Handling:**
- APScheduler persists jobs to database periodically
- On reconnection, schedules are restored
- Running jobs fail and are logged
- Next scheduled execution proceeds normally

---

## 11. Testing Strategy

### 11.1 Unit Tests

**File:** `tests/unit/test_scheduler_service.py`

```python
import pytest
from datetime import datetime
import pytz
from app.services.scheduler_service import SchedulerService
from app.models.domain import Ingestion


def test_build_cron_expression_hourly():
    """Test cron expression for hourly frequency."""
    service = SchedulerService()
    ingestion = Ingestion(schedule_frequency="hourly")

    cron = service._build_cron_expression(ingestion)
    assert cron == "0 * * * *"


def test_build_cron_expression_daily():
    """Test cron expression for daily frequency."""
    service = SchedulerService()
    ingestion = Ingestion(
        schedule_frequency="daily",
        schedule_time="14:30"
    )

    cron = service._build_cron_expression(ingestion)
    assert cron == "30 14 * * *"


def test_build_cron_expression_custom():
    """Test cron expression for custom frequency."""
    service = SchedulerService()
    ingestion = Ingestion(
        schedule_frequency="custom",
        schedule_cron="0 */6 * * *"
    )

    cron = service._build_cron_expression(ingestion)
    assert cron == "0 */6 * * *"


def test_get_upcoming_runs():
    """Test calculation of upcoming run times."""
    service = SchedulerService()
    ingestion = Ingestion(
        schedule_frequency="daily",
        schedule_time="02:00",
        schedule_timezone="UTC"
    )

    upcoming = service.get_upcoming_runs(ingestion, count=3)
    assert len(upcoming) == 3

    # All should be at 02:00
    for run_time in upcoming:
        assert run_time.hour == 2
        assert run_time.minute == 0


def test_timezone_handling():
    """Test schedule respects user timezone."""
    service = SchedulerService()

    # Schedule for 9 AM New York time
    ingestion = Ingestion(
        schedule_frequency="daily",
        schedule_time="09:00",
        schedule_timezone="America/New_York"
    )

    upcoming = service.get_upcoming_runs(ingestion, count=1)
    ny_time = upcoming[0].astimezone(pytz.timezone("America/New_York"))

    assert ny_time.hour == 9
    assert ny_time.minute == 0
```

### 11.2 Integration Tests

**File:** `tests/integration/test_scheduler_integration.py`

```python
import pytest
import time
from datetime import datetime, timedelta
from app.services.scheduler_service import SchedulerService
from app.services.ingestion_service import IngestionService
from app.models.schemas import IngestionCreate


def test_schedule_lifecycle(db_session, test_tenant_id):
    """Test complete schedule lifecycle: create, execute, disable."""

    scheduler = SchedulerService()
    scheduler.start()

    ingestion_service = IngestionService(db_session)

    # Create ingestion with schedule
    ingestion_data = IngestionCreate(
        name="Test Scheduled Ingestion",
        schedule_frequency="custom",
        schedule_cron="* * * * *",  # Every minute for testing
        schedule_enabled=True,
        schedule_timezone="UTC",
        # ... other required fields ...
    )

    ingestion = ingestion_service.create_ingestion(test_tenant_id, ingestion_data)

    # Verify job was scheduled
    next_run = scheduler.get_next_run_time(ingestion.id)
    assert next_run is not None
    assert next_run > datetime.now()

    # Wait for execution (max 65 seconds)
    time.sleep(65)

    # Check that a run was created
    from app.repositories.run_repository import RunRepository
    run_repo = RunRepository(db_session)
    runs = run_repo.list_runs(test_tenant_id, ingestion.id)
    assert len(runs) > 0

    # Disable schedule
    ingestion_service.disable_schedule(test_tenant_id, ingestion.id)

    # Verify job was removed
    next_run = scheduler.get_next_run_time(ingestion.id)
    assert next_run is None

    scheduler.shutdown()


def test_schedule_survives_restart(db_session, test_tenant_id):
    """Test that schedules persist across scheduler restarts."""

    # Create scheduler and schedule job
    scheduler1 = SchedulerService()
    scheduler1.start()

    ingestion_service = IngestionService(db_session)
    ingestion_data = IngestionCreate(
        name="Persistent Schedule Test",
        schedule_frequency="daily",
        schedule_time="02:00",
        schedule_enabled=True,
        # ... other fields ...
    )

    ingestion = ingestion_service.create_ingestion(test_tenant_id, ingestion_data)
    job_id = scheduler1._job_id(ingestion.id)

    # Verify job exists
    assert scheduler1.scheduler.get_job(job_id) is not None

    # Shutdown
    scheduler1.shutdown(wait=False)

    # Create new scheduler (simulates restart)
    scheduler2 = SchedulerService()
    scheduler2.start()

    # Job should still exist (loaded from database)
    assert scheduler2.scheduler.get_job(job_id) is not None

    scheduler2.shutdown()
```

### 11.3 E2E Test

**File:** `tests/e2e/test_scheduled_execution.py`

```python
import pytest
import time
from datetime import datetime
from fastapi.testclient import TestClient


def test_scheduled_execution_e2e(
    client: TestClient,
    test_tenant_id,
    minio_client,
    spark_session
):
    """
    End-to-end test of scheduled ingestion execution.

    Workflow:
    1. Upload test files to MinIO
    2. Create ingestion with schedule (every minute)
    3. Wait for scheduled execution
    4. Verify data in Iceberg table
    5. Verify run history
    """

    # 1. Upload test files
    bucket = "test-scheduled-bucket"
    minio_client.create_bucket(bucket)
    test_data = [{"id": 1, "value": "test"}]
    upload_json_file(minio_client, bucket, "data.json", test_data)

    # 2. Create ingestion with schedule
    create_payload = {
        "name": "E2E Scheduled Ingestion",
        "source_type": "s3",
        "source_path": f"s3a://{bucket}/",
        "format_type": "json",
        "destination_table": "test_db.scheduled_table",
        "schedule_frequency": "custom",
        "schedule_cron": "* * * * *",  # Every minute
        "schedule_enabled": True,
        "schedule_timezone": "UTC",
        # ... credentials ...
    }

    response = client.post("/api/v1/ingestions", json=create_payload)
    assert response.status_code == 201

    ingestion_id = response.json()["id"]

    # 3. Wait for scheduled execution (max 65 seconds)
    max_wait = 65
    start_time = time.time()
    run_found = False

    while time.time() - start_time < max_wait:
        runs_response = client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        runs = runs_response.json()["items"]

        if len(runs) > 0:
            run_found = True
            break

        time.sleep(5)

    assert run_found, "No scheduled run was executed within timeout"

    # 4. Verify data in Iceberg table
    df = spark_session.table("test_db.scheduled_table")
    assert df.count() == 1
    assert df.first()["id"] == 1

    # 5. Verify run history
    run = runs[0]
    assert run["status"] in ["SUCCESS", "RUNNING"]
    assert run["triggered_by"] == "scheduler"

    # Cleanup: disable schedule
    client.post(f"/api/v1/ingestions/{ingestion_id}/schedule/disable")
```

---

## 12. Deployment Considerations

### 12.1 Single Instance Deployment

**Current Implementation:** Single instance only

**Implications:**
- One scheduler per deployment
- All schedules run on single process
- Simple to deploy and monitor
- Sufficient for most use cases (<1000 ingestions)

**Scaling Limits:**
- Max ~10 concurrent jobs (configurable)
- Scheduler precision degrades under heavy load
- Single point of failure

### 12.2 High Availability (Future)

**For Multi-Instance Deployment:**

Would require:
- Distributed locking (Redis, PostgreSQL advisory locks)
- Leader election (only one instance runs scheduler)
- Job distribution across instances

**Not implemented in Phase 1** - single instance is sufficient for MVP.

### 12.3 Monitoring

**Metrics to Track:**
- Number of active schedules
- Missed executions (misfires)
- Average execution time per ingestion
- Failed scheduled runs
- Scheduler queue length

**Implementation:**
```python
from prometheus_client import Gauge, Counter

scheduled_jobs_total = Gauge(
    'autoloader_scheduled_jobs_total',
    'Total number of scheduled ingestions'
)

scheduled_executions_total = Counter(
    'autoloader_scheduled_executions_total',
    'Total scheduled executions',
    ['status']  # success, failed, skipped
)
```

### 12.4 Logging

**Key Log Events:**
- Scheduler start/stop
- Job scheduled/removed
- Execution triggered
- Execution completed (with duration)
- Execution failed (with error)
- Misfires and skipped runs

**Log Format:**
```
2025-11-05 14:30:00 INFO [scheduler] Executing scheduled run for ingestion abc-123
2025-11-05 14:32:15 INFO [scheduler] Scheduled run completed for ingestion abc-123. Run ID: xyz-456, Status: SUCCESS, Duration: 2m15s
```

---

## 13. Examples

### 13.1 Daily Ingestion at 2 AM UTC

**Request:**
```json
POST /api/v1/ingestions

{
  "name": "Daily AWS CloudTrail Ingestion",
  "source_type": "s3",
  "source_path": "s3a://my-logs-bucket/cloudtrail/",
  "format_type": "json",
  "destination_table": "security.cloudtrail_logs",
  "schedule_frequency": "daily",
  "schedule_time": "02:00",
  "schedule_timezone": "UTC",
  "schedule_enabled": true
}
```

**Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Daily AWS CloudTrail Ingestion",
  "status": "ACTIVE",
  "schedule_frequency": "daily",
  "schedule_time": "02:00",
  "schedule_timezone": "UTC",
  "schedule_enabled": true,
  "next_run_time": "2025-11-06T02:00:00Z",
  "created_at": "2025-11-05T14:30:00Z"
}
```

### 13.2 Hourly Ingestion

**Request:**
```json
POST /api/v1/ingestions

{
  "name": "Hourly Metrics Ingestion",
  "source_type": "s3",
  "source_path": "s3a://metrics-bucket/",
  "format_type": "parquet",
  "destination_table": "analytics.hourly_metrics",
  "schedule_frequency": "hourly",
  "schedule_timezone": "UTC",
  "schedule_enabled": true
}
```

**Cron equivalent:** `0 * * * *` (top of every hour)

### 13.3 Custom Cron: Every 6 Hours

**Request:**
```json
POST /api/v1/ingestions

{
  "name": "6-Hour Batch Ingestion",
  "source_type": "s3",
  "source_path": "s3a://data-lake/batch/",
  "format_type": "csv",
  "destination_table": "warehouse.batch_data",
  "schedule_frequency": "custom",
  "schedule_cron": "0 */6 * * *",
  "schedule_timezone": "UTC",
  "schedule_enabled": true
}
```

**Execution times:** 00:00, 06:00, 12:00, 18:00 UTC

### 13.4 Business Hours (9 AM London Time)

**Request:**
```json
POST /api/v1/ingestions

{
  "name": "Morning Report Ingestion",
  "source_type": "s3",
  "source_path": "s3a://reports-bucket/",
  "format_type": "json",
  "destination_table": "reporting.daily_reports",
  "schedule_frequency": "daily",
  "schedule_time": "09:00",
  "schedule_timezone": "Europe/London",
  "schedule_enabled": true
}
```

**Note:** Automatically handles BST/GMT transitions

### 13.5 Get Schedule Information

**Request:**
```
GET /api/v1/ingestions/{id}/schedule
```

**Response:**
```json
{
  "enabled": true,
  "frequency": "daily",
  "cron_expression": "0 2 * * *",
  "timezone": "UTC",
  "next_run_time": "2025-11-06T02:00:00Z",
  "upcoming_runs": [
    "2025-11-06T02:00:00Z",
    "2025-11-07T02:00:00Z",
    "2025-11-08T02:00:00Z",
    "2025-11-09T02:00:00Z",
    "2025-11-10T02:00:00Z"
  ]
}
```

### 13.6 Disable Schedule

**Request:**
```
POST /api/v1/ingestions/{id}/schedule/disable
```

**Effect:**
- Removes scheduled job
- Ingestion remains active
- Can still trigger manual runs
- Re-enable anytime without reconfiguration

---

## 14. References

### 14.1 Documentation

- **APScheduler Docs:** https://apscheduler.readthedocs.io/
- **Croniter Library:** https://github.com/kiorky/croniter
- **Cron Expression Guide:** https://crontab.guru/
- **IANA Timezone Database:** https://www.iana.org/time-zones

### 14.2 Related Files

- `app/models/domain.py:Ingestion` - Database model with schedule fields
- `app/services/batch_orchestrator.py` - Execution logic to be triggered
- `app/config.py` - Configuration settings
- `docs/batch-processing/batch-processing-implementation-guide.md` - Batch processing overview

### 14.3 Decision Records

**Why APScheduler over alternatives?**
- Python-native, no external dependencies (Redis, RabbitMQ)
- Built-in persistence via SQLAlchemy
- Timezone-aware scheduling
- Lightweight for single-instance deployment
- Battle-tested (10M+ downloads/month)

**Why not distributed scheduling in Phase 1?**
- Single instance sufficient for MVP (<1000 ingestions)
- Simpler to implement and debug
- Can be upgraded to distributed later if needed
- Most customers will run <100 scheduled ingestions

**Why ThreadPoolExecutor vs ProcessPoolExecutor?**
- Ingestion execution is I/O-bound (Spark Connect, S3 API)
- Threads share memory with lower overhead
- Easier debugging and logging
- Process pool would require serialization of all arguments

---

## Appendix A: Common Cron Expressions

| Expression | Meaning | Use Case |
|------------|---------|----------|
| `0 * * * *` | Every hour at minute 0 | Hourly metrics |
| `0 0 * * *` | Daily at midnight UTC | Daily reports |
| `0 2 * * *` | Daily at 2 AM UTC | Off-peak batch jobs |
| `0 */6 * * *` | Every 6 hours | Moderate frequency updates |
| `*/15 * * * *` | Every 15 minutes | Near real-time ingestion |
| `0 0 * * 0` | Weekly on Sunday at midnight | Weekly aggregations |
| `0 9 * * 1-5` | Weekdays at 9 AM | Business hours only |
| `0 0 1 * *` | Monthly on 1st at midnight | Monthly reports |

## Appendix B: Timezone Examples

| Timezone | IANA Code | Notes |
|----------|-----------|-------|
| UTC | `UTC` | Standard for most backends |
| New York | `America/New_York` | Handles EST/EDT automatically |
| London | `Europe/London` | Handles GMT/BST automatically |
| Tokyo | `Asia/Tokyo` | No DST |
| Sydney | `Australia/Sydney` | Southern hemisphere DST |
| Los Angeles | `America/Los_Angeles` | PST/PDT |

**Full list:** https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

---

**End of Document**
