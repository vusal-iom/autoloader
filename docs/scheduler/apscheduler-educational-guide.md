# APScheduler Educational Guide

**Document Version:** 1.0
**Created:** 2025-11-07
**Audience:** Developers learning APScheduler
**Related:** `scheduler-implementation-guide.md`, `scheduler-scaling-and-risks.md`

---

## Table of Contents

1. [What is APScheduler?](#what-is-apscheduler)
2. [Core Concepts](#core-concepts)
3. [Architecture Overview](#architecture-overview)
4. [Scheduling Methods](#scheduling-methods)
5. [Job Stores](#job-stores)
6. [Executors](#executors)
7. [Practical Examples](#practical-examples)
8. [Advanced Topics](#advanced-topics)
9. [Best Practices](#best-practices)
10. [Common Pitfalls](#common-pitfalls)
11. [Production Considerations](#production-considerations)
12. [References](#references)

---

## 1. What is APScheduler?

### 1.1 Overview

**APScheduler** (Advanced Python Scheduler) is a Python library that lets you schedule Python code to be executed later, either just once or periodically. Think of it as a Python-native alternative to cron, but with much more flexibility and power.

**Key Features:**
- Schedule jobs using various triggers (cron, interval, date)
- Persistent job storage (survive restarts)
- Multiple execution backends (threads, processes, async)
- Timezone-aware scheduling
- Python-native, no external services required

**Use Cases:**
- Periodic data processing (hourly reports, daily backups)
- Scheduled ETL pipelines
- Recurring cleanup tasks
- Reminder systems
- Scheduled API calls

### 1.2 Why APScheduler?

**Compared to Cron:**
- ✅ Python-native (no shell scripts)
- ✅ Timezone-aware
- ✅ Programmatic control (add/remove jobs at runtime)
- ✅ Better error handling
- ✅ Cross-platform (works on Windows)

**Compared to Celery Beat:**
- ✅ Lighter weight (no message broker required)
- ✅ Simpler setup
- ✅ Better for scheduled tasks (not distributed workers)
- ❌ Not designed for task distribution

**Compared to Airflow:**
- ✅ Much simpler (embedded in your app)
- ✅ No separate service to maintain
- ❌ Not designed for complex workflows (DAGs)
- ❌ Limited observability

### 1.3 Installation

```bash
# Basic installation
pip install apscheduler

# With specific job store backends
pip install apscheduler[postgresql]  # PostgreSQL support
pip install apscheduler[mongodb]     # MongoDB support
pip install apscheduler[redis]       # Redis support
```

---

## 2. Core Concepts

### 2.1 The Four Components

APScheduler has four main components that work together:

```
┌─────────────────────────────────────────────────┐
│              Scheduler                          │
│  Orchestrates everything                        │
└─────────────────────────────────────────────────┘
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   ┌────────┐  ┌────────┐  ┌─────────┐
   │Triggers│  │JobStore│  │Executors│
   └────────┘  └────────┘  └─────────┘
        │           │           │
        ▼           ▼           ▼
   When to      Where to     How to
   run?         store?       execute?
```

#### A. Scheduler

The main orchestrator that manages jobs, triggers, job stores, and executors.

**Types:**
- `BlockingScheduler` - Single-threaded, blocks the main thread
- `BackgroundScheduler` - Runs in a background thread
- `AsyncIOScheduler` - For asyncio applications
- `GeventScheduler` - For Gevent applications
- `TornadoScheduler` - For Tornado applications
- `TwistedScheduler` - For Twisted applications
- `QtScheduler` - For Qt applications

**Most Common:** `BackgroundScheduler` (for web applications like FastAPI)

#### B. Triggers

Define when jobs should run.

**Types:**
- `DateTrigger` - Run once at a specific date/time
- `IntervalTrigger` - Run periodically at fixed intervals
- `CronTrigger` - Run on cron-like schedules

**Examples:**
```python
# Date: Run once on 2025-12-31 at midnight
trigger = DateTrigger(run_date='2025-12-31 00:00:00')

# Interval: Run every 2 hours
trigger = IntervalTrigger(hours=2)

# Cron: Run daily at 2 AM
trigger = CronTrigger(hour=2, minute=0)
```

#### C. Job Stores

Where job metadata is stored (for persistence across restarts).

**Types:**
- `MemoryJobStore` - In-memory (lost on restart)
- `SQLAlchemyJobStore` - SQL database (PostgreSQL, MySQL, SQLite)
- `MongoDBJobStore` - MongoDB
- `RedisJobStore` - Redis

**Examples:**
```python
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': MemoryJobStore(),
    'persistent': SQLAlchemyJobStore(url='postgresql://...')
}
```

#### D. Executors

How jobs are executed.

**Types:**
- `ThreadPoolExecutor` - Execute jobs in threads (default)
- `ProcessPoolExecutor` - Execute jobs in processes
- `AsyncIOExecutor` - Execute async jobs

**Examples:**
```python
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

executors = {
    'default': ThreadPoolExecutor(max_workers=10),
    'processpool': ProcessPoolExecutor(max_workers=5)
}
```

### 2.2 Jobs

A **job** is a unit of work to be executed.

**Job Attributes:**
- `id` - Unique identifier
- `name` - Human-readable name
- `func` - The function to execute
- `args` - Positional arguments for the function
- `kwargs` - Keyword arguments for the function
- `trigger` - When to run
- `executor` - Which executor to use
- `next_run_time` - When the job will run next

**Job States:**
- Scheduled - Waiting to run
- Running - Currently executing
- Completed - Finished execution
- Missed - Failed to run within misfire grace time

---

## 3. Architecture Overview

### 3.1 How APScheduler Works

```
1. Application Startup
   ↓
2. Create Scheduler with Configuration
   (job stores, executors, defaults)
   ↓
3. Add Jobs with Triggers
   ↓
4. Start Scheduler
   ↓
5. Scheduler Loop:
   ┌─────────────────────────────────┐
   │ a) Check job store for due jobs │
   │ b) Submit job to executor       │
   │ c) Executor runs job function   │
   │ d) Update job's next_run_time   │
   └─────────────────────────────────┘
   ↓
6. Application Shutdown
   ↓
7. Scheduler.shutdown()
   (wait for running jobs)
```

### 3.2 Execution Flow

```
Time: 02:00:00
┌─────────────────────────────────────────┐
│ Scheduler checks job store every 1s     │
│ Finds: job_123 with next_run = 02:00:00 │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ Scheduler submits job_123 to executor   │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ Executor (ThreadPool) runs job_123      │
│ in a background thread                  │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ Job function executes:                  │
│   run_ingestion(ingestion_id="abc-123") │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ Scheduler calculates next_run_time:     │
│   next_run = 02:00:00 + 24h = tomorrow  │
└─────────────────────────────────────────┘
```

---

## 4. Scheduling Methods

### 4.1 Date-Based Scheduling

Run a job **once** at a specific date/time.

```python
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()

# Run once on a specific date
scheduler.add_job(
    func=my_function,
    trigger='date',
    run_date=datetime(2025, 12, 31, 23, 59, 59)
)

scheduler.start()
```

**Use Cases:**
- One-time reminders
- Scheduled maintenance windows
- Deferred tasks (run in 1 hour)

**Example: Run in 1 Hour**
```python
from datetime import datetime, timedelta

scheduler.add_job(
    func=send_reminder,
    trigger='date',
    run_date=datetime.now() + timedelta(hours=1),
    args=['Meeting in 1 hour!']
)
```

### 4.2 Interval-Based Scheduling

Run a job **periodically** at fixed intervals.

```python
# Run every 2 hours
scheduler.add_job(
    func=cleanup_temp_files,
    trigger='interval',
    hours=2
)

# Run every 30 seconds
scheduler.add_job(
    func=check_status,
    trigger='interval',
    seconds=30
)

# Run every 7 days at 3 AM
scheduler.add_job(
    func=weekly_report,
    trigger='interval',
    weeks=1,
    start_date='2025-11-07 03:00:00'
)
```

**Supported Units:**
- `weeks`
- `days`
- `hours`
- `minutes`
- `seconds`

**Use Cases:**
- Periodic health checks
- Regular data synchronization
- Recurring cleanups

**Advanced Interval Example:**
```python
from datetime import datetime

# Run every 6 hours, starting at midnight
scheduler.add_job(
    func=backup_database,
    trigger='interval',
    hours=6,
    start_date=datetime(2025, 11, 7, 0, 0, 0),
    end_date=datetime(2026, 11, 7, 0, 0, 0)  # Stop after 1 year
)
```

### 4.3 Cron-Based Scheduling

Run a job on **cron-like schedules** (most flexible).

```python
# Run daily at 2 AM
scheduler.add_job(
    func=daily_report,
    trigger='cron',
    hour=2,
    minute=0
)

# Run every weekday at 9 AM
scheduler.add_job(
    func=morning_sync,
    trigger='cron',
    day_of_week='mon-fri',
    hour=9,
    minute=0
)

# Run every 15 minutes
scheduler.add_job(
    func=check_queue,
    trigger='cron',
    minute='*/15'
)
```

**Cron Fields:**
- `year` - 4-digit year
- `month` - Month (1-12 or jan-dec)
- `day` - Day of month (1-31)
- `week` - ISO week number (1-53)
- `day_of_week` - Weekday (0-6 or mon-sun, 0=Monday)
- `hour` - Hour (0-23)
- `minute` - Minute (0-59)
- `second` - Second (0-59)

**Cron Expression Examples:**

```python
# Daily at 2:30 AM
trigger='cron', hour=2, minute=30

# Every hour on the hour
trigger='cron', minute=0

# Every 6 hours (0, 6, 12, 18)
trigger='cron', hour='0,6,12,18'

# Every 4 hours (alternative syntax)
trigger='cron', hour='*/4'

# Weekdays at 9 AM
trigger='cron', day_of_week='mon-fri', hour=9

# Last day of month at midnight
trigger='cron', day='last', hour=0

# First Monday of month at 10 AM
trigger='cron', day='1st mon', hour=10
```

**Cron Expression String:**
```python
from apscheduler.triggers.cron import CronTrigger

# Using cron expression string
trigger = CronTrigger.from_crontab('0 2 * * *')  # Daily at 2 AM

scheduler.add_job(
    func=my_function,
    trigger=trigger
)
```

### 4.4 Timezone Handling

APScheduler is **timezone-aware** by default.

```python
import pytz
from apscheduler.triggers.cron import CronTrigger

# Scheduler timezone (affects all jobs unless overridden)
scheduler = BackgroundScheduler(timezone=pytz.UTC)

# Job-specific timezone
ny_tz = pytz.timezone('America/New_York')
trigger = CronTrigger(hour=9, minute=0, timezone=ny_tz)

scheduler.add_job(
    func=morning_report,
    trigger=trigger
)
```

**Example: Schedule at 9 AM User's Local Time**
```python
def schedule_user_reminder(user_timezone: str):
    """Schedule reminder at 9 AM in user's timezone."""
    tz = pytz.timezone(user_timezone)

    scheduler.add_job(
        func=send_reminder,
        trigger='cron',
        hour=9,
        minute=0,
        timezone=tz,
        args=[user.id]
    )

# Schedule for London user
schedule_user_reminder('Europe/London')  # 9 AM GMT/BST

# Schedule for New York user
schedule_user_reminder('America/New_York')  # 9 AM EST/EDT
```

**DST (Daylight Saving Time) Handling:**

APScheduler automatically handles DST transitions:
```python
# Scheduled for 2 AM during DST transition
# APScheduler will:
# - Skip execution if 2 AM doesn't exist (spring forward)
# - Run once during fall back (doesn't run twice)
```

---

## 5. Job Stores

### 5.1 Memory Job Store

**Default, non-persistent** - jobs lost on restart.

```python
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore

jobstores = {
    'default': MemoryJobStore()
}

scheduler = BackgroundScheduler(jobstores=jobstores)
```

**Use When:**
- ✅ Prototyping
- ✅ Jobs are recreated on startup
- ✅ No need for persistence

**Don't Use When:**
- ❌ Jobs must survive restarts
- ❌ Production applications

### 5.2 SQLAlchemy Job Store

**Persistent** - jobs stored in SQL database.

```python
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
}

# PostgreSQL
jobstores = {
    'default': SQLAlchemyJobStore(
        url='postgresql://user:pass@localhost/scheduler'
    )
}

# MySQL
jobstores = {
    'default': SQLAlchemyJobStore(
        url='mysql+pymysql://user:pass@localhost/scheduler'
    )
}

scheduler = BackgroundScheduler(jobstores=jobstores)
scheduler.start()
```

**Table Structure:**
```sql
-- APScheduler creates this table automatically
CREATE TABLE apscheduler_jobs (
    id VARCHAR(191) PRIMARY KEY,
    next_run_time FLOAT,
    job_state BLOB  -- Pickled job configuration
);
```

**Custom Table Name:**
```python
jobstores = {
    'default': SQLAlchemyJobStore(
        url='postgresql://...',
        tablename='my_custom_jobs_table'
    )
}
```

**Use When:**
- ✅ Production applications
- ✅ Jobs must survive restarts
- ✅ Already using SQL database

### 5.3 Redis Job Store

**Persistent** - jobs stored in Redis.

```python
from apscheduler.jobstores.redis import RedisJobStore

jobstores = {
    'default': RedisJobStore(
        host='localhost',
        port=6379,
        db=0
    )
}

scheduler = BackgroundScheduler(jobstores=jobstores)
```

**Use When:**
- ✅ Already using Redis
- ✅ High-performance requirements
- ✅ Multiple scheduler instances (with distributed locking)

### 5.4 Multiple Job Stores

You can use multiple job stores simultaneously:

```python
jobstores = {
    'default': MemoryJobStore(),
    'persistent': SQLAlchemyJobStore(url='postgresql://...'),
    'cache': RedisJobStore(host='localhost')
}

scheduler = BackgroundScheduler(jobstores=jobstores)

# Add job to specific job store
scheduler.add_job(
    func=critical_task,
    trigger='cron',
    hour=2,
    jobstore='persistent'  # Use persistent store
)

scheduler.add_job(
    func=temp_task,
    trigger='interval',
    minutes=5,
    jobstore='default'  # Use memory store
)
```

---

## 6. Executors

### 6.1 Thread Pool Executor

**Default** - Execute jobs in threads.

```python
from apscheduler.executors.pool import ThreadPoolExecutor

executors = {
    'default': ThreadPoolExecutor(max_workers=10)
}

scheduler = BackgroundScheduler(executors=executors)
```

**Pros:**
- ✅ Low overhead
- ✅ Share memory with main process
- ✅ Good for I/O-bound tasks

**Cons:**
- ❌ GIL limits CPU parallelism
- ❌ Not suitable for CPU-bound tasks

**Use When:**
- ✅ I/O-bound jobs (API calls, database queries, file I/O)
- ✅ Jobs that need to share state
- ✅ Most common use case

### 6.2 Process Pool Executor

Execute jobs in **separate processes**.

```python
from apscheduler.executors.pool import ProcessPoolExecutor

executors = {
    'default': ThreadPoolExecutor(max_workers=10),
    'processpool': ProcessPoolExecutor(max_workers=3)
}

scheduler = BackgroundScheduler(executors=executors)

# Use process pool for CPU-bound job
scheduler.add_job(
    func=compute_heavy_task,
    trigger='interval',
    hours=1,
    executor='processpool'
)
```

**Pros:**
- ✅ True parallelism (no GIL)
- ✅ Good for CPU-bound tasks
- ✅ Process isolation (crash doesn't affect others)

**Cons:**
- ❌ Higher overhead
- ❌ Can't share memory
- ❌ Arguments must be picklable

**Use When:**
- ✅ CPU-bound jobs (data processing, ML inference)
- ✅ Jobs that might crash
- ✅ Jobs requiring process isolation

### 6.3 AsyncIO Executor

Execute **async** jobs.

```python
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler

executors = {
    'default': AsyncIOExecutor()
}

scheduler = AsyncIOScheduler(executors=executors)

# Async job function
async def async_task():
    async with httpx.AsyncClient() as client:
        response = await client.get('https://api.example.com')
        return response.json()

scheduler.add_job(async_task, 'interval', minutes=5)
scheduler.start()
```

**Use When:**
- ✅ AsyncIO application
- ✅ Async job functions
- ✅ High concurrency I/O

---

## 7. Practical Examples

### 7.1 Basic Example: Simple Background Scheduler

```python
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time

def my_job():
    print(f"Job executed at {datetime.now()}")

# Create scheduler
scheduler = BackgroundScheduler()

# Add job: run every 3 seconds
scheduler.add_job(my_job, 'interval', seconds=3)

# Start scheduler
scheduler.start()

# Keep the script running
try:
    while True:
        time.sleep(1)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
```

### 7.2 FastAPI Integration

```python
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = None

def scheduled_task():
    """Task that runs on schedule."""
    logger.info("Scheduled task executed")
    # Your business logic here

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""
    global scheduler

    # Startup
    logger.info("Starting scheduler...")

    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
    }

    scheduler = BackgroundScheduler(jobstores=jobstores)

    # Add jobs
    scheduler.add_job(
        scheduled_task,
        'cron',
        hour=2,
        minute=0,
        id='daily_task',
        replace_existing=True
    )

    scheduler.start()
    logger.info("Scheduler started")

    yield

    # Shutdown
    logger.info("Shutting down scheduler...")
    scheduler.shutdown(wait=True)
    logger.info("Scheduler stopped")

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

@app.get("/")
def root():
    return {"message": "Scheduler is running"}

@app.get("/jobs")
def list_jobs():
    """List all scheduled jobs."""
    jobs = scheduler.get_jobs()
    return {
        "total": len(jobs),
        "jobs": [
            {
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None
            }
            for job in jobs
        ]
    }
```

### 7.3 Dynamic Job Management

```python
from apscheduler.schedulers.background import BackgroundScheduler
from uuid import UUID, uuid4

scheduler = BackgroundScheduler()
scheduler.start()

def process_user_data(user_id: str):
    print(f"Processing data for user {user_id}")

class JobManager:
    """Manage scheduled jobs dynamically."""

    @staticmethod
    def add_user_job(user_id: UUID, cron_expression: str):
        """Add a new scheduled job for a user."""
        job_id = f"user_{user_id}"

        scheduler.add_job(
            func=process_user_data,
            trigger='cron',
            args=[str(user_id)],
            id=job_id,
            replace_existing=True,
            **parse_cron_expression(cron_expression)
        )

        return job_id

    @staticmethod
    def remove_user_job(user_id: UUID):
        """Remove a user's scheduled job."""
        job_id = f"user_{user_id}"

        try:
            scheduler.remove_job(job_id)
            return True
        except JobLookupError:
            return False

    @staticmethod
    def update_user_schedule(user_id: UUID, new_cron: str):
        """Update a user's schedule."""
        job_id = f"user_{user_id}"

        try:
            scheduler.reschedule_job(
                job_id,
                trigger='cron',
                **parse_cron_expression(new_cron)
            )
            return True
        except JobLookupError:
            return False

    @staticmethod
    def pause_user_job(user_id: UUID):
        """Pause a user's job (not deleted, just not scheduled)."""
        job_id = f"user_{user_id}"

        try:
            scheduler.pause_job(job_id)
            return True
        except JobLookupError:
            return False

    @staticmethod
    def resume_user_job(user_id: UUID):
        """Resume a paused job."""
        job_id = f"user_{user_id}"

        try:
            scheduler.resume_job(job_id)
            return True
        except JobLookupError:
            return False

# Usage
user_id = uuid4()

# Add job
JobManager.add_user_job(user_id, '0 2 * * *')  # Daily at 2 AM

# Update schedule
JobManager.update_user_schedule(user_id, '0 */6 * * *')  # Every 6 hours

# Pause job
JobManager.pause_user_job(user_id)

# Resume job
JobManager.resume_user_job(user_id)

# Remove job
JobManager.remove_user_job(user_id)
```

### 7.4 Error Handling

```python
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import logging

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()

def job_that_might_fail():
    """Job that may raise exceptions."""
    import random

    if random.random() < 0.5:
        raise Exception("Simulated failure")

    logger.info("Job succeeded")

def job_listener(event):
    """Listen to job execution events."""
    if event.exception:
        logger.error(f"Job {event.job_id} failed: {event.exception}")
    else:
        logger.info(f"Job {event.job_id} succeeded")

# Add event listeners
scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

# Add job
scheduler.add_job(job_that_might_fail, 'interval', seconds=10)

scheduler.start()
```

**Job-Level Error Handling:**
```python
def safe_job_wrapper(func):
    """Decorator to wrap jobs with error handling."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Job {func.__name__} failed: {e}", exc_info=True)
            # Optionally: send alert, update database, etc.
    return wrapper

@safe_job_wrapper
def my_job():
    # Job logic
    pass

scheduler.add_job(my_job, 'interval', hours=1)
```

---

## 8. Advanced Topics

### 8.1 Job Configuration

Control job behavior with configuration options:

```python
scheduler.add_job(
    func=my_function,
    trigger='cron',
    hour=2,

    # Job metadata
    id='my_unique_job_id',
    name='My Descriptive Job Name',

    # Execution control
    max_instances=1,        # Max concurrent instances (prevent overlaps)
    coalesce=True,          # Combine multiple missed runs into one
    misfire_grace_time=300, # 5 minutes grace period for misfires

    # Job store and executor
    jobstore='persistent',
    executor='processpool',

    # Replacement
    replace_existing=True   # Replace job with same ID if exists
)
```

**Configuration Explained:**

- **`max_instances`**: Prevent overlapping runs
  ```python
  # If job takes 2 hours but scheduled every hour:
  max_instances=1  # Skip new run if previous still running
  max_instances=3  # Allow up to 3 concurrent runs
  ```

- **`coalesce`**: Combine missed runs
  ```python
  # If scheduler was down for 5 hours and job runs hourly:
  coalesce=True   # Run once (combine all 5 missed runs)
  coalesce=False  # Run 5 times (catch up)
  ```

- **`misfire_grace_time`**: Grace period for late execution
  ```python
  # Job scheduled for 02:00, but scheduler started at 02:03
  misfire_grace_time=300  # Run if within 5 minutes
  misfire_grace_time=60   # Skip if more than 1 minute late
  ```

### 8.2 Job Defaults

Set default configuration for all jobs:

```python
job_defaults = {
    'coalesce': True,
    'max_instances': 1,
    'misfire_grace_time': 300
}

scheduler = BackgroundScheduler(job_defaults=job_defaults)

# All jobs will inherit these defaults
# Can still be overridden per job
```

### 8.3 Getting Job Information

```python
# Get a specific job
job = scheduler.get_job('my_job_id')

if job:
    print(f"Job ID: {job.id}")
    print(f"Name: {job.name}")
    print(f"Next run: {job.next_run_time}")
    print(f"Trigger: {job.trigger}")

# Get all jobs
jobs = scheduler.get_jobs()
print(f"Total jobs: {len(jobs)}")

# Get jobs from specific job store
jobs = scheduler.get_jobs(jobstore='persistent')
```

### 8.4 Modifying Scheduled Jobs

```python
# Reschedule a job (change trigger)
scheduler.reschedule_job(
    'my_job_id',
    trigger='cron',
    hour=3  # Changed from 2 AM to 3 AM
)

# Modify job (change function or arguments)
scheduler.modify_job(
    'my_job_id',
    args=['new_argument'],
    kwargs={'new_kwarg': 'value'}
)

# Pause a job (not deleted, just not scheduled)
scheduler.pause_job('my_job_id')

# Resume a paused job
scheduler.resume_job('my_job_id')

# Remove a job
scheduler.remove_job('my_job_id')

# Remove all jobs
scheduler.remove_all_jobs()
```

### 8.5 Event Listeners

Listen to scheduler events:

```python
from apscheduler.events import (
    EVENT_JOB_ADDED,
    EVENT_JOB_REMOVED,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_MISSED
)

def job_added_listener(event):
    print(f"Job {event.job_id} was added")

def job_executed_listener(event):
    print(f"Job {event.job_id} executed successfully")

def job_error_listener(event):
    print(f"Job {event.job_id} failed: {event.exception}")

def job_missed_listener(event):
    print(f"Job {event.job_id} missed its scheduled time")

# Add listeners
scheduler.add_listener(job_added_listener, EVENT_JOB_ADDED)
scheduler.add_listener(job_executed_listener, EVENT_JOB_EXECUTED)
scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
scheduler.add_listener(job_missed_listener, EVENT_JOB_MISSED)
```

**Practical Use Cases:**
- Logging job execution
- Sending alerts on failures
- Updating database status
- Triggering dependent workflows

### 8.6 Logging Configuration

```python
import logging

# Configure APScheduler logging
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

# Or more detailed
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        }
    },
    'loggers': {
        'apscheduler': {
            'handlers': ['console'],
            'level': 'INFO'
        }
    }
})
```

---

## 9. Best Practices

### 9.1 Job Design

**Keep Jobs Idempotent:**
```python
def process_daily_data(date):
    """Idempotent job - safe to run multiple times."""

    # Check if already processed
    if is_already_processed(date):
        logger.info(f"Data for {date} already processed, skipping")
        return

    # Process data
    data = fetch_data(date)
    process_data(data)

    # Mark as processed
    mark_as_processed(date)
```

**Keep Jobs Small and Focused:**
```python
# BAD: One large job doing everything
def monolithic_job():
    fetch_data()
    clean_data()
    transform_data()
    load_data()
    send_report()

# GOOD: Separate jobs for each step
scheduler.add_job(fetch_data, 'cron', hour=1)
scheduler.add_job(clean_data, 'cron', hour=2)
scheduler.add_job(transform_data, 'cron', hour=3)
scheduler.add_job(load_data, 'cron', hour=4)
scheduler.add_job(send_report, 'cron', hour=5)
```

**Handle Long-Running Jobs:**
```python
def long_running_job():
    """Job that might run longer than schedule interval."""

    # Use max_instances to prevent overlaps
    pass

scheduler.add_job(
    long_running_job,
    'interval',
    hours=1,
    max_instances=1  # Don't start new run if previous still running
)
```

### 9.2 Error Handling

**Always Wrap Jobs in Try-Except:**
```python
def robust_job():
    try:
        # Job logic
        result = do_work()
        logger.info(f"Job succeeded: {result}")
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        # Don't re-raise - let scheduler continue
```

**Implement Retry Logic:**
```python
def job_with_retry():
    max_retries = 3

    for attempt in range(max_retries):
        try:
            do_work()
            logger.info("Job succeeded")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Job failed after {max_retries} attempts: {e}")
            else:
                logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
```

### 9.3 Resource Management

**Clean Up Resources:**
```python
def job_with_resources():
    db_connection = None
    file_handle = None

    try:
        db_connection = get_database_connection()
        file_handle = open('/tmp/data.csv', 'w')

        # Do work
        process_data(db_connection, file_handle)

    finally:
        if db_connection:
            db_connection.close()
        if file_handle:
            file_handle.close()
```

**Use Context Managers:**
```python
def job_with_context_managers():
    with get_database_connection() as db:
        with open('/tmp/data.csv', 'w') as f:
            process_data(db, f)
    # Resources automatically cleaned up
```

### 9.4 Testing

**Test Job Functions Separately:**
```python
def my_job_function(arg1, arg2):
    """Job function that can be tested independently."""
    # Business logic
    return result

# Test without scheduler
def test_my_job_function():
    result = my_job_function('test', 'args')
    assert result == expected_result

# Use in scheduler
scheduler.add_job(my_job_function, 'cron', hour=2, args=['value1', 'value2'])
```

**Test Scheduling Logic:**
```python
import pytest
from datetime import datetime, timedelta
from apscheduler.triggers.cron import CronTrigger

def test_cron_trigger():
    """Test cron trigger generates correct run times."""
    trigger = CronTrigger(hour=2, minute=0)

    # Get next 3 run times
    now = datetime(2025, 11, 7, 0, 0, 0)
    runs = [trigger.get_next_fire_time(None, now)]

    for _ in range(2):
        runs.append(trigger.get_next_fire_time(runs[-1], runs[-1]))

    # Verify all at 2 AM
    for run in runs:
        assert run.hour == 2
        assert run.minute == 0
```

---

## 10. Common Pitfalls

### 10.1 Forgetting to Start the Scheduler

```python
# WRONG: Scheduler created but not started
scheduler = BackgroundScheduler()
scheduler.add_job(my_function, 'interval', seconds=10)
# Jobs won't run!

# CORRECT: Start the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(my_function, 'interval', seconds=10)
scheduler.start()  # Don't forget this!
```

### 10.2 Job Function Not Picklable

```python
# WRONG: Lambda functions can't be pickled
scheduler.add_job(
    lambda: print("Hello"),  # Will fail with ProcessPoolExecutor
    'interval',
    seconds=10
)

# CORRECT: Use named function
def print_hello():
    print("Hello")

scheduler.add_job(print_hello, 'interval', seconds=10)
```

### 10.3 Overlapping Jobs

```python
# PROBLEM: Long-running job causes overlaps
def slow_job():
    time.sleep(120)  # Takes 2 minutes

scheduler.add_job(slow_job, 'interval', seconds=60)
# Jobs will stack up!

# SOLUTION: Use max_instances
scheduler.add_job(
    slow_job,
    'interval',
    seconds=60,
    max_instances=1  # Skip if previous still running
)
```

### 10.4 Timezone Confusion

```python
# WRONG: Naive datetime (no timezone)
scheduler.add_job(
    my_function,
    'date',
    run_date=datetime(2025, 12, 31, 23, 59, 59)  # Ambiguous!
)

# CORRECT: Timezone-aware datetime
import pytz
scheduler.add_job(
    my_function,
    'date',
    run_date=datetime(2025, 12, 31, 23, 59, 59, tzinfo=pytz.UTC)
)
```

### 10.5 Not Handling Shutdown Gracefully

```python
# WRONG: Jobs interrupted on shutdown
scheduler = BackgroundScheduler()
scheduler.start()
# ... application runs ...
scheduler.shutdown()  # Kills running jobs!

# CORRECT: Wait for jobs to complete
scheduler.shutdown(wait=True)  # Waits for running jobs

# Even better: Use signal handlers
import signal

def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    scheduler.shutdown(wait=True)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
```

### 10.6 Memory Leaks with MemoryJobStore

```python
# PROBLEM: Jobs accumulate in memory
for i in range(1000):
    scheduler.add_job(
        my_function,
        'date',
        run_date=datetime.now() + timedelta(hours=i),
        id=f'job_{i}'
    )
# 1000 jobs in memory forever (even after execution)

# SOLUTION: Use SQLAlchemyJobStore for persistence
# or remove completed jobs
```

---

## 11. Production Considerations

### 11.1 Monitoring

**Track Key Metrics:**
```python
from prometheus_client import Gauge, Counter, Histogram

# Number of scheduled jobs
scheduled_jobs = Gauge('scheduler_jobs_total', 'Total scheduled jobs')

# Job execution count
job_executions = Counter(
    'scheduler_executions_total',
    'Total job executions',
    ['job_id', 'status']
)

# Job execution duration
job_duration = Histogram(
    'scheduler_execution_seconds',
    'Job execution duration',
    ['job_id']
)

def instrumented_job():
    start_time = time.time()

    try:
        do_work()
        job_executions.labels(job_id='my_job', status='success').inc()
    except Exception as e:
        job_executions.labels(job_id='my_job', status='failure').inc()
        raise
    finally:
        duration = time.time() - start_time
        job_duration.labels(job_id='my_job').observe(duration)

# Update job count periodically
def update_metrics():
    scheduled_jobs.set(len(scheduler.get_jobs()))

scheduler.add_job(update_metrics, 'interval', minutes=1)
```

### 11.2 Health Checks

```python
from fastapi import FastAPI, HTTPException

@app.get("/health/scheduler")
def scheduler_health():
    """Check if scheduler is running."""

    if not scheduler.running:
        raise HTTPException(status_code=503, detail="Scheduler not running")

    jobs = scheduler.get_jobs()

    return {
        "status": "healthy",
        "running": scheduler.running,
        "total_jobs": len(jobs),
        "jobstores": list(scheduler._jobstores.keys()),
        "executors": list(scheduler._executors.keys())
    }
```

### 11.3 Graceful Degradation

**Handle Job Store Failures:**
```python
try:
    scheduler = BackgroundScheduler(
        jobstores={'default': SQLAlchemyJobStore(url=database_url)}
    )
    scheduler.start()
except Exception as e:
    logger.error(f"Failed to start scheduler with persistent store: {e}")

    # Fall back to memory store
    scheduler = BackgroundScheduler(
        jobstores={'default': MemoryJobStore()}
    )
    scheduler.start()
    logger.warning("Scheduler started with memory store (jobs won't persist)")
```

### 11.4 Configuration Management

```python
from pydantic import BaseSettings

class SchedulerSettings(BaseSettings):
    """Scheduler configuration."""

    # General
    SCHEDULER_ENABLED: bool = True
    SCHEDULER_TIMEZONE: str = "UTC"

    # Job store
    SCHEDULER_JOBSTORE_URL: str = "sqlite:///jobs.sqlite"
    SCHEDULER_JOBSTORE_TABLENAME: str = "apscheduler_jobs"

    # Executors
    SCHEDULER_MAX_WORKERS: int = 10
    SCHEDULER_PROCESS_WORKERS: int = 3

    # Job defaults
    SCHEDULER_COALESCE: bool = True
    SCHEDULER_MAX_INSTANCES: int = 1
    SCHEDULER_MISFIRE_GRACE_TIME: int = 300

    class Config:
        env_file = ".env"

settings = SchedulerSettings()

# Build scheduler from settings
jobstores = {
    'default': SQLAlchemyJobStore(
        url=settings.SCHEDULER_JOBSTORE_URL,
        tablename=settings.SCHEDULER_JOBSTORE_TABLENAME
    )
}

executors = {
    'default': ThreadPoolExecutor(max_workers=settings.SCHEDULER_MAX_WORKERS),
    'processpool': ProcessPoolExecutor(max_workers=settings.SCHEDULER_PROCESS_WORKERS)
}

job_defaults = {
    'coalesce': settings.SCHEDULER_COALESCE,
    'max_instances': settings.SCHEDULER_MAX_INSTANCES,
    'misfire_grace_time': settings.SCHEDULER_MISFIRE_GRACE_TIME
}

scheduler = BackgroundScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults,
    timezone=pytz.timezone(settings.SCHEDULER_TIMEZONE)
)
```

### 11.5 Scaling Considerations

**Single Instance Limits:**
- Max ~1000 scheduled jobs
- Max ~10-20 concurrent executions (thread pool)
- Memory: 2-8 GB
- Single point of failure

**When to Scale Horizontally:**
- See `scheduler-scaling-and-risks.md` for detailed analysis
- TL;DR: Use IOMETE Jobs or Kubernetes CronJobs instead of distributed APScheduler

---

## 12. References

### 12.1 Official Documentation

- **APScheduler Docs:** https://apscheduler.readthedocs.io/
- **GitHub:** https://github.com/agronholm/apscheduler
- **PyPI:** https://pypi.org/project/APScheduler/

### 12.2 Related Resources

- **Cron Expression Reference:** https://crontab.guru/
- **Python Timezone Database:** https://pypi.org/project/pytz/
- **IANA Timezone List:** https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

### 12.3 Project-Specific Documents

- `scheduler-implementation-guide.md` - How we use APScheduler in IOMETE Autoloader
- `scheduler-scaling-and-risks.md` - Production risks and scaling analysis
- `apscheduler-horizontal-scaling.md` - Distributed APScheduler (not recommended)

---

## Summary

APScheduler is a powerful, flexible library for scheduling Python code execution. It's ideal for:

- **Simple Use Cases:** < 100 scheduled jobs, single instance
- **Python-Native:** No external dependencies (beyond optional job stores)
- **Production-Ready:** With proper configuration and monitoring

**Key Takeaways:**

1. **Choose the Right Scheduler Type:** `BackgroundScheduler` for most web apps
2. **Use Persistent Job Store:** `SQLAlchemyJobStore` for production
3. **Configure Job Defaults:** `max_instances=1`, `coalesce=True`
4. **Handle Errors Gracefully:** Try-except in all jobs
5. **Monitor and Alert:** Track job failures and execution times
6. **Know Your Limits:** Single instance good for <100 jobs, then consider alternatives

**When NOT to Use APScheduler:**

- Need horizontal scaling (>500 jobs)
- Complex workflow dependencies (use Airflow)
- Distributed task execution (use Celery)
- Already have orchestration platform (use IOMETE Jobs, K8s CronJobs)

For IOMETE Autoloader specifically, we use APScheduler for MVP (<100 ingestions) and plan migration to IOMETE Jobs for scale.

---

**End of Educational Guide**
