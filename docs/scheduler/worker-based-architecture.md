# Worker-Based Architecture for Horizontal Scaling

**Document Version:** 2.0
**Created:** 2025-11-07
**Updated:** 2025-11-07
**Status:** Technical Analysis
**Related:** `apscheduler-horizontal-scaling.md`, `scheduler-scaling-and-risks.md`

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Core Concept](#core-concept)
4. [Detailed Architecture](#detailed-architecture)
5. [Implementation Design](#implementation-design)
6. [Database Schema](#database-schema)
7. [Component Implementation](#component-implementation)
8. [Code Examples](#code-examples)
9. [Comparison with Alternatives](#comparison-with-alternatives)
10. [Failure Scenarios](#failure-scenarios)
11. [Operational Considerations](#operational-considerations)
12. [Cost-Benefit Analysis](#cost-benefit-analysis)
13. [Migration Path](#migration-path)
14. [Recommendation](#recommendation)

---

## 1. Executive Summary

### 1.1 The Idea

**Decoupled Server-Worker Architecture:**

```
┌────────────────────────────────────────────────┐
│           Server (Singleton)                   │
│  - FastAPI (Job Distribution API)              │
│  - APScheduler (Creates jobs on schedule)      │
│  - Job Queue Manager (DB access layer)         │
│  - Worker API (/jobs/claim, /jobs/complete)    │
└────────────────────────────────────────────────┘
                     │
                     │ (Owns DB)
                     ▼
         ┌───────────────────────┐
         │   Job Queue (DB)      │
         │   - Pending jobs      │
         │   - Status tracking   │
         └───────────────────────┘
                     │
                     │ HTTP API
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Worker 1    │ │  Worker 2    │ │  Worker N    │
│  (20 threads)│ │  (20 threads)│ │  (20 threads)│
│              │ │              │ │              │
│  Poll API    │ │  Poll API    │ │  Poll API    │
│  Execute 20× │ │  Execute 20× │ │  Execute 20× │
│  Report API  │ │  Report API  │ │  Report API  │
└──────────────┘ └──────────────┘ └──────────────┘

Scaling: 1,000 concurrent jobs = 50 workers × 20 threads
```

### 1.2 Key Benefits

| Aspect                   | Value                                              |
| ------------------------ | -------------------------------------------------- |
| **Complexity**           | 🟢 LOW - No distributed coordination needed        |
| **Scalability**          | 🟢 EXCELLENT - Add workers as needed               |
| **Implementation Time**  | 🟢 1 week (vs 3 weeks for distributed APScheduler) |
| **Operational Overhead** | 🟢 LOW - No Redis/ZooKeeper needed                 |
| **Debugging**            | 🟢 EASY - Clear separation of concerns             |
| **Cost**                 | 🟢 LOW - Database-only coordination                |

### 1.3 How It Works

**Server (Singleton):**
- Single instance (can have standby for HA)
- Runs APScheduler to create jobs on schedule
- Inserts pending jobs into database queue
- **Exposes Worker API** (HTTP endpoints for job distribution)
- Manages job lifecycle (claims, completion, failures)
- Provides UI and monitoring dashboards
- Detects stale jobs and triggers retries

**Workers (Multi-threaded, Horizontally Scalable):**
- **Pure HTTP clients** - No direct database access
- Each worker runs 10-50 concurrent threads (configurable)
- Poll server API: `GET /api/v1/jobs/claim?count=5`
- Execute ingestions in thread pool
- Report status via API: `POST /api/v1/jobs/{id}/complete`
- **Stateless** - Can restart anytime, no local persistence

**Database:**
- Single source of truth
- Owned exclusively by server (workers never touch it)
- Job queue (pending, in_progress, completed)
- Built-in locking via `SELECT FOR UPDATE SKIP LOCKED`
- No external coordination service needed

**Concurrency Model:**
- 1 worker × 20 threads = 20 concurrent jobs
- 10 workers × 20 threads = 200 concurrent jobs
- 50 workers × 20 threads = 1,000 concurrent jobs
- **Much more efficient than 1 job per pod!**

### 1.4 Why This is Better Than Distributed APScheduler

| Feature                | Worker-Based          | Distributed APScheduler             |
| ---------------------- | --------------------- | ----------------------------------- |
| **Coordination**       | Database (built-in)   | Redis/ZooKeeper (new service)       |
| **Complexity**         | Simple                | High (distributed systems)          |
| **Failure Modes**      | Few, well-understood  | Many (split-brain, lock contention) |
| **Horizontal Scaling** | Trivial (add workers) | Complex (rebalancing)               |
| **Development Time**   | 1 week                | 3 weeks                             |
| **Learning Curve**     | Low                   | High (distributed systems)          |
| **Debugging**          | Easy (DB queries)     | Hard (distributed tracing)          |

---

## 2. Architecture Overview

### 2.1 Component Roles

```
┌─────────────────────────────────────────────────────────────────┐
│                     SERVER (Singleton)                          │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐            │
│  │ FastAPI     │  │ APScheduler │  │ Job Queue    │            │
│  │ - UI APIs   │  │ - Cron      │  │ Manager      │            │
│  │ - Worker    │  │ - Triggers  │  │ - Enqueue    │            │
│  │   APIs      │  │ - Enqueue   │  │ - Claim      │            │
│  └─────────────┘  └─────────────┘  └──────────────┘            │
│         │                                    │                   │
│         │  Worker API Endpoints:             │                   │
│         │  - POST /jobs/claim?count=N        │                   │
│         │  - POST /jobs/{id}/heartbeat       │                   │
│         │  - POST /jobs/{id}/complete        │                   │
│         │  - POST /jobs/{id}/fail            │                   │
│         │                                    │                   │
└─────────┼────────────────────────────────────┼───────────────────┘
          │                                    │
          │ HTTP                               ▼ (Exclusive DB access)
          │                      ┌─────────────────────────┐
          │                      │   PostgreSQL Database   │
          │                      │  ┌──────────────────┐   │
          │                      │  │  job_queue       │   │
          │                      │  │  - status        │   │
          │                      │  │  - priority      │   │
          │                      │  └──────────────────┘   │
          │                      └─────────────────────────┘
          │
          │ (Workers call server API)
          │
   ┌──────┴──────┬──────────────┬──────────────┐
   │             │              │              │
   ▼             ▼              ▼              ▼
┌────────┐   ┌────────┐    ┌────────┐    ┌────────┐
│Worker 1│   │Worker 2│    │Worker 3│    │Worker N│
│        │   │        │    │        │    │        │
│Thread  │   │Thread  │    │Thread  │    │Thread  │
│ Pool   │   │ Pool   │    │ Pool   │    │ Pool   │
│(20)    │   │(20)    │    │(20)    │    │(20)    │
│        │   │        │    │        │    │        │
│[====]  │   │[====]  │    │[====]  │    │[====]  │
│[====]  │   │[====]  │    │[====]  │    │[====]  │
│[====]  │   │[====]  │    │[====]  │    │[====]  │
└────────┘   └────────┘    └────────┘    └────────┘

Flow:
1. Worker calls POST /jobs/claim?count=5
2. Server uses SELECT FOR UPDATE SKIP LOCKED
3. Server returns 5 jobs to worker
4. Worker executes in thread pool
5. Worker calls POST /jobs/{id}/complete for each
```

### 2.2 Sequence Diagram

```
Server API      Database        Worker 1 (20 threads)    Worker 2 (20 threads)
  │                 │                    │                        │
  │ (02:00 cron)    │                    │                        │
  │ INSERT 100 jobs │                    │                        │
  ├────────────────>│                    │                        │
  │                 │                    │                        │
  │                 │  POST /jobs/claim?count=10                  │
  │<─────────────────────────────────────┤                        │
  │                 │                    │                        │
  │ SELECT 10 jobs  │                    │                        │
  │ FOR UPDATE      │                    │                        │
  │ SKIP LOCKED     │                    │                        │
  ├────────────────>│                    │                        │
  │                 │                    │                        │
  │ Return [Job1..Job10]                 │                        │
  ├─────────────────────────────────────>│                        │
  │                 │                    │                        │
  │                 │           POST /jobs/claim?count=10         │
  │<───────────────────────────────────────────────────────────────┤
  │                 │                    │                        │
  │ SELECT 10 jobs  │                    │                        │
  │ (SKIP LOCKED    │                    │                        │
  │  skips Job1-10) │                    │                        │
  ├────────────────>│                    │                        │
  │                 │                    │                        │
  │ Return [Job11..Job20]                │                        │
  ├───────────────────────────────────────────────────────────────>│
  │                 │                    │                        │
  │                 │    Execute 10 jobs in thread pool           │
  │                 │    (Thread 1: Job1, Thread 2: Job2, ...)    │
  │                 │                    │                        │
  │                 │  POST /jobs/1/complete                      │
  │<─────────────────────────────────────┤                        │
  │ UPDATE job      │                    │                        │
  │ status=DONE     │                    │                        │
  ├────────────────>│                    │                        │
  │                 │                    │                        │
  │ 200 OK          │                    │                        │
  ├─────────────────────────────────────>│                        │
  │                 │                    │                        │
  │                 │   (Worker 1 continues completing Job2-10)   │
  │                 │   (Worker 2 executes Job11-20 in parallel)  │
```

**Key Points:**
- Workers call server API (not database)
- Server owns all database access (job claims via SELECT FOR UPDATE SKIP LOCKED)
- Workers handle multiple jobs concurrently via thread pool
- No duplicate claims (SKIP LOCKED ensures isolation)

### 2.3 Benefits

**Simplicity:**
- No distributed coordination logic
- Database provides all synchronization
- Well-understood failure modes

**Scalability:**
- Add workers independently
- No rebalancing logic needed
- Workers auto-discover work

**Reliability:**
- Database ACID guarantees
- Built-in pessimistic locking
- Dead worker detection via timeout

**Operational:**
- No new infrastructure (Redis/ZooKeeper)
- Standard database monitoring
- Easy to debug (SQL queries)

---

## 3. Core Concept

### 3.1 Job Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                     Job Lifecycle                            │
└─────────────────────────────────────────────────────────────┘

1. CREATED (Server)
   - APScheduler triggers at scheduled time
   - Server creates job record in DB with status=PENDING
   - Job has: ingestion_id, scheduled_time, status, priority

2. CLAIMED (Worker)
   - Worker polls DB: SELECT ... FOR UPDATE SKIP LOCKED
   - Worker updates: claimed_by=worker_id, status=IN_PROGRESS
   - Database lock prevents other workers from claiming same job

3. EXECUTING (Worker)
   - Worker executes ingestion (Spark job)
   - Worker sends heartbeat updates (still_alive_at timestamp)
   - If worker dies, job becomes stale (timeout detection)

4. COMPLETED (Worker)
   - Worker updates: status=COMPLETED, finished_at=NOW()
   - Or status=FAILED, error_message=... if failed

5. CLEANUP (Server)
   - Server periodically cleans old jobs (>30 days)
   - Server detects stale jobs (heartbeat timeout)
   - Server re-enqueues failed jobs (if retry policy allows)
```

### 3.2 Worker Poll Loop (Multi-threaded)

```python
# Multi-threaded worker logic

from concurrent.futures import ThreadPoolExecutor
import requests

class Worker:
    def __init__(self, worker_id: str, server_url: str, max_threads: int = 20):
        self.worker_id = worker_id
        self.server_url = server_url
        self.max_threads = max_threads
        self.executor = ThreadPoolExecutor(max_workers=max_threads)
        self.active_jobs = {}  # job_id -> Future

    def main_loop(self):
        """Main polling loop"""
        while True:
            # Check available capacity
            available = self.max_threads - len(self.active_jobs)

            if available > 0:
                # Claim multiple jobs at once
                jobs = self.claim_jobs(count=min(available, 10))

                for job in jobs:
                    # Execute in thread pool
                    future = self.executor.submit(self.execute_job, job)
                    self.active_jobs[job['id']] = future

            # Clean up completed jobs
            self.cleanup_completed()

            # Brief pause
            time.sleep(1)

    def claim_jobs(self, count: int) -> list:
        """Claim multiple jobs from server"""
        response = requests.post(
            f"{self.server_url}/api/v1/jobs/claim",
            params={"count": count, "worker_id": self.worker_id}
        )
        if response.status_code == 200:
            return response.json()['jobs']
        return []

    def execute_job(self, job: dict):
        """Execute job (runs in thread pool)"""
        try:
            # Execute ingestion
            run_ingestion(job['ingestion_id'])

            # Report completion
            requests.post(
                f"{self.server_url}/api/v1/jobs/{job['id']}/complete",
                json={"files_processed": 10, "records": 1000}
            )
        except Exception as e:
            # Report failure
            requests.post(
                f"{self.server_url}/api/v1/jobs/{job['id']}/fail",
                json={"error": str(e)}
            )

    def cleanup_completed(self):
        """Remove completed futures"""
        completed = [
            job_id for job_id, future in self.active_jobs.items()
            if future.done()
        ]
        for job_id in completed:
            del self.active_jobs[job_id]
```

**Key Benefits:**
- **Efficient concurrency**: 1 worker handles 20 jobs simultaneously
- **HTTP-based**: No database credentials needed in workers
- **Batch claiming**: Request multiple jobs per API call
- **Automatic cleanup**: Thread pool manages lifecycle

### 3.3 Server-Side Locking (Hidden from Workers)

**Server API Endpoint: POST /jobs/claim**

Workers call server API, server handles database locking internally:

```python
# Server-side job claim logic

@router.post("/jobs/claim")
def claim_jobs(count: int, worker_id: str, db: Session = Depends(get_db)):
    """
    Claim jobs for worker (server handles DB locking).

    Workers never touch database directly.
    """
    claimed_jobs = []

    # Use PostgreSQL's SELECT FOR UPDATE SKIP LOCKED
    for _ in range(count):
        result = db.execute("""
            SELECT id, ingestion_id, priority
            FROM job_queue
            WHERE status = 'PENDING'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED  -- Magic: atomic lock
        """).fetchone()

        if result is None:
            break  # No more jobs

        # Claim job
        db.execute("""
            UPDATE job_queue
            SET status = 'IN_PROGRESS',
                claimed_by = :worker_id,
                claimed_at = NOW(),
                heartbeat_at = NOW()
            WHERE id = :job_id
        """, {'worker_id': worker_id, 'job_id': result.id})

        claimed_jobs.append(result)

    db.commit()
    return {"jobs": claimed_jobs}
```

**How `SKIP LOCKED` Prevents Conflicts:**
- Request 1 (Worker 1): Locks Job A → Returns Job A
- Request 2 (Worker 2, simultaneous): **Skips** locked Job A → Returns Job B
- Request 3 (Worker 3, simultaneous): **Skips** Job A & B → Returns Job C
- **Result: Zero conflicts, perfect distribution**

**Why This Architecture is Better:**
- ✅ **Workers simpler**: Pure HTTP clients, no DB logic
- ✅ **Security**: Workers don't need database credentials
- ✅ **Flexibility**: Easy to change DB schema without updating workers
- ✅ **Tenant isolation**: Server enforces multi-tenancy rules
- ✅ **Built into PostgreSQL 9.5+**: No new dependencies

### 3.4 Scaling Math: Why Multi-threading Matters

**Scenario: 1,000 Concurrent Ingestions**

**❌ BAD: One Job Per Worker (Single-threaded)**
```
1,000 jobs ÷ 1 job/worker = 1,000 workers needed
1,000 Kubernetes pods × 512MB RAM = 512 GB RAM
1,000 pods × 0.5 CPU = 500 CPU cores

Cost: ~$5,000/month
Overhead: Massive (pod startup, networking, etc.)
```

**✅ GOOD: Multi-threaded Workers**
```
1,000 jobs ÷ 20 jobs/worker = 50 workers needed
50 Kubernetes pods × 2GB RAM = 100 GB RAM
50 pods × 2 CPU = 100 CPU cores

Cost: ~$500/month
Overhead: Minimal
```

**Savings: 90% reduction in infrastructure cost!**

**Realistic Scaling Tiers:**

| Concurrent Jobs | Workers Needed | Total Pods | Monthly Cost |
|-----------------|----------------|------------|--------------|
| 100 | 5 × 20 threads | 5 | ~$50 |
| 500 | 25 × 20 threads | 25 | ~$250 |
| 1,000 | 50 × 20 threads | 50 | ~$500 |
| 5,000 | 250 × 20 threads | 250 | ~$2,500 |
| 10,000 | 500 × 20 threads | 500 | ~$5,000 |

**Configuration:**
- Threads per worker: 10-50 (configurable via `WORKER_THREADS`)
- Start conservative (20), tune based on:
  - Job duration (longer jobs → fewer threads)
  - Memory usage (heavy jobs → fewer threads)
  - CPU usage (CPU-bound → match core count)

---

## 4. Detailed Architecture

### 4.1 Server Component

**Responsibilities:**

1. **User-Facing API (FastAPI)**
   - CRUD operations for ingestions
   - Run history queries
   - Manual triggers ("Run Now" button)
   - Cluster status dashboard

2. **Worker-Facing API (FastAPI)**
   - `POST /api/v1/jobs/claim?count=N` - Claim jobs for execution
   - `POST /api/v1/jobs/{id}/heartbeat` - Worker heartbeat
   - `POST /api/v1/jobs/{id}/complete` - Mark job complete
   - `POST /api/v1/jobs/{id}/fail` - Mark job failed
   - `POST /api/v1/workers/register` - Worker registration

3. **Job Scheduler (APScheduler - Single Instance)**
   - Maintains cron schedules for active ingestions
   - Creates job queue entries when triggered
   - Does NOT execute jobs (just enqueues)

4. **Job Queue Manager**
   - Enqueues jobs into `job_queue` table (on cron trigger)
   - Claims jobs for workers (SELECT FOR UPDATE SKIP LOCKED)
   - Monitors queue health
   - Detects stale jobs (heartbeat timeout)
   - Re-enqueues failed jobs (retry logic)

5. **Worker Health Monitor**
   - Tracks active workers (heartbeat table)
   - Detects dead workers
   - Provides metrics (Prometheus)
   - Auto-scaling recommendations

**Key Points:**
- Server owns ALL database access
- Workers are pure HTTP clients
- Server does NOT execute ingestions (workers do)

### 4.2 Worker Component (Multi-threaded)

**Architecture:**
```
Worker Pod
├── Main Thread (polling loop)
├── Thread Pool Executor (10-50 threads)
│   ├── Thread 1: Executing Job A
│   ├── Thread 2: Executing Job B
│   ├── Thread 3: Idle
│   └── ...
└── HTTP Client (requests to server API)
```

**Responsibilities:**

1. **Job Poller (Main Thread)**
   - Continuously polls server API: `POST /jobs/claim?count=N`
   - Calculates available capacity: `max_threads - active_jobs`
   - Requests multiple jobs per API call (batch claiming)
   - **Never touches database directly**

2. **Job Executor (Thread Pool)**
   - Each thread executes one ingestion at a time
   - Uses existing `BatchOrchestrator`
   - Sends periodic heartbeats: `POST /jobs/{id}/heartbeat`
   - Reports completion: `POST /jobs/{id}/complete`

3. **Status Reporter**
   - Reports success with metrics (files processed, duration, etc.)
   - Reports failures with error details
   - All communication via server API (no DB access)

4. **Capacity Manager**
   - Tracks active jobs (job_id → Future mapping)
   - Cleans up completed futures
   - Reports utilization to server on heartbeat

**Configuration:**
```python
# Environment variables
WORKER_ID = "worker-abc123"      # Unique ID
SERVER_URL = "http://server:8000"  # Server API URL
WORKER_THREADS = 20              # Concurrency
POLL_INTERVAL = 1                # Seconds between polls
```

**Key Points:**
- **Stateless**: No local database, no file storage
- **Pure HTTP client**: Only talks to server API
- **Multi-threaded**: Handles 10-50 jobs concurrently
- **Can restart anytime**: Stale jobs auto-retried by server

### 4.3 Database Tables

**Existing Tables:**
- `ingestions` - Ingestion configurations
- `runs` - Execution history (one per completed job)
- `processed_files` - File tracking

**New Table:**
- `job_queue` - Pending and in-progress jobs
- `worker_heartbeats` - Worker health tracking

---

## 5. Implementation Design

### 5.1 Technology Stack

**No New Dependencies:**
- ✅ PostgreSQL (existing)
- ✅ FastAPI (existing)
- ✅ APScheduler (existing)
- ✅ SQLAlchemy (existing)

**What We DON'T Need:**
- ❌ Redis
- ❌ ZooKeeper
- ❌ Message Queue (RabbitMQ, Kafka)
- ❌ Distributed lock library

### 5.2 Deployment Model

**Option A: Separate Processes**

```yaml
# docker-compose.yml

services:
  # Server (singleton)
  autoloader-server:
    image: autoloader:latest
    command: python -m app.main --mode=server
    ports:
      - "8000:8000"
    environment:
      MODE: server
    deploy:
      replicas: 1  # Only one server

  # Workers (horizontally scalable)
  autoloader-worker:
    image: autoloader:latest
    command: python -m app.worker --mode=worker
    environment:
      MODE: worker
      WORKER_ID: ${HOSTNAME}
    deploy:
      replicas: 3  # Scale as needed
```

**Option B: Kubernetes**

```yaml
# server-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: autoloader-server
spec:
  replicas: 1  # Singleton (use 2 with leader election for HA)
  template:
    spec:
      containers:
      - name: server
        image: autoloader:latest
        args: ["--mode=server"]
---
# worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: autoloader-worker
spec:
  replicas: 3  # Horizontal scaling
  template:
    spec:
      containers:
      - name: worker
        image: autoloader:latest
        args: ["--mode=worker"]
```

**Scaling:**
```bash
# Add more workers
kubectl scale deployment autoloader-worker --replicas=10

# Or auto-scale based on queue depth
kubectl autoscale deployment autoloader-worker \
  --min=3 --max=20 \
  --cpu-percent=70
```

### 5.3 High Availability

**Server HA (Optional):**

If server must be HA, use simple leader election:

```python
# Leader election via database (simpler than Redis)

CREATE TABLE server_leader (
    id INT PRIMARY KEY DEFAULT 1,
    server_id TEXT NOT NULL,
    lease_expires_at TIMESTAMP NOT NULL,
    CONSTRAINT single_leader CHECK (id = 1)
);

# Server tries to become leader
def try_become_leader(server_id: str):
    now = datetime.utcnow()
    lease_duration = timedelta(seconds=30)

    # Try to insert (only works if no leader)
    result = db.execute("""
        INSERT INTO server_leader (id, server_id, lease_expires_at)
        VALUES (1, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET server_id = %s, lease_expires_at = %s
        WHERE server_leader.lease_expires_at < %s
        RETURNING server_id
    """, (server_id, now + lease_duration,
          server_id, now + lease_duration, now))

    return result is not None and result[0] == server_id
```

**Worker HA:**
- No special HA needed
- Workers are stateless
- If worker dies, job becomes stale and gets re-enqueued

### 5.4 Server Worker API Specification

Complete API contract between server and workers:

```python
# app/api/v1/worker_api.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/jobs", tags=["worker-api"])

# Request/Response schemas
class JobClaimRequest(BaseModel):
    worker_id: str
    count: int = 10  # Number of jobs to claim
    capabilities: dict = {}  # Optional: worker capabilities

class JobResponse(BaseModel):
    id: str
    ingestion_id: str
    priority: int
    scheduled_at: datetime
    max_execution_time: int  # seconds

class JobCompletionRequest(BaseModel):
    files_processed: int
    records_processed: int
    duration_seconds: float

class JobFailureRequest(BaseModel):
    error_message: str
    error_type: str  # "timeout", "spark_error", "network_error", etc.

# Endpoints

@router.post("/claim")
def claim_jobs(request: JobClaimRequest, db: Session = Depends(get_db)):
    """
    Claim pending jobs for worker execution.

    Server uses SELECT FOR UPDATE SKIP LOCKED internally.
    Returns up to `count` jobs.
    """
    # Implementation in section 7.x
    pass

@router.post("/{job_id}/heartbeat")
def job_heartbeat(job_id: str, worker_id: str, db: Session = Depends(get_db)):
    """
    Update job heartbeat (worker still alive).

    Called periodically during long-running jobs.
    """
    pass

@router.post("/{job_id}/complete")
def complete_job(
    job_id: str,
    completion: JobCompletionRequest,
    db: Session = Depends(get_db)
):
    """
    Mark job as completed with metrics.
    """
    pass

@router.post("/{job_id}/fail")
def fail_job(
    job_id: str,
    failure: JobFailureRequest,
    db: Session = Depends(get_db)
):
    """
    Mark job as failed with error details.
    """
    pass

@router.post("/workers/register")
def register_worker(worker_id: str, max_threads: int, db: Session = Depends(get_db)):
    """
    Register worker (upsert worker_heartbeats table).
    """
    pass
```

**Authentication:**
- Workers authenticate via API key (shared secret)
- Or mTLS for production environments
- Server validates worker identity before job assignment

---

## 6. Database Schema

### 6.1 Job Queue Table

```sql
CREATE TABLE job_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Job identification
    ingestion_id UUID NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
    run_id UUID NULL REFERENCES runs(id),  -- Created when job starts

    -- Scheduling
    scheduled_at TIMESTAMP NOT NULL,  -- When job should run
    priority INT NOT NULL DEFAULT 0,  -- Higher = more important

    -- Status tracking
    status VARCHAR(20) NOT NULL,  -- PENDING, IN_PROGRESS, COMPLETED, FAILED, STALE

    -- Worker assignment
    claimed_by VARCHAR(255) NULL,    -- Worker ID that claimed job
    claimed_at TIMESTAMP NULL,       -- When job was claimed
    heartbeat_at TIMESTAMP NULL,     -- Last worker heartbeat

    -- Execution tracking
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,

    -- Error handling
    error_message TEXT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Tenant isolation
    tenant_id UUID NOT NULL,

    -- Indexes for efficient querying
    INDEX idx_job_queue_pending (status, scheduled_at, priority)
        WHERE status = 'PENDING',
    INDEX idx_job_queue_claimed (claimed_by, status)
        WHERE status = 'IN_PROGRESS',
    INDEX idx_job_queue_heartbeat (heartbeat_at)
        WHERE status = 'IN_PROGRESS',
    INDEX idx_job_queue_ingestion (ingestion_id)
);
```

### 6.2 Worker Heartbeat Table

```sql
CREATE TABLE worker_heartbeats (
    worker_id VARCHAR(255) PRIMARY KEY,

    -- Health status
    last_heartbeat_at TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,  -- ACTIVE, IDLE, BUSY, DEAD

    -- Capacity tracking
    total_threads INT NOT NULL,
    busy_threads INT NOT NULL,
    available_threads INT NOT NULL,

    -- Current work
    current_job_id UUID NULL REFERENCES job_queue(id),
    jobs_completed INT NOT NULL DEFAULT 0,
    jobs_failed INT NOT NULL DEFAULT 0,

    -- Metadata
    started_at TIMESTAMP NOT NULL,
    host VARCHAR(255) NOT NULL,
    version VARCHAR(50) NOT NULL,

    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    INDEX idx_worker_heartbeat_time (last_heartbeat_at)
);
```

### 6.3 Enums

```python
# app/models/enums.py

class JobStatus(str, Enum):
    PENDING = "PENDING"        # Waiting to be claimed
    IN_PROGRESS = "IN_PROGRESS"  # Claimed and executing
    COMPLETED = "COMPLETED"    # Successfully finished
    FAILED = "FAILED"          # Execution failed
    STALE = "STALE"            # Worker died, needs retry
    CANCELLED = "CANCELLED"    # Manually cancelled

class WorkerStatus(str, Enum):
    ACTIVE = "ACTIVE"    # Healthy and processing jobs
    IDLE = "IDLE"        # Healthy but no jobs to process
    BUSY = "BUSY"        # All threads occupied
    DEAD = "DEAD"        # Heartbeat timeout
```

---

## 7. Component Implementation

### 7.1 Server: Job Queue Manager

```python
# app/services/job_queue_manager.py

from datetime import datetime, timedelta
from uuid import UUID
from typing import List, Optional
from sqlalchemy.orm import Session

from app.models.domain import JobQueue, Ingestion
from app.models.enums import JobStatus, IngestionStatus
from app.repositories.job_queue_repository import JobQueueRepository
from app.config import settings

class JobQueueManager:
    """
    Manages job queue lifecycle on the server side.

    Responsibilities:
    - Enqueue jobs when APScheduler triggers
    - Monitor for stale jobs (dead workers)
    - Re-enqueue failed jobs (retry logic)
    - Cleanup old jobs
    """

    def __init__(self, db: Session):
        self.db = db
        self.job_repo = JobQueueRepository(db)

    def enqueue_job(
        self,
        ingestion_id: UUID,
        tenant_id: UUID,
        scheduled_at: datetime = None,
        priority: int = 0
    ) -> JobQueue:
        """
        Enqueue a new job for execution.

        Called by APScheduler when cron triggers.
        """
        job = JobQueue(
            ingestion_id=ingestion_id,
            tenant_id=tenant_id,
            scheduled_at=scheduled_at or datetime.utcnow(),
            priority=priority,
            status=JobStatus.PENDING,
            max_retries=settings.JOB_MAX_RETRIES
        )

        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        logger.info(f"Enqueued job {job.id} for ingestion {ingestion_id}")
        return job

    def detect_stale_jobs(self) -> List[JobQueue]:
        """
        Find jobs whose workers have died (heartbeat timeout).

        A job is stale if:
        - status = IN_PROGRESS
        - heartbeat_at > HEARTBEAT_TIMEOUT seconds ago
        """
        timeout = datetime.utcnow() - timedelta(
            seconds=settings.JOB_HEARTBEAT_TIMEOUT
        )

        stale_jobs = self.job_repo.find_stale_jobs(timeout)

        for job in stale_jobs:
            logger.warning(
                f"Job {job.id} is stale (worker {job.claimed_by} died). "
                f"Last heartbeat: {job.heartbeat_at}"
            )

            # Mark as stale
            job.status = JobStatus.STALE
            job.claimed_by = None
            job.claimed_at = None

        if stale_jobs:
            self.db.commit()

        return stale_jobs

    def retry_failed_jobs(self) -> List[JobQueue]:
        """
        Re-enqueue jobs that failed or went stale.

        Only retry if retry_count < max_retries.
        """
        retriable_jobs = self.job_repo.find_retriable_jobs()

        for job in retriable_jobs:
            if job.retry_count < job.max_retries:
                logger.info(
                    f"Retrying job {job.id} (attempt {job.retry_count + 1})"
                )

                job.status = JobStatus.PENDING
                job.retry_count += 1
                job.claimed_by = None
                job.claimed_at = None
                job.heartbeat_at = None
                job.error_message = None
            else:
                logger.error(
                    f"Job {job.id} exceeded max retries ({job.max_retries})"
                )
                job.status = JobStatus.FAILED

        if retriable_jobs:
            self.db.commit()

        return retriable_jobs

    def cleanup_old_jobs(self, retention_days: int = 30):
        """
        Delete completed/failed jobs older than retention period.
        """
        cutoff = datetime.utcnow() - timedelta(days=retention_days)

        deleted = self.job_repo.delete_old_jobs(
            cutoff,
            statuses=[JobStatus.COMPLETED, JobStatus.FAILED]
        )

        logger.info(f"Cleaned up {deleted} old jobs (older than {retention_days} days)")
        return deleted

    def monitor_queue_health(self) -> dict:
        """
        Get queue health metrics.
        """
        stats = self.job_repo.get_queue_statistics()

        # Alert if queue is backing up
        if stats['pending'] > settings.QUEUE_DEPTH_WARNING_THRESHOLD:
            logger.warning(
                f"Queue depth high: {stats['pending']} pending jobs. "
                f"Consider scaling workers."
            )

        return stats
```

### 7.2 Server: APScheduler Integration

```python
# app/services/scheduler_service.py

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from uuid import UUID

from app.services.job_queue_manager import JobQueueManager
from app.repositories.ingestion_repository import IngestionRepository

class SchedulerService:
    """
    Server-side scheduler that enqueues jobs (doesn't execute them).
    """

    def __init__(self, db: Session):
        self.db = db
        self.scheduler = BackgroundScheduler()
        self.job_queue_manager = JobQueueManager(db)
        self.ingestion_repo = IngestionRepository(db)

    def start(self):
        """Start scheduler and load active ingestions."""
        logger.info("Starting scheduler service")

        # Load all active scheduled ingestions
        ingestions = self.ingestion_repo.get_active_scheduled_ingestions()

        for ingestion in ingestions:
            self.schedule_ingestion(ingestion)

        self.scheduler.start()
        logger.info(f"Scheduler started with {len(ingestions)} active ingestions")

    def schedule_ingestion(self, ingestion: Ingestion):
        """
        Add ingestion to APScheduler.

        IMPORTANT: Job function only ENQUEUES, doesn't execute.
        """
        cron_parts = parse_cron_expression(ingestion.schedule_cron)

        self.scheduler.add_job(
            func=self._enqueue_job,  # Just enqueue
            args=[ingestion.id, ingestion.tenant_id],
            trigger=CronTrigger(**cron_parts),
            id=f"ingestion_{ingestion.id}",
            replace_existing=True
        )

        logger.info(f"Scheduled ingestion {ingestion.id} with cron {ingestion.schedule_cron}")

    def _enqueue_job(self, ingestion_id: UUID, tenant_id: UUID):
        """
        Called by APScheduler when cron triggers.

        This only creates a job queue entry.
        Workers will pick it up and execute.
        """
        logger.info(f"Cron triggered for ingestion {ingestion_id}. Enqueuing job.")

        # Create job in queue
        job = self.job_queue_manager.enqueue_job(
            ingestion_id=ingestion_id,
            tenant_id=tenant_id
        )

        logger.info(f"Job {job.id} enqueued for ingestion {ingestion_id}")
```

### 7.3 Worker: Main Loop (Multi-threaded)

```python
# app/worker.py

import time
import logging
import requests
from datetime import datetime
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

from app.config import settings
from app.services.batch_orchestrator import BatchOrchestrator

logger = logging.getLogger(__name__)

class MultiThreadedWorker:
    """
    Multi-threaded worker that polls server API and executes jobs concurrently.

    No database access - pure HTTP client.
    """

    def __init__(
        self,
        worker_id: str,
        server_url: str,
        max_threads: int = 20
    ):
        self.worker_id = worker_id
        self.server_url = server_url
        self.max_threads = max_threads
        self.executor = ThreadPoolExecutor(max_workers=max_threads)
        self.active_jobs: Dict[str, Future] = {}  # job_id -> Future

    def start(self):
        """Start worker main loop."""
        logger.info(
            f"Starting worker {self.worker_id} "
            f"(server={self.server_url}, threads={self.max_threads})"
        )

        # Register with server
        self.register()

        try:
            # Main polling loop
            while True:
                try:
                    # Calculate available capacity
                    available = self.max_threads - len(self.active_jobs)

                    if available > 0:
                        # Claim jobs from server
                        jobs = self.claim_jobs(count=min(available, 10))

                        for job in jobs:
                            # Submit to thread pool
                            future = self.executor.submit(self.execute_job, job)
                            self.active_jobs[job['id']] = future
                            logger.info(f"Submitted job {job['id']} to thread pool")

                    # Clean up completed jobs
                    self.cleanup_completed()

                    # Brief pause
                    time.sleep(settings.WORKER_POLL_INTERVAL or 1)

                except KeyboardInterrupt:
                    logger.info("Worker shutting down (KeyboardInterrupt)")
                    break

                except Exception as e:
                    logger.error(f"Polling loop error: {e}", exc_info=True)
                    time.sleep(5)

        finally:
            # Shutdown
            self.shutdown()

    def register(self):
        """Register worker with server."""
        try:
            response = requests.post(
                f"{self.server_url}/api/v1/workers/register",
                json={
                    "worker_id": self.worker_id,
                    "max_threads": self.max_threads,
                    "version": settings.VERSION
                }
            )
            response.raise_for_status()
            logger.info(f"Worker {self.worker_id} registered")
        except Exception as e:
            logger.error(f"Failed to register: {e}")
            raise

    def claim_jobs(self, count: int) -> List[dict]:
        """Claim jobs from server API."""
        try:
            response = requests.post(
                f"{self.server_url}/api/v1/jobs/claim",
                json={
                    "worker_id": self.worker_id,
                    "count": count
                },
                timeout=10
            )

            if response.status_code == 200:
                jobs = response.json().get('jobs', [])
                if jobs:
                    logger.info(f"Claimed {len(jobs)} jobs from server")
                return jobs
            elif response.status_code == 204:
                # No jobs available
                return []
            else:
                logger.warning(f"Claim failed: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Failed to claim jobs: {e}")
            return []

    def execute_job(self, job: dict):
        """
        Execute job (runs in thread pool).

        This is the actual ingestion execution logic.
        """
        job_id = job['id']
        ingestion_id = job['ingestion_id']

        try:
            logger.info(f"Executing job {job_id} (ingestion {ingestion_id})")

            # Execute ingestion using BatchOrchestrator
            orchestrator = BatchOrchestrator()
            result = orchestrator.run_scheduled_ingestion(
                ingestion_id=ingestion_id,
                run_id=job.get('run_id'),
                heartbeat_callback=lambda: self.send_heartbeat(job_id)
            )

            # Report completion to server
            self.complete_job(job_id, result)

            logger.info(
                f"Job {job_id} completed. "
                f"Processed {result['files_processed']} files, "
                f"{result['records_processed']} records"
            )

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)

            # Report failure to server
            self.fail_job(job_id, str(e))

    def send_heartbeat(self, job_id: str):
        """Send job heartbeat to server."""
        try:
            requests.post(
                f"{self.server_url}/api/v1/jobs/{job_id}/heartbeat",
                json={"worker_id": self.worker_id},
                timeout=5
            )
        except Exception as e:
            logger.warning(f"Heartbeat failed for job {job_id}: {e}")

    def complete_job(self, job_id: str, result: dict):
        """Report job completion to server."""
        try:
            response = requests.post(
                f"{self.server_url}/api/v1/jobs/{job_id}/complete",
                json={
                    "files_processed": result.get('files_processed', 0),
                    "records_processed": result.get('records_processed', 0),
                    "duration_seconds": result.get('duration_seconds', 0)
                },
                timeout=10
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to report completion for job {job_id}: {e}")

    def fail_job(self, job_id: str, error: str):
        """Report job failure to server."""
        try:
            response = requests.post(
                f"{self.server_url}/api/v1/jobs/{job_id}/fail",
                json={
                    "error_message": error,
                    "error_type": "execution_error"
                },
                timeout=10
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to report failure for job {job_id}: {e}")

    def cleanup_completed(self):
        """Remove completed futures from active jobs."""
        completed = [
            job_id for job_id, future in self.active_jobs.items()
            if future.done()
        ]

        for job_id in completed:
            del self.active_jobs[job_id]

        if completed:
            logger.debug(f"Cleaned up {len(completed)} completed jobs")

    def shutdown(self):
        """Gracefully shutdown worker."""
        logger.info(f"Shutting down worker {self.worker_id}")

        # Wait for active jobs to complete (with timeout)
        logger.info(f"Waiting for {len(self.active_jobs)} active jobs to complete...")
        self.executor.shutdown(wait=True, timeout=300)

        logger.info(f"Worker {self.worker_id} stopped")


def main():
    """Worker entry point."""
    worker_id = settings.WORKER_ID or f"worker-{uuid4()}"
    server_url = settings.SERVER_URL or "http://localhost:8000"
    max_threads = settings.WORKER_THREADS or 20

    worker = MultiThreadedWorker(
        worker_id=worker_id,
        server_url=server_url,
        max_threads=max_threads
    )

    worker.start()


if __name__ == "__main__":
    main()
```

**Key Differences from Single-threaded:**
- ✅ Thread pool executor (10-50 concurrent jobs)
- ✅ HTTP client only (no database access)
- ✅ Batch job claiming (request multiple jobs per API call)
- ✅ Automatic capacity management
- ✅ Graceful shutdown waits for active jobs

### 7.4 Server: Worker API Implementation

Server-side endpoints that workers call:

```python
# app/api/v1/worker_api.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime
from pydantic import BaseModel

from app.database import get_db
from app.models.domain import JobQueue, Run
from app.models.enums import JobStatus, RunStatus
from app.repositories.job_queue_repository import JobQueueRepository

router = APIRouter(prefix="/api/v1", tags=["worker-api"])

# Schemas
class JobClaimRequest(BaseModel):
    worker_id: str
    count: int = 10

class JobResponse(BaseModel):
    id: str
    ingestion_id: str
    priority: int
    scheduled_at: datetime

class JobCompletionRequest(BaseModel):
    files_processed: int
    records_processed: int
    duration_seconds: float

class JobFailureRequest(BaseModel):
    error_message: str
    error_type: str

# Endpoints

@router.post("/jobs/claim")
def claim_jobs(request: JobClaimRequest, db: Session = Depends(get_db)):
    """
    Claim jobs for worker execution.

    Uses SELECT FOR UPDATE SKIP LOCKED to prevent conflicts.
    """
    claimed_jobs = []

    for _ in range(request.count):
        # Atomically claim one job
        result = db.execute("""
            SELECT id, ingestion_id, priority, scheduled_at
            FROM job_queue
            WHERE status = 'PENDING'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """).fetchone()

        if result is None:
            break  # No more jobs

        # Claim job
        db.execute("""
            UPDATE job_queue
            SET status = 'IN_PROGRESS',
                claimed_by = :worker_id,
                claimed_at = NOW(),
                heartbeat_at = NOW(),
                started_at = NOW()
            WHERE id = :job_id
        """, {'worker_id': request.worker_id, 'job_id': result.id})

        claimed_jobs.append(JobResponse(
            id=str(result.id),
            ingestion_id=str(result.ingestion_id),
            priority=result.priority,
            scheduled_at=result.scheduled_at
        ))

    db.commit()

    if not claimed_jobs:
        return Response(status_code=204)  # No Content

    return {"jobs": claimed_jobs}


@router.post("/jobs/{job_id}/heartbeat")
def job_heartbeat(job_id: str, worker_id: str, db: Session = Depends(get_db)):
    """
    Update job heartbeat (worker still alive).
    """
    result = db.execute("""
        UPDATE job_queue
        SET heartbeat_at = NOW()
        WHERE id = :job_id
          AND claimed_by = :worker_id
          AND status = 'IN_PROGRESS'
    """, {'job_id': job_id, 'worker_id': worker_id})

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Job not found or not claimed by worker")

    db.commit()
    return {"status": "ok"}


@router.post("/jobs/{job_id}/complete")
def complete_job(
    job_id: str,
    completion: JobCompletionRequest,
    db: Session = Depends(get_db)
):
    """
    Mark job as completed with metrics.
    """
    # Get job
    job = db.query(JobQueue).filter(JobQueue.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Update job
    job.status = JobStatus.COMPLETED
    job.finished_at = datetime.utcnow()

    # Update run (if exists)
    if job.run_id:
        run = db.query(Run).filter(Run.id == job.run_id).first()
        if run:
            run.status = RunStatus.COMPLETED
            run.finished_at = datetime.utcnow()
            run.files_processed = completion.files_processed
            run.records_processed = completion.records_processed

    db.commit()

    logger.info(
        f"Job {job_id} completed. "
        f"Processed {completion.files_processed} files, "
        f"{completion.records_processed} records"
    )

    return {"status": "completed"}


@router.post("/jobs/{job_id}/fail")
def fail_job(
    job_id: str,
    failure: JobFailureRequest,
    db: Session = Depends(get_db)
):
    """
    Mark job as failed with error details.
    """
    # Get job
    job = db.query(JobQueue).filter(JobQueue.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Update job
    job.status = JobStatus.FAILED
    job.finished_at = datetime.utcnow()
    job.error_message = failure.error_message

    # Update run (if exists)
    if job.run_id:
        run = db.query(Run).filter(Run.id == job.run_id).first()
        if run:
            run.status = RunStatus.FAILED
            run.finished_at = datetime.utcnow()
            run.error_message = failure.error_message

    db.commit()

    logger.error(f"Job {job_id} failed: {failure.error_message}")

    return {"status": "failed"}


@router.post("/workers/register")
def register_worker(
    worker_id: str,
    max_threads: int,
    db: Session = Depends(get_db)
):
    """
    Register worker (upsert worker_heartbeats table).
    """
    db.execute("""
        INSERT INTO worker_heartbeats (
            worker_id, last_heartbeat_at, status,
            total_threads, busy_threads, available_threads,
            started_at, host, version
        ) VALUES (
            :worker_id, NOW(), 'ACTIVE',
            :threads, 0, :threads,
            NOW(), :host, :version
        )
        ON CONFLICT (worker_id) DO UPDATE
        SET last_heartbeat_at = NOW(),
            status = 'ACTIVE',
            total_threads = :threads,
            available_threads = :threads
    """, {
        'worker_id': worker_id,
        'threads': max_threads,
        'host': 'unknown',  # Worker could send this
        'version': settings.VERSION
    })

    db.commit()
    logger.info(f"Worker {worker_id} registered with {max_threads} threads")

    return {"status": "registered"}
```

**Key Points:**
- Server owns ALL database access
- Workers never see database credentials
- Atomic job claiming via `SELECT FOR UPDATE SKIP LOCKED`
- Clean HTTP API contract

---

## 8. Code Examples

### 8.1 Repository: Job Queue

```python
# app/repositories/job_queue_repository.py

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from app.models.domain import JobQueue
from app.models.enums import JobStatus

class JobQueueRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, job: JobQueue) -> JobQueue:
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        return job

    def get_by_id(self, job_id: UUID) -> Optional[JobQueue]:
        return self.db.query(JobQueue).filter(JobQueue.id == job_id).first()

    def find_pending_jobs(self, limit: int = 100) -> List[JobQueue]:
        """Get pending jobs ready to execute."""
        return (
            self.db.query(JobQueue)
            .filter(
                JobQueue.status == JobStatus.PENDING,
                JobQueue.scheduled_at <= datetime.utcnow()
            )
            .order_by(JobQueue.priority.desc(), JobQueue.created_at.asc())
            .limit(limit)
            .all()
        )

    def find_stale_jobs(self, timeout: datetime) -> List[JobQueue]:
        """Find jobs whose workers died (heartbeat timeout)."""
        return (
            self.db.query(JobQueue)
            .filter(
                JobQueue.status == JobStatus.IN_PROGRESS,
                JobQueue.heartbeat_at < timeout
            )
            .all()
        )

    def find_retriable_jobs(self) -> List[JobQueue]:
        """Find jobs that can be retried."""
        return (
            self.db.query(JobQueue)
            .filter(
                or_(
                    JobQueue.status == JobStatus.FAILED,
                    JobQueue.status == JobStatus.STALE
                ),
                JobQueue.retry_count < JobQueue.max_retries
            )
            .all()
        )

    def delete_old_jobs(
        self,
        cutoff: datetime,
        statuses: List[JobStatus]
    ) -> int:
        """Delete old completed/failed jobs."""
        result = (
            self.db.query(JobQueue)
            .filter(
                JobQueue.finished_at < cutoff,
                JobQueue.status.in_(statuses)
            )
            .delete(synchronize_session=False)
        )
        self.db.commit()
        return result

    def get_queue_statistics(self) -> dict:
        """Get queue health metrics."""
        from sqlalchemy import func

        stats = (
            self.db.query(
                JobQueue.status,
                func.count(JobQueue.id).label('count')
            )
            .group_by(JobQueue.status)
            .all()
        )

        return {row.status: row.count for row in stats}

    def get_jobs_by_ingestion(
        self,
        ingestion_id: UUID,
        limit: int = 10
    ) -> List[JobQueue]:
        """Get recent jobs for an ingestion."""
        return (
            self.db.query(JobQueue)
            .filter(JobQueue.ingestion_id == ingestion_id)
            .order_by(JobQueue.created_at.desc())
            .limit(limit)
            .all()
        )
```

### 8.2 Background Monitor (Server)

```python
# app/services/background_monitor.py

import time
import logging
from threading import Thread
from datetime import datetime, timedelta

from app.database import SessionLocal
from app.services.job_queue_manager import JobQueueManager
from app.config import settings

logger = logging.getLogger(__name__)

class BackgroundMonitor:
    """
    Background service that runs on server to:
    - Detect stale jobs
    - Retry failed jobs
    - Cleanup old jobs
    - Monitor queue health
    """

    def __init__(self):
        self.is_running = False
        self.thread = None

    def start(self):
        """Start background monitoring."""
        if self.is_running:
            return

        self.is_running = True
        self.thread = Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        logger.info("Background monitor started")

    def stop(self):
        """Stop background monitoring."""
        self.is_running = False
        if self.thread:
            self.thread.join()
        logger.info("Background monitor stopped")

    def _monitor_loop(self):
        """Main monitoring loop."""
        while self.is_running:
            try:
                db = SessionLocal()
                manager = JobQueueManager(db)

                # 1. Detect stale jobs (every 30 seconds)
                stale_jobs = manager.detect_stale_jobs()
                if stale_jobs:
                    logger.warning(f"Found {len(stale_jobs)} stale jobs")

                # 2. Retry failed/stale jobs (every 1 minute)
                if int(time.time()) % 60 == 0:
                    retried = manager.retry_failed_jobs()
                    if retried:
                        logger.info(f"Retried {len(retried)} jobs")

                # 3. Cleanup old jobs (every 1 hour)
                if int(time.time()) % 3600 == 0:
                    deleted = manager.cleanup_old_jobs(retention_days=30)
                    if deleted > 0:
                        logger.info(f"Cleaned up {deleted} old jobs")

                # 4. Monitor queue health (every 5 minutes)
                if int(time.time()) % 300 == 0:
                    stats = manager.monitor_queue_health()
                    logger.info(f"Queue stats: {stats}")

                db.close()

            except Exception as e:
                logger.error(f"Monitor loop error: {e}", exc_info=True)

            time.sleep(30)  # Check every 30 seconds
```

### 8.3 API Endpoint: Queue Status

```python
# app/api/v1/admin.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.repositories.job_queue_repository import JobQueueRepository
from app.models.schemas import QueueStatusResponse

router = APIRouter(prefix="/admin", tags=["admin"])

@router.get("/queue/status", response_model=QueueStatusResponse)
def get_queue_status(db: Session = Depends(get_db)):
    """
    Get job queue status for monitoring.

    Returns:
    - Pending jobs count
    - In-progress jobs count
    - Completed jobs (last 24h)
    - Failed jobs (last 24h)
    - Average wait time
    - Active workers
    """
    job_repo = JobQueueRepository(db)

    # Queue statistics
    stats = job_repo.get_queue_statistics()

    # Active workers
    active_workers = db.execute("""
        SELECT COUNT(*)
        FROM worker_heartbeats
        WHERE status IN ('ACTIVE', 'BUSY')
          AND last_heartbeat_at > NOW() - INTERVAL '1 minute'
    """).scalar()

    # Average wait time (time from created to claimed)
    avg_wait = db.execute("""
        SELECT AVG(EXTRACT(EPOCH FROM (claimed_at - created_at)))
        FROM job_queue
        WHERE status = 'IN_PROGRESS'
          AND claimed_at > NOW() - INTERVAL '1 hour'
    """).scalar() or 0

    return {
        "pending": stats.get(JobStatus.PENDING, 0),
        "in_progress": stats.get(JobStatus.IN_PROGRESS, 0),
        "completed_24h": stats.get(JobStatus.COMPLETED, 0),  # TODO: filter by time
        "failed_24h": stats.get(JobStatus.FAILED, 0),  # TODO: filter by time
        "active_workers": active_workers,
        "average_wait_seconds": avg_wait
    }

# Response:
# {
#   "pending": 5,
#   "in_progress": 12,
#   "completed_24h": 340,
#   "failed_24h": 3,
#   "active_workers": 3,
#   "average_wait_seconds": 2.5
# }
```

---

## 9. Comparison with Alternatives

### 9.1 Feature Matrix

| Feature | Worker-Based | Distributed APScheduler | IOMETE Jobs | K8s CronJobs |
|---------|--------------|------------------------|-------------|--------------|
| **Development Time** | 🟢 1 week | 🔴 3 weeks | 🟢 1.5 weeks | 🟡 2 weeks |
| **Complexity** | 🟢 LOW | 🔴 HIGH | 🟢 LOW | 🟡 MEDIUM |
| **New Dependencies** | 🟢 None | 🔴 Redis/ZooKeeper | 🟢 None (IOMETE) | 🟢 None (K8s) |
| **Horizontal Scaling** | 🟢 Trivial | 🟡 Complex | 🟢 Built-in | 🟢 Built-in |
| **Debugging** | 🟢 Easy (SQL) | 🔴 Hard | 🟢 Easy | 🟢 Easy |
| **Failure Modes** | 🟢 Few | 🔴 Many | 🟢 Few | 🟢 Few |
| **Operational Cost** | 🟢 Low | 🔴 High | 🟢 Low | 🟢 Low |
| **Vendor Lock-in** | 🟢 None | 🟢 None | 🔴 IOMETE | 🟡 Kubernetes |
| **Familiar API** | 🟢 Yes (APScheduler) | 🟢 Yes | 🔴 No | 🔴 No |

### 9.2 Why Worker-Based is Better

**vs. Distributed APScheduler:**
- ✅ No Redis/ZooKeeper dependency
- ✅ Simpler architecture (no leader election, job assignment)
- ✅ Easier debugging (SQL queries vs distributed tracing)
- ✅ Fewer failure modes
- ✅ Faster implementation (1 week vs 3 weeks)

**vs. IOMETE Jobs:**
- ✅ Keeps existing architecture (FastAPI + APScheduler)
- ✅ No learning curve (same codebase)
- ✅ No vendor lock-in
- ❌ More code to maintain (IOMETE Jobs is managed)

**vs. Kubernetes CronJobs:**
- ✅ No Kubernetes requirement
- ✅ Better for dynamic scheduling (cron changes don't require pod restarts)
- ✅ Centralized monitoring (all in database)
- ❌ Requires worker management (K8s manages pods)

### 9.3 When to Choose Worker-Based

**Choose Worker-Based If:**
- ✅ You want to keep APScheduler (familiar API)
- ✅ You want horizontal scaling without distributed systems complexity
- ✅ You're already using PostgreSQL
- ✅ You want simple deployment (Docker Compose or K8s)
- ✅ You want to avoid vendor lock-in
- ✅ You need <10,000 jobs (beyond that, consider Airflow)

**Don't Choose If:**
- ❌ You're already using IOMETE Jobs (why build it yourself?)
- ❌ You need complex workflows (use Airflow)
- ❌ You have <50 ingestions (single APScheduler is enough)

---

## 10. Failure Scenarios

### 10.1 Scenario 1: Worker Dies Mid-Execution

**Timeline:**
```
10:00:00 - Worker 1 claims Job A
10:00:05 - Worker 1 starts executing (Spark job)
10:00:30 - Worker 1 crashes (OOM)
10:00:30 - Job A status: IN_PROGRESS, heartbeat_at: 10:00:25 (5 sec ago)
10:02:30 - Server detects stale job (heartbeat timeout = 2 min)
10:02:31 - Server marks Job A as STALE
10:02:32 - Server retries: Job A status = PENDING
10:02:35 - Worker 2 claims Job A
10:02:40 - Worker 2 executes Job A
10:05:00 - Job A completed
```

**Recovery Time:** 2-3 minutes (heartbeat timeout)

**Data Safety:**
- Spark job may have partial writes
- File state service prevents duplicate processing (via processed_files table)
- Idempotent ingestion logic ensures safe retry

### 10.2 Scenario 2: Database Deadlock

**Timeline:**
```
10:00:00 - Worker 1 tries to claim Job A
10:00:00 - Worker 2 tries to claim Job A
10:00:01 - Database lock: Worker 1 wins (FOR UPDATE SKIP LOCKED)
10:00:01 - Worker 2 skips Job A, claims Job B
10:00:02 - Both workers executing different jobs (no deadlock)
```

**Result:** No deadlock. `SKIP LOCKED` prevents blocking.

### 10.3 Scenario 3: Server Crashes

**Timeline:**
```
10:00:00 - Server crashes
10:00:00 - Workers keep running (polling database)
10:00:05 - Workers continue executing claimed jobs
10:00:30 - Server restarts
10:00:35 - Server loads APScheduler state from database
10:01:00 - Next cron trigger: Server enqueues job
10:01:05 - Worker claims and executes job
```

**Impact:**
- No job execution interrupted (workers independent)
- New jobs delayed by server restart time (~30 sec)
- Existing jobs unaffected

### 10.4 Scenario 4: Database Outage

**Timeline:**
```
10:00:00 - Database crashes
10:00:01 - Workers fail to poll (connection error)
10:00:01 - Server fails to enqueue jobs
10:00:05 - Workers retry connection (exponential backoff)
10:00:30 - Database restarts
10:00:31 - Workers resume polling
10:00:32 - Server resumes enqueuing
```

**Impact:**
- All operations paused during database downtime
- No data loss (jobs in queue persist)
- Automatic recovery when database returns

### 10.5 Scenario 5: Queue Backup

**Timeline:**
```
10:00:00 - 3 workers, processing 10 jobs/hour
10:00:00 - Suddenly 100 jobs enqueued (burst)
10:00:05 - Queue depth: 100 pending
10:00:05 - Server alerts: "Queue depth high"
10:05:00 - Admin scales workers: 3 → 10
10:10:00 - Queue draining faster
10:30:00 - Queue cleared
```

**Mitigation:**
- Prometheus alert on queue depth
- Horizontal auto-scaling (Kubernetes HPA)
- Priority queuing (high-priority jobs first)

---

## 11. Operational Considerations

### 11.1 Monitoring

**Prometheus Metrics:**

```python
# app/metrics.py

from prometheus_client import Gauge, Counter, Histogram

# Queue depth
queue_depth = Gauge(
    'autoloader_queue_depth',
    'Number of pending jobs',
    ['status']
)

# Worker count
active_workers = Gauge(
    'autoloader_active_workers',
    'Number of active workers'
)

# Job duration
job_duration = Histogram(
    'autoloader_job_duration_seconds',
    'Job execution duration',
    buckets=[30, 60, 300, 600, 1800, 3600]  # 30s, 1m, 5m, 10m, 30m, 1h
)

# Job outcomes
jobs_completed = Counter(
    'autoloader_jobs_completed_total',
    'Total jobs completed'
)

jobs_failed = Counter(
    'autoloader_jobs_failed_total',
    'Total jobs failed'
)

# Queue wait time
queue_wait_time = Histogram(
    'autoloader_queue_wait_seconds',
    'Time job waits in queue before execution',
    buckets=[1, 5, 10, 30, 60, 300]
)
```

**Grafana Dashboard:**

```
┌──────────────────────────────────────────────────────────┐
│  Autoloader Job Queue Dashboard                          │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐    │
│  │  Pending    │  │ In Progress │  │ Active       │    │
│  │   15        │  │      8      │  │ Workers: 3   │    │
│  └─────────────┘  └─────────────┘  └──────────────┘    │
│                                                           │
│  Queue Depth (24h)                                        │
│  ┌────────────────────────────────────────────────────┐  │
│  │                    ╱╲                               │  │
│  │          ╱╲       ╱  ╲      ╱╲                     │  │
│  │ ────────╱──╲─────╱────╲────╱──╲─────────────────  │  │
│  └────────────────────────────────────────────────────┘  │
│                                                           │
│  Job Duration                    Worker Utilization       │
│  ┌─────────────────────────┐    ┌─────────────────────┐ │
│  │ p50: 45s                │    │ Worker 1: 80%       │ │
│  │ p95: 120s               │    │ Worker 2: 75%       │ │
│  │ p99: 300s               │    │ Worker 3: 65%       │ │
│  └─────────────────────────┘    └─────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### 11.2 Alerting

**Prometheus Alerts:**

```yaml
groups:
  - name: autoloader_queue
    rules:
      # Queue backing up
      - alert: QueueDepthHigh
        expr: autoloader_queue_depth{status="PENDING"} > 50
        for: 5m
        annotations:
          summary: "Job queue depth high ({{ $value }} pending jobs)"
          description: "Consider scaling workers"

      # No workers
      - alert: NoActiveWorkers
        expr: autoloader_active_workers == 0
        for: 1m
        annotations:
          summary: "No active workers detected"
          description: "Jobs will not be processed"

      # Jobs failing
      - alert: HighJobFailureRate
        expr: rate(autoloader_jobs_failed_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High job failure rate ({{ $value }}/sec)"

      # Long queue wait
      - alert: LongQueueWaitTime
        expr: histogram_quantile(0.95, autoloader_queue_wait_seconds) > 300
        for: 10m
        annotations:
          summary: "Jobs waiting >5min in queue (p95)"
          description: "Scale workers or check for issues"
```

### 11.3 Scaling

**Manual Scaling:**

```bash
# Docker Compose
docker-compose up -d --scale autoloader-worker=10

# Kubernetes
kubectl scale deployment autoloader-worker --replicas=10
```

**Auto-Scaling (Kubernetes HPA):**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: autoloader-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: autoloader-worker
  minReplicas: 3
  maxReplicas: 20
  metrics:
  # Scale based on CPU
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70

  # Scale based on queue depth (custom metric)
  - type: Pods
    pods:
      metric:
        name: queue_depth_per_worker
      target:
        type: AverageValue
        averageValue: "10"  # Max 10 pending jobs per worker
```

**Queue-Based Scaling Logic:**

```
Target Workers = ceil(pending_jobs / desired_jobs_per_worker)

Example:
- 100 pending jobs
- Desired: 10 jobs/worker
- Target: 100 / 10 = 10 workers
```

### 11.4 Deployment Strategy

**Blue-Green Deployment:**

```yaml
# Deploy new worker version alongside old
kubectl apply -f worker-deployment-v2.yaml

# Wait for new workers to be healthy
kubectl wait --for=condition=ready pod -l version=v2

# Scale down old workers
kubectl scale deployment autoloader-worker-v1 --replicas=0

# Verify no issues
# If issues, rollback: scale v1 up, v2 down
```

**Rolling Update:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: autoloader-worker
spec:
  replicas: 10
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 2  # Max 2 workers down during update
      maxSurge: 2        # Max 2 extra workers during update
```

---

## 12. Cost-Benefit Analysis

### 12.1 Engineering Cost

| Task | Effort | Complexity |
|------|--------|------------|
| Database schema (job_queue, worker_heartbeats) | 0.5 day | Low |
| Job queue manager (server) | 1 day | Low |
| Worker service implementation | 1.5 days | Medium |
| Background monitor | 0.5 day | Low |
| API endpoints (queue status) | 0.5 day | Low |
| Testing (unit, integration) | 1 day | Medium |
| Documentation & deployment | 0.5 day | Low |
| **Total** | **5.5 days (~1 week)** | **Low-Medium** |

**Ongoing Maintenance:**
- Debugging: ~0.5 days/month (easy, SQL-based)
- Monitoring: ~0.5 days/month
- **Total Yearly Cost:** ~12 days of engineering time

### 12.2 Infrastructure Cost

| Component | Monthly Cost | Purpose |
|-----------|--------------|---------|
| PostgreSQL | $0 | Already exists |
| Workers (3 instances) | $150-300 | Horizontal scaling |
| Load Balancer | $20-50 | Optional (for server HA) |
| **Total** | **$170-350/month** | - |

**Compared to Distributed APScheduler:**
- ✅ **60% cheaper** ($170 vs $450/month)
- ✅ No Redis cluster needed

### 12.3 ROI Comparison

**Worker-Based Architecture:**
- Engineering: 5.5 days + 1 day/month = **17.5 days/year**
- Infrastructure: $170-350/month = **$2,040-4,200/year**
- **Total Cost: ~$20,000-30,000/year**

**Distributed APScheduler:**
- Engineering: 15 days + 4 days/month = **63 days/year**
- Infrastructure: $270-850/month = **$3,240-10,200/year**
- **Total Cost: ~$50,000-100,000/year**

**Winner: Worker-Based (60% cheaper)**

**IOMETE Jobs:**
- Engineering: 10 days + 1 day/month = **22 days/year**
- Infrastructure: $0 (uses IOMETE)
- **Total Cost: ~$20,000-30,000/year**

**Tie: Worker-Based and IOMETE Jobs**

---

## 13. Migration Path

### 13.1 Phase 1: Preparation (Days 1-2)

**Step 1: Database Schema**

```bash
# Create Alembic migration
alembic revision -m "Add job queue and worker heartbeat tables"
```

```python
# alembic/versions/xxx_add_job_queue.py

def upgrade():
    # Create job_queue table
    op.create_table(
        'job_queue',
        sa.Column('id', postgresql.UUID(), nullable=False),
        # ... (see section 6.1)
    )

    # Create worker_heartbeats table
    op.create_table(
        'worker_heartbeats',
        # ... (see section 6.2)
    )

    # Run migration
    alembic upgrade head
```

**Step 2: Implement Repositories**

- `JobQueueRepository` (see section 8.1)
- Add methods to existing repositories

### 13.2 Phase 2: Server Implementation (Days 3-4)

**Step 1: Job Queue Manager**

- Implement `JobQueueManager` (see section 7.1)

**Step 2: Modify Scheduler**

- Change `SchedulerService` to enqueue jobs instead of executing (see section 7.2)

**Step 3: Background Monitor**

- Implement `BackgroundMonitor` (see section 8.2)

**Step 4: API Endpoints**

- Add `/admin/queue/status` endpoint (see section 8.3)

### 13.3 Phase 3: Worker Implementation (Days 5-6)

**Step 1: Worker Service**

- Implement `WorkerService` (see section 7.4)

**Step 2: Worker Main Loop**

- Implement `app/worker.py` (see section 7.3)

**Step 3: Docker Configuration**

```dockerfile
# Dockerfile (same for server and worker, different commands)

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app/ app/

# Server mode
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0"]

# Worker mode (override in docker-compose)
# CMD ["python", "-m", "app.worker"]
```

```yaml
# docker-compose.yml

services:
  server:
    build: .
    command: python -m uvicorn app.main:app --host 0.0.0.0
    environment:
      MODE: server
    ports:
      - "8000:8000"

  worker:
    build: .
    command: python -m app.worker
    environment:
      MODE: worker
    deploy:
      replicas: 3
```

### 13.4 Phase 4: Testing (Day 7)

**Unit Tests:**

```python
# tests/test_worker_service.py

def test_claim_next_job(db_session):
    # Create pending job
    job = JobQueue(
        ingestion_id=uuid4(),
        tenant_id=uuid4(),
        status=JobStatus.PENDING,
        scheduled_at=datetime.utcnow()
    )
    db_session.add(job)
    db_session.commit()

    # Worker claims job
    worker = WorkerService(db_session, "worker-1")
    claimed = worker.claim_next_job()

    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status == JobStatus.IN_PROGRESS
    assert claimed.claimed_by == "worker-1"

def test_no_duplicate_claims(db_session):
    # Create 1 pending job
    job = create_job(db_session)

    # Two workers try to claim
    worker1 = WorkerService(db_session, "worker-1")
    worker2 = WorkerService(db_session, "worker-2")

    claimed1 = worker1.claim_next_job()
    claimed2 = worker2.claim_next_job()

    # Only one succeeds
    assert claimed1 is not None
    assert claimed2 is None
```

**Integration Tests:**

```python
# tests/integration/test_worker_flow.py

def test_full_ingestion_flow(db_session):
    # 1. Server enqueues job
    manager = JobQueueManager(db_session)
    job = manager.enqueue_job(ingestion_id, tenant_id)

    # 2. Worker claims job
    worker = WorkerService(db_session, "worker-1")
    claimed = worker.claim_next_job()
    assert claimed.id == job.id

    # 3. Worker executes job (mock Spark)
    with mock.patch('app.services.batch_orchestrator.BatchOrchestrator'):
        worker.execute_job(claimed)

    # 4. Verify completion
    db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.run_id is not None
```

### 13.5 Phase 5: Rollout (Week 2)

**Day 1: Staging Deployment**

```bash
# Deploy to staging
docker-compose -f docker-compose.staging.yml up -d

# Verify server started
curl http://staging:8000/health

# Verify workers registered
curl http://staging:8000/admin/queue/status
# {"active_workers": 3, "pending": 0, ...}
```

**Day 2-3: Load Testing**

```python
# Create 100 test ingestions
for i in range(100):
    create_ingestion(f"test-{i}", cron="*/5 * * * *")  # Every 5 min

# Wait 10 minutes
time.sleep(600)

# Check results
runs = get_runs(limit=100)
assert len(runs) == 100
assert all(r.status == "COMPLETED" for r in runs)
```

**Day 4: Production Rollout**

```bash
# Blue-green deployment
# 1. Deploy new version alongside old
kubectl apply -f k8s/worker-v2.yaml

# 2. Verify new workers healthy
kubectl wait --for=condition=ready pod -l version=v2

# 3. Scale down old version
kubectl scale deployment autoloader-worker-v1 --replicas=0

# 4. Monitor for issues
watch kubectl get pods
watch 'curl http://api/admin/queue/status'
```

**Day 5: Monitoring & Tuning**

- Set up Grafana dashboards
- Configure alerts
- Tune worker count based on load

---

## 14. Recommendation

### 14.1 Summary

**Worker-Based Architecture is the BEST option for horizontally scaling Autoloader.**

**Why?**

1. **Simplicity:**
   - ✅ No distributed coordination (Redis/ZooKeeper)
   - ✅ Database provides all synchronization
   - ✅ Clear separation: Server schedules, Workers execute

2. **Cost:**
   - ✅ 1 week implementation (vs 3 weeks for distributed APScheduler)
   - ✅ $20K-30K/year total cost (vs $50K-100K for distributed APScheduler)
   - ✅ 60% cheaper than distributed APScheduler

3. **Scalability:**
   - ✅ Horizontal: Add workers as needed
   - ✅ Auto-scaling: Kubernetes HPA support
   - ✅ Proven pattern: Used by Celery, Sidekiq, etc.

4. **Reliability:**
   - ✅ Database ACID guarantees
   - ✅ Built-in locking (`SELECT FOR UPDATE SKIP LOCKED`)
   - ✅ Simple failure modes (well-understood)

5. **Operations:**
   - ✅ Easy monitoring (SQL queries, Prometheus)
   - ✅ Easy debugging (no distributed tracing)
   - ✅ No new infrastructure

### 14.2 Comparison Summary

| Approach | Dev Time | Complexity | Cost/Year | Scalability | Verdict |
|----------|----------|------------|-----------|-------------|---------|
| **Single APScheduler** | 0 days | Low | $0 | Poor | ✅ MVP only |
| **Worker-Based** | 1 week | Low | $20-30K | Excellent | ✅✅ **RECOMMENDED** |
| **Distributed APScheduler** | 3 weeks | High | $50-100K | Good | ❌ Too complex |
| **IOMETE Jobs** | 1.5 weeks | Low | $20-30K | Excellent | ✅ Alternative |
| **Kubernetes CronJobs** | 2 weeks | Medium | $30-45K | Excellent | ✅ If on K8s |

### 14.3 Implementation Recommendation

**Phase 1 (Week 1-2): MVP with Single APScheduler**
- ⏱️ Already implemented
- 📊 Capacity: <100 ingestions
- ✅ Fast to market

**Phase 2 (Week 3-4): Worker-Based Architecture**
- ⏱️ Implementation: 1 week
- 📊 Capacity: 1,000+ ingestions
- ✅ Production-ready horizontal scaling
- ✅ No distributed systems complexity
- ✅ Easy to maintain

**Skip: Distributed APScheduler**
- ❌ 3x longer implementation time
- ❌ 2-3x higher cost
- ❌ Significantly more complex
- ❌ No meaningful benefit over worker-based

### 14.4 Decision

**✅ RECOMMENDED: Implement Worker-Based Architecture**

**Rationale:**
1. Best cost-benefit ratio
2. Simple enough to build in 1 week
3. Scales to 1,000+ ingestions
4. Leverages existing PostgreSQL (no new dependencies)
5. Proven pattern (Celery, Sidekiq, etc.)
6. Easy to debug and operate

**Next Steps:**
1. Review this document with team
2. Approve architecture
3. Create implementation tasks
4. Start Phase 1 (database schema)
5. Deploy to staging in Week 1
6. Production rollout in Week 2

---

## 15. Conclusion

Worker-based architecture provides **the best balance of simplicity, cost, and scalability** for Autoloader.

**Key Insights:**

1. **Database is Enough**
   - PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` provides everything needed for distributed coordination
   - No Redis, ZooKeeper, or message queue required

2. **Separation of Concerns**
   - Server: Schedules jobs (APScheduler)
   - Workers: Execute jobs (stateless)
   - Database: Single source of truth

3. **Proven Pattern**
   - Used by Celery (Python), Sidekiq (Ruby), Bull (Node.js)
   - Battle-tested in production at scale
   - Well-understood failure modes

4. **Horizontal Scaling for Free**
   - Add workers: `kubectl scale deployment autoloader-worker --replicas=10`
   - No rebalancing, no coordination, no complexity

5. **Future-Proof**
   - Easy to add features (priority queues, job dependencies, etc.)
   - Can migrate to message queue later if needed (RabbitMQ, Kafka)
   - Compatible with existing Autoloader architecture

**Final Recommendation:**

✅ **Build worker-based architecture instead of distributed APScheduler or migrating to IOMETE Jobs/K8s CronJobs.**

This gives you:
- Horizontal scaling (like IOMETE Jobs / K8s CronJobs)
- Familiar API (APScheduler)
- No vendor lock-in
- Low complexity
- Low cost

**This is the sweet spot for Autoloader's scaling needs.**

---

**End of Document**

**Related Documents:**
- `apscheduler-horizontal-scaling.md` - Distributed APScheduler analysis
- `scheduler-scaling-and-risks.md` - Risk analysis
- `scheduler-implementation-guide.md` - Implementation details

**Decision Record:**
- **Status:** ✅ **RECOMMENDED**
- **Decision:** Implement worker-based architecture for horizontal scaling
- **Rationale:** Best cost-benefit ratio, simple, scalable, no new dependencies
- **Implementation Timeline:** 1 week
- **Expected Capacity:** 1,000+ scheduled ingestions

---

## 16. Architecture Summary

### Key Design Decisions

**1. Server-Based Job Distribution (Not Direct DB Access)**
- ✅ Workers poll server HTTP API, not database
- ✅ Server owns ALL database access (security boundary)
- ✅ Clean separation: workers are pure execution engines
- ✅ Easy to change DB schema without updating workers

**2. Multi-threaded Workers (Not One Job Per Pod)**
- ✅ Each worker handles 10-50 concurrent jobs via thread pool
- ✅ 50 workers × 20 threads = 1,000 concurrent jobs
- ✅ 90% cost reduction vs single-threaded approach
- ✅ Configurable concurrency per worker

**3. HTTP API Contract**
```
Workers → Server API → Database

POST /api/v1/jobs/claim?count=N     # Batch job claiming
POST /api/v1/jobs/{id}/heartbeat    # Keep-alive
POST /api/v1/jobs/{id}/complete     # Report success
POST /api/v1/jobs/{id}/fail         # Report failure
POST /api/v1/workers/register       # Worker registration
```

**4. Database Locking (Hidden from Workers)**
- Server uses `SELECT FOR UPDATE SKIP LOCKED` internally
- Workers never deal with locking logic
- Atomic, conflict-free job distribution

**5. Scaling Math**
```
Scenario: 1,000 concurrent ingestions

Single-threaded:  1,000 workers × 512MB = 512GB RAM ($5,000/mo)
Multi-threaded:      50 workers × 2GB   = 100GB RAM ($500/mo)

Savings: 90%
```

### Why This Design is Superior

| Aspect | Worker-Based (v2.0) | Distributed APScheduler | Single APScheduler |
|--------|---------------------|------------------------|-------------------|
| **Architecture** | Server API + Multi-threaded workers | Distributed coordinatio | Single process |
| **Concurrency** | 50 workers × 20 threads = 1,000 jobs | Complex rebalancing | Limited by single process |
| **Database Access** | Server only (secure) | Every worker (credentials leak) | Single process |
| **Infrastructure** | Database + Workers | Database + Redis + Workers | Database |
| **Scaling** | Add workers (trivial) | Complex (rebalancing) | Vertical only |
| **Cost (1K jobs)** | ~$500/month | ~$5,000/month | N/A (can't scale) |
| **Implementation** | 1 week | 3 weeks | 0 days (exists) |
| **Operational Complexity** | Low | High | Low |

### Implementation Checklist

- [ ] Phase 1: Database schema (job_queue, worker_heartbeats)
- [ ] Phase 2: Server worker API endpoints
- [ ] Phase 3: Multi-threaded worker implementation
- [ ] Phase 4: Integration testing (simulate 100+ concurrent jobs)
- [ ] Phase 5: Production deployment with auto-scaling

**Next Step:** Review and approve architecture, then begin Phase 1.
