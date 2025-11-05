# Scheduler Scaling and Risk Analysis

**Document Version:** 1.0
**Created:** 2025-11-05
**Status:** Architecture Analysis
**Related:** `scheduler-implementation-guide.md`

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Python-Level Scheduler Risks](#python-level-scheduler-risks)
3. [Scaling Limits](#scaling-limits)
4. [Failure Scenarios](#failure-scenarios)
5. [Alternative Architectures](#alternative-architectures)
6. [Recommended Architecture](#recommended-architecture)
7. [Migration Path](#migration-path)
8. [Decision Matrix](#decision-matrix)
9. [Implementation Recommendations](#implementation-recommendations)

---

## 1. Executive Summary

### 1.1 Key Findings

Managing multiple scheduled ingestions at the Python application level (APScheduler) introduces several **production risks**:

| Risk Category | Severity | Impact | Mitigation Complexity |
|--------------|----------|--------|----------------------|
| **Single Point of Failure** | 🔴 HIGH | All schedules stop | Medium |
| **Resource Contention** | 🟡 MEDIUM | Performance degradation | Low |
| **Scalability Ceiling** | 🟡 MEDIUM | Limited to ~1000 jobs | High |
| **Memory Leaks** | 🟡 MEDIUM | Requires restarts | Medium |
| **Thundering Herd** | 🟠 LOW-MEDIUM | Cluster overload | Medium |
| **State Inconsistency** | 🟡 MEDIUM | Lost schedules on crash | Low |

### 1.2 Recommendation

**For Production:** Use **external orchestration** (IOMETE Jobs or Kubernetes CronJobs) instead of Python-level scheduling.

**Why:**
- ✅ Better fault isolation (one ingestion failure doesn't affect others)
- ✅ Natural horizontal scaling (each job is independent)
- ✅ Observability built-in (job logs, metrics per ingestion)
- ✅ Leverages existing IOMETE infrastructure
- ✅ No single point of failure
- ✅ Easier debugging (one process per run)

**Tradeoff:**
- ❌ Slight increase in infrastructure complexity
- ❌ More moving parts to manage
- ❌ Requires IOMETE Job API integration

---

## 2. Python-Level Scheduler Risks

### 2.1 Risk #1: Single Point of Failure

**Problem:**
All scheduled ingestions run through one Python process. If it crashes, **all schedules stop**.

**Scenario:**
```
10:00 AM - Application starts, 100 ingestions scheduled
10:30 AM - Memory leak causes OOM, process dies
10:30 AM - 11:00 AM - ZERO ingestions run
11:00 AM - Process restarts, schedules resume
Result: 30 minutes of missed ingestions across ALL customers
```

**Impact:**
- SLA violations for all customers simultaneously
- Cascading failures if customers depend on timely data
- Difficult to debug (which ingestion caused the crash?)

**Mitigation Strategies:**

1. **Process Supervision** (Partial Solution)
   ```yaml
   # systemd unit file
   [Service]
   Restart=always
   RestartSec=5s
   ```
   - ✅ Automatic restart on crash
   - ❌ Still loses in-flight jobs
   - ❌ Restart delay (5-30 seconds)

2. **Health Monitoring** (Detection Only)
   ```python
   @app.get("/health/scheduler")
   def scheduler_health():
       if not scheduler.running:
           raise HTTPException(503, "Scheduler down")
   ```
   - ✅ Alerts on failure
   - ❌ Doesn't prevent failure

3. **Multi-Instance Deployment** (Complex)
   - Requires distributed locking (Redis, Zookeeper)
   - Leader election protocol
   - State synchronization
   - **Significant engineering effort** (2-3 weeks)

### 2.2 Risk #2: Resource Contention

**Problem:**
Multiple ingestions compete for shared resources (CPU, memory, database connections, Spark sessions).

**Scenario:**
```
Configured: 10 concurrent threads
Reality:
- 8 long-running ingestions (500GB+ files)
- 2 threads available for new schedules
- 20 schedules trigger at 02:00 (common time)
- 18 schedules delayed, creating backlog
```

**Resource Limits:**

| Resource | Typical Limit | Bottleneck |
|----------|---------------|------------|
| Thread Pool | 10-20 workers | CPU/Memory |
| Database Connections | 20-50 | PostgreSQL max_connections |
| Spark Sessions | 5-10 | IOMETE cluster capacity |
| Memory | 2-8 GB | Python process limit |
| File Descriptors | 1024-4096 | OS limit |

**Cascading Effects:**
```
High load → Slow execution → Longer run times → More overlaps → Higher load
```

**Mitigation Strategies:**

1. **Resource Quotas**
   ```python
   # Limit per-ingestion resources
   job_defaults = {
       'max_instances': 1,  # No overlapping runs
       'coalesce': True,    # Combine missed runs
       'misfire_grace_time': 300
   }
   ```
   - ✅ Prevents runaway resource usage
   - ❌ Causes schedule delays under load

2. **Priority Queues**
   ```python
   # Critical ingestions get priority
   scheduler.add_job(
       func=run_ingestion,
       priority=ingestion.priority  # 1-10
   )
   ```
   - ✅ Protects critical workloads
   - ❌ Starves low-priority jobs

3. **Auto-Scaling** (Not Possible)
   - Can't horizontally scale APScheduler easily
   - Requires distributed architecture

### 2.3 Risk #3: Thundering Herd Problem

**Problem:**
Many ingestions scheduled at the same popular times (midnight, 2 AM, 9 AM) cause resource spikes.

**Real-World Pattern:**
```
Time    | Scheduled Ingestions | Cluster Load
--------|---------------------|-------------
00:00   | 45 jobs             | 🔴 Saturated
01:00   | 3 jobs              | 🟢 Idle
02:00   | 38 jobs             | 🔴 Saturated
03:00   | 2 jobs              | 🟢 Idle
...
```

**Consequences:**
- IOMETE Spark cluster overwhelmed
- Slow query execution for all users
- Scheduler thread pool exhaustion
- Database connection pool exhaustion

**Customer Impact:**
```
Customer A schedules at 02:00, usually runs in 5 min
Today: 29 other jobs also run at 02:00
Result: Customer A's job takes 45 minutes
```

**Mitigation Strategies:**

1. **Schedule Jitter** (Automatic Spreading)
   ```python
   # Add random 0-15 minute delay to each job
   import random

   def add_jitter(cron_time):
       jitter = random.randint(0, 900)  # 0-15 minutes
       return cron_time + timedelta(seconds=jitter)
   ```
   - ✅ Spreads load naturally
   - ❌ User expects exact time execution
   - ❌ May violate SLAs

2. **Rate Limiting** (Queue-Based)
   ```python
   # Max 5 concurrent ingestions
   semaphore = asyncio.Semaphore(5)

   async def run_with_limit(ingestion_id):
       async with semaphore:
           await run_ingestion(ingestion_id)
   ```
   - ✅ Protects cluster
   - ❌ Delays execution (unpredictable)

3. **Capacity Planning** (Reactive)
   - Monitor peak times
   - Scale cluster accordingly
   - ❌ Expensive, wasteful during off-peak

### 2.4 Risk #4: Memory Leaks and Resource Exhaustion

**Problem:**
Long-running Python processes accumulate memory over time, especially with Spark Connect clients.

**Typical Memory Growth:**
```
Hour 0:  500 MB (startup)
Hour 6:  800 MB (normal operations)
Hour 24: 1.2 GB (slow leak)
Hour 48: 1.8 GB (concerning)
Hour 72: 2.5 GB (OOM risk)
```

**Common Leak Sources:**
- Spark session objects not properly closed
- Cached DataFrames in memory
- APScheduler job state accumulation
- SQLAlchemy session leaks
- Log buffers growing unbounded

**Mitigation Strategies:**

1. **Periodic Restarts** (Workaround)
   ```bash
   # Restart every 24 hours
   0 3 * * * systemctl restart autoloader
   ```
   - ✅ Prevents OOM
   - ❌ Brief downtime
   - ❌ Doesn't fix root cause

2. **Memory Monitoring** (Detection)
   ```python
   import psutil

   @app.get("/health/memory")
   def memory_health():
       process = psutil.Process()
       mem_mb = process.memory_info().rss / 1024 / 1024
       if mem_mb > 2048:
           raise HTTPException(503, "Memory exhausted")
   ```

3. **Proper Resource Cleanup** (Prevention)
   ```python
   def run_ingestion(ingestion_id):
       spark = None
       db = None
       try:
           spark = get_spark_session()
           db = get_db_session()
           # ... run ingestion ...
       finally:
           if spark:
               spark.stop()
           if db:
               db.close()
   ```

### 2.5 Risk #5: State Inconsistency

**Problem:**
APScheduler state (job store) can become inconsistent with application state (database) on crashes.

**Inconsistency Scenarios:**

| Scenario | Application DB | APScheduler Job Store | Result |
|----------|----------------|----------------------|--------|
| Schedule created, app crashes before commit | ❌ No record | ✅ Job exists | Ghost job runs |
| Schedule disabled, job store write fails | ✅ Disabled | ✅ Still scheduled | Runs when it shouldn't |
| Schedule updated, partial state sync | ✅ New config | ❌ Old config | Wrong parameters |

**Example:**
```python
# Transaction not atomic across two systems
def update_schedule(ingestion_id, new_cron):
    # Step 1: Update database
    ingestion.schedule_cron = new_cron
    db.commit()  # ✅ Committed

    # Step 2: Update APScheduler
    # --- CRASH HERE ---
    scheduler.update_job(ingestion_id, new_cron)  # ❌ Never executed

    # Result: DB has new_cron, scheduler has old_cron
```

**Mitigation:**

1. **Reconciliation on Startup**
   ```python
   def reconcile_schedules():
       """Sync APScheduler state with database."""
       db_schedules = get_all_enabled_ingestions()
       apscheduler_jobs = scheduler.get_jobs()

       # Remove orphaned jobs
       for job in apscheduler_jobs:
           if job.id not in db_schedules:
               scheduler.remove_job(job.id)

       # Add missing jobs
       for ingestion in db_schedules:
           if not scheduler.get_job(ingestion.id):
               scheduler.schedule_ingestion(ingestion)
   ```

2. **Idempotent Operations**
   ```python
   # Always safe to call multiple times
   scheduler.add_job(..., replace_existing=True)
   ```

### 2.6 Risk #6: Debugging Complexity

**Problem:**
When one of 100+ ingestions fails, difficult to isolate the cause.

**Challenges:**
- Shared logs (all ingestions in one file)
- Stack traces intermixed
- Resource usage attribution (which ingestion is using memory?)
- Reproducing issues (must recreate full environment)

**Example Log Chaos:**
```
2025-11-05 02:00:01 [INFO] Starting ingestion abc-123
2025-11-05 02:00:01 [INFO] Starting ingestion def-456
2025-11-05 02:00:02 [ERROR] S3 connection failed for xyz-789
2025-11-05 02:00:02 [INFO] Starting ingestion ghi-012
2025-11-05 02:00:03 [ERROR] Schema mismatch in abc-123
2025-11-05 02:00:03 [INFO] Completed ingestion def-456
... 1000 more lines ...
```

**Question:** Which error caused which failure?

**Mitigation:**

1. **Structured Logging**
   ```python
   logger.info(
       "Ingestion started",
       extra={
           "ingestion_id": str(ingestion_id),
           "tenant_id": str(tenant_id),
           "run_id": str(run_id)
       }
   )
   ```

2. **Per-Ingestion Log Files** (Complex)
   ```python
   # Create separate log file per run
   log_file = f"/var/log/autoloader/{ingestion_id}/{run_id}.log"
   handler = logging.FileHandler(log_file)
   logger.addHandler(handler)
   ```

---

## 3. Scaling Limits

### 3.1 Theoretical Limits

**APScheduler Capacity:**
- **Job Storage:** 10,000+ jobs (limited by database size)
- **Active Execution:** 10-50 concurrent jobs (limited by thread pool)
- **Scheduling Precision:** 1-second intervals (degrades under load)

**Python Process Limits:**
- **Memory:** 2-8 GB before GC pressure
- **Threads:** 100-200 before context switching overhead
- **File Descriptors:** 1024-4096 (default OS limit)

### 3.2 Practical Limits

Based on production experience with APScheduler:

| Metric | Small Deployment | Medium Deployment | Large Deployment |
|--------|-----------------|-------------------|------------------|
| **Total Ingestions** | < 100 | 100-500 | 500-2000 |
| **Concurrent Runs** | < 5 | 5-20 | 20-50 |
| **Memory Usage** | 500 MB | 1-2 GB | 2-4 GB |
| **CPU Usage (idle)** | < 5% | 5-10% | 10-20% |
| **Reliability** | 🟢 Good | 🟡 Acceptable | 🔴 Problematic |

**Recommendation:**
- ✅ **<100 ingestions:** Python scheduler acceptable
- ⚠️ **100-500 ingestions:** Requires careful monitoring
- ❌ **>500 ingestions:** External orchestration required

### 3.3 Performance Degradation Curve

```
Performance vs. Number of Scheduled Ingestions

100% |████████████▌
     |            ▼ Sweet spot (0-100 ingestions)
 90% |          ▼▼
     |        ▼▼    Performance starts degrading
 80% |      ▼▼
     |    ▼▼        Monitoring essential (100-500)
 70% |  ▼▼
     |▼▼            Critical issues (500+)
 60% └────────────────────────────────
     0   100   500   1000  2000  5000
         Number of Ingestions
```

### 3.4 Scaling Bottlenecks

**1. Thread Pool Exhaustion**
```
Max workers: 10
Active jobs: 10 (all long-running)
New schedule triggers: Blocked until slot available
Wait time: Minutes to hours
```

**2. Database Connection Pool**
```
Max connections: 20
Scheduler: 5 connections
Active jobs: 10 × 2 = 20 connections
Available: 0 (exhausted)
Result: Jobs fail with "connection pool exhausted"
```

**3. Spark Cluster Capacity**
```
IOMETE cluster: 10 executors
Each ingestion: 2 executors
Concurrent ingestions: 5 max
Beyond that: Queued (IOMETE's responsibility)
```

---

## 4. Failure Scenarios

### 4.1 Scenario 1: OOM Crash During Peak

**Timeline:**
```
02:00:00 - 50 ingestions scheduled
02:00:01 - All 50 start (queued in thread pool)
02:05:00 - Memory usage: 3 GB
02:10:00 - Memory usage: 4.5 GB (GC thrashing)
02:12:30 - OOM Kill (Linux kernel)
02:12:31 - Process dies, all 50 jobs aborted
02:13:00 - Process restarts (systemd)
02:13:30 - 50 jobs marked "misfire" (beyond grace period)
Result: Zero of 50 scheduled jobs completed
```

**Customer Impact:**
- All customers with 02:00 schedules miss their SLA
- Data is 24 hours stale (next run tomorrow)
- Manual intervention required to backfill

### 4.2 Scenario 2: Slow Job Blocking Queue

**Timeline:**
```
01:00 - Job A starts (normally 5 min)
01:05 - Job A discovers 10 TB of new files (unexpected)
01:30 - Job A still running (1/10 workers busy)
02:00 - 30 new jobs scheduled
02:00 - 29 jobs wait for 9 workers
02:45 - Job A finally completes
03:00 - Last queued job starts (1 hour late)
```

**Customer Impact:**
- 29 customers experience delays
- No visibility into why (Job A is the culprit)
- Difficulty reproducing (depends on data volume)

### 4.3 Scenario 3: Database Deadlock

**Timeline:**
```
02:00:00 - 10 jobs start simultaneously
02:00:01 - All try to update `runs` table
02:00:02 - PostgreSQL detects deadlock
02:00:02 - 5 jobs succeed, 5 jobs fail with deadlock error
02:00:03 - Failed jobs not retried (APScheduler limitation)
Result: 50% success rate
```

**Root Cause:**
```sql
-- Job 1
BEGIN;
UPDATE runs SET status='RUNNING' WHERE id='run-1';
UPDATE ingestions SET last_run='2025-11-05' WHERE id='ing-1';
-- Job 2 (reverse order)
UPDATE ingestions SET last_run='2025-11-05' WHERE id='ing-2';
UPDATE runs SET status='RUNNING' WHERE id='run-2';
-- DEADLOCK
```

### 4.4 Scenario 4: Config Change Disaster

**Timeline:**
```
10:00 - Operator changes SCHEDULER_MAX_WORKERS from 10 to 50
10:01 - Restart application
10:02 - 50 concurrent jobs start (previous queue released)
10:03 - Database connection pool exhausted (max 20)
10:03 - Spark cluster overwhelmed (max 10 concurrent)
10:05 - Cascading failures, all jobs fail
```

**Lesson:** Configuration changes require careful capacity planning.

---

## 5. Alternative Architectures

### 5.1 Option 1: Python APScheduler (Current Plan)

**Architecture:**
```
┌──────────────────────────────────────┐
│   FastAPI Application                │
│   ┌────────────────────────────┐     │
│   │   APScheduler              │     │
│   │   - 100+ scheduled jobs    │     │
│   │   - Thread pool (10)       │     │
│   │   - Shared DB connections  │     │
│   └────────────────────────────┘     │
└──────────────────────────────────────┘
```

**Pros:**
- ✅ Simple to implement (4-6 hours)
- ✅ No additional infrastructure
- ✅ Good for <100 ingestions
- ✅ Low operational overhead

**Cons:**
- ❌ Single point of failure
- ❌ Limited scalability (100-500 max)
- ❌ Resource contention
- ❌ Difficult debugging
- ❌ No horizontal scaling

**Verdict:** ⚠️ **Acceptable for MVP, risky for production**

### 5.2 Option 2: IOMETE Jobs (Recommended)

**Architecture:**
```
┌──────────────────────────────────────┐
│   FastAPI Application (Lightweight)  │
│   - API only                         │
│   - No scheduling logic              │
└──────────────────────────────────────┘
           │ Creates job
           ▼
┌──────────────────────────────────────┐
│   IOMETE Job Scheduler               │
│   ┌────────────────────────────┐     │
│   │ Job 1: Ingestion A         │     │
│   │ Schedule: 0 2 * * *        │     │
│   └────────────────────────────┘     │
│   ┌────────────────────────────┐     │
│   │ Job 2: Ingestion B         │     │
│   │ Schedule: 0 3 * * *        │     │
│   └────────────────────────────┘     │
└──────────────────────────────────────┘
           │ Each triggers
           ▼
┌──────────────────────────────────────┐
│   Autoloader API                     │
│   POST /ingestions/{id}/run          │
└──────────────────────────────────────┘
```

**How It Works:**
1. User creates ingestion with schedule in Autoloader UI
2. Autoloader creates corresponding IOMETE Job with same schedule
3. IOMETE Job runs on schedule, calls `POST /ingestions/{id}/run`
4. Autoloader executes ingestion as if manually triggered

**Pros:**
- ✅ Natural horizontal scaling (each job independent)
- ✅ Better fault isolation (one failure doesn't affect others)
- ✅ Leverages existing IOMETE infrastructure
- ✅ No single point of failure
- ✅ Built-in observability (IOMETE job logs)
- ✅ No resource contention in Autoloader process
- ✅ Easy debugging (one job = one ingestion)

**Cons:**
- ❌ Requires IOMETE Job API integration (~2 days)
- ❌ More infrastructure complexity
- ❌ Dependency on IOMETE Job scheduler reliability
- ❌ Potential API authentication complexity

**Verdict:** ✅ **Best for production (50+ ingestions)**

### 5.3 Option 3: Kubernetes CronJobs

**Architecture:**
```
┌──────────────────────────────────────┐
│   Kubernetes Cluster                 │
│   ┌────────────────────────────┐     │
│   │ CronJob: ingestion-a       │     │
│   │ Schedule: "0 2 * * *"      │     │
│   │ Command: curl POST /run    │     │
│   └────────────────────────────┘     │
│   ┌────────────────────────────┐     │
│   │ CronJob: ingestion-b       │     │
│   │ Schedule: "0 3 * * *"      │     │
│   └────────────────────────────┘     │
└──────────────────────────────────────┘
```

**Implementation:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: autoloader-ingestion-abc123
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: trigger
            image: curlimages/curl
            command:
            - curl
            - -X POST
            - -H "Authorization: Bearer $TOKEN"
            - http://autoloader-api/api/v1/ingestions/abc-123/run
          restartPolicy: OnFailure
```

**Pros:**
- ✅ Kubernetes-native (if already on K8s)
- ✅ Excellent observability (kubectl logs)
- ✅ Built-in retry logic
- ✅ Horizontal scaling by design
- ✅ No Python process overhead

**Cons:**
- ❌ Creates 1 CronJob per ingestion (500 ingestions = 500 CronJobs)
- ❌ K8s resource overhead (etcd storage, API server load)
- ❌ Complex lifecycle management (sync with database)
- ❌ Not portable (K8s-specific)

**Verdict:** 🟡 **Good if already on K8s, overkill otherwise**

### 5.4 Option 4: External Orchestration (Airflow, Dagster)

**Architecture:**
```
┌──────────────────────────────────────┐
│   Apache Airflow                     │
│   ┌────────────────────────────┐     │
│   │ DAG: autoloader_ingestions │     │
│   │   ├─ ingestion_a           │     │
│   │   ├─ ingestion_b           │     │
│   │   └─ ingestion_c           │     │
│   └────────────────────────────┘     │
└──────────────────────────────────────┘
```

**Pros:**
- ✅ Enterprise-grade features (retries, alerts, SLAs)
- ✅ Excellent observability
- ✅ Workflow DAGs (dependencies between ingestions)
- ✅ Battle-tested at scale

**Cons:**
- ❌ Requires separate Airflow deployment (heavy)
- ❌ Steep learning curve
- ❌ Operational overhead (maintain Airflow cluster)
- ❌ Overkill for simple scheduling

**Verdict:** ❌ **Too heavy for this use case**

---

## 6. Recommended Architecture

### 6.1 Hybrid Approach (Best of Both Worlds)

**Use APScheduler for <100 ingestions, migrate to IOMETE Jobs for scale.**

**Phase 1 (MVP): APScheduler**
- Fast to implement (4-6 hours)
- Sufficient for early customers
- Single-tenant deployments

**Phase 2 (Scale): IOMETE Jobs**
- Triggered when customer reaches 50+ ingestions
- Automatic migration
- Multi-tenant production

**Architecture:**
```
┌─────────────────────────────────────────┐
│   Autoloader API                        │
│   ┌───────────────────────────────┐     │
│   │ SchedulerService              │     │
│   │  - strategy: "apscheduler"    │     │
│   │  - or: "iomete_jobs"          │     │
│   │  - or: "kubernetes"           │     │
│   └───────────────────────────────┘     │
└─────────────────────────────────────────┘
```

**Strategy Pattern:**
```python
class SchedulerStrategy(ABC):
    @abstractmethod
    def schedule_ingestion(self, ingestion: Ingestion):
        pass

    @abstractmethod
    def remove_ingestion(self, ingestion_id: UUID):
        pass

class APSchedulerStrategy(SchedulerStrategy):
    """For small deployments (<100 ingestions)"""
    def schedule_ingestion(self, ingestion):
        self.scheduler.add_job(...)

class IOMETEJobStrategy(SchedulerStrategy):
    """For production scale (100+ ingestions)"""
    def schedule_ingestion(self, ingestion):
        iomete_client.create_job(
            name=f"autoloader-{ingestion.id}",
            schedule=ingestion.schedule_cron,
            script=f"curl POST /ingestions/{ingestion.id}/run"
        )

# Configuration
scheduler_strategy = (
    APSchedulerStrategy() if num_ingestions < 100
    else IOMETEJobStrategy()
)
```

### 6.2 Decision Tree

```
Start
  │
  ├─ Total ingestions < 100?
  │  └─ YES → Use APScheduler (simple, fast)
  │  └─ NO  → ↓
  │
  ├─ Already on Kubernetes?
  │  └─ YES → Use K8s CronJobs
  │  └─ NO  → ↓
  │
  ├─ IOMETE Jobs available?
  │  └─ YES → Use IOMETE Jobs (recommended)
  │  └─ NO  → ↓
  │
  └─ Complex workflows (DAGs)?
     └─ YES → Use Airflow/Dagster
     └─ NO  → Use APScheduler (with monitoring)
```

### 6.3 Migration Example

**Start with APScheduler:**
```python
# Initial implementation
scheduler = APSchedulerStrategy()
scheduler.schedule_ingestion(ingestion)
```

**Migrate to IOMETE Jobs (seamless):**
```python
# In config
SCHEDULER_STRATEGY = "iomete_jobs"  # Changed from "apscheduler"

# No code changes needed
scheduler = get_scheduler_strategy()  # Returns IOMETEJobStrategy
scheduler.schedule_ingestion(ingestion)  # Same interface
```

**Zero downtime migration:**
1. Deploy new version with `SCHEDULER_STRATEGY=iomete_jobs`
2. Existing APScheduler jobs continue running
3. New ingestions use IOMETE Jobs
4. Gradually migrate old schedules in background
5. Remove APScheduler once all migrated

---

## 7. Migration Path

### 7.1 APScheduler → IOMETE Jobs Migration

**Step 1: Detect Need for Migration**
```python
def should_migrate_to_iomete_jobs() -> bool:
    total_ingestions = db.query(Ingestion).filter(
        Ingestion.schedule_enabled == True
    ).count()

    return total_ingestions >= 100
```

**Step 2: Create IOMETE Job per Ingestion**
```python
async def migrate_ingestion_to_iomete(ingestion: Ingestion):
    # Create IOMETE job
    iomete_job = await iomete_client.create_job(
        name=f"autoloader-ingestion-{ingestion.id}",
        schedule=ingestion.schedule_cron,
        timezone=ingestion.schedule_timezone,
        script=f"""
#!/bin/bash
curl -X POST \
  -H "Authorization: Bearer $AUTOLOADER_TOKEN" \
  -H "Content-Type: application/json" \
  {settings.AUTOLOADER_API_URL}/api/v1/ingestions/{ingestion.id}/run
        """,
        cluster_id=ingestion.cluster_id
    )

    # Store IOMETE job ID
    ingestion.iomete_job_id = iomete_job.id
    db.commit()

    # Remove APScheduler job
    apscheduler.remove_job(f"ingestion_{ingestion.id}")
```

**Step 3: Gradual Migration**
```python
async def migrate_all_schedules():
    """Migrate all APScheduler jobs to IOMETE Jobs."""
    ingestions = db.query(Ingestion).filter(
        Ingestion.schedule_enabled == True,
        Ingestion.iomete_job_id == None  # Not yet migrated
    ).all()

    for ingestion in ingestions:
        try:
            await migrate_ingestion_to_iomete(ingestion)
            logger.info(f"Migrated ingestion {ingestion.id}")
        except Exception as e:
            logger.error(f"Failed to migrate {ingestion.id}: {e}")

    logger.info(f"Migration complete: {len(ingestions)} ingestions")
```

### 7.2 Rollback Plan

**If IOMETE Jobs have issues, rollback to APScheduler:**

```python
async def rollback_to_apscheduler(ingestion: Ingestion):
    # Delete IOMETE job
    if ingestion.iomete_job_id:
        await iomete_client.delete_job(ingestion.iomete_job_id)
        ingestion.iomete_job_id = None

    # Recreate APScheduler job
    apscheduler.schedule_ingestion(ingestion)

    db.commit()
```

---

## 8. Decision Matrix

### 8.1 Feature Comparison

| Feature | APScheduler | IOMETE Jobs | K8s CronJobs | Airflow |
|---------|------------|-------------|--------------|---------|
| **Implementation Time** | 4-6 hours | 1-2 days | 2-3 days | 1-2 weeks |
| **Max Ingestions** | 100-500 | 10,000+ | 1,000+ | 100,000+ |
| **Fault Isolation** | ❌ None | ✅ Full | ✅ Full | ✅ Full |
| **Horizontal Scaling** | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |
| **Observability** | 🟡 Logs only | ✅ UI + Logs | ✅ kubectl | ✅ Rich UI |
| **Operational Overhead** | ✅ Low | 🟡 Medium | 🟡 Medium | 🔴 High |
| **Dependencies** | None | IOMETE | Kubernetes | Airflow cluster |
| **Debugging** | 🔴 Hard | 🟡 Medium | ✅ Easy | ✅ Easy |
| **Cost** | ✅ Free | ✅ Free | 🟡 K8s costs | 🔴 Expensive |

### 8.2 Use Case Recommendation

| Use Case | Recommended Solution | Reasoning |
|----------|---------------------|-----------|
| **MVP / Prototype** | APScheduler | Fast, simple, no dependencies |
| **Small Deployments (<50)** | APScheduler | Sufficient, low overhead |
| **Medium Deployments (50-500)** | IOMETE Jobs | Better isolation, scalability |
| **Large Deployments (500+)** | IOMETE Jobs or K8s | Required for stability |
| **Enterprise / SaaS** | IOMETE Jobs | Multi-tenancy, observability |
| **Already on K8s** | K8s CronJobs | Native integration |
| **Complex Workflows** | Airflow | DAG support |

---

## 9. Implementation Recommendations

### 9.1 Immediate Actions (Next Sprint)

1. **Implement APScheduler** (as planned)
   - Sufficient for MVP
   - Low risk for small deployments
   - Fast time-to-market

2. **Add Telemetry**
   ```python
   # Monitor for migration trigger
   prometheus_client.Gauge(
       'autoloader_scheduled_ingestions_total',
       'Total scheduled ingestions'
   )

   # Alert if > 80
   alert: ScheduledIngestionsHigh
     expr: autoloader_scheduled_ingestions_total > 80
     for: 1h
     annotations:
       summary: "Consider migrating to IOMETE Jobs"
   ```

3. **Design Strategy Pattern**
   - Abstract scheduler behind interface
   - Easy to swap implementations later
   - No code changes in services

### 9.2 Medium-Term (2-3 Months)

1. **Implement IOMETE Jobs Strategy**
   - When first customer reaches 50 ingestions
   - Or after 3 months of production experience
   - Parallel implementation (doesn't disrupt existing)

2. **Migration Tool**
   - CLI command: `autoloader migrate-scheduler`
   - Automated migration script
   - Validation and rollback

3. **Documentation**
   - Operational runbook for scheduler
   - Migration guide
   - Troubleshooting guide

### 9.3 Long-Term (6+ Months)

1. **Deprecate APScheduler**
   - Once IOMETE Jobs proven stable
   - All customers migrated
   - Remove code complexity

2. **Advanced Features**
   - Workflow DAGs (ingestion dependencies)
   - Dynamic scheduling (data-driven)
   - Cost optimization (off-peak scheduling)

---

## 10. Production Recommendations

### 10.1 If Using APScheduler

**Must-Have Safeguards:**

1. **Resource Limits**
   ```python
   SCHEDULER_MAX_WORKERS = 10  # Don't increase without testing
   SCHEDULER_MAX_INGESTIONS = 100  # Hard limit
   ```

2. **Monitoring**
   ```python
   # Alert on critical metrics
   - Memory usage > 2 GB
   - Thread pool saturation > 80%
   - Misfire rate > 5%
   - Average job duration > 30 min
   ```

3. **Health Checks**
   ```python
   @app.get("/health/scheduler")
   def health():
       return {
           "running": scheduler.running,
           "active_jobs": len(scheduler.get_jobs()),
           "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024
       }
   ```

4. **Automatic Restart**
   ```ini
   [Service]
   Restart=always
   RestartSec=10
   MemoryMax=2G
   ```

5. **Operational Procedures**
   - Daily memory usage review
   - Weekly schedule distribution analysis
   - Monthly capacity planning review

### 10.2 If Using IOMETE Jobs

**Must-Have Features:**

1. **Job Lifecycle Management**
   ```python
   # Sync ingestion state → IOMETE job
   def sync_schedule_to_iomete(ingestion):
       if ingestion.schedule_enabled:
           create_or_update_iomete_job(ingestion)
       else:
           delete_iomete_job(ingestion.iomete_job_id)
   ```

2. **Failure Handling**
   ```python
   # IOMETE job calls webhook on failure
   @app.post("/webhooks/job-failed")
   def job_failed(job_id: str):
       ingestion = find_ingestion_by_job_id(job_id)
       send_alert(ingestion.owner_email)
   ```

3. **Cost Tracking**
   ```python
   # Track IOMETE job costs per ingestion
   def calculate_monthly_cost(ingestion_id):
       runs = get_runs_last_30_days(ingestion_id)
       return sum(run.spark_cost for run in runs)
   ```

---

## 11. Conclusion

### 11.1 Recommendation Summary

**For Production IOMETE Autoloader:**

✅ **Phase 1 (MVP):** Implement APScheduler
- Fast to market (4-6 hours)
- Sufficient for early customers
- Document known limitations

✅ **Phase 2 (Scale):** Migrate to IOMETE Jobs
- Triggered at 50-100 ingestions
- Better fault isolation and scalability
- Leverages existing infrastructure

❌ **Avoid:** Complex solutions (Airflow, distributed APScheduler)
- Over-engineering for current needs
- High operational overhead
- Longer time to market

### 11.2 Key Takeaways

1. **Python-level scheduling is risky** for production scale (>100 ingestions)
2. **Single point of failure** is the biggest concern
3. **Resource contention** affects all customers simultaneously
4. **IOMETE Jobs provides best balance** of simplicity and scalability
5. **Strategy pattern enables migration** without code rewrites

### 11.3 Success Metrics

**Track these to know when to migrate:**

| Metric | Threshold | Action |
|--------|-----------|--------|
| Total scheduled ingestions | > 80 | Plan migration to IOMETE Jobs |
| Memory usage | > 2 GB | Investigate leaks |
| Misfire rate | > 5% | Reduce max_workers or migrate |
| Average job duration | > 30 min | Optimize or increase workers |
| Concurrent peak | > 8/10 workers | Scale horizontally or migrate |

---

**End of Document**

**Related Documents:**
- `scheduler-implementation-guide.md` - Implementation details
- `batch-processing-implementation-guide.md` - Execution logic
- `architecture-decision-file-tracking.md` - System architecture

**Decision Record:**
- **Status:** ✅ Recommended
- **Decision:** Implement APScheduler for MVP, design for IOMETE Jobs migration
- **Rationale:** Balance speed-to-market with production scalability
- **Review Date:** After 50 ingestions in production
