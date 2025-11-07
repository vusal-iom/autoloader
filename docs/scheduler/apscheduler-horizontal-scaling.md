# APScheduler Horizontal Scaling Implementation Guide

**Document Version:** 1.0
**Created:** 2025-11-07
**Status:** Technical Analysis
**Related:** `scheduler-scaling-and-risks.md`, `scheduler-implementation-guide.md`

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [What is Horizontal Scaling?](#what-is-horizontal-scaling)
3. [APScheduler's Native Limitations](#apschedulers-native-limitations)
4. [Distributed APScheduler Architecture](#distributed-apscheduler-architecture)
5. [Implementation Requirements](#implementation-requirements)
6. [Technical Challenges](#technical-challenges)
7. [Implementation Steps](#implementation-steps)
8. [Code Examples](#code-examples)
9. [Failure Scenarios](#failure-scenarios)
10. [Operational Complexity](#operational-complexity)
11. [Cost-Benefit Analysis](#cost-benefit-analysis)
12. [Alternative Comparison](#alternative-comparison)
13. [Recommendation](#recommendation)

---

## 1. Executive Summary

### 1.1 What We Want to Achieve

**Goal:** Run multiple instances of the Autoloader application simultaneously, each capable of executing scheduled ingestions, providing:

- **High Availability:** If one instance crashes, others continue
- **Load Distribution:** Spread 500+ ingestions across multiple processes
- **Fault Isolation:** One instance's failure doesn't affect others
- **Horizontal Scalability:** Add more instances as load increases

### 1.2 Key Findings

| Aspect | Summary |
|--------|---------|
| **Feasibility** | ✅ Technically possible |
| **Complexity** | 🔴 HIGH - Requires distributed systems expertise |
| **Implementation Time** | 🔴 2-3 weeks initial + ongoing maintenance |
| **Risk** | 🔴 HIGH - Race conditions, split-brain, data corruption |
| **Operational Overhead** | 🔴 HIGH - Redis/ZooKeeper, monitoring, debugging |
| **Benefit vs Cost** | 🟡 QUESTIONABLE - Simpler alternatives exist |

### 1.3 Bottom Line

**Horizontal scaling APScheduler is possible but expensive to build and maintain.**

**Tradeoff:**
- ✅ PRO: Keeps familiar APScheduler API
- ❌ CON: Introduces distributed systems complexity
- ❌ CON: 2-3 weeks engineering effort
- ❌ CON: Ongoing operational burden
- ❌ CON: New failure modes (split-brain, lock contention)

**Alternative:** IOMETE Jobs or Kubernetes CronJobs provide horizontal scaling "for free" without these costs.

---

## 2. What is Horizontal Scaling?

### 2.1 Single Instance (Current)

```
┌─────────────────────────────────┐
│   Autoloader Instance 1         │
│   ┌─────────────────────────┐   │
│   │ APScheduler             │   │
│   │ - 100 scheduled jobs    │   │
│   │ - 10 worker threads     │   │
│   └─────────────────────────┘   │
└─────────────────────────────────┘
         ↓
   Single Point of Failure
```

**Problem:**
- If this instance dies, ALL schedules stop
- Limited by single process resources (memory, CPU, threads)

### 2.2 Horizontal Scaling (Goal)

```
┌─────────────────────────────────┐
│   Autoloader Instance 1         │
│   ┌─────────────────────────┐   │
│   │ APScheduler             │   │
│   │ - 50 jobs (leader)      │   │
│   └─────────────────────────┘   │
└─────────────────────────────────┘

┌─────────────────────────────────┐
│   Autoloader Instance 2         │
│   ┌─────────────────────────┐   │
│   │ APScheduler             │   │
│   │ - 0 jobs (standby)      │   │
│   └─────────────────────────┘   │
└─────────────────────────────────┘

┌─────────────────────────────────┐
│   Autoloader Instance 3         │
│   ┌─────────────────────────┐   │
│   │ APScheduler             │   │
│   │ - 50 jobs (co-leader)   │   │
│   └─────────────────────────┘   │
└─────────────────────────────────┘

         ↓
   Shared Coordination Layer
   (Redis, ZooKeeper, etcd)
```

**Benefit:**
- Instance 1 dies → Instance 2 takes over
- Load distributed across multiple processes
- Horizontal growth (add more instances)

---

## 3. APScheduler's Native Limitations

### 3.1 Why APScheduler Doesn't Scale Horizontally Out-of-the-Box

APScheduler was **designed for single-process use**. It has no built-in support for:

1. **Job Distribution Across Instances**
   - Each instance maintains its own in-memory scheduler
   - No coordination between instances

2. **Job Ownership**
   - No concept of "which instance owns which job"
   - Multiple instances would execute the same job simultaneously

3. **Failover**
   - No automatic reassignment of jobs when instance dies
   - No leader election

4. **Distributed Locking**
   - No mechanism to prevent duplicate execution
   - Race conditions on shared job store

### 3.2 What Happens Without Coordination?

**Scenario: Two instances, no coordination**

```python
# Instance 1
scheduler.add_job(run_ingestion, trigger='cron', args=['abc-123'], cron='0 2 * * *')

# Instance 2 (same config)
scheduler.add_job(run_ingestion, trigger='cron', args=['abc-123'], cron='0 2 * * *')

# Result at 02:00:
# BOTH instances execute the same job!
# Duplicate processing, wasted resources, potential data corruption
```

**This is why we need distributed coordination.**

---

## 4. Distributed APScheduler Architecture

### 4.1 High-Level Architecture

```
┌────────────────────────────────────────────────────────────┐
│                   Load Balancer (Optional)                 │
└────────────────────────────────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         ▼                  ▼                  ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Instance 1     │ │  Instance 2     │ │  Instance 3     │
│  (Leader)       │ │  (Follower)     │ │  (Follower)     │
│                 │ │                 │ │                 │
│  APScheduler    │ │  APScheduler    │ │  APScheduler    │
│  - 50 jobs      │ │  - Standby      │ │  - 50 jobs      │
└─────────────────┘ └─────────────────┘ └─────────────────┘
         │                  │                  │
         └──────────────────┼──────────────────┘
                            ▼
                ┌───────────────────────┐
                │   Coordination Layer  │
                │   (Redis / ZooKeeper) │
                │                       │
                │   - Leader election   │
                │   - Job locks         │
                │   - Job assignments   │
                └───────────────────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │   Shared Job Store    │
                │   (PostgreSQL)        │
                │                       │
                │   - Job metadata      │
                │   - Execution history │
                └───────────────────────┘
```

### 4.2 Key Components

#### A. Coordination Layer (Redis or ZooKeeper)

**Purpose:** Distributed coordination, locking, and leader election

**Responsibilities:**
- **Leader Election:** Determine which instance(s) schedule jobs
- **Distributed Locks:** Prevent duplicate job execution
- **Job Assignment:** Assign jobs to specific instances
- **Health Monitoring:** Detect dead instances and reassign jobs

**Technology Options:**

| Technology | Pros | Cons |
|------------|------|------|
| **Redis** | Fast, simple, familiar | Not designed for coordination (can have edge cases) |
| **ZooKeeper** | Battle-tested for coordination | Complex setup, JVM dependency |
| **etcd** | Modern, Kubernetes-native | Another service to maintain |
| **Consul** | Service discovery built-in | Learning curve |

**Recommendation:** Redis with Redlock algorithm (good balance of simplicity and reliability)

#### B. Shared Job Store (PostgreSQL)

APScheduler supports PostgreSQL as a job store:

```python
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': SQLAlchemyJobStore(url='postgresql://...')
}
scheduler = BackgroundScheduler(jobstores=jobstores)
```

**Benefits:**
- Persistent job state across restarts
- Shared view of all jobs across instances

**Challenges:**
- Race conditions on job updates
- Requires pessimistic locking
- Performance bottleneck under high concurrency

#### C. Distributed Lock Manager

**Purpose:** Ensure only ONE instance executes each job

**Implementation Pattern:**
```python
import redis
from contextlib import contextmanager

redis_client = redis.Redis(host='localhost', port=6379)

@contextmanager
def distributed_lock(job_id: str, timeout: int = 60):
    """Acquire distributed lock for job execution."""
    lock_key = f"autoloader:job_lock:{job_id}"
    lock_acquired = redis_client.set(lock_key, "locked", nx=True, ex=timeout)

    if not lock_acquired:
        raise Exception(f"Job {job_id} already running on another instance")

    try:
        yield
    finally:
        redis_client.delete(lock_key)

# Usage
def run_ingestion_with_lock(ingestion_id: str):
    with distributed_lock(ingestion_id):
        # Only executes if lock acquired
        run_ingestion(ingestion_id)
```

#### D. Leader Election

**Purpose:** Determine which instance(s) actively schedule jobs

**Options:**

**Option 1: Single Leader (Active-Passive)**
```
Instance 1 (Leader)  → Schedules all jobs
Instance 2 (Standby) → Does nothing, waits for failover
Instance 3 (Standby) → Does nothing, waits for failover
```

- ✅ Simple: No job coordination needed
- ❌ Wastes resources (standbys idle)
- ❌ Still a single point of failure (until failover)

**Option 2: Multiple Leaders (Active-Active)**
```
Instance 1 → Schedules jobs 1-100
Instance 2 → Schedules jobs 101-200
Instance 3 → Schedules jobs 201-300
```

- ✅ True horizontal scaling
- ✅ Load distributed
- ❌ Complex: Job assignment and rebalancing required

**Implementation (Redis-based leader election):**

```python
import redis
import uuid
import time
from threading import Thread

class LeaderElection:
    def __init__(self, redis_client: redis.Redis, instance_id: str):
        self.redis = redis_client
        self.instance_id = instance_id
        self.leader_key = "autoloader:leader"
        self.lease_duration = 10  # seconds
        self.is_leader = False

    def start(self):
        """Start leader election in background thread."""
        thread = Thread(target=self._election_loop, daemon=True)
        thread.start()

    def _election_loop(self):
        """Continuously try to become/remain leader."""
        while True:
            try:
                # Try to become leader
                became_leader = self.redis.set(
                    self.leader_key,
                    self.instance_id,
                    nx=True,  # Only if key doesn't exist
                    ex=self.lease_duration
                )

                if became_leader:
                    self.is_leader = True
                    print(f"Instance {self.instance_id} became leader")
                else:
                    # Check if current leader is still alive
                    current_leader = self.redis.get(self.leader_key)
                    if current_leader == self.instance_id.encode():
                        # Still leader, renew lease
                        self.redis.expire(self.leader_key, self.lease_duration)
                        self.is_leader = True
                    else:
                        self.is_leader = False

                time.sleep(self.lease_duration / 2)  # Renew halfway

            except Exception as e:
                print(f"Leader election error: {e}")
                self.is_leader = False
                time.sleep(5)

# Usage
instance_id = str(uuid.uuid4())
election = LeaderElection(redis_client, instance_id)
election.start()

# Only schedule jobs if leader
if election.is_leader:
    scheduler.start()
else:
    scheduler.shutdown()
```

---

## 5. Implementation Requirements

### 5.1 Infrastructure Requirements

**New Services:**
1. **Redis Cluster** (or ZooKeeper)
   - High availability (3+ nodes)
   - Persistent storage
   - Monitoring and alerts

2. **Load Balancer**
   - Distribute API traffic across instances
   - Health checks

3. **Shared PostgreSQL**
   - Already required for application
   - APScheduler job store in same DB

**Deployment:**
```yaml
# docker-compose.yml (simplified)
services:
  autoloader-1:
    image: autoloader:latest
    environment:
      INSTANCE_ID: instance-1
      REDIS_URL: redis://redis:6379
      DATABASE_URL: postgresql://...

  autoloader-2:
    image: autoloader:latest
    environment:
      INSTANCE_ID: instance-2
      REDIS_URL: redis://redis:6379
      DATABASE_URL: postgresql://...

  autoloader-3:
    image: autoloader:latest
    environment:
      INSTANCE_ID: instance-3
      REDIS_URL: redis://redis:6379
      DATABASE_URL: postgresql://...

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

  postgres:
    image: postgres:15
    volumes:
      - postgres_data:/var/lib/postgresql/data
```

### 5.2 Code Requirements

**New Modules:**

```
app/
├── services/
│   ├── scheduler/
│   │   ├── distributed_scheduler.py       # Main orchestrator
│   │   ├── leader_election.py             # Leader election logic
│   │   ├── distributed_lock.py            # Job execution locks
│   │   ├── job_assignment.py              # Job-to-instance assignment
│   │   └── health_monitor.py              # Instance health tracking
│   └── ...
└── ...
```

**Estimated Lines of Code:** 1,500-2,000 LOC

### 5.3 Configuration Requirements

**New Settings:**

```python
# app/config.py additions

class Settings(BaseSettings):
    # ... existing settings ...

    # Distributed Scheduler
    SCHEDULER_MODE: str = "single"  # "single" or "distributed"
    INSTANCE_ID: str = Field(default_factory=lambda: str(uuid.uuid4()))

    # Redis Coordination
    REDIS_URL: str = "redis://localhost:6379"
    REDIS_LEADER_KEY: str = "autoloader:leader"
    REDIS_LOCK_PREFIX: str = "autoloader:job_lock"

    # Leader Election
    LEADER_LEASE_DURATION: int = 10  # seconds
    LEADER_ELECTION_INTERVAL: int = 5  # seconds

    # Job Assignment
    JOB_ASSIGNMENT_STRATEGY: str = "round_robin"  # or "consistent_hashing"
    MAX_JOBS_PER_INSTANCE: int = 100

    # Health Monitoring
    INSTANCE_HEARTBEAT_INTERVAL: int = 5  # seconds
    INSTANCE_TIMEOUT: int = 30  # seconds (3x heartbeat)
```

---

## 6. Technical Challenges

### 6.1 Challenge #1: Split-Brain Scenario

**Problem:** Network partition causes multiple leaders

**Scenario:**
```
Time 0: Instance 1 is leader
Time 1: Network partition separates Instance 1 from Redis
Time 2: Instance 1 thinks it's still leader (hasn't detected partition)
Time 3: Instance 2 becomes leader (Redis lease expired)
Time 4: BOTH instances schedule jobs
Result: Duplicate executions
```

**Mitigation:**

1. **Fencing Tokens** (Advanced)
   ```python
   # Every action requires incrementing token
   def schedule_job_with_fencing(job_id):
       token = redis.incr("autoloader:fencing_token")
       # Include token in all operations
       # Reject operations with old tokens
   ```

2. **Pessimistic Locking**
   ```python
   # Always check Redis before scheduling
   def schedule_job_safe(job_id):
       if not am_i_still_leader():
           raise NotLeaderException()
       # Proceed with scheduling
   ```

3. **Idempotent Operations**
   ```python
   # Make duplicate execution safe
   def run_ingestion_idempotent(ingestion_id, run_id):
       # Check if run_id already processed
       if run_repository.exists(run_id):
           return  # Already done
       # Proceed with execution
   ```

### 6.2 Challenge #2: Job Assignment and Rebalancing

**Problem:** How to distribute 500 jobs across 3 instances fairly?

**Strategy 1: Round Robin**
```python
def assign_job_to_instance(job_id: str, num_instances: int) -> int:
    """Simple but doesn't handle failures well."""
    job_hash = hash(job_id)
    return job_hash % num_instances

# Job abc-123 → Instance (hash(abc-123) % 3) = Instance 1
```

**Challenge:** If Instance 1 dies, Job abc-123 is orphaned until rebalance

**Strategy 2: Consistent Hashing** (Better)
```python
import hashlib
from typing import List

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        self.ring = {}
        self.sorted_keys = []

        for node in nodes:
            for i in range(virtual_nodes):
                key = self._hash(f"{node}:{i}")
                self.ring[key] = node

        self.sorted_keys = sorted(self.ring.keys())

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def get_node(self, item: str) -> str:
        """Find instance responsible for this job."""
        item_hash = self._hash(item)

        for key in self.sorted_keys:
            if item_hash <= key:
                return self.ring[key]

        return self.ring[self.sorted_keys[0]]

# Usage
instances = ["instance-1", "instance-2", "instance-3"]
hash_ring = ConsistentHash(instances)

job_id = "abc-123"
assigned_instance = hash_ring.get_node(job_id)
# Only assigned_instance schedules this job
```

**Benefit:** When instance dies, only ~33% of jobs need reassignment (not 100%)

**Challenge:** Still need to detect dead instances and trigger rebalance

### 6.3 Challenge #3: Job Lock Contention

**Problem:** High concurrency on Redis locks

**Scenario:**
```
02:00:00 - 50 jobs trigger simultaneously
02:00:00 - All instances try to acquire locks
02:00:00 - Redis receives 150 SET operations (3 instances × 50 jobs)
02:00:01 - Lock contention causes delays
```

**Mitigation:**

1. **Lock Batching**
   ```python
   # Acquire multiple locks in one operation (Lua script)
   lua_script = """
   local locks = {}
   for i, key in ipairs(KEYS) do
       if redis.call('set', key, 'locked', 'NX', 'EX', ARGV[1]) then
           table.insert(locks, key)
       end
   end
   return locks
   """

   acquired = redis.eval(lua_script, len(job_ids), *job_ids, timeout)
   ```

2. **Job Priority Queues**
   ```python
   # High-priority jobs get locks first
   jobs = sorted(jobs, key=lambda j: j.priority, reverse=True)
   for job in jobs:
       try_acquire_lock(job.id)
   ```

### 6.4 Challenge #4: Failure Detection and Recovery

**Problem:** How quickly can we detect a dead instance and reassign its jobs?

**Timeline:**
```
02:00:00 - Instance 1 crashes
02:00:05 - Last heartbeat expires (5 sec interval)
02:00:15 - Health monitor detects failure (10 sec timeout)
02:00:20 - Leader election triggered
02:00:25 - Instance 2 becomes leader
02:00:30 - Jobs reassigned and scheduled
Total Downtime: 30 seconds
```

**Tradeoff:**
- Shorter timeout → Faster recovery, more false positives
- Longer timeout → Fewer false positives, slower recovery

**Implementation:**

```python
class HealthMonitor:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.instance_id = get_instance_id()
        self.heartbeat_interval = 5
        self.timeout = 15

    def start_heartbeat(self):
        """Send periodic heartbeats."""
        def heartbeat_loop():
            while True:
                self.redis.setex(
                    f"autoloader:heartbeat:{self.instance_id}",
                    self.timeout,
                    int(time.time())
                )
                time.sleep(self.heartbeat_interval)

        Thread(target=heartbeat_loop, daemon=True).start()

    def get_alive_instances(self) -> List[str]:
        """Get list of healthy instances."""
        keys = self.redis.keys("autoloader:heartbeat:*")
        return [key.decode().split(":")[-1] for key in keys]

    def detect_failures(self):
        """Check for dead instances and trigger reassignment."""
        all_instances = get_registered_instances()  # From DB
        alive_instances = self.get_alive_instances()
        dead_instances = set(all_instances) - set(alive_instances)

        if dead_instances:
            reassign_jobs(dead_instances)
```

### 6.5 Challenge #5: Database Deadlocks

**Problem:** Multiple instances updating same rows

**Scenario:**
```sql
-- Instance 1
BEGIN;
UPDATE ingestions SET last_run = NOW() WHERE id = 'abc-123';
UPDATE runs SET status = 'COMPLETED' WHERE id = 'run-1';

-- Instance 2 (simultaneous)
UPDATE runs SET status = 'COMPLETED' WHERE id = 'run-2';
UPDATE ingestions SET last_run = NOW() WHERE id = 'abc-123';

-- DEADLOCK: Instance 1 waits for Instance 2, Instance 2 waits for Instance 1
```

**Mitigation:**

1. **Consistent Lock Ordering**
   ```python
   # Always acquire locks in same order
   def update_ingestion_and_run(ingestion_id, run_id):
       # Sort IDs to ensure consistent order
       ids = sorted([ingestion_id, run_id])

       # Lock in order
       db.execute(f"SELECT * FROM ingestions WHERE id = '{ids[0]}' FOR UPDATE")
       db.execute(f"SELECT * FROM runs WHERE id = '{ids[1]}' FOR UPDATE")

       # Now safe to update
   ```

2. **Row-Level Locking**
   ```python
   # Use SELECT FOR UPDATE SKIP LOCKED
   result = db.execute(
       "SELECT * FROM ingestions WHERE id = %s FOR UPDATE SKIP LOCKED",
       (ingestion_id,)
   )
   if not result:
       # Another instance already locked this row
       return
   ```

3. **Optimistic Locking**
   ```python
   # Use version column
   class Ingestion(Base):
       version = Column(Integer, default=0)

   def update_ingestion(ingestion):
       old_version = ingestion.version
       ingestion.last_run = datetime.now()
       ingestion.version += 1

       rows_updated = db.execute(
           "UPDATE ingestions SET last_run = %s, version = %s "
           "WHERE id = %s AND version = %s",
           (ingestion.last_run, ingestion.version, ingestion.id, old_version)
       )

       if rows_updated == 0:
           raise ConcurrentModificationException()
   ```

---

## 7. Implementation Steps

### 7.1 Phase 1: Infrastructure Setup (Week 1)

**Step 1: Deploy Redis**
```bash
# docker-compose.yml
redis:
  image: redis:7-alpine
  command: redis-server --appendonly yes
  volumes:
    - redis_data:/data
  ports:
    - "6379:6379"
```

**Step 2: Configure APScheduler Job Store**
```python
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': SQLAlchemyJobStore(url=settings.DATABASE_URL)
}

scheduler = BackgroundScheduler(
    jobstores=jobstores,
    job_defaults={
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 300
    }
)
```

**Step 3: Implement Distributed Lock**
```python
# app/services/scheduler/distributed_lock.py
# (See code example in section 4.2.C)
```

**Step 4: Implement Leader Election**
```python
# app/services/scheduler/leader_election.py
# (See code example in section 4.2.D)
```

### 7.2 Phase 2: Job Distribution (Week 2)

**Step 5: Implement Job Assignment**
```python
# app/services/scheduler/job_assignment.py

class JobAssignmentService:
    def __init__(self):
        self.hash_ring = None

    def initialize(self, instances: List[str]):
        """Initialize consistent hash ring."""
        self.hash_ring = ConsistentHash(instances)

    def should_i_schedule_job(self, job_id: str, my_instance_id: str) -> bool:
        """Check if this instance should schedule the job."""
        assigned_instance = self.hash_ring.get_node(job_id)
        return assigned_instance == my_instance_id

    def rebalance(self, dead_instances: List[str]):
        """Reassign jobs from dead instances."""
        alive_instances = [
            i for i in get_all_instances()
            if i not in dead_instances
        ]
        self.hash_ring = ConsistentHash(alive_instances)

# Usage in scheduler service
job_assignment = JobAssignmentService()

def schedule_ingestion(ingestion: Ingestion):
    if job_assignment.should_i_schedule_job(str(ingestion.id), instance_id):
        scheduler.add_job(
            run_ingestion_with_lock,
            args=[ingestion.id],
            trigger='cron',
            **parse_cron(ingestion.schedule_cron)
        )
```

**Step 6: Implement Health Monitoring**
```python
# app/services/scheduler/health_monitor.py
# (See code example in section 6.4)
```

**Step 7: Wrap Job Execution with Locks**
```python
def run_ingestion_with_lock(ingestion_id: UUID):
    """Execute ingestion with distributed lock."""
    try:
        with distributed_lock(str(ingestion_id), timeout=3600):
            # Lock acquired, safe to run
            run_ingestion(ingestion_id)
    except LockAcquisitionException:
        # Another instance is running this job
        logger.info(f"Ingestion {ingestion_id} already running on another instance")
    except Exception as e:
        logger.error(f"Ingestion {ingestion_id} failed: {e}")
        raise
```

### 7.3 Phase 3: Integration and Testing (Week 3)

**Step 8: Integrate Components**
```python
# app/services/scheduler/distributed_scheduler.py

class DistributedSchedulerService:
    def __init__(self):
        self.scheduler = BackgroundScheduler(...)
        self.redis = redis.Redis.from_url(settings.REDIS_URL)
        self.instance_id = settings.INSTANCE_ID

        self.leader_election = LeaderElection(self.redis, self.instance_id)
        self.job_assignment = JobAssignmentService()
        self.health_monitor = HealthMonitor(self.redis)

    def start(self):
        """Start distributed scheduler."""
        # Start health monitoring
        self.health_monitor.start_heartbeat()

        # Start leader election
        self.leader_election.start()

        # Wait to become leader or get assignment
        while not self.leader_election.is_leader:
            time.sleep(1)

        # Load jobs for this instance
        self.load_assigned_jobs()

        # Start scheduler
        self.scheduler.start()

        # Monitor for rebalancing
        self.monitor_cluster()

    def load_assigned_jobs(self):
        """Load only jobs assigned to this instance."""
        all_ingestions = ingestion_repository.get_scheduled_ingestions()

        for ingestion in all_ingestions:
            if self.job_assignment.should_i_schedule_job(
                str(ingestion.id),
                self.instance_id
            ):
                self.schedule_ingestion(ingestion)

    def monitor_cluster(self):
        """Monitor for instance failures and rebalance."""
        def monitor_loop():
            while True:
                dead_instances = self.health_monitor.detect_failures()
                if dead_instances:
                    self.job_assignment.rebalance(dead_instances)
                    self.load_assigned_jobs()  # Reload with new assignment
                time.sleep(30)

        Thread(target=monitor_loop, daemon=True).start()
```

**Step 9: Testing**

```python
# tests/test_distributed_scheduler.py

def test_leader_election():
    """Test that only one leader is elected."""
    instances = [start_instance() for _ in range(3)]
    time.sleep(15)  # Wait for election

    leaders = [i for i in instances if i.is_leader]
    assert len(leaders) == 1

def test_job_distribution():
    """Test that jobs are distributed across instances."""
    # Create 100 ingestions
    for i in range(100):
        create_ingestion(f"ingestion-{i}")

    # Start 3 instances
    instances = [start_instance() for _ in range(3)]
    time.sleep(10)

    # Check distribution
    job_counts = [i.scheduler.get_jobs() for i in instances]
    assert all(20 <= len(jobs) <= 40 for jobs in job_counts)  # Roughly equal

def test_failover():
    """Test that jobs are reassigned when instance dies."""
    # Start 2 instances
    instance1 = start_instance()
    instance2 = start_instance()

    time.sleep(10)

    # Kill leader
    leader = instance1 if instance1.is_leader else instance2
    leader.kill()

    # Wait for failover
    time.sleep(30)

    # Check that follower became leader
    follower = instance2 if leader == instance1 else instance1
    assert follower.is_leader

    # Check that all jobs are still scheduled
    total_jobs = len(follower.scheduler.get_jobs())
    assert total_jobs == 100

def test_no_duplicate_execution():
    """Test that jobs run only once despite multiple instances."""
    # Create 1 ingestion scheduled every second
    ingestion = create_ingestion("test", cron="* * * * * *")

    # Start 3 instances
    instances = [start_instance() for _ in range(3)]

    # Wait for 10 executions
    time.sleep(10)

    # Check run count
    runs = run_repository.get_runs_for_ingestion(ingestion.id)
    assert len(runs) == 10  # Not 30 (would be duplicates)
```

---

## 8. Code Examples

### 8.1 Complete Distributed Scheduler Service

```python
# app/services/scheduler/distributed_scheduler.py

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import redis
import uuid
from typing import List, Optional
from threading import Thread
import time

from app.config import settings
from app.repositories.ingestion_repository import IngestionRepository
from .leader_election import LeaderElection
from .job_assignment import JobAssignmentService
from .health_monitor import HealthMonitor
from .distributed_lock import distributed_lock


class DistributedSchedulerService:
    """
    Distributed APScheduler implementation for horizontal scaling.

    Features:
    - Leader election (Redis-based)
    - Job assignment via consistent hashing
    - Distributed locks to prevent duplicate execution
    - Automatic failover and rebalancing
    """

    def __init__(
        self,
        ingestion_repository: IngestionRepository,
        redis_url: str = None,
        instance_id: str = None
    ):
        self.instance_id = instance_id or str(uuid.uuid4())
        self.redis = redis.Redis.from_url(redis_url or settings.REDIS_URL)
        self.ingestion_repository = ingestion_repository

        # APScheduler with shared job store
        jobstores = {
            'default': SQLAlchemyJobStore(url=settings.DATABASE_URL)
        }
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            job_defaults={
                'coalesce': True,
                'max_instances': 1,
                'misfire_grace_time': 300
            }
        )

        # Distributed components
        self.leader_election = LeaderElection(self.redis, self.instance_id)
        self.job_assignment = JobAssignmentService(self.redis)
        self.health_monitor = HealthMonitor(self.redis, self.instance_id)

        self.is_running = False

    def start(self):
        """Start the distributed scheduler."""
        if self.is_running:
            return

        logger.info(f"Starting distributed scheduler (instance: {self.instance_id})")

        # Start health monitoring (heartbeat)
        self.health_monitor.start_heartbeat()

        # Start leader election
        self.leader_election.start()

        # Wait to determine role
        self._wait_for_cluster_ready()

        # Initialize job assignment
        alive_instances = self.health_monitor.get_alive_instances()
        self.job_assignment.initialize(alive_instances)

        # Load assigned jobs
        self._load_assigned_jobs()

        # Start APScheduler
        self.scheduler.start()

        # Start cluster monitoring (for rebalancing)
        self._start_cluster_monitor()

        self.is_running = True
        logger.info(f"Distributed scheduler started")

    def shutdown(self):
        """Gracefully shutdown scheduler."""
        logger.info(f"Shutting down distributed scheduler (instance: {self.instance_id})")

        self.is_running = False
        self.scheduler.shutdown(wait=True)
        self.health_monitor.stop_heartbeat()

        # Remove from cluster
        self.redis.delete(f"autoloader:heartbeat:{self.instance_id}")

    def schedule_ingestion(self, ingestion: Ingestion):
        """
        Schedule an ingestion (only if assigned to this instance).
        """
        # Check if this instance should schedule this job
        if not self._should_i_schedule(ingestion.id):
            logger.debug(f"Ingestion {ingestion.id} assigned to another instance")
            return

        # Parse cron expression
        cron_parts = parse_cron_expression(ingestion.schedule_cron)

        # Add job with distributed lock wrapper
        self.scheduler.add_job(
            func=self._run_ingestion_with_lock,
            args=[ingestion.id],
            trigger='cron',
            id=f"ingestion_{ingestion.id}",
            replace_existing=True,
            **cron_parts
        )

        logger.info(f"Scheduled ingestion {ingestion.id} on instance {self.instance_id}")

    def unschedule_ingestion(self, ingestion_id: UUID):
        """Remove scheduled job."""
        job_id = f"ingestion_{ingestion_id}"
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Unscheduled ingestion {ingestion_id}")
        except JobLookupError:
            # Job not on this instance (expected in distributed mode)
            pass

    def _run_ingestion_with_lock(self, ingestion_id: UUID):
        """
        Execute ingestion with distributed lock to prevent duplicate runs.
        """
        lock_key = f"ingestion_{ingestion_id}"

        try:
            # Try to acquire distributed lock (1 hour timeout)
            with distributed_lock(self.redis, lock_key, timeout=3600):
                logger.info(f"Lock acquired for ingestion {ingestion_id} on instance {self.instance_id}")

                # Execute ingestion
                from app.services.batch_orchestrator import BatchOrchestrator
                orchestrator = BatchOrchestrator(...)
                orchestrator.run_scheduled_ingestion(ingestion_id)

                logger.info(f"Ingestion {ingestion_id} completed on instance {self.instance_id}")

        except LockAcquisitionException:
            # Another instance is already running this job
            logger.info(
                f"Ingestion {ingestion_id} already running on another instance. Skipping."
            )

        except Exception as e:
            logger.error(f"Ingestion {ingestion_id} failed on instance {self.instance_id}: {e}")
            # Don't re-raise - let APScheduler handle retry logic

    def _should_i_schedule(self, ingestion_id: UUID) -> bool:
        """Check if this instance should schedule the given job."""
        return self.job_assignment.should_i_schedule_job(
            str(ingestion_id),
            self.instance_id
        )

    def _wait_for_cluster_ready(self):
        """Wait until cluster state is stable."""
        logger.info("Waiting for cluster to be ready...")

        # Wait up to 30 seconds for leader election
        for _ in range(30):
            if self.leader_election.is_leader or self._has_leader():
                logger.info("Cluster ready")
                return
            time.sleep(1)

        logger.warning("Cluster not ready after 30s, proceeding anyway")

    def _has_leader(self) -> bool:
        """Check if cluster has a leader."""
        leader = self.redis.get("autoloader:leader")
        return leader is not None

    def _load_assigned_jobs(self):
        """Load all jobs assigned to this instance."""
        logger.info(f"Loading assigned jobs for instance {self.instance_id}")

        # Get all scheduled ingestions
        ingestions = self.ingestion_repository.get_scheduled_ingestions()

        assigned_count = 0
        for ingestion in ingestions:
            if self._should_i_schedule(ingestion.id):
                self.schedule_ingestion(ingestion)
                assigned_count += 1

        logger.info(
            f"Loaded {assigned_count} jobs (out of {len(ingestions)} total) "
            f"on instance {self.instance_id}"
        )

    def _start_cluster_monitor(self):
        """Monitor cluster for failures and trigger rebalancing."""
        def monitor_loop():
            logger.info("Starting cluster monitor")

            while self.is_running:
                try:
                    # Get current cluster state
                    alive_instances = self.health_monitor.get_alive_instances()
                    previous_instances = self.job_assignment.get_instances()

                    # Check if cluster composition changed
                    if set(alive_instances) != set(previous_instances):
                        logger.warning(
                            f"Cluster change detected. "
                            f"Previous: {previous_instances}, Current: {alive_instances}"
                        )

                        # Trigger rebalance
                        self._rebalance(alive_instances)

                    time.sleep(30)  # Check every 30 seconds

                except Exception as e:
                    logger.error(f"Cluster monitor error: {e}")
                    time.sleep(30)

        Thread(target=monitor_loop, daemon=True, name="cluster-monitor").start()

    def _rebalance(self, alive_instances: List[str]):
        """Rebalance jobs across alive instances."""
        logger.info(f"Rebalancing jobs across {len(alive_instances)} instances")

        # Update job assignment
        self.job_assignment.initialize(alive_instances)

        # Remove jobs no longer assigned to this instance
        current_jobs = self.scheduler.get_jobs()
        for job in current_jobs:
            # Extract ingestion_id from job id
            ingestion_id = UUID(job.id.replace("ingestion_", ""))

            if not self._should_i_schedule(ingestion_id):
                self.scheduler.remove_job(job.id)
                logger.info(f"Removed job {job.id} (reassigned to another instance)")

        # Add jobs newly assigned to this instance
        self._load_assigned_jobs()

        logger.info("Rebalancing complete")

    def get_cluster_status(self) -> dict:
        """Get current cluster status (for debugging/monitoring)."""
        alive_instances = self.health_monitor.get_alive_instances()
        leader = self.redis.get("autoloader:leader")

        return {
            "instance_id": self.instance_id,
            "is_leader": self.leader_election.is_leader,
            "cluster_leader": leader.decode() if leader else None,
            "alive_instances": alive_instances,
            "total_instances": len(alive_instances),
            "scheduled_jobs": len(self.scheduler.get_jobs()),
            "is_running": self.is_running
        }


# Singleton instance
_scheduler_instance: Optional[DistributedSchedulerService] = None

def get_distributed_scheduler() -> DistributedSchedulerService:
    """Get singleton scheduler instance."""
    global _scheduler_instance

    if _scheduler_instance is None:
        from app.dependencies import get_ingestion_repository

        _scheduler_instance = DistributedSchedulerService(
            ingestion_repository=get_ingestion_repository(),
            redis_url=settings.REDIS_URL,
            instance_id=settings.INSTANCE_ID
        )

    return _scheduler_instance
```

### 8.2 API Endpoint for Cluster Status

```python
# app/api/v1/cluster.py

from fastapi import APIRouter, Depends
from app.services.scheduler.distributed_scheduler import get_distributed_scheduler

router = APIRouter(prefix="/cluster", tags=["cluster"])

@router.get("/status")
def get_cluster_status():
    """Get distributed scheduler cluster status."""
    scheduler = get_distributed_scheduler()
    return scheduler.get_cluster_status()

# Response:
# {
#   "instance_id": "a1b2c3d4-...",
#   "is_leader": true,
#   "cluster_leader": "a1b2c3d4-...",
#   "alive_instances": ["a1b2c3d4-...", "e5f6g7h8-...", "i9j0k1l2-..."],
#   "total_instances": 3,
#   "scheduled_jobs": 167,
#   "is_running": true
# }
```

---

## 9. Failure Scenarios

### 9.1 Scenario 1: Leader Dies

**Timeline:**
```
10:00:00 - Instance 1 (leader) schedules 100 jobs
10:00:05 - Instance 1 crashes (OOM)
10:00:10 - Instance 1's heartbeat expires (5 sec timeout)
10:00:15 - Instance 2 detects failure via health monitor
10:00:16 - Instance 2 attempts leader election
10:00:17 - Instance 2 becomes leader (Redis SET NX succeeds)
10:00:20 - Instance 2 loads all 100 jobs
10:00:25 - Scheduling resumes
```

**Downtime:** ~20 seconds (configurable based on timeout)

**Jobs Affected:**
- Jobs scheduled between 10:00:05 - 10:00:25 may misfire
- APScheduler's `misfire_grace_time` (300s) allows delayed execution

### 9.2 Scenario 2: Network Partition (Split-Brain)

**Timeline:**
```
10:00:00 - 3 instances, Instance 1 is leader
10:00:05 - Network partition: Instance 1 separated from Redis
10:00:10 - Instance 1 still thinks it's leader (hasn't detected partition)
10:00:15 - Instance 1's lease expires in Redis
10:00:16 - Instance 2 becomes new leader
10:00:20 - Both Instance 1 and Instance 2 schedule jobs
10:00:25 - Duplicate executions
```

**Mitigation:**
- Distributed locks prevent duplicate **execution** (even if duplicate **scheduling** occurs)
- Fencing tokens (advanced) prevent stale leaders from taking actions

### 9.3 Scenario 3: Redis Outage

**Timeline:**
```
10:00:00 - All instances running normally
10:00:05 - Redis crashes
10:00:06 - All instances unable to acquire locks
10:00:07 - Leader election fails
10:00:08 - No jobs execute (fail-safe)
10:00:10 - Redis restarts (30 sec downtime)
10:00:40 - Leader re-elected
10:00:45 - Scheduling resumes
```

**Impact:**
- **Zero jobs execute during Redis outage** (fail-safe behavior)
- All jobs between 10:00:05 - 10:00:45 misfire
- APScheduler's coalesce logic combines missed runs

**Prevention:**
- Redis Sentinel (3+ nodes) for high availability
- Redis Cluster for partitioning

### 9.4 Scenario 4: Database Deadlock

**Timeline:**
```
02:00:00 - 3 instances, 50 jobs trigger simultaneously
02:00:01 - All instances update `runs` table
02:00:02 - PostgreSQL detects deadlock
02:00:02 - 5 jobs rolled back, 45 succeed
02:00:03 - APScheduler does NOT retry rolled-back jobs
Result: 5 jobs lost
```

**Mitigation:**
- Use `SELECT FOR UPDATE SKIP LOCKED` (Postgres 9.5+)
- Retry logic in job wrapper
- Optimistic locking with version column

---

## 10. Operational Complexity

### 10.1 New Monitoring Requirements

**Metrics to Track:**

```python
# Prometheus metrics

from prometheus_client import Gauge, Counter, Histogram

# Cluster health
cluster_instances_total = Gauge(
    'autoloader_cluster_instances_total',
    'Total instances in cluster'
)

cluster_leader_changes = Counter(
    'autoloader_cluster_leader_changes_total',
    'Number of leader elections'
)

# Lock contention
lock_acquisition_duration = Histogram(
    'autoloader_lock_acquisition_seconds',
    'Time to acquire distributed lock'
)

lock_acquisition_failures = Counter(
    'autoloader_lock_acquisition_failures_total',
    'Failed lock acquisitions'
)

# Job distribution
jobs_per_instance = Gauge(
    'autoloader_jobs_per_instance',
    'Number of jobs on this instance',
    ['instance_id']
)

# Rebalancing
rebalance_events = Counter(
    'autoloader_rebalance_events_total',
    'Number of rebalancing events'
)
```

**Alerts:**

```yaml
# Prometheus alert rules

groups:
  - name: autoloader_distributed
    rules:
      # No leader
      - alert: NoLeader
        expr: absent(autoloader_cluster_leader)
        for: 1m
        annotations:
          summary: "No cluster leader elected"

      # Frequent leader changes (instability)
      - alert: FrequentLeaderChanges
        expr: rate(autoloader_cluster_leader_changes_total[5m]) > 0.5
        for: 5m
        annotations:
          summary: "Cluster unstable (frequent leader changes)"

      # Lock contention
      - alert: HighLockContention
        expr: rate(autoloader_lock_acquisition_failures_total[5m]) > 10
        for: 5m
        annotations:
          summary: "High lock contention (jobs competing)"

      # Unbalanced load
      - alert: UnbalancedJobDistribution
        expr: stddev(autoloader_jobs_per_instance) > 20
        for: 10m
        annotations:
          summary: "Jobs unevenly distributed across instances"

      # Redis down
      - alert: RedisDown
        expr: up{job="redis"} == 0
        for: 1m
        annotations:
          summary: "Redis coordination layer down - scheduler will fail"
```

### 10.2 Debugging Complexity

**Common Issues:**

1. **"Why is this job running twice?"**
   - Check: Multiple instances without distributed locks
   - Check: Lock timeout too short (job still running when lock expires)
   - Check: Redis split-brain scenario

2. **"Why is this job not running at all?"**
   - Check: Job assignment (might be on different instance)
   - Check: Instance that owns job is dead
   - Check: Rebalancing didn't trigger
   - Check: Lock held by dead process (orphaned lock)

3. **"Why is the cluster unstable (frequent leader changes)?"**
   - Check: Network latency to Redis
   - Check: Instance resource exhaustion (CPU, memory)
   - Check: Redis overloaded

**Debugging Tools Needed:**

```bash
# CLI tool to inspect cluster state
autoloader cluster status

# Output:
# Instance ID       | Leader | Alive | Jobs | Last Heartbeat
# ----------------- | ------ | ----- | ---- | --------------
# instance-1        | YES    | YES   | 167  | 2s ago
# instance-2        | NO     | YES   | 163  | 1s ago
# instance-3        | NO     | NO    | 0    | 45s ago (DEAD)

autoloader cluster jobs

# Output:
# Job ID            | Assigned To | Last Run  | Next Run
# ----------------- | ----------- | --------- | ---------
# ingestion-abc123  | instance-1  | 2m ago    | 58m
# ingestion-def456  | instance-2  | 1m ago    | 59m
# ingestion-ghi789  | instance-3  | MISSING   | 2m ago (MISFIRE)

autoloader cluster locks

# Output:
# Lock Key               | Held By    | Acquired | TTL
# ---------------------- | ---------- | -------- | ---
# ingestion_abc123       | instance-1 | 5m ago   | 55m
# ingestion_def456       | (none)     | -        | -
# ingestion_orphan       | instance-X | 2h ago   | ORPHANED
```

### 10.3 Runbook Additions

**New Operational Procedures:**

```markdown
## Procedure: Add New Instance

1. Deploy new instance with unique INSTANCE_ID
2. Ensure REDIS_URL points to cluster
3. Start instance
4. Verify heartbeat in Redis: `redis-cli GET autoloader:heartbeat:<instance-id>`
5. Wait 30s for rebalancing
6. Check job distribution: `autoloader cluster status`

## Procedure: Remove Instance

1. Gracefully shutdown: `systemctl stop autoloader`
2. Wait 30s for heartbeat to expire
3. Check that jobs reassigned: `autoloader cluster jobs`
4. Remove from deployment config

## Procedure: Redis Failover

1. Redis Sentinel detects master failure
2. Sentinel promotes replica to master
3. All instances reconnect to new master (automatic)
4. Verify leader election: `redis-cli GET autoloader:leader`
5. Verify jobs running: `autoloader cluster status`

## Procedure: Clear Orphaned Locks

# Identify orphaned locks (held by dead instances)
redis-cli KEYS "autoloader:job_lock:*"

# For each lock, check if instance is alive
redis-cli GET autoloader:heartbeat:<instance-id>

# If instance dead, delete lock
redis-cli DEL autoloader:job_lock:<job-id>

## Procedure: Force Leader Re-election

# Delete leader key
redis-cli DEL autoloader:leader

# Wait 5-10 seconds for new leader
redis-cli GET autoloader:leader
```

---

## 11. Cost-Benefit Analysis

### 11.1 Engineering Cost

| Task | Effort | Complexity |
|------|--------|------------|
| Infrastructure setup (Redis, LB) | 1 day | Low |
| Leader election implementation | 2 days | Medium |
| Distributed locking | 2 days | Medium |
| Job assignment (consistent hashing) | 2 days | High |
| Health monitoring & rebalancing | 3 days | High |
| Testing (unit, integration, chaos) | 4 days | High |
| Documentation & runbooks | 1 day | Low |
| **Total** | **15 days (3 weeks)** | **High** |

**Ongoing Maintenance:**
- Debugging distributed issues: ~2 days/month
- Redis cluster maintenance: ~1 day/month
- Monitoring and tuning: ~0.5 day/month

**Total Yearly Cost:** ~40 days of engineering time

### 11.2 Infrastructure Cost

| Component | Monthly Cost | Purpose |
|-----------|--------------|---------|
| Redis Cluster (3 nodes) | $150-300 | Coordination |
| Load Balancer | $20-50 | Traffic distribution |
| Extra compute (2+ instances) | $100-500 | Horizontal scaling |
| **Total** | **$270-850/month** | - |

**Note:** Costs vary by cloud provider and instance size

### 11.3 Benefit Analysis

**What Do We Gain?**

| Benefit | Value |
|---------|-------|
| **High Availability** | Eliminates single point of failure |
| **Horizontal Scalability** | Support 1000+ ingestions |
| **Load Distribution** | Better resource utilization |
| **Fault Isolation** | One instance failure doesn't affect all |

**What Do We Lose?**

| Cost | Impact |
|------|--------|
| **Complexity** | Harder to understand, debug, maintain |
| **New Failure Modes** | Split-brain, lock contention, rebalancing bugs |
| **Operational Burden** | More services to monitor and maintain |
| **Development Time** | 3 weeks + ongoing maintenance |

### 11.4 ROI Calculation

**Scenario: 500 Scheduled Ingestions**

**Option A: Distributed APScheduler**
- Engineering: 15 days upfront + 4 days/month ongoing = **63 days/year**
- Infrastructure: $270-850/month = **$3,240-10,200/year**
- Reliability: HIGH
- **Total Cost: ~$50,000-100,000/year** (engineering + infrastructure)

**Option B: IOMETE Jobs**
- Engineering: 10 days upfront + 1 day/month ongoing = **22 days/year**
- Infrastructure: $0 (uses existing IOMETE)
- Reliability: HIGH
- **Total Cost: ~$20,000-30,000/year** (engineering only)

**Winner: IOMETE Jobs (70% cost savings)**

**Option C: Kubernetes CronJobs**
- Engineering: 12 days upfront + 2 days/month ongoing = **36 days/year**
- Infrastructure: $0 (uses existing K8s)
- Reliability: HIGH
- **Total Cost: ~$30,000-45,000/year** (engineering only)

**Winner: Still better than Distributed APScheduler**

---

## 12. Alternative Comparison

### 12.1 Feature Matrix

| Feature | Distributed APScheduler | IOMETE Jobs | K8s CronJobs | Airflow |
|---------|------------------------|-------------|--------------|---------|
| **Development Time** | 🔴 3 weeks | 🟢 1.5 weeks | 🟡 2 weeks | 🔴 4+ weeks |
| **Operational Complexity** | 🔴 HIGH | 🟢 LOW | 🟡 MEDIUM | 🔴 HIGH |
| **Infrastructure Cost** | 🔴 High ($3-10K/yr) | 🟢 Free | 🟢 Free | 🔴 Very High ($10K+/yr) |
| **Scalability** | 🟡 1000+ jobs | 🟢 10,000+ jobs | 🟢 5,000+ jobs | 🟢 100,000+ jobs |
| **Fault Isolation** | 🟡 Partial | 🟢 Full | 🟢 Full | 🟢 Full |
| **Debugging Ease** | 🔴 Hard | 🟢 Easy | 🟢 Easy | 🟡 Medium |
| **Vendor Lock-in** | 🟢 None | 🟡 IOMETE | 🟡 Kubernetes | 🟡 Airflow |
| **Learning Curve** | 🔴 Distributed systems | 🟢 Minimal | 🟡 K8s knowledge | 🔴 Airflow concepts |

### 12.2 When to Choose Each

**Choose Distributed APScheduler If:**
- ❌ (No good reason - alternatives are better)

**Choose IOMETE Jobs If:**
- ✅ You're already using IOMETE
- ✅ You want minimal operational overhead
- ✅ You need production-grade reliability
- ✅ You want to iterate quickly

**Choose Kubernetes CronJobs If:**
- ✅ You're already running on Kubernetes
- ✅ You want cloud-native architecture
- ✅ You need vendor portability

**Choose Airflow If:**
- ✅ You need complex workflows (DAGs)
- ✅ You have dedicated data engineering team
- ✅ You're willing to invest in infrastructure

---

## 13. Recommendation

### 13.1 Summary

**Distributed APScheduler is technically feasible but economically questionable.**

**Why Build It?**
- ✅ Keeps familiar APScheduler API
- ✅ No external dependencies (besides Redis)
- ✅ Full control over scheduling logic

**Why NOT Build It?**
- ❌ High engineering cost (3 weeks + ongoing)
- ❌ High operational complexity (Redis, monitoring, debugging)
- ❌ New failure modes (split-brain, lock contention)
- ❌ Inferior to alternatives (IOMETE Jobs, K8s CronJobs)
- ❌ Reinventing the wheel (job orchestration is solved problem)

### 13.2 Recommended Path

**Phase 1 (MVP): Single-Instance APScheduler**
- ⏱️ Implementation: 4-6 hours
- 📊 Capacity: <100 ingestions
- ✅ Fast to market
- ⚠️ Document limitations

**Phase 2 (Growth): Migration Trigger**
- 📈 When: 50-80 ingestions OR 3 months of production experience
- 🔍 Decide: Based on actual usage patterns and pain points

**Phase 3 (Scale): IOMETE Jobs**
- ⏱️ Implementation: 1.5 weeks
- 📊 Capacity: 10,000+ ingestions
- ✅ Horizontal scaling built-in
- ✅ No distributed systems complexity

**Skip: Distributed APScheduler**
- ❌ Not worth the engineering investment
- ❌ Alternatives provide better value

### 13.3 Decision Framework

**Ask yourself:**

1. **"Do we really need horizontal scaling?"**
   - If <100 ingestions: NO → Use single APScheduler
   - If 100-500 ingestions: MAYBE → Performance test first
   - If 500+ ingestions: YES → But not via distributed APScheduler

2. **"What's the actual bottleneck?"**
   - CPU/Memory: Vertical scaling might be enough
   - Spark cluster: Scale IOMETE cluster, not scheduler
   - Database connections: Connection pooling, not horizontal scaling

3. **"What's our risk tolerance?"**
   - Low risk tolerance: Don't build distributed systems from scratch
   - High risk tolerance: Still, is this the best use of engineering time?

4. **"What's our engineering bandwidth?"**
   - Limited: Use managed solutions (IOMETE Jobs)
   - Abundant: Still, is this higher priority than features?

### 13.4 Final Recommendation

**DO NOT implement distributed APScheduler.**

**Instead:**

1. **Start with single APScheduler** (as planned)
   - Build it well (monitoring, safeguards)
   - Document known limits
   - Get to market fast

2. **Performance test early**
   - Simulate 200+ ingestions
   - Identify actual bottlenecks
   - Make data-driven decision

3. **Design for migration**
   - Use Strategy Pattern
   - Abstract scheduler behind interface
   - Zero code changes when migrating

4. **Migrate to IOMETE Jobs** (when needed)
   - Triggered by metrics (>80 ingestions, >2GB memory, etc.)
   - Cleaner architecture
   - Better ROI

**This path balances speed-to-market with production scalability while avoiding unnecessary complexity.**

---

## 14. Conclusion

Horizontal scaling APScheduler is a **technically interesting but economically poor decision**.

**Key Insights:**

1. **Complexity Explodes**
   - Distributed systems are hard
   - Debugging is 10x harder
   - Operational burden is high

2. **Better Alternatives Exist**
   - IOMETE Jobs: Purpose-built for this
   - Kubernetes CronJobs: If already on K8s
   - Both provide horizontal scaling "for free"

3. **Premature Optimization**
   - Single APScheduler handles <100 ingestions fine
   - Test before scaling
   - Scale when data demands it

4. **Engineering Time is Precious**
   - 3 weeks on distributed scheduler
   - OR 3 weeks on customer-facing features?
   - Opportunity cost is real

**Final Word:**

If you're convinced you need horizontal scaling, **use a tool designed for it** (IOMETE Jobs, Kubernetes CronJobs, Airflow). Don't build distributed systems from scratch unless it's your core competency.

APScheduler is excellent for single-process scheduling. Let it do what it does best.

---

**End of Document**

**Related Documents:**
- `scheduler-scaling-and-risks.md` - Risk analysis
- `scheduler-implementation-guide.md` - Implementation details
- `architecture-decision-file-tracking.md` - System architecture

**Decision Record:**
- **Status:** ❌ NOT Recommended
- **Decision:** Use single APScheduler for MVP, migrate to IOMETE Jobs for scale
- **Rationale:** Distributed APScheduler has poor cost-benefit ratio compared to alternatives
- **Review Date:** After performance testing with 100+ ingestions
