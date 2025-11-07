# Prefect Architecture: Work Pools, Work Queues, and Workers Explained

**Document Version:** 1.0
**Created:** 2025-11-07
**Status:** Educational
**Related:** `prefect-worker-types-explained.md`, `prefect-orchestration-guide.md`

---

## Table of Contents

1. [The Big Picture](#the-big-picture)
2. [Component Breakdown](#component-breakdown)
3. [Work Pools Explained](#work-pools-explained)
4. [Work Queues Explained](#work-queues-explained)
5. [Workers Explained](#workers-explained)
6. [How They Work Together](#how-they-work-together)
7. [Where Does Each Live?](#where-does-each-live)
8. [IOMETE Autoloader Design](#iomete-autoloader-design)
9. [Complete Example](#complete-example)
10. [Common Patterns](#common-patterns)

---

## 1. The Big Picture

### 1.1 The Hierarchy

```
Prefect Server (Central Brain)
    │
    ├─ Work Pool 1: "kubernetes-pool" ──────────────┐
    │   │                                           │
    │   ├─ Work Queue: "default"                    │
    │   ├─ Work Queue: "high-memory"                │
    │   └─ Work Queue: "priority"                   │
    │                                                │
    ├─ Work Pool 2: "process-pool" ─────────────────┤
    │   │                                           │
    │   ├─ Work Queue: "default"                    │
    │   └─ Work Queue: "low-priority"               │
    │                                                │
    └─ Work Pool 3: "ecs-pool" ─────────────────────┘
        │
        └─ Work Queue: "aws-only"

                        ▼

Workers (Execution Agents)
    │
    ├─ Worker 1 → Polls: kubernetes-pool / default
    ├─ Worker 2 → Polls: kubernetes-pool / default
    ├─ Worker 3 → Polls: kubernetes-pool / high-memory
    ├─ Worker 4 → Polls: process-pool / default
    └─ Worker 5 → Polls: ecs-pool / aws-only
```

### 1.2 Simple Analogy: Restaurant

```
┌─────────────────────────────────────────────────────────┐
│                    Restaurant Manager                   │
│                    (Prefect Server)                     │
└─────────────────────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   Kitchen    │ │   Bar        │ │   Bakery     │
│  (Work Pool) │ │ (Work Pool)  │ │ (Work Pool)  │
└──────────────┘ └──────────────┘ └──────────────┘
        │               │               │
    ┌───┴───┐       ┌───┴───┐       ┌───┴───┐
    ▼       ▼       ▼       ▼       ▼       ▼
  ┌────┐ ┌────┐  ┌────┐ ┌────┐  ┌────┐ ┌────┐
  │Hot │ │Cold│  │Beer│ │Wine│  │Bread│ │Cake│
  │Food│ │Food│  │    │ │    │  │     │ │    │
  └────┘ └────┘  └────┘ └────┘  └────┘ └────┘
  Work   Work     Work   Work    Work   Work
  Queue  Queue    Queue  Queue   Queue  Queue

        ▼               ▼               ▼
  ┌─────────┐     ┌─────────┐     ┌─────────┐
  │ Chef 1  │     │Bartender│     │ Baker 1 │
  │ Chef 2  │     │         │     │ Baker 2 │
  └─────────┘     └─────────┘     └─────────┘
    Workers         Worker          Workers
```

**Explanation:**
- **Restaurant Manager** = Prefect Server (coordinates everything)
- **Kitchen/Bar/Bakery** = Work Pools (different execution environments)
- **Hot Food/Cold Food/Beer/Wine** = Work Queues (order types within each environment)
- **Chefs/Bartenders/Bakers** = Workers (actual executors)

---

## 2. Component Breakdown

### 2.1 Prefect Server

**What it is:**
- Central orchestration service
- Stores all metadata (deployments, flows, runs, schedules)
- Manages work pools and work queues
- Tracks flow run states
- Provides UI and API

**What it does:**
- Receives deployment definitions
- Schedules flow runs based on cron/interval
- Places flow runs in appropriate work queues
- Tracks worker health and status
- Provides observability (logs, metrics, UI)

**What it does NOT do:**
- Execute your flows (workers do this)
- Poll work queues (workers do this)
- Interact with infrastructure directly (workers do this)

**Where it lives:**
- Kubernetes pod (in your cluster)
- OR Prefect Cloud (managed service)

---

### 2.2 Work Pool

**What it is:**
- A **logical grouping** of work queues
- Represents an **execution environment** or **infrastructure type**
- Defined on Prefect Server
- Workers connect to a specific work pool

**Purpose:**
- Organize work by infrastructure type (Kubernetes, ECS, Process, Docker, etc.)
- Allow workers to specialize (Kubernetes workers only pull from Kubernetes pools)
- Provide default configuration for infrastructure

**Analogy:**
- Kitchen in a restaurant (where food is prepared)
- All kitchen workers (chefs) work in the kitchen, not the bar

**Key Attributes:**
- **Name**: e.g., "kubernetes-pool", "process-pool"
- **Type**: e.g., "kubernetes", "process", "docker", "ecs"
- **Default Infrastructure Config**: Default resources, image, namespace, etc.
- **Concurrency Limits**: Max concurrent runs across all workers in pool

**Where it lives:**
- Defined in Prefect Server database
- Visible in Prefect UI: Settings → Work Pools

---

### 2.3 Work Queue

**What it is:**
- A **named queue** within a work pool
- Holds pending flow runs
- Workers poll specific queues
- Priority-based (optional)

**Purpose:**
- Prioritize work (high-priority queue vs. low-priority)
- Route work to specialized workers (GPU queue, high-memory queue)
- Isolate tenants or workloads

**Analogy:**
- Order queue in a kitchen (e.g., "Hot Food" vs. "Cold Food")
- Different chefs specialize in different queues

**Key Attributes:**
- **Name**: e.g., "default", "high-memory", "priority"
- **Priority**: Higher priority queues are processed first (optional)
- **Concurrency Limit**: Max concurrent runs in this queue
- **Paused State**: Can pause a queue to stop processing

**Where it lives:**
- Defined in Prefect Server database (under a work pool)
- Visible in Prefect UI: Work Pools → [Pool Name] → Queues

---

### 2.4 Worker

**What it is:**
- A **process** that pulls work from a queue and executes it
- Long-running (continuously polls)
- Can be in-process executor OR job submitter (see previous guide)

**Purpose:**
- Poll work queues for new flow runs
- Execute flows (in-process or by submitting jobs)
- Report status back to Prefect Server
- Handle retries and failures

**Analogy:**
- Chef in a kitchen
- Continuously checks order queue
- Cooks the dish
- Marks order as complete

**Key Attributes:**
- **Pool**: Which work pool it connects to
- **Queues**: Which queue(s) it polls (can poll multiple)
- **Type**: process, kubernetes, docker, etc.
- **Concurrency Limit**: Max concurrent flows per worker
- **Heartbeat**: Periodic check-in with server

**Where it lives:**
- Kubernetes pod (in your cluster)
- OR local process (for development)
- NOT on Prefect Server (workers are separate)

---

## 3. Work Pools Explained

### 3.1 What Problem Do Work Pools Solve?

**Scenario: Mixed Infrastructure**

You have:
- Some flows that run on Kubernetes
- Some flows that run on AWS ECS
- Some flows that run as local processes

**Without Work Pools:**
```
All flows in one giant queue
    ↓
Workers blindly pull work
    ↓
Problem: Kubernetes worker pulls ECS flow → Can't execute!
```

**With Work Pools:**
```
kubernetes-pool          ecs-pool             process-pool
    │                       │                      │
    ├─ K8s flows           ├─ ECS flows          ├─ Process flows
    │                       │                      │
    ▼                       ▼                      ▼
K8s Workers             ECS Workers           Process Workers
(can execute)           (can execute)         (can execute)
```

### 3.2 Work Pool Structure

```
┌────────────────────────────────────────────────────────┐
│  Work Pool: "kubernetes-pool"                          │
│  Type: kubernetes                                      │
│                                                        │
│  Default Configuration:                                │
│    - namespace: iomete                                 │
│    - image: iomete/autoloader-worker:latest            │
│    - service_account: prefect-worker                   │
│    - job_watch_timeout: 3600                           │
│                                                        │
│  Work Queues (within this pool):                       │
│    ├─ default (priority: 0)                            │
│    ├─ high-memory (priority: 10)                       │
│    └─ priority (priority: 100)                         │
│                                                        │
│  Workers Connected: 5                                  │
│  Concurrency Limit: 50                                 │
└────────────────────────────────────────────────────────┘
```

### 3.3 Creating a Work Pool

**Via Prefect CLI:**
```bash
# Create Kubernetes work pool
prefect work-pool create kubernetes-pool \
  --type kubernetes \
  --set-as-default

# Create Process work pool
prefect work-pool create process-pool \
  --type process

# List work pools
prefect work-pool ls

# Output:
# ┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
# ┃ Name               ┃ Type       ┃ Queues         ┃
# ┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
# │ kubernetes-pool    │ kubernetes │ default        │
# │ process-pool       │ process    │ default        │
# └────────────────────┴────────────┴────────────────┘
```

**Via Prefect UI:**
```
1. Navigate to: Settings → Work Pools
2. Click: "Create Work Pool"
3. Select Type: "Kubernetes"
4. Enter Name: "kubernetes-pool"
5. Configure defaults (namespace, image, etc.)
6. Click: "Create"
```

**Via Python API:**
```python
from prefect.client import get_client
from prefect.server.schemas.actions import WorkPoolCreate

async def create_work_pool():
    async with get_client() as client:
        work_pool = await client.create_work_pool(
            WorkPoolCreate(
                name="kubernetes-pool",
                type="kubernetes",
                base_job_template={
                    "job_configuration": {
                        "namespace": "iomete",
                        "image": "iomete/autoloader-worker:latest",
                        "service_account_name": "prefect-worker",
                    }
                }
            )
        )
        print(f"Created work pool: {work_pool.name}")
```

### 3.4 Work Pool Configuration

**Example: Kubernetes Work Pool with Defaults**

```yaml
# Work Pool Configuration (stored in Prefect Server)
name: kubernetes-pool
type: kubernetes

# Default job template (used for all flows unless overridden)
base_job_template:
  job_configuration:
    # Kubernetes-specific
    namespace: iomete
    service_account_name: prefect-worker
    image_pull_policy: IfNotPresent
    finished_job_ttl: 3600  # Delete jobs after 1 hour

    # Container defaults
    image: "{{ image }}"  # Provided by deployment
    command:
      - python
      - -m
      - prefect.engine

    # Environment variables
    env:
      PREFECT_API_URL: "{{ prefect_api_url }}"
      DATABASE_URL: "{{ database_url }}"
      SPARK_CONNECT_URL: "sc://spark-connect-service:15002"

    # Resource defaults (can be overridden per deployment)
    cpu_request: "2"
    cpu_limit: "4"
    memory_request: "4Gi"
    memory_limit: "8Gi"

  # Variables (can be overridden)
  variables:
    properties:
      image:
        default: "iomete/autoloader-worker:latest"
      cpu_request:
        default: "2"
      memory_request:
        default: "4Gi"
```

**Overriding Defaults in Deployment:**

```python
from prefect.deployments import Deployment
from prefect.infrastructure import KubernetesJob

# Deployment with custom resources (overrides pool defaults)
deployment = Deployment.build_from_flow(
    flow=run_ingestion_flow,
    name="large-ingestion",
    work_pool_name="kubernetes-pool",
    work_queue_name="high-memory",
    # Override defaults
    job_variables={
        "cpu_request": "8",
        "cpu_limit": "16",
        "memory_request": "32Gi",
        "memory_limit": "64Gi",
    }
)
```

---

## 4. Work Queues Explained

### 4.1 What Problem Do Work Queues Solve?

**Scenario: Different Workload Types**

Within Kubernetes (same infrastructure), you have:
- Standard ingestions (2 CPU, 4GB RAM)
- Large ingestions (8 CPU, 32GB RAM)
- Manual triggers (need fast response)

**Without Work Queues:**
```
All flows in one queue
    ↓
All workers pull from same queue
    ↓
Problems:
  - Large ingestion blocks small ones
  - Manual triggers wait behind scheduled runs
  - No resource specialization
```

**With Work Queues:**
```
kubernetes-pool
    │
    ├─ default queue → Standard workers (3-5 pods)
    ├─ high-memory queue → High-memory workers (2 pods)
    └─ priority queue → Fast-response workers (1-2 pods)
```

### 4.2 Work Queue Structure

```
┌────────────────────────────────────────────────────────┐
│  Work Queue: "high-memory"                             │
│  Pool: kubernetes-pool                                 │
│                                                        │
│  Priority: 10 (higher = processed first)               │
│  Concurrency Limit: 5 (max 5 concurrent runs)          │
│  Paused: false                                         │
│                                                        │
│  Pending Runs: 3                                       │
│    ├─ Flow Run: ingestion-abc-123 (created 2 min ago) │
│    ├─ Flow Run: ingestion-def-456 (created 1 min ago) │
│    └─ Flow Run: ingestion-ghi-789 (created 30 sec ago)│
│                                                        │
│  Running Runs: 2                                       │
│    ├─ Flow Run: ingestion-xyz-001 (started 5 min ago) │
│    └─ Flow Run: ingestion-xyz-002 (started 3 min ago) │
│                                                        │
│  Workers Polling: 2                                    │
└────────────────────────────────────────────────────────┘
```

### 4.3 Creating Work Queues

**Via Prefect CLI:**
```bash
# Create work queue in a pool
prefect work-queue create high-memory \
  --pool kubernetes-pool \
  --priority 10

# Create default queue (auto-created if not exists)
prefect work-queue create default \
  --pool kubernetes-pool \
  --priority 0

# List work queues
prefect work-queue ls --pool kubernetes-pool

# Output:
# ┏━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━┓
# ┃ Name         ┃ Priority ┃ Concurrency    ┃ Paused ┃
# ┡━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━┩
# │ priority     │ 100      │ 3              │ False  │
# │ high-memory  │ 10       │ 5              │ False  │
# │ default      │ 0        │ 10             │ False  │
# └──────────────┴──────────┴────────────────┴────────┘
```

**Via Prefect UI:**
```
1. Navigate to: Work Pools → kubernetes-pool
2. Click: "Create Queue"
3. Enter Name: "high-memory"
4. Set Priority: 10
5. Set Concurrency Limit: 5
6. Click: "Create"
```

**Via Python API:**
```python
from prefect.client import get_client

async def create_work_queue():
    async with get_client() as client:
        queue = await client.create_work_queue(
            name="high-memory",
            work_pool_name="kubernetes-pool",
            priority=10,
            concurrency_limit=5,
        )
        print(f"Created work queue: {queue.name}")
```

### 4.4 Queue Priority

**How Priority Works:**

```python
# Priority values (higher number = higher priority)
priority_queue: priority = 100
high_memory_queue: priority = 10
default_queue: priority = 0

# Worker polling logic:
while True:
    # Check queues in priority order
    for queue in sorted_queues_by_priority_desc():
        if queue.has_pending_runs() and queue.concurrency_available():
            flow_run = queue.pull_run()
            execute(flow_run)
            break

    time.sleep(poll_interval)
```

**Example Scenario:**

```
Time: 10:00:00
Queues:
  - priority (100): 2 pending runs
  - high-memory (10): 5 pending runs
  - default (0): 10 pending runs

Worker polls:
  1. Check priority queue → Pull run (priority queue now has 1 pending)
  2. Loop
  3. Check priority queue → Pull run (priority queue now empty)
  4. Loop
  5. Check priority queue → Empty
  6. Check high-memory queue → Pull run
  7. ...continues with high-memory...
  8. ...eventually processes default queue...

Result: Priority work always processed first
```

---

## 5. Workers Explained

### 5.1 Worker Lifecycle

```
┌────────────────────────────────────────────────────────┐
│  Worker Startup                                        │
└────────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  1. Connect to Prefect Server                          │
│     - Authenticate (if using Prefect Cloud)            │
│     - Register worker instance                         │
└────────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  2. Subscribe to Work Pool + Queue(s)                  │
│     - Work Pool: kubernetes-pool                       │
│     - Queue: high-memory                               │
└────────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  3. Main Loop (Continuous)                             │
│                                                        │
│     while True:                                        │
│         # Send heartbeat                               │
│         send_heartbeat_to_server()                     │
│                                                        │
│         # Poll for work                                │
│         flow_run = poll_work_queue()                   │
│                                                        │
│         if flow_run:                                   │
│             # Check concurrency limit                  │
│             if can_accept_work():                      │
│                 # Execute (in-process or submit job)   │
│                 execute_flow_run(flow_run)             │
│                                                        │
│         sleep(poll_interval)  # 10 seconds default     │
└────────────────────────────────────────────────────────┘
```

### 5.2 Worker Configuration

**Starting a Worker:**

```bash
# Basic worker (process type)
prefect worker start \
  --pool process-pool \
  --work-queue default

# Kubernetes worker with multiple queues
prefect worker start \
  --pool kubernetes-pool \
  --work-queue default \
  --work-queue high-memory \
  --limit 5  # Max 5 concurrent flows

# Worker with custom poll interval
prefect worker start \
  --pool kubernetes-pool \
  --work-queue priority \
  --prefetch-seconds 10  # Poll every 10 seconds
```

**Kubernetes Deployment:**

```yaml
# kubernetes/prefect-worker.yaml
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
          - kubernetes-pool
          - --work-queue
          - default
          - --type
          - process  # OR kubernetes
          - --limit
          - "5"  # Max 5 concurrent flows per worker
        env:
        - name: PREFECT_API_URL
          value: "http://prefect-server:4200/api"
        - name: PREFECT_WORKER_HEARTBEAT_SECONDS
          value: "30"  # Heartbeat every 30 seconds
        - name: PREFECT_WORKER_PREFETCH_SECONDS
          value: "10"  # Poll every 10 seconds
        resources:
          requests:
            cpu: 2
            memory: 4Gi
          limits:
            cpu: 4
            memory: 8Gi
```

### 5.3 Worker Polling Behavior

**Single Queue:**

```python
# Worker polls single queue
worker = Worker(
    pool="kubernetes-pool",
    queues=["default"]
)

# Polling logic:
while True:
    flow_run = poll_queue("default")
    if flow_run:
        execute(flow_run)
    sleep(10)
```

**Multiple Queues (Priority Order):**

```python
# Worker polls multiple queues (in order)
worker = Worker(
    pool="kubernetes-pool",
    queues=["priority", "high-memory", "default"]  # Order matters!
)

# Polling logic:
while True:
    # Check queues in order specified
    for queue in ["priority", "high-memory", "default"]:
        flow_run = poll_queue(queue)
        if flow_run:
            execute(flow_run)
            break  # Execute one, then loop
    sleep(10)
```

**Concurrency Limit:**

```python
# Worker with concurrency limit
worker = Worker(
    pool="kubernetes-pool",
    queues=["default"],
    limit=5  # Max 5 concurrent flows
)

# Polling logic:
active_runs = []

while True:
    # Only poll if below limit
    if len(active_runs) < 5:
        flow_run = poll_queue("default")
        if flow_run:
            # Execute in background thread/process
            task = execute_async(flow_run)
            active_runs.append(task)

    # Clean up completed runs
    active_runs = [t for t in active_runs if not t.done()]

    sleep(10)
```

---

## 6. How They Work Together

### 6.1 Full Flow Execution

```
┌─────────────────────────────────────────────────────────────┐
│  Step 1: User Creates Deployment (via API)                  │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 2: Prefect Server Stores Deployment                   │
│                                                             │
│  Deployment:                                                │
│    - flow_id: run_ingestion_flow                            │
│    - schedule: cron("0 2 * * *")                            │
│    - work_pool_name: "kubernetes-pool"                      │
│    - work_queue_name: "high-memory"                         │
│    - parameters: {"ingestion_id": "abc-123"}                │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 3: Scheduler Creates Flow Run (at 02:00 AM)           │
│                                                             │
│  Flow Run:                                                  │
│    - id: run-xyz-789                                        │
│    - state: SCHEDULED                                       │
│    - deployment_id: deployment-abc                          │
│    - work_pool_name: "kubernetes-pool"                      │
│    - work_queue_name: "high-memory"                         │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 4: Flow Run Placed in Work Queue                      │
│                                                             │
│  Work Queue: "high-memory"                                  │
│    Pending Runs: [run-xyz-789, ...]                         │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 5: Worker Polls Work Queue                            │
│                                                             │
│  Worker:                                                    │
│    - Pool: kubernetes-pool                                  │
│    - Queues: [high-memory]                                  │
│    - Type: kubernetes (submits jobs)                        │
│                                                             │
│  GET /work_pools/kubernetes-pool/queues/high-memory/runs    │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 6: Worker Receives Flow Run                           │
│                                                             │
│  Response:                                                  │
│    - flow_run_id: run-xyz-789                               │
│    - deployment_id: deployment-abc                          │
│    - parameters: {"ingestion_id": "abc-123"}                │
│    - infrastructure: KubernetesJob(...)                     │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 7: Worker Updates State to RUNNING                    │
│                                                             │
│  POST /flow_runs/run-xyz-789/set_state                      │
│    state: RUNNING                                           │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 8: Worker Executes Flow                               │
│                                                             │
│  If Process Worker:                                         │
│    - Execute flow in-process                                │
│                                                             │
│  If Kubernetes Worker:                                      │
│    - Create Kubernetes Job                                  │
│    - Submit to K8s API                                      │
│    - Monitor Job status                                     │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 9: Flow Execution (Your Code Runs)                    │
│                                                             │
│  run_ingestion_flow(ingestion_id="abc-123")                 │
│    → discover_files()                                       │
│    → process_files()                                        │
│    → update_state()                                         │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 10: Worker Updates State to COMPLETED                 │
│                                                             │
│  POST /flow_runs/run-xyz-789/set_state                      │
│    state: COMPLETED                                         │
│    result: {"records_ingested": 1000, ...}                  │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 11: Prefect Server Stores Result                      │
│                                                             │
│  Flow Run:                                                  │
│    - state: COMPLETED                                       │
│    - end_time: 2025-11-07 02:05:30                          │
│    - total_run_time: 330s                                   │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 Diagram: Complete Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                        Prefect Server                             │
│                        (Kubernetes Pod)                           │
│                                                                   │
│  Database (PostgreSQL):                                           │
│    ├─ Deployments                                                │
│    ├─ Flow Runs                                                  │
│    ├─ Work Pools                                                 │
│    └─ Work Queues                                                │
│                                                                   │
│  Scheduler:                                                       │
│    - Triggers flow runs based on schedules                        │
│    - Places runs in work queues                                   │
│                                                                   │
│  API:                                                             │
│    - GET /work_pools/{pool}/queues/{queue}/runs (workers poll)   │
│    - POST /flow_runs/{id}/set_state (workers update state)       │
│    - GET /deployments (user queries)                             │
└───────────────────────────────────────────────────────────────────┘
                              │
                ┌─────────────┴─────────────┐
                │                           │
                ▼                           ▼
┌─────────────────────────────┐   ┌─────────────────────────────┐
│  Work Pool: kubernetes-pool │   │  Work Pool: process-pool    │
│  Type: kubernetes           │   │  Type: process              │
│                             │   │                             │
│  Queues:                    │   │  Queues:                    │
│    ├─ default               │   │    └─ default               │
│    ├─ high-memory           │   │                             │
│    └─ priority              │   │  Pending Runs: 2            │
│                             │   └─────────────────────────────┘
│  Pending Runs: 5            │                 │
└─────────────────────────────┘                 │
         │                                      │
         │                                      ▼
         │                        ┌─────────────────────────────┐
         │                        │  Worker 4 (Process Type)    │
         │                        │  - Polls: process-pool/     │
         │                        │           default           │
         │                        │  - Executes in-process      │
         │                        └─────────────────────────────┘
         │
         ├──────────────┬─────────────┬─────────────┐
         ▼              ▼             ▼             ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Worker 1    │ │  Worker 2    │ │  Worker 3    │ │  Worker 5    │
│  (K8s Type)  │ │  (K8s Type)  │ │  (Process)   │ │  (K8s Type)  │
│              │ │              │ │              │ │              │
│  Polls:      │ │  Polls:      │ │  Polls:      │ │  Polls:      │
│  kubernetes- │ │  kubernetes- │ │  kubernetes- │ │  kubernetes- │
│  pool/       │ │  pool/       │ │  pool/       │ │  pool/       │
│  default     │ │  default     │ │  priority    │ │  high-memory │
│              │ │              │ │              │ │              │
│  Submits:    │ │  Submits:    │ │  Executes:   │ │  Submits:    │
│  K8s Jobs    │ │  K8s Jobs    │ │  In-process  │ │  K8s Jobs    │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
       │                │                │                │
       │ Creates Job    │                │                │ Creates Job
       ▼                ▼                │                ▼
┌──────────────┐ ┌──────────────┐       │         ┌──────────────┐
│  K8s Job Pod │ │  K8s Job Pod │       │         │  K8s Job Pod │
│  (Run 1)     │ │  (Run 2)     │       │         │  (Run 3)     │
│              │ │              │       │         │              │
│  Executes    │ │  Executes    │       │         │  Executes    │
│  flow code   │ │  flow code   │       │         │  flow code   │
│              │ │              │       │         │  (high mem)  │
└──────────────┘ └──────────────┘       │         └──────────────┘
                                        │
                                        ▼
                              (Executes in Worker 3 pod)
```

---

## 7. Where Does Each Live?

### 7.1 Component Locations

```
┌──────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Prefect Server Pod                                    │ │
│  │  - Image: prefecthq/prefect:2.14-python3.11            │ │
│  │  - Service: prefect-server:4200                        │ │
│  │                                                        │ │
│  │  Contains:                                             │ │
│  │    ├─ REST API server                                 │ │
│  │    ├─ Scheduler (cron triggers)                       │ │
│  │    ├─ Work pool definitions (stored in DB)            │ │
│  │    ├─ Work queue definitions (stored in DB)           │ │
│  │    └─ Flow run state (stored in DB)                   │ │
│  │                                                        │ │
│  │  Does NOT contain:                                     │ │
│  │    ✗ Workers (separate pods)                          │ │
│  │    ✗ Your flow code (in worker images)               │ │
│  │    ✗ Execution logic (workers handle)                │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  PostgreSQL Pod (Prefect Database)                     │ │
│  │  - Image: postgres:15                                  │ │
│  │                                                        │ │
│  │  Stores:                                               │ │
│  │    ├─ Work pools (metadata)                           │ │
│  │    ├─ Work queues (metadata)                          │ │
│  │    ├─ Deployments                                     │ │
│  │    ├─ Flow runs (state, results)                      │ │
│  │    ├─ Logs                                            │ │
│  │    └─ Task runs                                       │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Worker Pods (Multiple)                                │ │
│  │  - Image: iomete/autoloader-worker:latest              │ │
│  │  - Replicas: 3-5                                       │ │
│  │                                                        │ │
│  │  Contains:                                             │ │
│  │    ├─ Prefect worker process (polling)                │ │
│  │    ├─ Your flow code (run_ingestion_flow, etc.)       │ │
│  │    ├─ Dependencies (Spark, boto3, etc.)               │ │
│  │    └─ Execution logic                                 │ │
│  │                                                        │ │
│  │  Connects to:                                          │ │
│  │    ├─ Prefect Server (API)                            │ │
│  │    ├─ PostgreSQL (Autoloader DB)                      │ │
│  │    └─ Spark Connect                                   │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Job Pods (Kubernetes Worker creates these)           │ │
│  │  - Image: iomete/autoloader-worker:latest              │ │
│  │  - Lifecycle: Created → Run → Complete → Delete       │ │
│  │                                                        │ │
│  │  Created dynamically by Kubernetes workers             │ │
│  │  (NOT by process workers)                              │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### 7.2 Summary Table

| Component | Lives In | Created By | Stored In |
|-----------|----------|------------|-----------|
| **Work Pool** | Prefect Server (metadata) | Admin/CLI/API | PostgreSQL (Prefect DB) |
| **Work Queue** | Prefect Server (metadata) | Admin/CLI/API | PostgreSQL (Prefect DB) |
| **Worker** | Kubernetes Pod (separate from server) | Kubernetes Deployment | N/A (process) |
| **Deployment** | Prefect Server (metadata) | User/API | PostgreSQL (Prefect DB) |
| **Flow Run** | Prefect Server (state) | Scheduler/Manual trigger | PostgreSQL (Prefect DB) |
| **Flow Code** | Worker image | Build process | Container Registry |
| **Execution** | Worker pod OR Job pod | Worker (pulls/submits) | N/A (runtime) |

---

## 8. IOMETE Autoloader Design

### 8.1 Recommended Architecture

```
┌────────────────────────────────────────────────────────────┐
│  Prefect Server                                            │
│  - Deployment: 2 replicas (HA)                             │
│  - Service: prefect-server:4200                            │
│  - Database: PostgreSQL (shared with Autoloader)           │
└────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────────┐
│  Work Pool: "autoloader-process-pool"                      │
│  Type: process                                             │
│                                                            │
│  Queues:                                                   │
│    ├─ default (priority: 0)                                │
│    └─ priority (priority: 100)                             │
└────────────────────────────────────────────────────────────┘
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Process     │ │  Process     │ │  Process     │
│  Worker 1    │ │  Worker 2    │ │  Worker 3    │
│              │ │              │ │              │
│  Polls:      │ │  Polls:      │ │  Polls:      │
│  - default   │ │  - default   │ │  - priority  │
│              │ │              │ │              │
│  Limit: 5    │ │  Limit: 5    │ │  Limit: 3    │
└──────────────┘ └──────────────┘ └──────────────┘

┌────────────────────────────────────────────────────────────┐
│  Work Pool: "autoloader-k8s-pool"                          │
│  Type: kubernetes                                          │
│                                                            │
│  Queues:                                                   │
│    └─ high-memory (priority: 10)                           │
└────────────────────────────────────────────────────────────┘
                        │
         ┌──────────────┴──────────────┐
         ▼                             ▼
┌──────────────┐               ┌──────────────┐
│  K8s Worker 1│               │  K8s Worker 2│
│  (submitter) │               │  (submitter) │
│              │               │              │
│  Polls:      │               │  Polls:      │
│  - high-     │               │  - high-     │
│    memory    │               │    memory    │
│              │               │              │
│  Submits Jobs│               │  Submits Jobs│
└──────────────┘               └──────────────┘
       │                              │
       ▼ Creates                      ▼ Creates
┌──────────────┐               ┌──────────────┐
│  Job Pod 1   │               │  Job Pod 2   │
│  8 CPU       │               │  8 CPU       │
│  32GB RAM    │               │  32GB RAM    │
└──────────────┘               └──────────────┘
```

### 8.2 Configuration

**Create Work Pools:**

```bash
# Process pool (for standard ingestions)
prefect work-pool create autoloader-process-pool \
  --type process \
  --set-as-default

# Kubernetes pool (for large ingestions)
prefect work-pool create autoloader-k8s-pool \
  --type kubernetes
```

**Create Work Queues:**

```bash
# Process pool queues
prefect work-queue create default \
  --pool autoloader-process-pool \
  --priority 0 \
  --concurrency-limit 10

prefect work-queue create priority \
  --pool autoloader-process-pool \
  --priority 100 \
  --concurrency-limit 5

# Kubernetes pool queue
prefect work-queue create high-memory \
  --pool autoloader-k8s-pool \
  --priority 10 \
  --concurrency-limit 5
```

**Deploy Workers:**

```yaml
# Process workers
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-process
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: worker
        image: iomete/autoloader-worker:latest
        command:
          - prefect
          - worker
          - start
          - --pool
          - autoloader-process-pool
          - --work-queue
          - default
          - --work-queue
          - priority  # Poll both queues (priority first)
          - --type
          - process
          - --limit
          - "5"
---
# Kubernetes workers
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-k8s
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: worker
        image: prefecthq/prefect:2.14-python3.11
        command:
          - prefect
          - worker
          - start
          - --pool
          - autoloader-k8s-pool
          - --work-queue
          - high-memory
          - --type
          - kubernetes
```

### 8.3 Routing Logic

```python
# app/services/prefect_service.py

class PrefectService:
    def get_deployment_config(self, ingestion: Ingestion) -> dict:
        """
        Determine work pool, queue, and infrastructure.
        """
        # Estimate ingestion size
        estimated_size_gb = self._estimate_size(ingestion)

        # Manual trigger → priority queue (process pool)
        if ingestion.is_manual_trigger:
            return {
                "work_pool_name": "autoloader-process-pool",
                "work_queue_name": "priority",
                "infrastructure": None,  # Process worker executes
            }

        # Large ingestion → Kubernetes pool
        elif estimated_size_gb > 10:
            return {
                "work_pool_name": "autoloader-k8s-pool",
                "work_queue_name": "high-memory",
                "infrastructure": KubernetesJob(
                    image="iomete/autoloader-worker:latest",
                    cpu_request="8",
                    memory_request="32Gi",
                ),
            }

        # Default → process pool
        else:
            return {
                "work_pool_name": "autoloader-process-pool",
                "work_queue_name": "default",
                "infrastructure": None,  # Process worker executes
            }

    async def create_deployment(self, ingestion: Ingestion) -> str:
        """
        Create deployment with appropriate config.
        """
        config = self.get_deployment_config(ingestion)

        async with get_client() as client:
            deployment = await client.create_deployment(
                flow_id=self.ingestion_flow_id,
                name=f"ingestion-{ingestion.id}",
                parameters={"ingestion_id": str(ingestion.id)},
                schedule=CronSchedule(cron=ingestion.schedule_cron),
                **config,  # work_pool_name, work_queue_name, infrastructure
            )

            return str(deployment.id)
```

---

## 9. Complete Example

### 9.1 Scenario: Create and Execute Ingestion

**Step 1: Setup (One-time)**

```bash
# 1. Create work pools
prefect work-pool create autoloader-process-pool --type process
prefect work-pool create autoloader-k8s-pool --type kubernetes

# 2. Create work queues
prefect work-queue create default --pool autoloader-process-pool
prefect work-queue create high-memory --pool autoloader-k8s-pool

# 3. Deploy workers (Kubernetes)
kubectl apply -f kubernetes/prefect-workers.yaml

# 4. Verify
prefect work-pool ls
prefect work-queue ls --pool autoloader-process-pool
kubectl get pods -l app=prefect-worker
```

**Step 2: Create Ingestion (API)**

```bash
# User creates ingestion via API
curl -X POST http://autoloader-api/api/v1/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "AWS Logs Ingestion",
    "source_type": "S3",
    "source_path": "s3://my-bucket/logs/",
    "schedule_cron": "0 2 * * *",
    "destination_table": "logs.aws_logs"
  }'

# Response:
# {
#   "id": "abc-123",
#   "name": "AWS Logs Ingestion",
#   "prefect_deployment_id": "deploy-xyz-789",
#   "status": "ACTIVE"
# }
```

**Behind the Scenes (API Handler):**

```python
@router.post("/ingestions")
async def create_ingestion(ingestion_data: IngestionCreate):
    # 1. Create in database
    ingestion = ingestion_service.create(ingestion_data)

    # 2. Determine work pool/queue
    config = prefect_service.get_deployment_config(ingestion)
    # Result: {"work_pool_name": "autoloader-process-pool",
    #          "work_queue_name": "default"}

    # 3. Create Prefect deployment
    deployment_id = await prefect_service.create_deployment(ingestion)

    # 4. Update ingestion
    ingestion.prefect_deployment_id = deployment_id
    db.commit()

    return ingestion
```

**Behind the Scenes (Prefect Server):**

```sql
-- Prefect Server stores deployment in database

INSERT INTO deployments (
  id,
  name,
  flow_id,
  schedule,
  work_pool_id,
  work_queue_id,
  parameters
) VALUES (
  'deploy-xyz-789',
  'ingestion-abc-123',
  'flow-run-ingestion',
  '{"cron": "0 2 * * *"}',
  'pool-process',
  'queue-default',
  '{"ingestion_id": "abc-123"}'
);
```

**Step 3: Scheduled Execution (02:00 AM)**

```
02:00:00 - Prefect Scheduler runs (internal process)
    ↓
    SELECT * FROM deployments WHERE schedule matches NOW()
    ↓
    Found: deployment-xyz-789 (cron: "0 2 * * *")
    ↓
    CREATE flow_run:
      - deployment_id: deploy-xyz-789
      - state: SCHEDULED
      - work_queue_id: queue-default
    ↓
    INSERT INTO flow_runs (
      id: 'run-abc-001',
      deployment_id: 'deploy-xyz-789',
      state: 'SCHEDULED',
      work_queue_id: 'queue-default'
    )
    ↓
    UPDATE flow_runs SET state = 'PENDING' WHERE id = 'run-abc-001'
    ↓
    Flow run now in work queue (ready for workers to poll)
```

**Step 4: Worker Polls and Executes**

```
Worker 1 (process worker):
    ↓
    Polling loop (every 10 seconds)
    ↓
    GET /work_pools/autoloader-process-pool/queues/default/runs
    ↓
    Response: flow_run_id = run-abc-001
    ↓
    POST /flow_runs/run-abc-001/set_state {"state": "RUNNING"}
    ↓
    Execute flow in-process:
      from app.prefect.flows.run_ingestion import run_ingestion_flow
      result = run_ingestion_flow(ingestion_id="abc-123")
    ↓
    Flow completes successfully
    ↓
    POST /flow_runs/run-abc-001/set_state {
      "state": "COMPLETED",
      "result": {"records_ingested": 1000}
    }
    ↓
    Continue polling...
```

**Step 5: User Queries Status**

```bash
# User queries run history
curl http://autoloader-api/api/v1/ingestions/abc-123/runs

# API queries Prefect
GET /deployments/deploy-xyz-789/flow_runs

# Response:
# {
#   "runs": [
#     {
#       "id": "run-abc-001",
#       "state": "COMPLETED",
#       "start_time": "2025-11-07T02:00:00",
#       "end_time": "2025-11-07T02:05:30",
#       "result": {"records_ingested": 1000}
#     }
#   ]
# }
```

---

## 10. Common Patterns

### 10.1 Pattern: Multi-Tenant Isolation

```python
# Create work pool per tenant
for tenant in ["acme", "globex", "initech"]:
    prefect work-pool create f"autoloader-{tenant}-pool" --type process

# Create deployment with tenant-specific pool
def create_deployment(ingestion: Ingestion):
    work_pool = f"autoloader-{ingestion.tenant_id}-pool"

    return client.create_deployment(
        flow_id=ingestion_flow_id,
        work_pool_name=work_pool,  # Tenant-specific
        work_queue_name="default",
        tags=[f"tenant:{ingestion.tenant_id}"],
    )

# Deploy workers per tenant
# kubernetes/prefect-worker-acme.yaml
# kubernetes/prefect-worker-globex.yaml
# kubernetes/prefect-worker-initech.yaml
```

### 10.2 Pattern: Environment Separation

```python
# Work pools per environment
prefect work-pool create autoloader-dev-pool --type process
prefect work-pool create autoloader-staging-pool --type process
prefect work-pool create autoloader-prod-pool --type process

# Deployments use environment-specific pools
def create_deployment(ingestion: Ingestion, environment: str):
    work_pool = f"autoloader-{environment}-pool"

    return client.create_deployment(
        flow_id=ingestion_flow_id,
        work_pool_name=work_pool,
        tags=[f"env:{environment}"],
    )
```

### 10.3 Pattern: Resource-Based Routing

```python
# Queues based on resource needs
queues = {
    "small": {"cpu": "1", "memory": "2Gi"},
    "medium": {"cpu": "2", "memory": "4Gi"},
    "large": {"cpu": "4", "memory": "8Gi"},
    "xlarge": {"cpu": "8", "memory": "16Gi"},
}

def get_queue(estimated_size_gb: float) -> str:
    if estimated_size_gb < 1:
        return "small"
    elif estimated_size_gb < 5:
        return "medium"
    elif estimated_size_gb < 20:
        return "large"
    else:
        return "xlarge"

# Workers specialized for each queue
# prefect-worker-small (10 workers, 1 CPU, 2GB each)
# prefect-worker-medium (5 workers, 2 CPU, 4GB each)
# prefect-worker-large (3 workers, 4 CPU, 8GB each)
# prefect-worker-xlarge (2 workers, 8 CPU, 16GB each)
```

---

## 11. Summary

### 11.1 Key Concepts

**Work Pool:**
- Logical grouping of infrastructure
- Represents WHERE work can run (Kubernetes, ECS, Process, etc.)
- Defined on Prefect Server
- Contains work queues

**Work Queue:**
- Named queue within a work pool
- Represents WHAT work to prioritize
- Holds pending flow runs
- Workers poll specific queues

**Worker:**
- Process that executes work
- Polls work queues for flow runs
- Can execute in-process OR submit jobs
- Runs outside Prefect Server (separate pods)

**Hierarchy:**
```
Prefect Server (Brain)
    │
    ├─ Work Pool (Infrastructure Type)
    │   │
    │   ├─ Work Queue (Prioritization)
    │   │   │
    │   │   └─ Flow Runs (Pending Work)
    │   │
    │   └─ Workers (Executors, poll queues)
```

### 11.2 Restaurant Analogy Summary

```
Restaurant Manager   = Prefect Server
Kitchen/Bar/Bakery   = Work Pools
Hot Food/Cold Food   = Work Queues
Chefs/Bartenders     = Workers
Orders               = Flow Runs
Customers            = Users (API)

Flow:
Customer places order → Manager assigns to kitchen/queue →
Chef picks up order → Chef cooks → Dish served
```

### 11.3 For IOMETE Autoloader

**Recommended Setup:**
- **2 Work Pools**: process-pool (standard), k8s-pool (large)
- **3 Work Queues**: default, priority, high-memory
- **5-7 Workers**: 3-5 process workers, 2 Kubernetes workers
- **Routing Logic**: Automatic based on ingestion size/priority

**This gives you:**
- ✅ Flexible resource allocation
- ✅ Priority handling
- ✅ Cost optimization
- ✅ Horizontal scaling

---

**End of Document**

**Key Takeaway:**
- **Work Pools** = Infrastructure types (WHERE)
- **Work Queues** = Prioritization (WHAT)
- **Workers** = Executors (WHO)
- **All coordinated by Prefect Server (BRAIN)**

