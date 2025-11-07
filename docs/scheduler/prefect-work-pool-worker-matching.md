# Prefect: Work Pool Type vs Worker Type Matching

**Document Version:** 1.0
**Created:** 2025-11-07
**Status:** Educational
**Related:** `prefect-architecture-explained.md`, `prefect-worker-types-explained.md`

---

## Table of Contents

1. [The Critical Rule](#the-critical-rule)
2. [Why The Match Matters](#why-the-match-matters)
3. [What Happens If They Don't Match](#what-happens-if-they-dont-match)
4. [All Combinations Explained](#all-combinations-explained)
5. [How Prefect Enforces The Match](#how-prefect-enforces-the-match)
6. [Common Confusion Clarified](#common-confusion-clarified)
7. [Practical Examples](#practical-examples)
8. [IOMETE Autoloader Configuration](#iomete-autoloader-configuration)

---

## 1. The Critical Rule

### 1.1 The Rule

**⚠️ CRITICAL: Work Pool Type MUST match Worker Type**

```
Work Pool Type = "kubernetes"  →  Worker Type MUST be "kubernetes"
Work Pool Type = "process"     →  Worker Type MUST be "process"
Work Pool Type = "docker"      →  Worker Type MUST be "docker"
Work Pool Type = "ecs"         →  Worker Type MUST be "ecs"
```

**If they don't match, the worker CANNOT execute the flow run.**

### 1.2 Why?

**Each worker type knows HOW to execute flows in a specific way:**

- **Process worker** knows how to execute flows **in-process**
- **Kubernetes worker** knows how to **submit Kubernetes Jobs**
- **Docker worker** knows how to **run Docker containers**
- **ECS worker** knows how to **submit AWS ECS tasks**

**Each work pool type defines WHERE/HOW flows should run:**

- **Process pool** says: "Flows should run in-process"
- **Kubernetes pool** says: "Flows should run as Kubernetes Jobs"
- **Docker pool** says: "Flows should run as Docker containers"
- **ECS pool** says: "Flows should run as ECS tasks"

**Mismatch = Worker doesn't know how to execute what the pool is asking for.**

---

## 2. Why The Match Matters

### 2.1 Work Pool Defines Execution Strategy

**Work Pool Configuration Contains Infrastructure Details:**

```yaml
# Kubernetes Work Pool (stored in Prefect Server)
name: kubernetes-pool
type: kubernetes  # ← This is critical!

base_job_template:
  job_configuration:
    # Kubernetes-specific configuration
    namespace: iomete
    service_account_name: prefect-worker
    image: iomete/autoloader-worker:latest
    command: ["python", "-m", "prefect.engine"]

    # These only make sense for Kubernetes
    cpu_request: "2"
    memory_request: "4Gi"
    restart_policy: Never
    ttl_seconds_after_finished: 3600
```

**Process workers DON'T understand Kubernetes configuration:**
- They don't know what `namespace` means
- They can't create Kubernetes Jobs
- They can't set `cpu_request` on a Kubernetes Job

**Result:** Process worker would fail to execute flows from a Kubernetes pool.

---

### 2.2 Worker Type Defines Execution Capability

**Kubernetes Worker:**
```python
class KubernetesWorker:
    def execute_flow_run(self, flow_run, deployment):
        # 1. Parse Kubernetes-specific config from deployment
        namespace = deployment.infrastructure.namespace
        image = deployment.infrastructure.image
        cpu = deployment.infrastructure.cpu_request

        # 2. Generate Kubernetes Job manifest
        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "image": image,
                            "resources": {
                                "requests": {"cpu": cpu}
                            }
                        }]
                    }
                }
            }
        }

        # 3. Submit to Kubernetes API
        kubernetes_api.create_job(job_manifest)

        # 4. Monitor Job status
        watch_job_until_complete()
```

**Process Worker:**
```python
class ProcessWorker:
    def execute_flow_run(self, flow_run, deployment):
        # 1. Parse process-specific config (minimal)
        # No Kubernetes concepts!

        # 2. Execute flow directly in this process
        from prefect.engine import run_flow
        result = run_flow(
            flow=flow_run.flow,
            parameters=flow_run.parameters
        )

        # 3. No external infrastructure interaction
        return result
```

**If Process Worker tries to execute from Kubernetes Pool:**
```python
# Process worker receives flow run from Kubernetes pool
deployment.infrastructure.namespace  # ← Process worker: "What's namespace??"
deployment.infrastructure.cpu_request  # ← Process worker: "What's cpu_request??"

# Process worker doesn't know how to create Kubernetes Jobs
kubernetes_api.create_job(...)  # ← ERROR: kubernetes_api doesn't exist!
```

---

## 3. What Happens If They Don't Match

### 3.1 Scenario 1: Process Worker + Kubernetes Pool

**Setup:**
```python
# Create Kubernetes work pool
prefect work-pool create k8s-pool --type kubernetes

# Start PROCESS worker (wrong type!)
prefect worker start --pool k8s-pool --type process
```

**What Happens:**

```
Worker startup:
    ↓
    Trying to connect to work pool "k8s-pool"
    ↓
    Prefect Server checks:
      - Work Pool Type: kubernetes
      - Worker Type: process
      - Match? NO
    ↓
    ❌ ERROR: Worker type "process" cannot serve pool type "kubernetes"
    ↓
    Worker fails to start
```

**Error Message:**
```
WorkerTypeNotSupportedError: Worker type 'process' is not compatible with
work pool type 'kubernetes'. Worker type must be 'kubernetes'.
```

**Result:** Worker refuses to start.

---

### 3.2 Scenario 2: Kubernetes Worker + Process Pool

**Setup:**
```python
# Create process work pool
prefect work-pool create process-pool --type process

# Start KUBERNETES worker (wrong type!)
prefect worker start --pool process-pool --type kubernetes
```

**What Happens:**

```
Worker startup:
    ↓
    Trying to connect to work pool "process-pool"
    ↓
    Prefect Server checks:
      - Work Pool Type: process
      - Worker Type: kubernetes
      - Match? NO
    ↓
    ❌ ERROR: Worker type "kubernetes" cannot serve pool type "process"
    ↓
    Worker fails to start
```

**Error Message:**
```
WorkerTypeNotSupportedError: Worker type 'kubernetes' is not compatible with
work pool type 'process'. Worker type must be 'process'.
```

**Result:** Worker refuses to start.

---

### 3.3 Prefect Prevents This At Startup

**Worker Registration Process:**

```python
# When worker starts up

# 1. Worker connects to Prefect Server
worker.connect_to_server()

# 2. Worker sends registration request
registration = {
    "worker_type": "process",  # What the worker CAN do
    "work_pool_name": "k8s-pool"  # What pool it wants to serve
}

# 3. Prefect Server validates
work_pool = get_work_pool("k8s-pool")

if work_pool.type != registration.worker_type:
    raise WorkerTypeNotSupportedError(
        f"Worker type '{registration.worker_type}' is not compatible "
        f"with work pool type '{work_pool.type}'. "
        f"Worker type must be '{work_pool.type}'."
    )

# 4. If validation passes, worker is allowed to register
allow_worker_registration()
```

**This validation happens BEFORE the worker starts polling queues.**

---

## 4. All Combinations Explained

### 4.1 Valid Combinations (✅ Work)

| Work Pool Type | Worker Type | Result | Use Case |
|----------------|-------------|--------|----------|
| `process` | `process` | ✅ **WORKS** | Worker executes flows in-process |
| `kubernetes` | `kubernetes` | ✅ **WORKS** | Worker submits Kubernetes Jobs |
| `docker` | `docker` | ✅ **WORKS** | Worker runs Docker containers |
| `ecs` | `ecs` | ✅ **WORKS** | Worker submits ECS tasks |
| `cloud-run` | `cloud-run` | ✅ **WORKS** | Worker submits GCP Cloud Run jobs |
| `vertex-ai` | `vertex-ai` | ✅ **WORKS** | Worker submits Vertex AI jobs |

### 4.2 Invalid Combinations (❌ Don't Work)

| Work Pool Type | Worker Type | Result | Why It Fails |
|----------------|-------------|--------|--------------|
| `kubernetes` | `process` | ❌ **FAILS** | Process worker can't create K8s Jobs |
| `process` | `kubernetes` | ❌ **FAILS** | K8s worker expects K8s config |
| `kubernetes` | `docker` | ❌ **FAILS** | Docker worker can't create K8s Jobs |
| `ecs` | `kubernetes` | ❌ **FAILS** | K8s worker can't submit ECS tasks |
| `docker` | `process` | ❌ **FAILS** | Process worker can't run Docker |
| `process` | `ecs` | ❌ **FAILS** | ECS worker expects AWS config |

### 4.3 Visual Comparison

**✅ CORRECT: Matching Types**

```
┌─────────────────────────────────┐
│  Work Pool: "k8s-pool"          │
│  Type: kubernetes               │
│                                 │
│  Config:                        │
│    - namespace: iomete          │
│    - image: my-image:latest     │
│    - cpu_request: "2"           │
└─────────────────────────────────┘
                │
                │ Worker connects
                ▼
┌─────────────────────────────────┐
│  Worker                         │
│  Type: kubernetes               │
│                                 │
│  Knows how to:                  │
│    ✅ Parse K8s config          │
│    ✅ Create K8s Jobs           │
│    ✅ Monitor K8s Jobs          │
└─────────────────────────────────┘

Result: Worker successfully executes flows!
```

**❌ INCORRECT: Mismatched Types**

```
┌─────────────────────────────────┐
│  Work Pool: "k8s-pool"          │
│  Type: kubernetes               │
│                                 │
│  Config:                        │
│    - namespace: iomete          │
│    - image: my-image:latest     │
│    - cpu_request: "2"           │
└─────────────────────────────────┘
                │
                │ Worker tries to connect
                ▼
┌─────────────────────────────────┐
│  Worker                         │
│  Type: process                  │
│                                 │
│  Knows how to:                  │
│    ❌ Parse K8s config (NO!)    │
│    ❌ Create K8s Jobs (NO!)     │
│    ❌ Monitor K8s Jobs (NO!)    │
└─────────────────────────────────┘

Result: ❌ Worker refuses to start
        (WorkerTypeNotSupportedError)
```

---

## 5. How Prefect Enforces The Match

### 5.1 Worker Registration Validation

**Code Flow (Simplified):**

```python
# prefect/server/api/workers.py

@router.post("/work_pools/{work_pool_name}/workers")
async def register_worker(
    work_pool_name: str,
    worker_data: WorkerCreate
):
    """
    Register a worker with a work pool.
    Validates that worker type matches pool type.
    """
    # 1. Get work pool from database
    work_pool = await db.get_work_pool(work_pool_name)

    if not work_pool:
        raise WorkPoolNotFoundError(work_pool_name)

    # 2. Validate worker type matches pool type
    if worker_data.type != work_pool.type:
        raise WorkerTypeNotSupportedError(
            f"Worker type '{worker_data.type}' is not compatible "
            f"with work pool type '{work_pool.type}'. "
            f"Expected worker type: '{work_pool.type}'."
        )

    # 3. If validation passes, register worker
    worker = await db.create_worker(
        work_pool_id=work_pool.id,
        worker_type=worker_data.type,
        name=worker_data.name,
    )

    return worker
```

### 5.2 When Validation Happens

```
Worker Process Starts
    ↓
    prefect worker start --pool k8s-pool --type process
    ↓
Worker attempts to register with Prefect Server
    ↓
    POST /work_pools/k8s-pool/workers
    {
      "type": "process",
      "name": "worker-1"
    }
    ↓
Prefect Server validates
    ↓
    work_pool.type = "kubernetes"
    worker.type = "process"
    ↓
    kubernetes != process
    ↓
    ❌ REJECT
    ↓
Worker receives error response
    ↓
Worker exits with error message
```

**This happens IMMEDIATELY when the worker starts, not when it tries to execute a flow.**

---

## 6. Common Confusion Clarified

### 6.1 "Can I have multiple worker types in one work pool?"

**❌ NO**

```
# This is NOT possible:

Work Pool: "mixed-pool"
Type: kubernetes  # ← Only ONE type allowed

Workers:
  - Worker 1 (type: process)      ❌ Won't connect
  - Worker 2 (type: kubernetes)   ✅ Will connect
  - Worker 3 (type: docker)       ❌ Won't connect
```

**Solution:** Create separate work pools for each type:

```
Work Pool: "process-pool"
Type: process
  ↓
  Workers: process type only

Work Pool: "k8s-pool"
Type: kubernetes
  ↓
  Workers: kubernetes type only

Work Pool: "docker-pool"
Type: docker
  ↓
  Workers: docker type only
```

---

### 6.2 "Can I change the work pool type after creation?"

**❌ NO (mostly)**

```bash
# Create pool
prefect work-pool create my-pool --type process

# Try to change type (NOT SUPPORTED)
prefect work-pool update my-pool --type kubernetes  # ❌ Error
```

**Prefect does not support changing work pool type after creation.**

**Reason:** Workers might already be registered, deployments might exist, infrastructure config would be incompatible.

**Solution:** Create a new work pool with the desired type.

```bash
# Delete old pool
prefect work-pool delete my-pool

# Create new pool with correct type
prefect work-pool create my-pool --type kubernetes
```

---

### 6.3 "My worker is starting but flows aren't executing. Why?"

**Common Causes:**

**1. Worker polling wrong queue:**
```bash
# Worker polls "default" queue
prefect worker start --pool k8s-pool --work-queue default

# But deployment uses "high-memory" queue
deployment.work_queue_name = "high-memory"

# Result: Worker never sees the flow run
```

**Solution:** Make sure worker polls the queue where deployments are placed.

```bash
# Poll multiple queues
prefect worker start --pool k8s-pool \
  --work-queue default \
  --work-queue high-memory
```

**2. Concurrency limit reached:**
```bash
# Worker limit: 5
prefect worker start --pool k8s-pool --limit 5

# Already running: 5 flows
# New flow arrives → Worker can't accept (at limit)
```

**Solution:** Increase limit or add more workers.

**3. Work queue paused:**
```bash
# Queue is paused
prefect work-queue pause high-memory --pool k8s-pool

# Workers won't pull from paused queues
```

**Solution:** Resume queue.

```bash
prefect work-queue resume high-memory --pool k8s-pool
```

---

## 7. Practical Examples

### 7.1 Example: IOMETE Autoloader Setup

**Goal:** Use process workers for standard ingestions, Kubernetes workers for large ingestions.

**Step 1: Create Work Pools**

```bash
# Process pool for standard ingestions
prefect work-pool create autoloader-process-pool \
  --type process

# Kubernetes pool for large ingestions
prefect work-pool create autoloader-k8s-pool \
  --type kubernetes
```

**Step 2: Create Work Queues**

```bash
# Queues in process pool
prefect work-queue create default \
  --pool autoloader-process-pool

prefect work-queue create priority \
  --pool autoloader-process-pool

# Queue in kubernetes pool
prefect work-queue create high-memory \
  --pool autoloader-k8s-pool
```

**Step 3: Deploy Workers (Kubernetes Manifests)**

**Process Workers:**
```yaml
# kubernetes/prefect-worker-process.yaml
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
          - autoloader-process-pool  # ← Process pool
          - --type
          - process  # ← Process type (MUST MATCH!)
          - --work-queue
          - default
          - --work-queue
          - priority
          - --limit
          - "5"
```

**Kubernetes Workers:**
```yaml
# kubernetes/prefect-worker-k8s.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-k8s
spec:
  replicas: 2
  template:
    spec:
      serviceAccountName: prefect-worker  # Needs Job creation permission
      containers:
      - name: worker
        image: prefecthq/prefect:2.14-python3.11
        command:
          - prefect
          - worker
          - start
          - --pool
          - autoloader-k8s-pool  # ← Kubernetes pool
          - --type
          - kubernetes  # ← Kubernetes type (MUST MATCH!)
          - --work-queue
          - high-memory
```

**Step 4: Verify**

```bash
# Check work pools
prefect work-pool ls

# Output:
# ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┓
# ┃ Name                     ┃ Type       ┃ Workers  ┃
# ┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━┩
# │ autoloader-process-pool  │ process    │ 3        │
# │ autoloader-k8s-pool      │ kubernetes │ 2        │
# └──────────────────────────┴────────────┴──────────┘

# Check workers
kubectl get pods -l app=prefect-worker

# Output:
# NAME                                    READY   STATUS
# prefect-worker-process-abc123-xyz       1/1     Running
# prefect-worker-process-abc123-def       1/1     Running
# prefect-worker-process-abc123-ghi       1/1     Running
# prefect-worker-k8s-xyz789-abc           1/1     Running
# prefect-worker-k8s-xyz789-def           1/1     Running
```

**Step 5: Create Deployments with Correct Pools**

```python
# app/services/prefect_service.py

async def create_deployment(self, ingestion: Ingestion) -> str:
    """
    Create deployment with correct work pool based on size.
    """
    estimated_size_gb = self._estimate_size(ingestion)

    # Large ingestion → Kubernetes pool
    if estimated_size_gb > 10:
        work_pool_name = "autoloader-k8s-pool"
        work_queue_name = "high-memory"
        infrastructure = KubernetesJob(
            image="iomete/autoloader-worker:latest",
            cpu_request="8",
            memory_request="32Gi",
        )

    # Standard ingestion → Process pool
    else:
        work_pool_name = "autoloader-process-pool"
        work_queue_name = "default"
        infrastructure = None  # Process worker executes in-process

    async with get_client() as client:
        deployment = await client.create_deployment(
            flow_id=self.ingestion_flow_id,
            name=f"ingestion-{ingestion.id}",
            parameters={"ingestion_id": str(ingestion.id)},
            schedule=CronSchedule(cron=ingestion.schedule_cron),
            work_pool_name=work_pool_name,  # ← Determines which workers can execute
            work_queue_name=work_queue_name,
            infrastructure=infrastructure,
        )

        return str(deployment.id)
```

**Flow:**
```
Small ingestion created
    ↓
    Deployment assigned to: autoloader-process-pool / default
    ↓
    Flow run placed in: autoloader-process-pool / default queue
    ↓
    Process workers poll this queue
    ↓
    Process worker executes in-process
    ↓
    ✅ Success

Large ingestion created
    ↓
    Deployment assigned to: autoloader-k8s-pool / high-memory
    ↓
    Flow run placed in: autoloader-k8s-pool / high-memory queue
    ↓
    Kubernetes workers poll this queue
    ↓
    Kubernetes worker submits K8s Job
    ↓
    Job pod executes flow
    ↓
    ✅ Success
```

---

### 7.2 Example: What Happens If You Mix Them Up

**Mistake: Deploy process worker to kubernetes pool**

```yaml
# kubernetes/prefect-worker-WRONG.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-wrong
spec:
  template:
    spec:
      containers:
      - name: worker
        command:
          - prefect
          - worker
          - start
          - --pool
          - autoloader-k8s-pool  # ← Kubernetes pool
          - --type
          - process  # ❌ WRONG TYPE!
```

**Apply:**
```bash
kubectl apply -f kubernetes/prefect-worker-WRONG.yaml

# Pod starts
kubectl get pods

# Output:
# NAME                                READY   STATUS
# prefect-worker-wrong-abc123-xyz     0/1     CrashLoopBackOff
```

**Check logs:**
```bash
kubectl logs prefect-worker-wrong-abc123-xyz

# Output:
# ERROR: WorkerTypeNotSupportedError: Worker type 'process' is not
# compatible with work pool type 'kubernetes'. Worker type must be
# 'kubernetes'.
#
# Worker failed to start.
```

**Fix:**
```yaml
# Change type to match pool
- --type
- kubernetes  # ✅ Correct!
```

---

## 8. IOMETE Autoloader Configuration

### 8.1 Recommended Configuration

**Two Work Pools, Two Worker Types:**

```
┌─────────────────────────────────────────────────────────┐
│  Work Pool: autoloader-process-pool                     │
│  Type: process                                          │
│                                                         │
│  Queues:                                                │
│    ├─ default (80% of ingestions)                      │
│    └─ priority (manual triggers)                       │
└─────────────────────────────────────────────────────────┘
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Process      │ │ Process      │ │ Process      │
│ Worker 1     │ │ Worker 2     │ │ Worker 3     │
│ Type: process│ │ Type: process│ │ Type: process│
└──────────────┘ └──────────────┘ └──────────────┘

┌─────────────────────────────────────────────────────────┐
│  Work Pool: autoloader-k8s-pool                         │
│  Type: kubernetes                                       │
│                                                         │
│  Queues:                                                │
│    └─ high-memory (20% of ingestions, large files)     │
└─────────────────────────────────────────────────────────┘
                        │
         ┌──────────────┴──────────────┐
         ▼                             ▼
┌──────────────┐               ┌──────────────┐
│ K8s Worker 1 │               │ K8s Worker 2 │
│ Type:        │               │ Type:        │
│ kubernetes   │               │ kubernetes   │
└──────────────┘               └──────────────┘
       │                              │
       ▼ Creates Jobs                 ▼ Creates Jobs
┌──────────────┐               ┌──────────────┐
│  Job Pod 1   │               │  Job Pod 2   │
│  8 CPU       │               │  8 CPU       │
│  32GB RAM    │               │  32GB RAM    │
└──────────────┘               └──────────────┘
```

### 8.2 Setup Commands

```bash
# 1. Create work pools
prefect work-pool create autoloader-process-pool --type process
prefect work-pool create autoloader-k8s-pool --type kubernetes

# 2. Create work queues
prefect work-queue create default --pool autoloader-process-pool --priority 0
prefect work-queue create priority --pool autoloader-process-pool --priority 100
prefect work-queue create high-memory --pool autoloader-k8s-pool --priority 10

# 3. Deploy workers
kubectl apply -f kubernetes/prefect-worker-process.yaml
kubectl apply -f kubernetes/prefect-worker-k8s.yaml

# 4. Verify types match
prefect work-pool inspect autoloader-process-pool
# Type: process

prefect work-pool inspect autoloader-k8s-pool
# Type: kubernetes

kubectl logs -l app=prefect-worker --tail=10 | grep "Worker type"
# Worker type: process (for process workers)
# Worker type: kubernetes (for k8s workers)
```

---

## 9. Summary

### 9.1 Key Rules

**Rule #1: Types Must Match**
```
Work Pool Type = Worker Type
```

**Rule #2: Prefect Enforces This**
```
Worker tries to connect to pool with wrong type
    ↓
Prefect Server rejects registration
    ↓
Worker fails to start
```

**Rule #3: One Type Per Pool**
```
Each work pool has exactly ONE type
You cannot change the type after creation
```

**Rule #4: Multiple Pools = Multiple Types**
```
Want both process and kubernetes execution?
    ↓
Create TWO work pools:
  - autoloader-process-pool (type: process)
  - autoloader-k8s-pool (type: kubernetes)
```

### 9.2 Common Mistakes

**❌ Mistake 1: Wrong Type**
```bash
prefect work-pool create k8s-pool --type kubernetes
prefect worker start --pool k8s-pool --type process  # ❌ ERROR
```

**✅ Correct:**
```bash
prefect work-pool create k8s-pool --type kubernetes
prefect worker start --pool k8s-pool --type kubernetes  # ✅ OK
```

**❌ Mistake 2: Assuming Types Are Interchangeable**
```
"Process workers are simpler, I'll just use them for everything"
    ↓
Creates kubernetes pool but deploys process workers
    ↓
❌ Workers refuse to start
```

**✅ Correct:**
```
"I need kubernetes execution for large jobs"
    ↓
Create kubernetes pool AND deploy kubernetes workers
    ↓
✅ Types match, workers start successfully
```

### 9.3 Quick Reference

| Work Pool Type | Compatible Worker Type | Execution Method |
|----------------|------------------------|------------------|
| `process` | `process` only | In-process |
| `kubernetes` | `kubernetes` only | Kubernetes Jobs |
| `docker` | `docker` only | Docker containers |
| `ecs` | `ecs` only | AWS ECS tasks |

### 9.4 For IOMETE Autoloader

**Use TWO configurations:**

**Configuration 1: Standard Ingestions**
- Work Pool: `autoloader-process-pool` (type: `process`)
- Workers: Process type
- Execution: In-process (low overhead)

**Configuration 2: Large Ingestions**
- Work Pool: `autoloader-k8s-pool` (type: `kubernetes`)
- Workers: Kubernetes type
- Execution: Kubernetes Jobs (isolated, custom resources)

**Both work pools, both worker types, both running simultaneously. No conflicts.**

---

**End of Document**

**Key Takeaway:**
Work Pool Type and Worker Type MUST match. Prefect enforces this at worker startup. You cannot mix them. Create separate pools for different types.
