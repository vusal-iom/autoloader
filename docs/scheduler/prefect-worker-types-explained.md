# Prefect Worker Types: Educational Guide

**Document Version:** 1.0
**Created:** 2025-11-07
**Status:** Educational
**Related:** `prefect-orchestration-guide.md`

---

## Table of Contents

1. [The Confusion: Worker vs Infrastructure](#the-confusion-worker-vs-infrastructure)
2. [Prefect Worker Types](#prefect-worker-types)
3. [Execution Models Explained](#execution-models-explained)
4. [Which Model for IOMETE Autoloader?](#which-model-for-iomete-autoloader)
5. [Detailed Comparison](#detailed-comparison)
6. [Recommendation](#recommendation)

---

## 1. The Confusion: Worker vs Infrastructure

### 1.1 Your Question

> "I thought worker submit the job somewhere (another pod in kubernetes)?"

**You're thinking of TWO different concepts:**

1. **Worker** = Process that pulls work from queue
2. **Infrastructure** = WHERE the work actually executes

**The confusion comes from Prefect's flexibility:**

```
┌────────────────────────────────────────────────────────────────┐
│                    Option A: Worker Executes Work              │
│                    (Process Worker)                            │
└────────────────────────────────────────────────────────────────┘

Worker Pod                    Prefect Server
┌──────────────────────┐      ┌─────────────────┐
│  Prefect Worker      │◄─────│  Work Queue     │
│                      │ Pull │                 │
│  ┌────────────────┐  │      └─────────────────┘
│  │ Execute Flow   │  │
│  │ (In-process)   │  │
│  │                │  │
│  │ [Your Code]    │  │
│  └────────────────┘  │
└──────────────────────┘

▲ Worker EXECUTES the work ITSELF (in its own process)


┌────────────────────────────────────────────────────────────────┐
│                    Option B: Worker Submits Work               │
│                    (Kubernetes Worker)                         │
└────────────────────────────────────────────────────────────────┘

Worker Pod                    Prefect Server
┌──────────────────────┐      ┌─────────────────┐
│  Prefect Worker      │◄─────│  Work Queue     │
│                      │ Pull │                 │
│  ┌────────────────┐  │      └─────────────────┘
│  │ Submit K8s Job │  │
│  │ (Orchestrate)  │  │              ┌─────────────────────┐
│  └────────────────┘  │─────────────►│  Kubernetes Job Pod │
└──────────────────────┘     Create   │                     │
                                      │  ┌────────────────┐ │
                                      │  │ Execute Flow   │ │
                                      │  │ [Your Code]    │ │
                                      │  └────────────────┘ │
                                      └─────────────────────┘

▲ Worker SUBMITS work to Kubernetes, which creates a NEW pod
```

---

## 2. Prefect Worker Types

### 2.1 Process Worker

**What it is:**
- A long-running process that pulls work and **executes it in the same process**

**Architecture:**
```
┌─────────────────────────────────────────────────────┐
│  Worker Pod (Long-running)                          │
│                                                     │
│  Main Process:                                      │
│    - Poll work queue                                │
│    - Pull flow run                                  │
│    - Execute flow in-process                        │
│    - Send results back                              │
│    - Loop (continue polling)                        │
│                                                     │
│  Memory: Shared across all flow runs               │
│  Lifecycle: Runs forever (until stopped)           │
└─────────────────────────────────────────────────────┘
```

**Configuration:**
```python
# Start a process worker
prefect worker start --pool default-pool --type process
```

**Characteristics:**
- ✅ Low overhead (no pod creation)
- ✅ Fast startup (already running)
- ✅ Can handle multiple tasks concurrently (configurable)
- ❌ Shared resources (one task can affect others)
- ❌ Memory leaks accumulate
- ❌ Limited isolation

**Example Deployment:**
```yaml
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
        image: prefecthq/prefect:2.14-python3.11
        command:
          - prefect
          - worker
          - start
          - --pool
          - default-pool
          - --type
          - process  # ← Process worker
        resources:
          requests:
            cpu: 2
            memory: 4Gi
```

**When worker starts:**
```
Worker Pod starts
    ↓
Registers with Prefect Server
    ↓
Continuously polls work queue
    ↓
Flow run available?
    ↓ Yes
Pull flow run
    ↓
Execute flow IN THIS POD (same process)
    ↓
Send results
    ↓
Continue polling (loop back)
```

---

### 2.2 Kubernetes Worker (Job Infrastructure)

**What it is:**
- A long-running process that pulls work and **submits Kubernetes Jobs** to execute it

**Architecture:**
```
┌─────────────────────────────────────────────────────┐
│  Worker Pod (Long-running, orchestrator only)       │
│                                                     │
│  Main Process:                                      │
│    - Poll work queue                                │
│    - Pull flow run                                  │
│    - Create Kubernetes Job manifest                │
│    - Submit Job to K8s API                          │
│    - Monitor Job status                             │
│    - Send results back                              │
│    - Loop (continue polling)                        │
│                                                     │
│  Does NOT execute flow itself!                      │
└─────────────────────────────────────────────────────┘
                    │
                    │ kubectl create job
                    ▼
┌─────────────────────────────────────────────────────┐
│  Kubernetes Job Pod (Created per flow run)          │
│                                                     │
│  Container:                                         │
│    - Execute flow                                   │
│    - Send results to Prefect                        │
│    - Exit (pod terminates)                          │
│                                                     │
│  Lifecycle: Created → Run → Complete → Delete      │
└─────────────────────────────────────────────────────┘
```

**Configuration:**
```python
# Start a Kubernetes worker
prefect worker start --pool k8s-pool --type kubernetes
```

**Characteristics:**
- ✅ Maximum isolation (new pod per flow run)
- ✅ No memory leaks (pod is deleted)
- ✅ Custom resources per job
- ✅ Clean environment every time
- ❌ Higher overhead (pod creation ~10-30 seconds)
- ❌ More Kubernetes resources used
- ❌ More complex (Job API, RBAC, image management)

**Example Deployment:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-kubernetes
spec:
  replicas: 2  # Fewer workers needed (they just orchestrate)
  template:
    spec:
      serviceAccountName: prefect-worker  # Needs permission to create Jobs
      containers:
      - name: worker
        image: prefecthq/prefect:2.14-python3.11
        command:
          - prefect
          - worker
          - start
          - --pool
          - k8s-pool
          - --type
          - kubernetes  # ← Kubernetes worker
```

**When worker starts:**
```
Worker Pod starts
    ↓
Registers with Prefect Server
    ↓
Continuously polls work queue
    ↓
Flow run available?
    ↓ Yes
Pull flow run
    ↓
Generate Kubernetes Job manifest:
  - Image: my-flow-image:latest
  - Resources: 4 CPU, 8GB RAM
  - Environment variables
    ↓
Submit Job to Kubernetes API
    ↓
Kubernetes creates new pod
    ↓
New pod executes flow
    ↓
Worker monitors Job status
    ↓
Job completes
    ↓
Worker sends results to Prefect
    ↓
Kubernetes deletes Job pod (after TTL)
    ↓
Worker continues polling (loop back)
```

---

### 2.3 Other Worker Types (Brief)

**Docker Worker:**
```
Worker → Submits work to Docker daemon → Docker container executes
```
- Similar to Kubernetes worker, but uses Docker containers
- Good for single-node deployments

**ECS Worker:**
```
Worker → Submits work to AWS ECS → ECS task executes
```
- For AWS ECS (Elastic Container Service)

**Cloud Run Worker:**
```
Worker → Submits work to GCP Cloud Run → Cloud Run instance executes
```
- For Google Cloud Run

**Vertex AI Worker:**
```
Worker → Submits work to GCP Vertex AI → Vertex AI job executes
```
- For ML workloads on GCP

---

## 3. Execution Models Explained

### 3.1 In-Process Execution (Process Worker)

```python
# What happens inside the worker pod

# Worker main loop
while True:
    # 1. Poll for work
    flow_run = poll_work_queue()

    if flow_run:
        # 2. Execute DIRECTLY (in this process)
        result = execute_flow(flow_run)

        # 3. Send results
        send_results(result)

    # 4. Continue polling
    time.sleep(1)


# execute_flow() runs YOUR code:
def execute_flow(flow_run):
    # This runs in the WORKER POD
    from app.prefect.flows.run_ingestion import run_ingestion_flow

    # Execute flow function
    result = run_ingestion_flow(ingestion_id="abc-123")

    return result
```

**Visualization:**
```
Time: 0s
┌──────────────────────────┐
│  Worker Pod              │
│  - Idle (polling)        │
└──────────────────────────┘

Time: 1s (flow run arrives)
┌──────────────────────────┐
│  Worker Pod              │
│  - Executing flow run 1  │
│    [Your Code Running]   │
└──────────────────────────┘

Time: 60s (flow completes)
┌──────────────────────────┐
│  Worker Pod              │
│  - Idle (polling)        │
└──────────────────────────┘

Time: 61s (another flow arrives)
┌──────────────────────────┐
│  Worker Pod              │
│  - Executing flow run 2  │
│    [Your Code Running]   │
└──────────────────────────┘

Same pod, different executions (in sequence or parallel)
```

---

### 3.2 Job Submission (Kubernetes Worker)

```python
# What happens inside the worker pod

# Worker main loop
while True:
    # 1. Poll for work
    flow_run = poll_work_queue()

    if flow_run:
        # 2. Generate Kubernetes Job manifest
        job_manifest = create_job_manifest(flow_run)

        # 3. Submit Job to Kubernetes
        kubernetes_api.create_job(job_manifest)

        # 4. Monitor Job status
        monitor_job_until_complete(flow_run.id)

        # 5. Send results
        send_results_from_job(flow_run.id)

    # 6. Continue polling
    time.sleep(1)


# create_job_manifest() generates:
def create_job_manifest(flow_run):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"prefect-job-{flow_run.id}",
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": "flow-run",
                        "image": "my-flow-image:latest",
                        "command": ["python", "-m", "prefect.engine"],
                        "args": ["flow-run", "execute", flow_run.id],
                        "env": [...],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"}
                        }
                    }],
                    "restartPolicy": "Never"
                }
            }
        }
    }
```

**Visualization:**
```
Time: 0s
┌──────────────────────────┐
│  Worker Pod              │
│  - Idle (polling)        │
└──────────────────────────┘

Time: 1s (flow run arrives)
┌──────────────────────────┐
│  Worker Pod              │
│  - Creating K8s Job      │
│  - Submitting to K8s API │
└──────────────────────────┘
           │
           │ kubectl create
           ▼
┌──────────────────────────┐
│  Job Pod (NEW)           │
│  - Starting...           │
└──────────────────────────┘

Time: 10s (Job pod running)
┌──────────────────────────┐
│  Worker Pod              │
│  - Monitoring Job status │
└──────────────────────────┘

┌──────────────────────────┐
│  Job Pod                 │
│  - Executing flow run 1  │
│    [Your Code Running]   │
└──────────────────────────┘

Time: 60s (Job completes)
┌──────────────────────────┐
│  Worker Pod              │
│  - Fetching results      │
│  - Idle (polling)        │
└──────────────────────────┘

┌──────────────────────────┐
│  Job Pod                 │
│  - Completed             │
│  - Terminating...        │
└──────────────────────────┘

Time: 90s (TTL cleanup)
┌──────────────────────────┐
│  Worker Pod              │
│  - Idle (polling)        │
└──────────────────────────┘

Job Pod deleted by Kubernetes
```

---

## 4. Which Model for IOMETE Autoloader?

### 4.1 Hybrid Approach (Recommended)

**Use BOTH worker types for different workloads:**

```
┌─────────────────────────────────────────────────────────────┐
│  Work Queue: autoloader-default                             │
│  Worker Type: PROCESS                                       │
│  - Replicas: 3-5 pods                                       │
│  - Concurrency: 5 tasks per worker                          │
│  - Resources: 2 CPU, 4GB RAM per pod                        │
│  - Use Case: Standard ingestions (frequent, predictable)    │
│  - Why: Low overhead, fast startup, good for high-frequency │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Work Queue: autoloader-high-memory                         │
│  Worker Type: KUBERNETES (Job)                              │
│  - Replicas: 2 worker pods (orchestrators)                  │
│  - Job Resources: 8 CPU, 32GB RAM per job                   │
│  - Use Case: Large file ingestions (>10GB, unpredictable)   │
│  - Why: Isolation, custom resources, no memory leaks        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Work Queue: autoloader-priority                            │
│  Worker Type: PROCESS                                       │
│  - Replicas: 1-2 pods                                       │
│  - Concurrency: 3 tasks per worker                          │
│  - Resources: 2 CPU, 4GB RAM per pod                        │
│  - Use Case: Manual triggers, previews, retries             │
│  - Why: Fast response, low latency                          │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Configuration Examples

**Process Worker (Default Queue):**
```yaml
# kubernetes/prefect-worker-process.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-process
  namespace: iomete
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: worker
        image: iomete/autoloader-worker:latest  # Has your flow code
        command:
          - prefect
          - worker
          - start
          - --pool
          - default-pool
          - --type
          - process  # ← Executes flows in-process
          - --limit
          - "5"  # Max 5 concurrent flows per worker
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
```

**Kubernetes Worker (High Memory Queue):**
```yaml
# kubernetes/prefect-worker-k8s.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-k8s
  namespace: iomete
spec:
  replicas: 2  # Fewer workers (they just orchestrate)
  template:
    spec:
      serviceAccountName: prefect-worker  # Needs Job creation permission
      containers:
      - name: worker
        image: prefecthq/prefect:2.14-python3.11  # Base image (no flow code needed)
        command:
          - prefect
          - worker
          - start
          - --pool
          - k8s-pool
          - --type
          - kubernetes  # ← Submits Kubernetes Jobs
        env:
        - name: PREFECT_API_URL
          value: "http://prefect-server:4200/api"
        # Job configuration (what pods to create)
        - name: PREFECT_KUBERNETES_CLUSTER_CONFIG
          value: |
            job_template:
              apiVersion: batch/v1
              kind: Job
              spec:
                ttlSecondsAfterFinished: 3600  # Delete after 1 hour
                template:
                  spec:
                    containers:
                    - name: flow-run
                      image: iomete/autoloader-worker:latest  # Has flow code
                      resources:
                        requests:
                          cpu: 8
                          memory: 32Gi
                        limits:
                          cpu: 16
                          memory: 64Gi
                      env:
                      - name: DATABASE_URL
                        valueFrom:
                          secretKeyRef:
                            name: autoloader-db-secret
                            key: connection-url
                      - name: SPARK_CONNECT_URL
                        value: "sc://spark-connect-service:15002"
                    restartPolicy: Never
        resources:
          requests:
            cpu: 500m  # Worker pod is lightweight
            memory: 1Gi
          limits:
            cpu: 1
            memory: 2Gi
---
# RBAC for Kubernetes worker
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: prefect-worker-k8s
  namespace: iomete
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "get", "list", "watch", "delete"]
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: prefect-worker-k8s-binding
  namespace: iomete
subjects:
- kind: ServiceAccount
  name: prefect-worker
  namespace: iomete
roleRef:
  kind: Role
  name: prefect-worker-k8s
  apiGroup: rbac.authorization.k8s.io
```

### 4.3 Deployment Configuration in Code

```python
# app/services/prefect_service.py

from prefect.infrastructure import KubernetesJob, Process

class PrefectService:
    async def create_deployment(self, ingestion: Ingestion) -> str:
        """
        Create deployment with appropriate infrastructure.
        """
        async with get_client() as client:
            # Determine work queue and infrastructure
            if self._is_high_memory_ingestion(ingestion):
                # Use Kubernetes Job infrastructure
                infrastructure = KubernetesJob(
                    image="iomete/autoloader-worker:latest",
                    namespace="iomete",
                    service_account_name="prefect-worker",
                    job_watch_timeout_seconds=3600,
                    finished_job_ttl=3600,
                    pod_watch_timeout_seconds=300,
                    # Custom resources for large ingestions
                    cpu_request=8,
                    cpu_limit=16,
                    memory_request="32Gi",
                    memory_limit="64Gi",
                )
                work_queue = "autoloader-high-memory"
            else:
                # Use process infrastructure (default)
                infrastructure = None  # Worker handles execution
                work_queue = "autoloader-default"

            # Create deployment
            deployment = await client.create_deployment(
                flow_id=self.ingestion_flow_id,
                name=f"ingestion-{ingestion.id}",
                parameters={"ingestion_id": str(ingestion.id)},
                schedule=CronSchedule(cron=ingestion.schedule_cron),
                work_queue_name=work_queue,
                infrastructure=infrastructure,  # None for process, KubernetesJob for jobs
                tags=[...],
            )

            return str(deployment.id)

    def _is_high_memory_ingestion(self, ingestion: Ingestion) -> bool:
        """
        Determine if ingestion needs Kubernetes Job infrastructure.
        """
        # Estimate based on file size, count, etc.
        estimated_size_gb = self._estimate_size(ingestion)
        return estimated_size_gb > 10  # >10GB → Use K8s Jobs
```

---

## 5. Detailed Comparison

### 5.1 Feature Matrix

| Feature | Process Worker | Kubernetes Worker |
|---------|---------------|-------------------|
| **Execution Location** | Worker pod itself | Separate Job pod |
| **Pod Lifecycle** | Long-running | Created per flow run |
| **Startup Time** | Instant | 10-30 seconds (pod creation) |
| **Resource Isolation** | Shared across flows | Dedicated per flow |
| **Memory Leaks** | Accumulate over time | Cleaned on pod deletion |
| **Custom Resources** | Fixed per worker | Dynamic per flow |
| **Concurrency** | Configurable (1-10+) | 1 per Job pod |
| **Kubernetes Overhead** | Low | High (Job API, pod scheduling) |
| **RBAC Requirements** | Minimal | Job creation permissions |
| **Failure Impact** | Affects all flows in worker | Isolated to single flow |
| **Cost** | Lower (fewer pods) | Higher (more pods) |
| **Best For** | Frequent, predictable workloads | Unpredictable, resource-intensive |

### 5.2 Resource Usage Comparison

**Scenario: 100 flow runs per hour**

**Process Worker:**
```
Worker Pods: 3
  - Each runs 33 flows/hour
  - Resources: 2 CPU, 4GB RAM each
  - Total: 6 CPU, 12GB RAM

Total Kubernetes Resources:
  - Pods: 3 (long-running)
  - CPU: 6
  - Memory: 12GB
```

**Kubernetes Worker:**
```
Worker Pods: 2 (orchestrators only)
  - Each handles 50 Job submissions/hour
  - Resources: 500m CPU, 1GB RAM each
  - Total: 1 CPU, 2GB RAM

Job Pods: 100 (created dynamically)
  - Average runtime: 2 minutes
  - Average concurrency: 100 / (60/2) = ~3.3 running at once
  - Resources per Job: 4 CPU, 8GB RAM
  - Total: 3.3 × 4 CPU, 3.3 × 8GB = 13.2 CPU, 26.4GB RAM

Total Kubernetes Resources:
  - Pods: 2 workers + ~3.3 jobs = ~5.3 average
  - CPU: 1 + 13.2 = 14.2
  - Memory: 2GB + 26.4GB = 28.4GB
```

**Cost Analysis:**
- Process Worker: **Lower cost** (less resources)
- Kubernetes Worker: **Higher cost** (more resources, more pod churn)

**When to use each:**
- Process: Frequent, similar workloads (e.g., hourly ingestions)
- Kubernetes: Variable workloads, strict isolation needed

### 5.3 Failure Scenarios

**Process Worker Failure:**
```
Scenario: Worker pod crashes
Impact: All flows running in that worker FAIL
Recovery:
  - Kubernetes restarts worker pod
  - Flows marked as CRASHED
  - User must manually retry

Mitigation:
  - Multiple worker replicas (3-5)
  - Proper resource limits
  - Pod restart policy
```

**Kubernetes Worker Failure:**
```
Scenario: Job pod crashes
Impact: Only that specific flow run FAILS
Recovery:
  - Kubernetes marks Job as Failed
  - Prefect records failure
  - Other flows unaffected

Mitigation:
  - Job restartPolicy: Never (don't auto-retry)
  - Prefect flow-level retries
  - Better isolation means less impact
```

---

## 6. Recommendation

### 6.1 For IOMETE Autoloader

**Use a HYBRID approach:**

```python
# Decision logic in PrefectService

def get_infrastructure_for_ingestion(ingestion: Ingestion):
    """
    Determine which worker type to use.
    """
    estimated_size_gb = estimate_ingestion_size(ingestion)

    # Large ingestions → Kubernetes Jobs (isolation, custom resources)
    if estimated_size_gb > 10:
        return {
            "type": "kubernetes",
            "work_queue": "autoloader-high-memory",
            "infrastructure": KubernetesJob(
                image="iomete/autoloader-worker:latest",
                cpu_request=8,
                memory_request="32Gi",
                # ... other config
            )
        }

    # Standard ingestions → Process workers (low overhead)
    else:
        return {
            "type": "process",
            "work_queue": "autoloader-default",
            "infrastructure": None  # Worker executes in-process
        }
```

**Deployment:**
```
┌────────────────────────────────────────────────────────┐
│  3-5 Process Worker Pods                               │
│  - Handle 80% of ingestions (standard workloads)       │
│  - Resources: 2 CPU, 4GB RAM each                      │
│  - Work Queue: autoloader-default                      │
│  - Execution: In-process (low overhead)                │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│  2 Kubernetes Worker Pods (orchestrators)              │
│  - Handle 20% of ingestions (large workloads)          │
│  - Resources: 500m CPU, 1GB RAM each                   │
│  - Work Queue: autoloader-high-memory                  │
│  - Execution: Submit Kubernetes Jobs                   │
│    - Job Resources: 8 CPU, 32GB RAM                    │
│    - Job TTL: 1 hour                                   │
└────────────────────────────────────────────────────────┘
```

### 6.2 Decision Criteria

**Use Process Workers when:**
- ✅ Frequent executions (hourly, daily)
- ✅ Predictable resource usage
- ✅ Small to medium data (<10GB)
- ✅ Need low latency (instant startup)
- ✅ Cost-sensitive

**Use Kubernetes Workers when:**
- ✅ Unpredictable resource needs
- ✅ Large data (>10GB)
- ✅ Need strict isolation
- ✅ Risk of memory leaks
- ✅ Custom resources per flow
- ✅ Long-running flows (>1 hour)

### 6.3 Migration Path

**Phase 1: Start with Process Workers**
- Deploy 3-5 process worker pods
- All ingestions use process workers
- Monitor resource usage and failures

**Phase 2: Add Kubernetes Workers for Large Ingestions**
- Deploy 2 Kubernetes worker pods
- Identify large ingestions (>10GB)
- Move them to high-memory queue (Kubernetes Jobs)
- Monitor cost and performance

**Phase 3: Optimize**
- Fine-tune thresholds (when to use Jobs vs Process)
- Adjust worker counts based on load
- Implement auto-scaling

---

## 7. Summary

### 7.1 The Key Distinction

**Process Worker:**
- Worker = Executor
- Your code runs **inside the worker pod**
- Low overhead, shared resources

**Kubernetes Worker:**
- Worker = Orchestrator
- Worker **submits Jobs** to Kubernetes
- Your code runs **in separate Job pods**
- High overhead, isolated resources

### 7.2 Analogy

**Process Worker = Kitchen**
```
You have 3 chefs (workers)
Each chef cooks multiple dishes (flows) in their own kitchen
Dishes are cooked sequentially or in parallel (concurrency)
All dishes share the same kitchen resources
If the kitchen burns down, all dishes fail
```

**Kubernetes Worker = Restaurant Manager**
```
You have 2 managers (workers)
When an order comes in, the manager:
  1. Finds an available chef (creates Job pod)
  2. Gives them the recipe (flow code)
  3. Lets them cook in their own temporary kitchen (Job pod)
  4. Waits for the dish to be ready
  5. Cleans up the temporary kitchen (deletes Job pod)

Each dish has its own dedicated kitchen
If one kitchen burns down, only that dish fails
But you need more kitchens (more resources)
```

### 7.3 Final Recommendation

**For IOMETE Autoloader:**

```
Standard ingestions (80%):
  → Process Workers
  → Fast, cheap, predictable

Large ingestions (20%):
  → Kubernetes Workers
  → Isolated, custom resources, clean slate

This gives you the best of both worlds.
```

---

**End of Document**

**Key Takeaway:**
- **Process Workers** execute flows **in-process** (low overhead)
- **Kubernetes Workers** execute flows **in separate Job pods** (high isolation)
- Use **both** for optimal cost and performance

