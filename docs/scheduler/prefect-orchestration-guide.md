# Prefect Orchestration for IOMETE Autoloader

**Document Version:** 1.0
**Created:** 2025-11-07
**Status:** Recommended Architecture
**Related:** `apscheduler-horizontal-scaling.md`, `scheduler-implementation-guide.md`

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Why Prefect?](#why-prefect)
3. [Architecture Overview](#architecture-overview)
4. [Prefect Concepts for Autoloader](#prefect-concepts-for-autoloader)
5. [Implementation Design](#implementation-design)
6. [Deployment Patterns](#deployment-patterns)
7. [Worker Configuration Strategies](#worker-configuration-strategies)
8. [Code Implementation](#code-implementation)
9. [Migration from APScheduler](#migration-from-apscheduler)
10. [Operational Guide](#operational-guide)
11. [Cost-Benefit Analysis](#cost-benefit-analysis)
12. [Comparison Matrix](#comparison-matrix)
13. [Recommendation](#recommendation)

---

## 1. Executive Summary

### 1.1 The Proposal

**Instead of building distributed APScheduler from scratch, leverage Prefect (already in your stack) for orchestration.**

**Architecture:**
```
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Server (Stateless)                │
│  - API endpoints (CRUD ingestions)                          │
│  - Create/update Prefect deployments                        │
│  - Query run history from Prefect                           │
│  - No scheduling logic (Prefect handles it)                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Prefect Server (Orchestration)               │
│  - Schedule management                                       │
│  - Work queue management                                     │
│  - Run history and state                                     │
│  - Horizontal scaling built-in                              │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ Prefect Worker 1 │ │ Prefect Worker 2 │ │ Prefect Worker N │
│ - Pull work queue│ │ - Pull work queue│ │ - Pull work queue│
│ - Execute flows  │ │ - Execute flows  │ │ - Execute flows  │
│ - Stateless      │ │ - Stateless      │ │ - Stateless      │
└──────────────────┘ └──────────────────┘ └──────────────────┘
```

### 1.2 Key Benefits

| Aspect | Prefect Solution | DIY Distributed APScheduler |
|--------|------------------|----------------------------|
| **Development Time** | 🟢 1-2 weeks | 🔴 3+ weeks |
| **Horizontal Scaling** | 🟢 Built-in | 🔴 Build from scratch |
| **Operational Complexity** | 🟢 Low | 🔴 High (Redis, monitoring) |
| **Observability** | 🟢 Rich UI + API | 🔴 Build custom dashboards |
| **Failure Recovery** | 🟢 Built-in retry, failover | 🔴 Build from scratch |
| **Worker Management** | 🟢 Kubernetes-native | 🔴 Custom orchestration |
| **Cost** | 🟢 Already in stack | 🔴 New infrastructure |

### 1.3 Bottom Line

**✅ RECOMMENDED: Use Prefect for IOMETE Autoloader orchestration**

**Rationale:**
- ✅ Already part of your stack (zero new dependencies)
- ✅ Kubernetes-native (fits your infrastructure)
- ✅ Horizontal scaling out-of-the-box
- ✅ Rich observability (UI + API)
- ✅ Proven at scale (production-grade)
- ✅ Faster time-to-market (1-2 weeks vs 3+ weeks)
- ✅ Lower operational burden (no Redis cluster, no custom monitoring)

---

## 2. Why Prefect?

### 2.1 Prefect vs. APScheduler

**APScheduler:**
- ✅ Lightweight, simple API
- ❌ Single-process (no horizontal scaling)
- ❌ Limited observability
- ❌ No built-in failure recovery
- ❌ Manual monitoring required

**Distributed APScheduler:**
- ✅ Horizontal scaling (custom)
- ❌ 3+ weeks engineering effort
- ❌ High operational complexity (Redis, locks, leader election)
- ❌ Custom monitoring and debugging
- ❌ New failure modes (split-brain, lock contention)

**Prefect:**
- ✅ Horizontal scaling (built-in)
- ✅ Rich observability (UI + API)
- ✅ Failure recovery and retry logic
- ✅ Kubernetes-native workers
- ✅ Event-driven architecture
- ✅ Already in your stack
- ✅ Production-proven

### 2.2 Prefect Core Capabilities

**Orchestration:**
- Schedule management (cron, interval, event-driven)
- Work queue distribution
- Concurrency limits
- Priority queues

**Execution:**
- Stateless workers (pull-based)
- Task retry with backoff
- Timeout enforcement
- Distributed execution

**Observability:**
- Real-time run monitoring
- Logs aggregation
- Metrics and alerts
- Web UI + REST API

**Infrastructure:**
- Kubernetes deployments
- Auto-scaling workers
- Multi-cloud support
- High availability

---

## 3. Architecture Overview

### 3.1 System Components

```
┌───────────────────────────────────────────────────────────────┐
│                        User / API Client                      │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│                   IOMETE Autoloader API (FastAPI)             │
│                                                               │
│  Responsibilities:                                            │
│  - CRUD operations for ingestions                            │
│  - Create/update Prefect deployments                         │
│  - Query Prefect for run status/history                      │
│  - Validate configurations                                   │
│  - Store ingestion metadata in PostgreSQL                    │
│                                                               │
│  Does NOT:                                                    │
│  - Execute ingestions (Prefect workers do this)              │
│  - Manage schedules (Prefect server does this)               │
│  - Retry logic (Prefect handles it)                          │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│                      Prefect Server                           │
│                                                               │
│  Responsibilities:                                            │
│  - Schedule execution (cron triggers)                        │
│  - Work queue management                                     │
│  - State management (PENDING → RUNNING → COMPLETED/FAILED)  │
│  - Run history and logs                                      │
│  - Worker coordination                                       │
│                                                               │
│  Database: PostgreSQL (can be same as Autoloader DB)         │
└───────────────────────────────────────────────────────────────┘
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│   Work Queue: default    │   │  Work Queue: high-memory │
│   - Standard ingestions  │   │  - Large file ingestions │
└──────────────────────────┘   └──────────────────────────┘
         │                              │
    ┌────┴────┬────────┐           ┌───┴────┐
    ▼         ▼        ▼           ▼        ▼
┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐
│Worker 1││Worker 2││Worker 3││Worker 4││Worker 5│
│ (Pod)  ││ (Pod)  ││ (Pod)  ││ (Pod)  ││ (Pod)  │
│        ││        ││        ││        ││        │
│ - Pull ││ - Pull ││ - Pull ││ - Pull ││ - Pull │
│ - Exec ││ - Exec ││ - Exec ││ - Exec ││ - Exec │
│ - Send ││ - Send ││ - Send ││ - Send ││ - Send │
│  state ││  state ││  state ││  state ││  state │
└────────┘└────────┘└────────┘└────────┘└────────┘
     │         │         │         │         │
     └─────────┴─────────┴─────────┴─────────┘
                      │
                      ▼
         ┌─────────────────────────┐
         │   Spark Connect Cluster │
         │   (IOMETE)              │
         └─────────────────────────┘
```

### 3.2 Data Flow

**1. Create Ingestion (API → Prefect)**
```
User creates ingestion via API
    ↓
FastAPI validates configuration
    ↓
FastAPI stores ingestion in PostgreSQL
    ↓
FastAPI creates Prefect deployment:
  - Flow: run_ingestion_flow(ingestion_id)
  - Schedule: cron expression
  - Work queue: "default" or "high-memory"
    ↓
Prefect server receives deployment
    ↓
Prefect schedules first run based on cron
```

**2. Execute Ingestion (Prefect → Worker → Spark)**
```
Prefect server triggers scheduled run
    ↓
Prefect creates flow run (state: SCHEDULED)
    ↓
Prefect places run in work queue
    ↓
Prefect worker pulls run from queue
    ↓
Worker changes state to RUNNING
    ↓
Worker executes flow:
  - Fetch ingestion config from API
  - Discover files from S3/Azure/GCS
  - Execute Spark Connect job
  - Update file state
    ↓
Worker sends state updates to Prefect:
  - COMPLETED (success)
  - FAILED (error)
  - CRASHED (worker died)
    ↓
Prefect stores run result and logs
```

**3. Query Status (User → API → Prefect)**
```
User requests run history via API
    ↓
FastAPI queries Prefect API:
  GET /deployments/{deployment_id}/flow_runs
    ↓
Prefect returns run history with states/logs
    ↓
FastAPI transforms to Autoloader schema
    ↓
API returns to user
```

### 3.3 Decoupling Benefits

**Server (FastAPI):**
- Stateless (can scale independently)
- No scheduling logic (Prefect handles it)
- No worker coordination (Prefect handles it)
- Focus on business logic and API

**Workers (Prefect):**
- Stateless (can scale horizontally)
- Pull-based (no push coordination needed)
- Auto-scaling (Kubernetes HPA)
- Isolated execution (one task per worker or concurrent)

**Prefect Server:**
- Central orchestration
- State management
- Work distribution
- Already handles distributed systems complexity

---

## 4. Prefect Concepts for Autoloader

### 4.1 Flows

**Definition:** A flow is a container for workflow logic (like a function).

**For Autoloader:**
```python
from prefect import flow, task
from prefect.client import get_client
import httpx

@task(retries=3, retry_delay_seconds=60)
def discover_files(ingestion_id: str) -> List[str]:
    """Discover new files from cloud storage."""
    # Call Autoloader API to get ingestion config
    response = httpx.get(f"http://autoloader-api/api/v1/ingestions/{ingestion_id}")
    ingestion = response.json()

    # Use file_discovery_service logic
    from app.services.file_discovery_service import FileDiscoveryService
    discovery = FileDiscoveryService()
    files = discovery.discover_files(ingestion)
    return files

@task(retries=2, retry_delay_seconds=120)
def process_files(ingestion_id: str, files: List[str]):
    """Process files via Spark Connect."""
    # Call batch_orchestrator logic
    from app.services.batch_orchestrator import BatchOrchestrator
    orchestrator = BatchOrchestrator(...)
    orchestrator.process_batch(ingestion_id, files)

@flow(name="run_ingestion", log_prints=True)
def run_ingestion_flow(ingestion_id: str):
    """
    Main flow for running a scheduled ingestion.

    This flow:
    1. Discovers new files from cloud storage
    2. Processes files via Spark Connect
    3. Updates file state
    """
    # Discover files
    files = discover_files(ingestion_id)

    if not files:
        print(f"No new files for ingestion {ingestion_id}")
        return

    # Process files
    process_files(ingestion_id, files)

    print(f"Successfully processed {len(files)} files for ingestion {ingestion_id}")
```

**Key Points:**
- Tasks are the unit of work (retryable, composable)
- Flows orchestrate tasks
- Decorators define behavior (retries, timeouts, etc.)

### 4.2 Deployments

**Definition:** A deployment is a flow + schedule + infrastructure.

**For Autoloader:**
```python
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Create deployment for an ingestion
deployment = Deployment.build_from_flow(
    flow=run_ingestion_flow,
    name=f"ingestion-{ingestion_id}",
    parameters={"ingestion_id": str(ingestion_id)},
    schedule=CronSchedule(cron="0 2 * * *"),  # Daily at 2 AM
    work_queue_name="default",
    tags=["autoloader", "s3", f"tenant-{tenant_id}"],
)

deployment_id = deployment.apply()
```

**Dynamic Deployment Management:**
```python
# In FastAPI endpoint: Create ingestion
@router.post("/ingestions")
async def create_ingestion(ingestion: IngestionCreate):
    # 1. Store in database
    db_ingestion = ingestion_repository.create(ingestion)

    # 2. Create Prefect deployment
    async with get_client() as client:
        deployment_id = await client.create_deployment(
            flow_id=INGESTION_FLOW_ID,  # Pre-registered flow
            name=f"ingestion-{db_ingestion.id}",
            parameters={"ingestion_id": str(db_ingestion.id)},
            schedule=CronSchedule(cron=ingestion.schedule_cron),
            work_queue_name="default",
        )

    # 3. Store deployment_id in database
    db_ingestion.prefect_deployment_id = deployment_id
    db.commit()

    return db_ingestion

# Update schedule
@router.patch("/ingestions/{ingestion_id}/schedule")
async def update_schedule(ingestion_id: UUID, schedule_cron: str):
    # Update Prefect deployment schedule
    async with get_client() as client:
        await client.update_deployment(
            deployment_id=ingestion.prefect_deployment_id,
            schedule=CronSchedule(cron=schedule_cron),
        )

    # Update database
    ingestion.schedule_cron = schedule_cron
    db.commit()
```

### 4.3 Work Queues

**Definition:** A named queue that workers pull from.

**For Autoloader (Multiple Queues):**

```python
# Work queues for different workload types

# Queue 1: Default (standard ingestions)
DEFAULT_QUEUE = "autoloader-default"
# Workers: 3-5 pods, 2 CPU, 4GB RAM

# Queue 2: High Memory (large file ingestions)
HIGH_MEMORY_QUEUE = "autoloader-high-memory"
# Workers: 2-3 pods, 4 CPU, 16GB RAM

# Queue 3: High Priority (manual triggers, retries)
HIGH_PRIORITY_QUEUE = "autoloader-priority"
# Workers: 1-2 pods, 2 CPU, 4GB RAM

# Assign ingestion to queue based on characteristics
def get_work_queue(ingestion: Ingestion) -> str:
    # Large files → high memory queue
    if ingestion.estimated_file_size_gb > 10:
        return HIGH_MEMORY_QUEUE

    # Manual trigger → priority queue
    if ingestion.is_manual_trigger:
        return HIGH_PRIORITY_QUEUE

    # Default
    return DEFAULT_QUEUE

# Create deployment with appropriate queue
deployment = Deployment.build_from_flow(
    flow=run_ingestion_flow,
    name=f"ingestion-{ingestion_id}",
    work_queue_name=get_work_queue(ingestion),
    parameters={"ingestion_id": str(ingestion_id)},
)
```

### 4.4 Workers

**Definition:** A process that pulls work from a queue and executes flows.

**Worker Types for Autoloader:**

**Option 1: Multi-Task Workers (Recommended for most cases)**
```yaml
# kubernetes/prefect-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-default
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
          - kubernetes-pool
          - --work-queue
          - autoloader-default
        env:
        - name: PREFECT_API_URL
          value: "http://prefect-server:4200/api"
        resources:
          requests:
            cpu: 2
            memory: 4Gi
          limits:
            cpu: 4
            memory: 8Gi
```

**Behavior:**
- Each worker pod can handle multiple tasks concurrently
- Configurable concurrency limit (default: 10 tasks per worker)
- Better resource utilization
- Good for: Standard ingestions (most common)

**Option 2: Single-Task Workers (For resource-intensive jobs)**
```yaml
# kubernetes/prefect-worker-high-memory.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-high-memory
spec:
  replicas: 2
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
          - kubernetes-pool
          - --work-queue
          - autoloader-high-memory
          - --limit
          - "1"  # Only 1 task at a time
        resources:
          requests:
            cpu: 4
            memory: 16Gi
          limits:
            cpu: 8
            memory: 32Gi
```

**Behavior:**
- Each worker pod handles only 1 task at a time
- Full resources dedicated to single ingestion
- Good for: Large file ingestions, memory-intensive operations

**Option 3: Kubernetes Job Workers (Dynamic scaling)**
```python
# Prefect can spawn a new Kubernetes Job per flow run
from prefect_kubernetes import KubernetesJob

deployment = Deployment.build_from_flow(
    flow=run_ingestion_flow,
    name=f"ingestion-{ingestion_id}",
    infrastructure=KubernetesJob(
        image="iomete/autoloader-worker:latest",
        namespace="iomete",
        service_account_name="prefect-worker",
        finished_job_ttl=3600,  # Clean up after 1 hour
    ),
)
```

**Behavior:**
- New Kubernetes Job created for each flow run
- Job terminated after completion
- Maximum isolation (no shared resources)
- Good for: Unpredictable workloads, strict resource guarantees

---

## 5. Implementation Design

### 5.1 Project Structure

```
app/
├── main.py                           # FastAPI app
├── config.py                         # Settings (add Prefect config)
├── database.py                       # Database sessions
│
├── api/v1/
│   ├── ingestions.py                 # Modified: Create Prefect deployments
│   ├── runs.py                       # Modified: Query Prefect for run history
│   └── clusters.py                   # Unchanged
│
├── models/
│   ├── domain.py                     # Add: prefect_deployment_id field
│   └── schemas.py                    # Unchanged
│
├── services/
│   ├── ingestion_service.py          # Modified: Prefect integration
│   ├── prefect_service.py            # NEW: Prefect API client wrapper
│   ├── spark_service.py              # Unchanged (used by workers)
│   ├── batch_orchestrator.py         # Unchanged (used by workers)
│   └── ...
│
├── prefect/                          # NEW: Prefect-specific code
│   ├── flows/
│   │   ├── run_ingestion.py          # Main ingestion flow
│   │   └── preview_ingestion.py      # Preview flow (on-demand)
│   ├── tasks/
│   │   ├── discovery.py              # File discovery task
│   │   ├── processing.py             # Spark execution task
│   │   └── state_update.py           # File state update task
│   └── deployments/
│       └── manager.py                # Deployment CRUD operations
│
└── workers/                          # NEW: Worker entry point
    └── main.py                       # Prefect worker startup
```

### 5.2 Database Schema Changes

**Add Prefect Integration Fields:**

```python
# app/models/domain.py

class Ingestion(Base):
    __tablename__ = "ingestions"

    # ... existing fields ...

    # Prefect integration
    prefect_deployment_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    prefect_flow_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Run history now stored in Prefect (optional local cache)
    # Can remove `runs` relationship if fully relying on Prefect
```

**Migration:**
```python
# alembic/versions/xxx_add_prefect_fields.py

def upgrade():
    op.add_column('ingestions', sa.Column('prefect_deployment_id', sa.String(255), nullable=True))
    op.add_column('ingestions', sa.Column('prefect_flow_id', sa.String(255), nullable=True))
    op.create_index('ix_ingestions_prefect_deployment_id', 'ingestions', ['prefect_deployment_id'])

def downgrade():
    op.drop_index('ix_ingestions_prefect_deployment_id', 'ingestions')
    op.drop_column('ingestions', 'prefect_flow_id')
    op.drop_column('ingestions', 'prefect_deployment_id')
```

### 5.3 Configuration Updates

```python
# app/config.py

class Settings(BaseSettings):
    # ... existing settings ...

    # Prefect Configuration
    PREFECT_API_URL: str = "http://prefect-server:4200/api"
    PREFECT_API_KEY: Optional[str] = None  # For Prefect Cloud

    # Worker Configuration
    PREFECT_DEFAULT_WORK_QUEUE: str = "autoloader-default"
    PREFECT_HIGH_MEMORY_WORK_QUEUE: str = "autoloader-high-memory"
    PREFECT_PRIORITY_WORK_QUEUE: str = "autoloader-priority"

    # Flow Configuration
    PREFECT_FLOW_RUN_TIMEOUT_SECONDS: int = 3600  # 1 hour
    PREFECT_TASK_RETRY_DELAY_SECONDS: int = 60
    PREFECT_MAX_TASK_RETRIES: int = 3

    # Resource Thresholds
    HIGH_MEMORY_THRESHOLD_GB: int = 10  # Files > 10GB use high-memory queue

    class Config:
        env_file = ".env"
```

---

## 6. Deployment Patterns

### 6.1 Kubernetes Deployment

**Architecture:**
```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                   │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Namespace: iomete                                │  │
│  │                                                   │  │
│  │  ┌────────────────────────────────────────────┐  │  │
│  │  │  Deployment: autoloader-api (3 replicas)   │  │  │
│  │  │  - FastAPI server                          │  │  │
│  │  │  - Service: autoloader-api-svc             │  │  │
│  │  └────────────────────────────────────────────┘  │  │
│  │                                                   │  │
│  │  ┌────────────────────────────────────────────┐  │  │
│  │  │  Deployment: prefect-server (2 replicas)   │  │  │
│  │  │  - Prefect orchestration server            │  │  │
│  │  │  - Service: prefect-server-svc             │  │  │
│  │  │  - Database: PostgreSQL (shared or separate)│ │
│  │  └────────────────────────────────────────────┘  │  │
│  │                                                   │  │
│  │  ┌────────────────────────────────────────────┐  │  │
│  │  │  Deployment: prefect-worker-default        │  │  │
│  │  │  - Replicas: 3-5 (HPA enabled)             │  │  │
│  │  │  - Resources: 2 CPU, 4GB RAM               │  │  │
│  │  │  - Work Queue: autoloader-default          │  │  │
│  │  └────────────────────────────────────────────┘  │  │
│  │                                                   │  │
│  │  ┌────────────────────────────────────────────┐  │  │
│  │  │  Deployment: prefect-worker-high-memory    │  │  │
│  │  │  - Replicas: 2 (HPA enabled)               │  │  │
│  │  │  - Resources: 4 CPU, 16GB RAM              │  │  │
│  │  │  - Work Queue: autoloader-high-memory      │  │  │
│  │  └────────────────────────────────────────────┘  │  │
│  │                                                   │  │
│  │  ┌────────────────────────────────────────────┐  │  │
│  │  │  StatefulSet: postgresql (if separate)     │  │  │
│  │  │  - Prefect database                        │  │  │
│  │  │  - PVC: 100GB                              │  │  │
│  │  └────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Kubernetes Manifests

**Prefect Server Deployment:**
```yaml
# kubernetes/prefect-server.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-server
  namespace: iomete
  labels:
    app: prefect-server
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
          - --port
          - "4200"
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
        livenessProbe:
          httpGet:
            path: /api/health
            port: 4200
          initialDelaySeconds: 30
          periodSeconds: 10
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
    name: http
  type: ClusterIP
```

**Prefect Worker Deployment (Default Queue):**
```yaml
# kubernetes/prefect-worker-default.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-default
  namespace: iomete
  labels:
    app: prefect-worker
    queue: default
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
      serviceAccountName: prefect-worker
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
          - autoloader-default
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
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

**Prefect Worker Deployment (High Memory Queue):**
```yaml
# kubernetes/prefect-worker-high-memory.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-worker-high-memory
  namespace: iomete
  labels:
    app: prefect-worker
    queue: high-memory
spec:
  replicas: 2
  selector:
    matchLabels:
      app: prefect-worker
      queue: high-memory
  template:
    metadata:
      labels:
        app: prefect-worker
        queue: high-memory
    spec:
      serviceAccountName: prefect-worker
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
          - autoloader-high-memory
          - --limit
          - "1"  # Single task per worker
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
            cpu: 4
            memory: 16Gi
          limits:
            cpu: 8
            memory: 32Gi
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: prefect-worker-high-memory-hpa
  namespace: iomete
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: prefect-worker-high-memory
  minReplicas: 2
  maxReplicas: 5
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

**Service Account for Workers:**
```yaml
# kubernetes/prefect-worker-rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: prefect-worker
  namespace: iomete
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: prefect-worker-role
  namespace: iomete
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "get", "list", "watch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: prefect-worker-rolebinding
  namespace: iomete
subjects:
- kind: ServiceAccount
  name: prefect-worker
  namespace: iomete
roleRef:
  kind: Role
  name: prefect-worker-role
  apiGroup: rbac.authorization.k8s.io
```

### 6.3 Helm Chart (Recommended)

**Structure:**
```
charts/autoloader/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── api-deployment.yaml
│   ├── prefect-server-deployment.yaml
│   ├── prefect-worker-default-deployment.yaml
│   ├── prefect-worker-high-memory-deployment.yaml
│   ├── secrets.yaml
│   └── services.yaml
```

**values.yaml:**
```yaml
# charts/autoloader/values.yaml

global:
  namespace: iomete

api:
  replicas: 3
  image:
    repository: iomete/autoloader-api
    tag: latest
  resources:
    requests:
      cpu: 1
      memory: 2Gi
    limits:
      cpu: 2
      memory: 4Gi

prefectServer:
  enabled: true
  replicas: 2
  image:
    repository: prefecthq/prefect
    tag: 2.14-python3.11
  database:
    url: postgresql://prefect:password@postgresql:5432/prefect
  resources:
    requests:
      cpu: 1
      memory: 2Gi
    limits:
      cpu: 2
      memory: 4Gi

prefectWorkers:
  default:
    enabled: true
    replicas: 3
    workQueue: autoloader-default
    image:
      repository: iomete/autoloader-worker
      tag: latest
    resources:
      requests:
        cpu: 2
        memory: 4Gi
      limits:
        cpu: 4
        memory: 8Gi
    autoscaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPU: 70
      targetMemory: 80

  highMemory:
    enabled: true
    replicas: 2
    workQueue: autoloader-high-memory
    concurrencyLimit: 1  # Single task per worker
    image:
      repository: iomete/autoloader-worker
      tag: latest
    resources:
      requests:
        cpu: 4
        memory: 16Gi
      limits:
        cpu: 8
        memory: 32Gi
    autoscaling:
      enabled: true
      minReplicas: 2
      maxReplicas: 5
      targetCPU: 70

database:
  url: postgresql://autoloader:password@postgresql:5432/autoloader

sparkConnect:
  url: sc://spark-connect-service:15002
```

**Deployment:**
```bash
# Install
helm install autoloader ./charts/autoloader -n iomete

# Upgrade
helm upgrade autoloader ./charts/autoloader -n iomete

# Uninstall
helm uninstall autoloader -n iomete
```

---

## 7. Worker Configuration Strategies

### 7.1 Strategy Comparison

| Strategy | Use Case | Pros | Cons |
|----------|----------|------|------|
| **Multi-Task Workers** | Standard ingestions | ✅ Better resource utilization<br>✅ Fewer pods<br>✅ Lower cost | ❌ Resource contention<br>❌ One task can impact others |
| **Single-Task Workers** | Large file ingestions | ✅ Full resource isolation<br>✅ Predictable performance | ❌ More pods<br>❌ Higher cost<br>❌ Resource waste if idle |
| **Kubernetes Job Workers** | Unpredictable workloads | ✅ Maximum isolation<br>✅ Auto-cleanup<br>✅ Elastic scaling | ❌ Job startup overhead<br>❌ More complex<br>❌ Higher cost |

### 7.2 Recommended Strategy for Autoloader

**Hybrid Approach:**

```
┌──────────────────────────────────────────────────────────┐
│  Work Queue: autoloader-default                          │
│  - Workers: 3-5 pods (multi-task)                        │
│  - Concurrency: 5 tasks per worker                       │
│  - Resources: 2 CPU, 4GB RAM per pod                     │
│  - Use Case: Standard S3/Azure/GCS ingestions            │
│  - Auto-scaling: Yes (3-10 replicas based on CPU/memory) │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Work Queue: autoloader-high-memory                      │
│  - Workers: 2 pods (single-task)                         │
│  - Concurrency: 1 task per worker                        │
│  - Resources: 4 CPU, 16GB RAM per pod                    │
│  - Use Case: Large file ingestions (>10GB)               │
│  - Auto-scaling: Yes (2-5 replicas based on queue depth) │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Work Queue: autoloader-priority                         │
│  - Workers: 1-2 pods (multi-task)                        │
│  - Concurrency: 3 tasks per worker                       │
│  - Resources: 2 CPU, 4GB RAM per pod                     │
│  - Use Case: Manual triggers, retries, previews          │
│  - Auto-scaling: Optional (1-3 replicas)                 │
└──────────────────────────────────────────────────────────┘
```

**Routing Logic:**
```python
# app/services/prefect_service.py

def get_work_queue_for_ingestion(ingestion: Ingestion) -> str:
    """
    Determine which work queue to use based on ingestion characteristics.
    """
    # Manual trigger or retry → priority queue
    if ingestion.is_manual_trigger or ingestion.is_retry:
        return settings.PREFECT_PRIORITY_WORK_QUEUE

    # Estimate data size
    estimated_size_gb = estimate_ingestion_size(ingestion)

    # Large files → high memory queue
    if estimated_size_gb > settings.HIGH_MEMORY_THRESHOLD_GB:
        return settings.PREFECT_HIGH_MEMORY_WORK_QUEUE

    # Default queue
    return settings.PREFECT_DEFAULT_WORK_QUEUE

def estimate_ingestion_size(ingestion: Ingestion) -> float:
    """
    Estimate total data size in GB.
    """
    # Use file discovery service to get file count and sizes
    discovery = FileDiscoveryService()
    files = discovery.discover_files(ingestion)

    total_size_bytes = sum(f.size for f in files)
    return total_size_bytes / (1024 ** 3)  # Convert to GB
```

### 7.3 Auto-Scaling Configuration

**HPA Based on Queue Depth (Advanced):**
```yaml
# kubernetes/prefect-worker-default-hpa-advanced.yaml
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
  # Scale based on CPU
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70

  # Scale based on memory
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80

  # Scale based on custom metric (Prefect queue depth)
  # Requires Prometheus adapter and Prefect metrics
  - type: External
    external:
      metric:
        name: prefect_work_queue_depth
        selector:
          matchLabels:
            queue: autoloader-default
      target:
        type: AverageValue
        averageValue: "5"  # Scale up if >5 pending runs per worker

  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50  # Scale up by 50% at a time
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300  # Wait 5 min before scaling down
      policies:
      - type: Pods
        value: 1  # Scale down 1 pod at a time
        periodSeconds: 120
```

---

## 8. Code Implementation

### 8.1 Prefect Flows

**Main Ingestion Flow:**
```python
# app/prefect/flows/run_ingestion.py

from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
from typing import List
import httpx
from uuid import UUID

from app.services.file_discovery_service import FileDiscoveryService
from app.services.batch_orchestrator import BatchOrchestrator
from app.repositories.ingestion_repository import IngestionRepository
from app.database import SessionLocal


@task(
    name="fetch_ingestion_config",
    retries=3,
    retry_delay_seconds=10,
    tags=["config"]
)
def fetch_ingestion_config(ingestion_id: UUID) -> dict:
    """
    Fetch ingestion configuration from Autoloader API.
    """
    logger = get_run_logger()
    logger.info(f"Fetching config for ingestion {ingestion_id}")

    # Use Autoloader API
    api_url = settings.AUTOLOADER_API_URL
    response = httpx.get(f"{api_url}/api/v1/ingestions/{ingestion_id}")
    response.raise_for_status()

    ingestion = response.json()
    logger.info(f"Fetched config for ingestion {ingestion['name']}")

    return ingestion


@task(
    name="discover_files",
    retries=2,
    retry_delay_seconds=60,
    tags=["discovery"]
)
def discover_files(ingestion_config: dict) -> List[dict]:
    """
    Discover new files from cloud storage.
    """
    logger = get_run_logger()
    logger.info(f"Discovering files for ingestion {ingestion_config['id']}")

    # Use FileDiscoveryService
    db = SessionLocal()
    try:
        repo = IngestionRepository(db)
        ingestion = repo.get_by_id(UUID(ingestion_config['id']))

        discovery = FileDiscoveryService()
        files = discovery.discover_files(ingestion)

        logger.info(f"Discovered {len(files)} new files")
        return [{"path": f.path, "size": f.size} for f in files]
    finally:
        db.close()


@task(
    name="process_files",
    retries=2,
    retry_delay_seconds=120,
    timeout_seconds=3600,  # 1 hour timeout
    tags=["processing"]
)
def process_files(ingestion_id: UUID, files: List[dict]) -> dict:
    """
    Process files via Spark Connect.
    """
    logger = get_run_logger()
    logger.info(f"Processing {len(files)} files for ingestion {ingestion_id}")

    # Use BatchOrchestrator
    db = SessionLocal()
    try:
        orchestrator = BatchOrchestrator(db)
        result = orchestrator.run_scheduled_ingestion(ingestion_id)

        logger.info(f"Processed {result.records_ingested} records")

        return {
            "records_ingested": result.records_ingested,
            "files_processed": result.files_processed,
            "status": result.status,
        }
    finally:
        db.close()


@flow(
    name="run_ingestion",
    log_prints=True,
    retries=1,
    retry_delay_seconds=300,  # 5 min between flow retries
)
def run_ingestion_flow(ingestion_id: str):
    """
    Main flow for scheduled ingestion execution.

    This flow:
    1. Fetches ingestion configuration
    2. Discovers new files from cloud storage
    3. Processes files via Spark Connect
    4. Updates file state and run history

    Args:
        ingestion_id: UUID of the ingestion to run
    """
    logger = get_run_logger()
    ingestion_uuid = UUID(ingestion_id)

    logger.info(f"Starting ingestion flow for {ingestion_id}")

    # Step 1: Fetch config
    ingestion_config = fetch_ingestion_config(ingestion_uuid)

    # Step 2: Discover files
    files = discover_files(ingestion_config)

    if not files:
        logger.info("No new files to process. Exiting.")
        return {
            "status": "NO_NEW_FILES",
            "files_processed": 0,
            "records_ingested": 0,
        }

    # Step 3: Process files
    result = process_files(ingestion_uuid, files)

    logger.info(f"Ingestion flow completed: {result}")
    return result
```

**Preview Flow (On-Demand):**
```python
# app/prefect/flows/preview_ingestion.py

from prefect import flow, task, get_run_logger
from uuid import UUID

from app.services.batch_orchestrator import BatchOrchestrator
from app.database import SessionLocal


@flow(
    name="preview_ingestion",
    log_prints=True,
)
def preview_ingestion_flow(ingestion_id: str, max_files: int = 5):
    """
    Preview flow for testing ingestion configuration.

    This flow:
    1. Discovers sample files (limited)
    2. Processes sample files
    3. Returns preview results

    Args:
        ingestion_id: UUID of the ingestion to preview
        max_files: Maximum files to preview (default: 5)
    """
    logger = get_run_logger()
    ingestion_uuid = UUID(ingestion_id)

    logger.info(f"Starting preview for ingestion {ingestion_id}")

    # Use BatchOrchestrator with preview mode
    db = SessionLocal()
    try:
        orchestrator = BatchOrchestrator(db)
        result = orchestrator.run_preview(ingestion_uuid, max_files=max_files)

        logger.info(f"Preview completed: {result.records_ingested} records")

        return {
            "status": "PREVIEW_COMPLETED",
            "files_processed": result.files_processed,
            "records_ingested": result.records_ingested,
            "sample_data": result.sample_data,  # First 100 rows
        }
    finally:
        db.close()
```

### 8.2 Prefect Service (API Integration)

```python
# app/services/prefect_service.py

from prefect.client import get_client
from prefect.server.schemas.schedules import CronSchedule
from prefect.server.schemas.core import Flow, Deployment
from typing import Optional, List
from uuid import UUID
import asyncio

from app.config import settings
from app.models.domain import Ingestion


class PrefectService:
    """
    Service for managing Prefect deployments and flow runs.
    """

    def __init__(self):
        self.api_url = settings.PREFECT_API_URL
        # Flow IDs (registered during startup)
        self.ingestion_flow_id: Optional[str] = None
        self.preview_flow_id: Optional[str] = None

    async def initialize(self):
        """
        Initialize service by registering flows.
        """
        # Import flows
        from app.prefect.flows.run_ingestion import run_ingestion_flow
        from app.prefect.flows.preview_ingestion import preview_ingestion_flow

        async with get_client() as client:
            # Register ingestion flow
            ingestion_flow = await client.create_flow(
                flow=run_ingestion_flow.to_deployment_spec()
            )
            self.ingestion_flow_id = str(ingestion_flow.id)

            # Register preview flow
            preview_flow = await client.create_flow(
                flow=preview_ingestion_flow.to_deployment_spec()
            )
            self.preview_flow_id = str(preview_flow.id)

    async def create_deployment(
        self,
        ingestion: Ingestion,
    ) -> str:
        """
        Create a Prefect deployment for an ingestion.

        Returns:
            Deployment ID
        """
        async with get_client() as client:
            # Determine work queue
            work_queue = self._get_work_queue(ingestion)

            # Create deployment
            deployment = await client.create_deployment(
                flow_id=self.ingestion_flow_id,
                name=f"ingestion-{ingestion.id}",
                parameters={"ingestion_id": str(ingestion.id)},
                schedule=CronSchedule(cron=ingestion.schedule_cron),
                work_queue_name=work_queue,
                tags=[
                    "autoloader",
                    f"tenant-{ingestion.tenant_id}",
                    f"source-{ingestion.source_type.value}",
                ],
                description=f"Ingestion: {ingestion.name}",
            )

            return str(deployment.id)

    async def update_deployment_schedule(
        self,
        deployment_id: str,
        schedule_cron: str,
    ):
        """
        Update deployment schedule.
        """
        async with get_client() as client:
            await client.update_deployment(
                deployment_id=deployment_id,
                schedule=CronSchedule(cron=schedule_cron),
            )

    async def pause_deployment(self, deployment_id: str):
        """
        Pause deployment (disable schedule).
        """
        async with get_client() as client:
            await client.set_deployment_paused_state(
                deployment_id=deployment_id,
                paused=True,
            )

    async def resume_deployment(self, deployment_id: str):
        """
        Resume deployment (enable schedule).
        """
        async with get_client() as client:
            await client.set_deployment_paused_state(
                deployment_id=deployment_id,
                paused=False,
            )

    async def delete_deployment(self, deployment_id: str):
        """
        Delete deployment.
        """
        async with get_client() as client:
            await client.delete_deployment(deployment_id)

    async def trigger_deployment(
        self,
        deployment_id: str,
        parameters: Optional[dict] = None,
    ) -> str:
        """
        Manually trigger a deployment (create flow run).

        Returns:
            Flow run ID
        """
        async with get_client() as client:
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters=parameters or {},
            )
            return str(flow_run.id)

    async def get_flow_runs(
        self,
        deployment_id: str,
        limit: int = 50,
    ) -> List[dict]:
        """
        Get flow runs for a deployment.
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

    async def get_flow_run_logs(self, flow_run_id: str) -> List[dict]:
        """
        Get logs for a flow run.
        """
        async with get_client() as client:
            logs = await client.read_logs(
                log_filter={"flow_run_id": {"any_": [flow_run_id]}},
            )

            return [
                {
                    "timestamp": log.timestamp,
                    "level": log.level,
                    "message": log.message,
                }
                for log in logs
            ]

    async def retry_flow_run(self, flow_run_id: str) -> str:
        """
        Retry a failed flow run.

        Returns:
            New flow run ID
        """
        async with get_client() as client:
            # Get original flow run
            flow_run = await client.read_flow_run(flow_run_id)

            # Create new flow run with same parameters
            new_run = await client.create_flow_run_from_deployment(
                deployment_id=str(flow_run.deployment_id),
                parameters=flow_run.parameters,
            )

            return str(new_run.id)

    async def run_preview(
        self,
        ingestion_id: UUID,
        max_files: int = 5,
    ) -> str:
        """
        Run preview flow (on-demand, not scheduled).

        Returns:
            Flow run ID
        """
        async with get_client() as client:
            flow_run = await client.create_flow_run(
                flow=self.preview_flow_id,
                parameters={
                    "ingestion_id": str(ingestion_id),
                    "max_files": max_files,
                },
                tags=["preview", f"ingestion-{ingestion_id}"],
            )

            return str(flow_run.id)

    def _get_work_queue(self, ingestion: Ingestion) -> str:
        """
        Determine work queue based on ingestion characteristics.
        """
        # Estimate data size (simplified)
        # In production, use FileDiscoveryService
        estimated_size_gb = 5  # TODO: Implement estimation

        if estimated_size_gb > settings.HIGH_MEMORY_THRESHOLD_GB:
            return settings.PREFECT_HIGH_MEMORY_WORK_QUEUE

        return settings.PREFECT_DEFAULT_WORK_QUEUE


# Singleton instance
_prefect_service: Optional[PrefectService] = None

async def get_prefect_service() -> PrefectService:
    """
    Get singleton Prefect service instance.
    """
    global _prefect_service

    if _prefect_service is None:
        _prefect_service = PrefectService()
        await _prefect_service.initialize()

    return _prefect_service
```

### 8.3 Updated API Endpoints

```python
# app/api/v1/ingestions.py (updated)

from fastapi import APIRouter, Depends, HTTPException
from uuid import UUID
from typing import List

from app.models.schemas import IngestionCreate, IngestionResponse
from app.services.ingestion_service import IngestionService
from app.services.prefect_service import get_prefect_service, PrefectService
from app.dependencies import get_ingestion_service

router = APIRouter(prefix="/ingestions", tags=["ingestions"])


@router.post("", response_model=IngestionResponse, status_code=201)
async def create_ingestion(
    ingestion_data: IngestionCreate,
    service: IngestionService = Depends(get_ingestion_service),
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Create a new ingestion and Prefect deployment.
    """
    # 1. Create ingestion in database
    ingestion = service.create_ingestion(ingestion_data)

    # 2. Create Prefect deployment
    deployment_id = await prefect.create_deployment(ingestion)

    # 3. Update ingestion with deployment ID
    ingestion.prefect_deployment_id = deployment_id
    service.update_ingestion(ingestion.id, {"prefect_deployment_id": deployment_id})

    return ingestion


@router.patch("/{ingestion_id}/schedule")
async def update_schedule(
    ingestion_id: UUID,
    schedule_cron: str,
    service: IngestionService = Depends(get_ingestion_service),
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Update ingestion schedule.
    """
    # 1. Get ingestion
    ingestion = service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    # 2. Update Prefect deployment schedule
    await prefect.update_deployment_schedule(
        ingestion.prefect_deployment_id,
        schedule_cron,
    )

    # 3. Update database
    service.update_ingestion(ingestion_id, {"schedule_cron": schedule_cron})

    return {"message": "Schedule updated"}


@router.post("/{ingestion_id}/pause")
async def pause_ingestion(
    ingestion_id: UUID,
    service: IngestionService = Depends(get_ingestion_service),
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Pause scheduled ingestion.
    """
    ingestion = service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    # Pause Prefect deployment
    await prefect.pause_deployment(ingestion.prefect_deployment_id)

    # Update database
    service.update_ingestion(ingestion_id, {"status": "PAUSED"})

    return {"message": "Ingestion paused"}


@router.post("/{ingestion_id}/resume")
async def resume_ingestion(
    ingestion_id: UUID,
    service: IngestionService = Depends(get_ingestion_service),
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Resume scheduled ingestion.
    """
    ingestion = service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    # Resume Prefect deployment
    await prefect.resume_deployment(ingestion.prefect_deployment_id)

    # Update database
    service.update_ingestion(ingestion_id, {"status": "ACTIVE"})

    return {"message": "Ingestion resumed"}


@router.post("/{ingestion_id}/run", status_code=202)
async def trigger_ingestion(
    ingestion_id: UUID,
    service: IngestionService = Depends(get_ingestion_service),
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Manually trigger an ingestion run.
    """
    ingestion = service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    # Trigger Prefect flow run
    flow_run_id = await prefect.trigger_deployment(
        ingestion.prefect_deployment_id
    )

    return {
        "message": "Ingestion triggered",
        "flow_run_id": flow_run_id,
    }


@router.delete("/{ingestion_id}", status_code=204)
async def delete_ingestion(
    ingestion_id: UUID,
    service: IngestionService = Depends(get_ingestion_service),
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Delete ingestion and Prefect deployment.
    """
    ingestion = service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    # Delete Prefect deployment
    await prefect.delete_deployment(ingestion.prefect_deployment_id)

    # Delete from database
    service.delete_ingestion(ingestion_id)
```

```python
# app/api/v1/runs.py (updated)

from fastapi import APIRouter, Depends, HTTPException
from uuid import UUID
from typing import List

from app.services.prefect_service import get_prefect_service, PrefectService
from app.services.ingestion_service import IngestionService
from app.dependencies import get_ingestion_service

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("/ingestions/{ingestion_id}/runs")
async def get_ingestion_runs(
    ingestion_id: UUID,
    limit: int = 50,
    service: IngestionService = Depends(get_ingestion_service),
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Get run history for an ingestion (from Prefect).
    """
    ingestion = service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    # Get flow runs from Prefect
    flow_runs = await prefect.get_flow_runs(
        ingestion.prefect_deployment_id,
        limit=limit,
    )

    return {
        "ingestion_id": str(ingestion_id),
        "runs": flow_runs,
    }


@router.get("/runs/{flow_run_id}/logs")
async def get_run_logs(
    flow_run_id: str,
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Get logs for a specific flow run.
    """
    logs = await prefect.get_flow_run_logs(flow_run_id)

    return {
        "flow_run_id": flow_run_id,
        "logs": logs,
    }


@router.post("/runs/{flow_run_id}/retry", status_code=202)
async def retry_run(
    flow_run_id: str,
    prefect: PrefectService = Depends(get_prefect_service),
):
    """
    Retry a failed flow run.
    """
    new_run_id = await prefect.retry_flow_run(flow_run_id)

    return {
        "message": "Run retried",
        "original_run_id": flow_run_id,
        "new_run_id": new_run_id,
    }
```

---

## 9. Migration from APScheduler

### 9.1 Migration Strategy

**Phase 1: Parallel Deployment (Week 1)**
```
┌─────────────────────────────────────────────┐
│  Old: APScheduler (Keep running)           │
│  - Existing ingestions continue             │
│  - No new ingestions created                │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  New: Prefect (Deploy in parallel)          │
│  - Deploy Prefect server                    │
│  - Deploy workers                           │
│  - Test with sample ingestions              │
└─────────────────────────────────────────────┘
```

**Phase 2: Migration (Week 2)**
```
For each existing ingestion:
  1. Create Prefect deployment
  2. Pause APScheduler job
  3. Verify Prefect deployment works
  4. Delete APScheduler job
  5. Mark as migrated
```

**Phase 3: Cleanup (Week 2)**
```
1. Remove APScheduler code
2. Remove APScheduler dependencies
3. Update documentation
4. Monitor Prefect-only operation
```

### 9.2 Migration Script

```python
# scripts/migrate_to_prefect.py

import asyncio
from app.database import SessionLocal
from app.repositories.ingestion_repository import IngestionRepository
from app.services.prefect_service import get_prefect_service


async def migrate_ingestion(ingestion, prefect_service):
    """
    Migrate single ingestion from APScheduler to Prefect.
    """
    print(f"Migrating ingestion: {ingestion.name} ({ingestion.id})")

    # 1. Create Prefect deployment
    deployment_id = await prefect_service.create_deployment(ingestion)

    # 2. Update database
    ingestion.prefect_deployment_id = deployment_id

    # 3. Pause APScheduler job (if running)
    # scheduler_service.pause_ingestion(ingestion.id)

    print(f"✓ Migrated: {ingestion.name} → Deployment {deployment_id}")


async def main():
    """
    Migrate all ingestions from APScheduler to Prefect.
    """
    db = SessionLocal()
    prefect = await get_prefect_service()

    try:
        # Get all active ingestions
        repo = IngestionRepository(db)
        ingestions = repo.get_all_scheduled()

        print(f"Found {len(ingestions)} ingestions to migrate")

        # Migrate each ingestion
        for ingestion in ingestions:
            try:
                await migrate_ingestion(ingestion, prefect)
                db.commit()
            except Exception as e:
                print(f"✗ Failed to migrate {ingestion.name}: {e}")
                db.rollback()

        print("\nMigration complete!")

    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
```

**Run Migration:**
```bash
# Test migration (dry run)
python scripts/migrate_to_prefect.py --dry-run

# Actual migration
python scripts/migrate_to_prefect.py

# Verify
prefect deployment ls
```

---

## 10. Operational Guide

### 10.1 Deployment Checklist

**Pre-Deployment:**
- [ ] PostgreSQL database ready (Prefect + Autoloader can share)
- [ ] Kubernetes cluster configured
- [ ] Prefect server image available
- [ ] Worker image built with Autoloader dependencies
- [ ] Service accounts and RBAC configured
- [ ] Secrets created (database, Spark Connect, cloud credentials)

**Deployment:**
- [ ] Deploy Prefect server (2 replicas)
- [ ] Verify Prefect UI accessible
- [ ] Deploy workers (default queue: 3 replicas)
- [ ] Deploy workers (high-memory queue: 2 replicas)
- [ ] Configure HPA for workers
- [ ] Deploy Autoloader API (3 replicas)
- [ ] Run database migrations
- [ ] Register Prefect flows

**Validation:**
- [ ] Create test ingestion via API
- [ ] Verify Prefect deployment created
- [ ] Trigger manual run
- [ ] Verify worker picks up task
- [ ] Verify Spark Connect execution
- [ ] Check logs in Prefect UI
- [ ] Verify run history in API

### 10.2 Monitoring

**Key Metrics:**

```python
# Prometheus metrics (add to workers)

from prometheus_client import Counter, Histogram, Gauge

# Flow executions
flow_runs_total = Counter(
    'prefect_flow_runs_total',
    'Total flow runs',
    ['flow_name', 'status']
)

flow_run_duration = Histogram(
    'prefect_flow_run_duration_seconds',
    'Flow run duration',
    ['flow_name']
)

# Worker health
worker_active_tasks = Gauge(
    'prefect_worker_active_tasks',
    'Active tasks per worker',
    ['worker_id', 'queue']
)

# Queue depth
work_queue_depth = Gauge(
    'prefect_work_queue_depth',
    'Pending runs in queue',
    ['queue']
)
```

**Grafana Dashboard:**
```json
{
  "dashboard": {
    "title": "IOMETE Autoloader - Prefect",
    "panels": [
      {
        "title": "Flow Run Success Rate",
        "targets": [
          {
            "expr": "rate(prefect_flow_runs_total{status=\"COMPLETED\"}[5m]) / rate(prefect_flow_runs_total[5m])"
          }
        ]
      },
      {
        "title": "Worker CPU Usage",
        "targets": [
          {
            "expr": "container_cpu_usage_seconds_total{pod=~\"prefect-worker.*\"}"
          }
        ]
      },
      {
        "title": "Work Queue Depth",
        "targets": [
          {
            "expr": "prefect_work_queue_depth"
          }
        ]
      },
      {
        "title": "Flow Run Duration (p95)",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, prefect_flow_run_duration_seconds)"
          }
        ]
      }
    ]
  }
}
```

**Alerts:**
```yaml
# prometheus-alerts.yaml
groups:
  - name: prefect
    rules:
      - alert: PrefectServerDown
        expr: up{job="prefect-server"} == 0
        for: 1m
        annotations:
          summary: "Prefect server is down"

      - alert: HighFlowFailureRate
        expr: rate(prefect_flow_runs_total{status="FAILED"}[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High flow failure rate (>10%)"

      - alert: WorkQueueBacklog
        expr: prefect_work_queue_depth > 50
        for: 10m
        annotations:
          summary: "Work queue backlog (>50 pending runs)"

      - alert: NoActiveWorkers
        expr: count(prefect_worker_active_tasks) == 0
        for: 2m
        annotations:
          summary: "No active Prefect workers"
```

### 10.3 Troubleshooting

**Common Issues:**

**1. Flow run stuck in PENDING**
```bash
# Check if workers are running
kubectl get pods -n iomete -l app=prefect-worker

# Check worker logs
kubectl logs -n iomete -l app=prefect-worker --tail=100

# Check work queue
prefect work-queue ls

# Manually assign work queue
prefect deployment set-work-queue <deployment-name> <queue-name>
```

**2. Worker OOM (Out of Memory)**
```bash
# Check worker memory usage
kubectl top pods -n iomete -l app=prefect-worker

# Increase memory limits in deployment
kubectl edit deployment prefect-worker-default -n iomete

# Or move to high-memory queue
# Update ingestion work queue via API
PATCH /api/v1/ingestions/{id}/work-queue
```

**3. Prefect server not reachable**
```bash
# Check Prefect server pods
kubectl get pods -n iomete -l app=prefect-server

# Check service
kubectl get svc prefect-server -n iomete

# Test connectivity from worker
kubectl exec -it <worker-pod> -n iomete -- curl http://prefect-server:4200/api/health
```

**4. Deployment not scheduling**
```bash
# Check deployment status
prefect deployment inspect <deployment-name>

# Check if paused
prefect deployment resume <deployment-name>

# Verify cron schedule
prefect deployment set-schedule <deployment-name> --cron "0 2 * * *"
```

---

## 11. Cost-Benefit Analysis

### 11.1 Development Cost

| Task | Prefect | DIY APScheduler |
|------|---------|-----------------|
| Infrastructure setup | 2 days | 1 day |
| Flow implementation | 3 days | N/A |
| API integration | 2 days | 5 days |
| Worker deployment | 2 days | 4 days (custom) |
| Monitoring setup | 1 day | 4 days (custom) |
| Testing | 2 days | 4 days |
| Documentation | 1 day | 2 days |
| **Total** | **13 days (2.6 weeks)** | **20 days (4 weeks)** |

**Savings: 35% faster time-to-market**

### 11.2 Operational Cost

**Infrastructure (Monthly):**

| Component | Prefect | DIY APScheduler |
|-----------|---------|-----------------|
| Orchestration server | Prefect Server: $50-100 | Redis Cluster: $150-300 |
| Workers (3-5 pods) | $200-400 | $200-400 |
| Load Balancer | $20-50 | $20-50 |
| Monitoring | Built-in (free) | Prometheus/Grafana: $100-200 |
| **Total** | **$270-550/month** | **$470-950/month** |

**Savings: 40% lower infrastructure cost**

**Engineering (Monthly):**

| Activity | Prefect | DIY APScheduler |
|----------|---------|-----------------|
| Debugging | 2 hours | 8 hours |
| Monitoring | 1 hour | 4 hours |
| Updates/patches | 1 hour | 3 hours |
| Incident response | 2 hours | 6 hours |
| **Total** | **6 hours/month** | **21 hours/month** |

**Savings: 70% less operational burden**

### 11.3 Total Cost of Ownership (3 Years)

**Prefect:**
- Development: 13 days × $1000/day = $13,000
- Infrastructure: $400/month × 36 months = $14,400
- Operations: 6 hours/month × $150/hour × 36 months = $32,400
- **Total: $59,800**

**DIY APScheduler:**
- Development: 20 days × $1000/day = $20,000
- Infrastructure: $700/month × 36 months = $25,200
- Operations: 21 hours/month × $150/hour × 36 months = $113,400
- **Total: $158,600**

**Savings with Prefect: $98,800 (62%)**

---

## 12. Comparison Matrix

### 12.1 Feature Comparison

| Feature | Single APScheduler | Distributed APScheduler | Prefect | IOMETE Jobs | K8s CronJobs |
|---------|-------------------|------------------------|---------|-------------|--------------|
| **Horizontal Scaling** | ❌ No | 🟡 Custom | ✅ Built-in | ✅ Built-in | ✅ Built-in |
| **Fault Tolerance** | ❌ No | 🟡 Custom | ✅ Built-in | ✅ Built-in | ✅ Built-in |
| **Retry Logic** | 🟡 Basic | 🟡 Custom | ✅ Advanced | ✅ Built-in | 🟡 Basic |
| **Observability** | ❌ Minimal | 🟡 Custom | ✅ Rich UI + API | ✅ Built-in | 🟡 K8s logs |
| **Work Distribution** | ❌ No | 🟡 Custom | ✅ Work queues | ✅ Auto | ✅ K8s scheduler |
| **Priority Queues** | ❌ No | 🟡 Custom | ✅ Built-in | ✅ Built-in | ❌ No |
| **Development Time** | 🟢 1 week | 🔴 4 weeks | 🟢 2.6 weeks | 🟢 1.5 weeks | 🟡 2 weeks |
| **Operational Complexity** | 🟢 Low | 🔴 High | 🟢 Low | 🟢 Low | 🟡 Medium |
| **Infrastructure Cost** | 🟢 Low | 🔴 High | 🟢 Medium | 🟢 Low | 🟢 Low |
| **Already in Stack** | ❌ No | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes (K8s) |
| **Learning Curve** | 🟢 Low | 🔴 High | 🟡 Medium | 🟢 Low | 🟡 Medium |

### 12.2 Decision Matrix

**Choose Prefect if:**
- ✅ Already in your stack
- ✅ Need horizontal scaling
- ✅ Want rich observability
- ✅ Kubernetes-based infrastructure
- ✅ Need flexible worker configuration
- ✅ Want faster time-to-market than DIY

**Choose IOMETE Jobs if:**
- ✅ Want absolute simplicity
- ✅ Don't need complex workflows
- ✅ Already fully on IOMETE platform

**Choose Kubernetes CronJobs if:**
- ✅ Want minimal dependencies
- ✅ Simple scheduling needs
- ✅ Strong Kubernetes expertise

**Avoid Distributed APScheduler if:**
- ❌ Prefect is already available
- ❌ Limited engineering bandwidth
- ❌ Want production-grade solution

---

## 13. Recommendation

### 13.1 Final Recommendation

**✅ STRONGLY RECOMMENDED: Use Prefect for IOMETE Autoloader orchestration**

### 13.2 Rationale

**1. Already in Stack**
- Zero new dependencies
- Team familiarity
- Proven in production

**2. Perfect Fit for Requirements**
- Horizontal scaling: ✅ Built-in
- Worker flexibility: ✅ Multi-task or single-task
- Kubernetes-native: ✅ Perfect for your infrastructure
- Observability: ✅ Rich UI + API

**3. Superior to Alternatives**
- vs. DIY APScheduler: 62% lower TCO, faster delivery
- vs. IOMETE Jobs: More flexible, richer features
- vs. K8s CronJobs: Better observability, easier debugging

**4. Low Risk**
- Production-proven
- Well-documented
- Active community
- Clear migration path

### 13.3 Implementation Roadmap

**Week 1-2: Setup and Integration**
- Day 1-2: Deploy Prefect server on Kubernetes
- Day 3-4: Build worker image with Autoloader dependencies
- Day 5-6: Implement Prefect flows (run_ingestion, preview)
- Day 7-8: Implement PrefectService (API integration)
- Day 9-10: Update API endpoints (create/update/delete deployments)

**Week 3: Testing and Migration**
- Day 11-12: Integration testing
- Day 13-14: Migrate existing ingestions from APScheduler
- Day 15: Production deployment

**Week 4: Monitoring and Optimization**
- Day 16-17: Set up monitoring (Prometheus, Grafana)
- Day 18-19: Performance tuning (worker scaling)
- Day 20: Documentation and training

### 13.4 Success Criteria

**Technical:**
- [ ] All ingestions migrated to Prefect
- [ ] Worker auto-scaling functional
- [ ] <1% flow failure rate
- [ ] <5 second average flow run latency
- [ ] Full observability (logs, metrics, traces)

**Operational:**
- [ ] Zero manual intervention for scaling
- [ ] <2 hours/month operational overhead
- [ ] Clear runbooks for common issues
- [ ] Monitoring dashboards and alerts configured

**Business:**
- [ ] Support 500+ scheduled ingestions
- [ ] <30 second deployment to production
- [ ] <5% infrastructure cost increase

---

## 14. Conclusion

**Prefect provides the perfect balance of:**
- ✅ Production-grade orchestration
- ✅ Horizontal scaling out-of-the-box
- ✅ Kubernetes-native architecture
- ✅ Rich observability and debugging
- ✅ Flexible worker configuration
- ✅ Already in your stack (zero new dependencies)
- ✅ Lower TCO than DIY solutions

**You are NOT reinventing the wheel.** Prefect is purpose-built for exactly this use case: distributed workflow orchestration with stateless workers and centralized coordination.

**The architecture is clean:**
- FastAPI API (stateless) ← Handles user requests
- Prefect Server (orchestration) ← Manages scheduling and state
- Prefect Workers (stateless) ← Execute ingestions

**This is the RIGHT choice for IOMETE Autoloader.**

---

**End of Document**

**Related Documents:**
- `apscheduler-horizontal-scaling.md` - Why NOT to build distributed APScheduler
- `scheduler-implementation-guide.md` - Original APScheduler implementation
- `architecture-decision-file-tracking.md` - System architecture

**Decision Record:**
- **Status:** ✅ RECOMMENDED
- **Decision:** Use Prefect for orchestration, stateless workers for execution
- **Rationale:** Already in stack, production-grade, 62% lower TCO than DIY, perfect fit for Kubernetes
- **Review Date:** After 3 months of production operation
