# Prefect Integration - Phase 1 Foundation

This directory contains the Prefect orchestration integration for IOMETE Autoloader.

**Prefect Deployment:** Self-hosted as part of the IOMETE platform stack. We do NOT use Prefect Cloud.

## Status: Phase 1 - Foundation Complete ✅

The foundation has been implemented but **NOT yet integrated** with the API. This allows testing flows independently before full integration.

## Directory Structure

```
app/prefect/
├── flows/
│   └── run_ingestion.py      # Main ingestion flow
├── tasks/
│   ├── discovery.py           # File discovery task
│   ├── state.py               # File state checking task
│   ├── processing.py          # File processing task
│   └── run_management.py      # Run record management tasks
└── README.md                  # This file
```

## What's Implemented

- ✅ Prefect dependencies added to `requirements.txt`
- ✅ Prefect flow that wraps `BatchOrchestrator` logic
- ✅ Tasks for discovery, state checking, and processing
- ✅ Run record management (create, complete, fail)
- ✅ Prefect configuration in `app/config.py`
- ✅ Database migration for `prefect_deployment_id` and `prefect_flow_id` fields

## What's NOT Yet Done (Phase 2)

- ❌ `PrefectService` for deployment management
- ❌ API integration (create/update/delete deployments)
- ❌ Kubernetes manifests (Prefect server + workers)
- ❌ Migration script to convert existing ingestions
- ❌ Full integration tests

## Testing the Flow Locally

### 1. Install Prefect (if not already installed)

```bash
pip install -r requirements.txt
```

### 2. Start Self-Hosted Prefect Server

For local development:
```bash
prefect server start
```

This will start the Prefect UI at http://localhost:4200

For production, Prefect server runs as part of the IOMETE platform (see Kubernetes manifests in Phase 2).

### 3. Run the Flow Directly (for testing)

You can test the flow without deploying it:

```python
from app.prefect.flows.run_ingestion import run_ingestion_flow

# Run for a specific ingestion
result = run_ingestion_flow(
    ingestion_id="your-ingestion-uuid",
    trigger="manual"
)

print(result)
```

### 4. Or Use the Prefect CLI

```bash
# Set Prefect API URL to local server
export PREFECT_API_URL="http://localhost:4200/api"

# Run the flow
python -c "from app.prefect.flows.run_ingestion import run_ingestion_flow; run_ingestion_flow('ingestion-uuid')"
```

**Note:** In production, `PREFECT_API_URL` points to the self-hosted Prefect server running in the IOMETE Kubernetes cluster.

## Flow Architecture

The `run_ingestion_flow` replicates the logic of `BatchOrchestrator.run_ingestion()`:

```
1. create_run_record_task() → Creates Run with status=RUNNING
2. discover_files_task() → Discovers files from S3/Azure/GCS
3. check_file_state_task() → Gets already-processed files
4. Compute new_files = discovered - processed
5. process_files_task() → Processes new files via Spark
6. complete_run_record_task() → Updates Run with metrics
```

## Configuration

Prefect settings are in `app/config.py`:

```python
# Prefect Configuration (self-hosted as part of IOMETE stack)
prefect_api_url: str = "http://prefect-server:4200/api"

# Work Queues
prefect_default_work_queue: str = "autoloader-default"
prefect_high_memory_work_queue: str = "autoloader-high-memory"

# Flow Configuration
prefect_flow_run_timeout_seconds: int = 3600  # 1 hour
prefect_task_retry_delay_seconds: int = 60
prefect_max_task_retries: int = 3
```

Override via environment variables:
```bash
# Local development
export PREFECT_API_URL="http://localhost:4200/api"

# Production (automatically set in IOMETE deployment)
export PREFECT_API_URL="http://prefect-server.iomete-system:4200/api"
```

## Database Schema Changes

The migration adds two fields to the `ingestions` table:

```sql
ALTER TABLE ingestions ADD COLUMN prefect_deployment_id VARCHAR(255);
ALTER TABLE ingestions ADD COLUMN prefect_flow_id VARCHAR(255);
CREATE INDEX ix_ingestions_prefect_deployment_id ON ingestions(prefect_deployment_id);
```

Run migration:
```bash
alembic upgrade head
```

## Next Steps (Phase 2)

1. **Implement `PrefectService`** (`app/services/prefect_service.py`)
   - Create/update/delete deployments
   - Trigger flow runs
   - Query run history

2. **Update API Endpoints** (`app/api/v1/ingestions.py`)
   - `create_ingestion()` → Create Prefect deployment
   - `update_ingestion()` → Update deployment schedule
   - `delete_ingestion()` → Delete deployment
   - `trigger_run()` → Trigger via Prefect

3. **Deploy Infrastructure** (self-hosted in IOMETE cluster)
   - Kubernetes manifests for Prefect server
   - Kubernetes manifests for workers
   - Configure work pools and queues

4. **Migration Script**
   - Convert existing ingestions to Prefect deployments
   - Dry-run mode for validation

## Comparison: BatchOrchestrator vs Prefect Flow

| Aspect | BatchOrchestrator | Prefect Flow |
|--------|-------------------|--------------|
| **Entry Point** | `orchestrator.run_ingestion(ingestion)` | `run_ingestion_flow(ingestion_id)` |
| **Trigger** | Manual API call | Manual or scheduled |
| **Retries** | None (manual retry via API) | Automatic with configurable backoff |
| **Observability** | Basic logging | Prefect UI + logs + metrics |
| **Scaling** | Single process | Distributed workers |
| **State** | Database only | Prefect + Database |

## Troubleshooting

### Flow fails with "Ingestion not found"
- Ensure the ingestion exists in the database
- Check that `ingestion_id` is a valid UUID string

### Flow fails with Spark connection error
- Verify `get_spark_connect_credentials()` returns correct URL/token
- Check Spark Connect service is running

### Tasks fail with database errors
- Ensure database migrations are applied: `alembic upgrade head`
- Check database connection string in `.env`

## References

- [Prefect Documentation](https://docs.prefect.io/)
- [Migration Guide](../../docs/scheduler/PREFECT_MIGRATION_GUIDE.md)
- [Prefect Architecture](../../docs/scheduler/prefect-architecture-explained.md)
