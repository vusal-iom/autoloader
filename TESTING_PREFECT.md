# Testing Prefect Integration Locally

Quick guide to test the Prefect integration on your local machine.

## Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# Apply database migrations
alembic upgrade head
```

## Step 1: Start Prefect Server

```bash
# Terminal 1
prefect server start
```

**Prefect UI:** http://localhost:4200

## Step 2: Configure Environment

```bash
export PREFECT_API_URL="http://localhost:4200/api"
export DATABASE_URL="sqlite:///./autoloader.db"
```

## Step 3: Start FastAPI Application

```bash
# Terminal 2
uvicorn app.main:app --reload
```

**Expected startup logs:**
```
INFO:     🚀 Starting IOMETE Autoloader
INFO:     ✅ Database initialized
INFO:     ✅ Prefect service initialized and connected
INFO:     Uvicorn running on http://127.0.0.1:8000
```

## Step 4: Test Health Check

```bash
curl http://localhost:8000/health
```

**Expected response:**
```json
{
  "status": "healthy",
  "database": "connected",
  "prefect": "connected"
}
```

## Step 5: Create Scheduled Ingestion

```bash
curl -X POST http://localhost:8000/api/v1/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Hourly S3 Ingestion",
    "cluster_id": "cluster_123",
    "source": {
      "type": "s3",
      "path": "s3://my-bucket/data/",
      "file_pattern": "*.json",
      "credentials": {
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "aws_region": "us-east-1"
      }
    },
    "format": {
      "type": "json",
      "schema": {
        "inference": "auto",
        "evolution_enabled": true
      }
    },
    "destination": {
      "catalog": "spark_catalog",
      "database": "default",
      "table": "test_table",
      "write_mode": "append"
    },
    "schedule": {
      "frequency": "hourly",
      "timezone": "UTC"
    }
  }'
```

**What happens:**
1. Ingestion created in database
2. Prefect deployment created automatically
3. Cron schedule: `0 * * * *` (every hour at minute 0)

## Step 6: Verify in Prefect UI

1. Open http://localhost:4200
2. Navigate to **Deployments**
3. You should see: `ingestion-{ingestion_id}`
4. Click on it to see schedule details

## Step 7: Manually Trigger a Run

```bash
# Get ingestion_id from previous response
curl -X POST http://localhost:8000/api/v1/ingestions/{ingestion_id}/run
```

**Expected response:**
```json
{
  "status": "triggered",
  "method": "prefect",
  "flow_run_id": "uuid-here",
  "message": "Ingestion run triggered via Prefect"
}
```

## Step 8: Monitor Flow Run

1. Open http://localhost:4200
2. Navigate to **Flow Runs**
3. Find the flow run with your `flow_run_id`
4. Click on it to see:
   - Task execution timeline
   - Detailed logs
   - State transitions
   - Output/metrics

## Test Scenarios

### Scenario 1: Daily Schedule at Specific Time

```json
"schedule": {
  "frequency": "daily",
  "time": "14:30",
  "timezone": "America/New_York"
}
```

**Result:** Cron `30 14 * * *` in America/New_York timezone

### Scenario 2: Custom Cron Expression

```json
"schedule": {
  "frequency": "custom",
  "cron_expression": "0 */6 * * *",
  "timezone": "UTC"
}
```

**Result:** Runs every 6 hours

### Scenario 3: Update Schedule

```bash
curl -X PUT http://localhost:8000/api/v1/ingestions/{ingestion_id} \
  -H "Content-Type: application/json" \
  -d '{
    "schedule": {
      "frequency": "daily",
      "time": "09:00",
      "timezone": "UTC"
    }
  }'
```

**Result:** Prefect deployment schedule updated automatically

### Scenario 4: Pause/Resume

```bash
# Pause
curl -X POST http://localhost:8000/api/v1/ingestions/{ingestion_id}/pause

# Resume
curl -X POST http://localhost:8000/api/v1/ingestions/{ingestion_id}/resume
```

**Result:** Prefect deployment paused/resumed (no new scheduled runs)

### Scenario 5: Delete Ingestion

```bash
curl -X DELETE http://localhost:8000/api/v1/ingestions/{ingestion_id}
```

**Result:** Prefect deployment deleted + database record removed

## Test Graceful Degradation

**Stop Prefect server:**
```bash
# Kill prefect server process (Ctrl+C in Terminal 1)
```

**Restart FastAPI:**
```bash
# It will start but with warning:
# WARNING: Prefect features will be unavailable
```

**Create ingestion:**
```bash
# Still works! But logs warning:
# "Ingestion created but scheduling unavailable"
```

**Trigger manual run:**
```bash
curl -X POST http://localhost:8000/api/v1/ingestions/{id}/run
```

**Response:**
```json
{
  "status": "completed",
  "method": "direct",
  "run_id": "uuid-here",
  "message": "Ingestion run completed via direct execution"
}
```

**Result:** Falls back to BatchOrchestrator (no Prefect required)

## Troubleshooting

### Problem: "Prefect service not initialized"

**Check Prefect server:**
```bash
curl http://localhost:4200/api/health
```

If fails, restart Prefect server.

### Problem: "Deployment not found"

**Verify deployment exists:**
```bash
prefect deployment ls
```

If missing, update ingestion to recreate deployment.

### Problem: Flow run fails

**Check logs in Prefect UI:**
1. Navigate to failed flow run
2. Click on failed tasks
3. View error messages and stack traces

**Common causes:**
- Spark Connect unreachable
- Invalid S3 credentials
- Database connection issues

## Cleanup

```bash
# Delete test ingestions
curl -X DELETE http://localhost:8000/api/v1/ingestions/{id}

# Stop Prefect server
# Ctrl+C in Terminal 1

# Stop FastAPI
# Ctrl+C in Terminal 2

# Clean Prefect database (optional)
prefect database reset
```

## Next: Production Deployment

See `docs/prefect-migration/PREFECT_MIGRATION_GUIDE.md` Section 9 for Kubernetes deployment.
