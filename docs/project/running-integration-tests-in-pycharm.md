# Running Integration Tests in PyCharm

This guide explains how to run and debug Phase 1 batch processing integration tests in PyCharm IDE.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Initial Setup](#initial-setup)
3. [Starting Test Infrastructure](#starting-test-infrastructure)
4. [Database Migrations](#database-migrations)
5. [Configuring PyCharm](#configuring-pycharm)
6. [Running Tests](#running-tests)
7. [Debugging Tests](#debugging-tests)
8. [Troubleshooting](#troubleshooting)
9. [Cleanup](#cleanup)

---

## Prerequisites

### Required Software

- **PyCharm Professional or Community Edition** (2021.3 or later)
- **Docker Desktop** installed and running
- **Docker Compose** (usually included with Docker Desktop)
- **Python 3.9+** with virtual environment
- **PostgreSQL client tools** (optional, for verification):
  ```bash
  brew install postgresql  # macOS
  ```

### Project Setup

Ensure you have completed the basic project setup:

```bash
# Clone repository and navigate to project
cd /path/to/autoloader

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run initial test setup (if available)
make setup-test  # or pip install -r requirements.txt
```

---

## Initial Setup

### 1. Verify Docker is Running

```bash
# Check Docker is running
docker --version
docker-compose --version

# Verify Docker daemon is active
docker ps
```

### 2. Check for Port Conflicts

**IMPORTANT:** Port 5432 must be available for PostgreSQL.

```bash
# Check if anything is using port 5432
lsof -i :5432

# If you see kubectl port-forward or other services:
kill <PID>  # Replace <PID> with the process ID
```

Common conflicts:
- **kubectl port-forward** - Kill the process
- **Local PostgreSQL** - Stop it: `brew services stop postgresql`
- **Other Docker containers** - Check with `docker ps` and stop if needed

---

## Starting Test Infrastructure

### 1. Start Docker Compose Services

From the project root:

```bash
# Start all test services (PostgreSQL, MinIO, Spark Connect)
docker-compose -f docker-compose.test.yml up -d

# Wait 10-15 seconds for services to initialize
sleep 15

# Verify all services are healthy
docker-compose -f docker-compose.test.yml ps
```

Expected output - all services should show **"Up (healthy)"**:
```
NAME                            STATUS
autoloader-test-postgres        Up (healthy)
autoloader-test-minio           Up (healthy)
autoloader-test-spark-connect   Up (healthy)
```

### 2. Verify Service Connectivity

**PostgreSQL:**
```bash
# Test from inside container
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test -c "SELECT 1"

# Test from host (should work if no port conflicts)
PGPASSWORD=test_password psql -h 127.0.0.1 -U test_user -d autoloader_test -c "SELECT 1"
```

**MinIO:**
```bash
# Check health endpoint
curl http://localhost:9000/minio/health/live

# Should return: 200 OK

# Access MinIO console (optional)
# Open http://localhost:9001 in browser
# Username: minioadmin
# Password: minioadmin
```

**Spark Connect:**
```bash
# Check Spark UI is accessible
curl -I http://localhost:4040

# Should return: HTTP/1.1 200 OK

# Access Spark UI (optional)
# Open http://localhost:4040 in browser
```

---

## Database Migrations

Phase 1 tests require the `processed_files` table. Run migrations:

### 1. Set Database URL

```bash
export DATABASE_URL="postgresql://test_user:test_password@localhost:5432/autoloader_test"
```

### 2. Run Alembic Migrations

```bash
# Apply all migrations
alembic upgrade head

# Verify migration succeeded
alembic current
```

Expected output:
```
INFO  [alembic.runtime.migration] Running upgrade  -> 67371aeb5ae0, initial migration with processed_files
67371aeb5ae0 (head)
```

### 3. Verify Table Exists

```bash
# Check table structure
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test -c "\d processed_files"
```

You should see columns like: `id`, `ingestion_id`, `file_path`, `status`, etc.

---

## Configuring PyCharm

### 1. Set Python Interpreter

1. **File → Settings** (or **PyCharm → Preferences** on macOS)
2. **Project: autoloader → Python Interpreter**
3. Click gear icon → **Add**
4. Select **Existing environment**
5. Browse to: `/path/to/autoloader/.venv/bin/python`
6. Click **OK**

### 2. Configure pytest as Default Test Runner

1. **File → Settings → Tools → Python Integrated Tools**
2. Under **Testing**, set **Default test runner** to: `pytest`
3. Click **OK**

### 3. Mark Source Root

1. Right-click project root folder in Project view
2. Select **Mark Directory as → Sources Root**

This ensures proper imports like `from app.models.domain import ...`

---

## Running Tests

### Method 1: Run Single Test (Quickest)

1. Open `tests/e2e/test_phase1_batch_processing_e2e.py`
2. Find the test method: `test_complete_batch_ingestion_happy_path`
3. Look for the green play button (▶️) in the gutter next to the test method
4. Click the play button
5. Select **"Run 'pytest in test_phase1...'**

### Method 2: Create Reusable Run Configuration

#### Step 1: Create Configuration

1. **Run → Edit Configurations**
2. Click **+** button (top-left)
3. Select **Python tests → pytest**

#### Step 2: Configure Settings

Fill in the following fields:

| Field | Value |
|-------|-------|
| **Name** | `Phase 1 E2E Tests` |
| **Target** | Select **"Script path"** |
| **Script path** | `tests/e2e/test_phase1_batch_processing_e2e.py` |
| **Python interpreter** | Your `.venv` interpreter |
| **Working directory** | Project root (e.g., `/Users/you/autoloader`) |

#### Step 3: Set Environment Variables

Click the **folder icon** next to "Environment variables" and add:

```
TEST_MINIO_ENDPOINT=http://localhost:9000
TEST_MINIO_ACCESS_KEY=minioadmin
TEST_MINIO_SECRET_KEY=minioadmin
TEST_SPARK_CONNECT_URL=sc://localhost:15002
TEST_SPARK_CONNECT_TOKEN=
TEST_DATABASE_URL=postgresql://test_user:test_password@localhost:5432/autoloader_test
TEST_TENANT_ID=test-tenant-001
TEST_CLUSTER_ID=test-cluster-001
```

**Alternative:** Use `.env.test` file (automatically loaded by pytest-dotenv)

#### Step 4: Additional Arguments

In **Additional Arguments** field:
```
-v -s --log-cli-level=INFO
```

Explanation:
- `-v` = Verbose output
- `-s` = Show print statements
- `--log-cli-level=INFO` = Show INFO level logs

#### Step 5: Save and Run

1. Click **OK**
2. Select the configuration from dropdown (top-right)
3. Click the green play button ▶️

---

## Debugging Tests

### Setting Breakpoints

1. Open any test file or source file
2. Click in the gutter (left of line numbers) to set a red breakpoint
3. Common places to set breakpoints:
   - `tests/e2e/test_phase1_batch_processing_e2e.py` - Test logic
   - `app/services/batch_orchestrator.py:71` - Main orchestration
   - `app/services/batch_file_processor.py:60` - File processing
   - `app/services/file_discovery_service.py:120` - S3 file listing
   - `app/services/file_state_service.py:45` - File locking

### Start Debugging

**Option 1: Quick Debug**
- Right-click test method → **Debug 'pytest in test_phase1...'**

**Option 2: Use Debug Button**
- Select your run configuration
- Click the debug button 🐛 (next to play button)

### Debug Controls

When breakpoint hits:
- **F8** - Step over (next line)
- **F7** - Step into (enter function)
- **Shift+F8** - Step out (exit function)
- **F9** - Resume (continue to next breakpoint)
- **Alt+F9** - Run to cursor

### Inspecting Variables

In the **Debug** panel:
- **Variables** tab - See all local/global variables
- **Watches** - Add expressions to monitor (e.g., `ingestion.id`, `len(files)`)
- **Console** - Execute Python code in current context

### Debug Console Tips

While paused at breakpoint, try:
```python
# Inspect variables
print(ingestion.id)
print(len(processed_files))

# Check database state
from app.models.domain import ProcessedFile
db_files = test_db_postgres.query(ProcessedFile).all()
print(f"Files in DB: {len(db_files)}")

# Test expressions
file_record.status == "SUCCESS"
```

---

## Test Execution Flow

### What Happens When You Run the Test

**Phase 1: Setup (Fixtures)**
1. `test_config` - Loads test configuration
2. `test_db_postgres` - Creates PostgreSQL session
3. `s3_client` - Creates boto3 client for MinIO
4. `test_bucket` - Creates bucket `test-ingestion-bucket`
5. `test_data_s3` - Uploads 3 JSON files (3000 total records)

**Phase 2: Test Execution**
1. Creates `Ingestion` record in PostgreSQL
2. Calls `BatchOrchestrator.run_ingestion()`
   - FileDiscoveryService lists 3 files from MinIO
   - FileStateService checks processed_files (empty first time)
   - BatchFileProcessor processes each file:
     - Locks file in database
     - Reads with Spark
     - Writes to Iceberg table
     - Marks SUCCESS
   - Updates Run record with metrics

**Phase 3: Verification**
1. Checks Run status = SUCCESS
2. Checks 3 ProcessedFile records exist
3. Queries Iceberg table (verifies 3000 records)
4. Re-runs to test idempotency (0 files processed)

**Phase 4: Cleanup**
1. Deletes test data from PostgreSQL
2. Deletes MinIO bucket and files
3. Closes database session

### Expected Console Output

```
tests/e2e/test_phase1_batch_processing_e2e.py::TestPhase1BatchProcessing::test_complete_batch_ingestion_happy_path

✓ Created ingestion: ing_a1b2c3d4
✓ Starting batch ingestion run...
INFO  [FileDiscoveryService] Listing files from S3: bucket=test-ingestion-bucket, prefix=data/
INFO  [FileDiscoveryService] Discovered 3 files
INFO  [BatchOrchestrator] Processing 3 new files
INFO  [BatchFileProcessor] Processing file: s3a://test-ingestion-bucket/data/batch_0.json
INFO  [BatchFileProcessor] Successfully processed: 1000 records
INFO  [BatchFileProcessor] Processing file: s3a://test-ingestion-bucket/data/batch_1.json
INFO  [BatchFileProcessor] Successfully processed: 1000 records
INFO  [BatchFileProcessor] Processing file: s3a://test-ingestion-bucket/data/batch_2.json
INFO  [BatchFileProcessor] Successfully processed: 1000 records
✓ Run completed: run_e5f6g7h8, status=SUCCESS
✓ Run record verified: 3 files, 15s
✓ ProcessedFile records verified: 3 files, 3000 total records
✓ Iceberg table verified: 3000 records
✓ Testing idempotency - running again...
✓ Idempotency verified: Second run skipped already-processed files

✅ Complete happy path test PASSED

PASSED                                                      [100%]
```

---

## Troubleshooting

### Common Issues

#### 1. Import Errors

**Error:** `ModuleNotFoundError: No module named 'app'`

**Solution:**
- Check Python interpreter is set to `.venv`
- Mark project root as Sources Root
- Verify working directory is project root in run configuration

#### 2. Port Conflicts (PostgreSQL)

**Error:** `password authentication failed for user "test_user"`

**Check for conflicts:**
```bash
lsof -i :5432
```

**Solution:**
```bash
# Kill conflicting process (e.g., kubectl port-forward)
kill <PID>

# Or stop local PostgreSQL
brew services stop postgresql  # macOS
sudo systemctl stop postgresql # Linux
```

#### 3. Docker Services Not Running

**Error:** `Connection refused` to MinIO, PostgreSQL, or Spark

**Solution:**
```bash
# Check services
docker-compose -f docker-compose.test.yml ps

# Restart services
docker-compose -f docker-compose.test.yml restart

# Check logs
docker logs autoloader-test-postgres
docker logs autoloader-test-minio
docker logs autoloader-test-spark-connect
```

#### 4. Migrations Not Applied

**Error:** `relation "processed_files" does not exist`

**Solution:**
```bash
# Set database URL
export DATABASE_URL="postgresql://test_user:test_password@localhost:5432/autoloader_test"

# Run migrations
alembic upgrade head

# Verify
alembic current
```

#### 5. Spark Connect Not Ready

**Error:** `Failed to connect to Spark Connect`

**Solution:**
```bash
# Spark takes 60-90 seconds to start
# Check Spark UI is up
curl http://localhost:4040

# Check Spark logs
docker logs autoloader-test-spark-connect

# Wait and retry
sleep 30
```

#### 6. Test Timeout

**Error:** Test hangs or times out

**Causes:**
- Spark job stuck (check Spark UI: http://localhost:4040)
- Database lock (check PostgreSQL connections)
- MinIO not responding

**Solution:**
```bash
# Check Spark jobs
open http://localhost:4040

# Check database connections
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test -c "SELECT * FROM pg_stat_activity;"

# Restart services
docker-compose -f docker-compose.test.yml restart
```

#### 7. Old Test Data

**Error:** Tests fail with "file already processed"

**Solution:**
```bash
# Clean test data
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test << EOF
DELETE FROM processed_files;
DELETE FROM runs;
DELETE FROM ingestions;
EOF
```

---

## Cleanup

### Between Test Runs

If you want to run tests multiple times:

```bash
# Option 1: Let tests clean up automatically (default)
# Tests have cleanup in fixtures - just re-run

# Option 2: Manual cleanup (if tests crashed)
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test -c "DELETE FROM processed_files; DELETE FROM runs; DELETE FROM ingestions;"
```

### Stop Services (Keep Data)

```bash
# Stop containers but keep volumes
docker-compose -f docker-compose.test.yml stop

# Restart later
docker-compose -f docker-compose.test.yml start
```

### Complete Cleanup (Remove Everything)

```bash
# Stop and remove containers + volumes
docker-compose -f docker-compose.test.yml down -v

# This deletes:
# - All containers
# - All test data
# - All volumes (PostgreSQL data, MinIO buckets, Spark checkpoints)
```

### Verify Cleanup

```bash
# Should return empty
docker ps | grep autoloader-test

# Should return empty
docker volume ls | grep autoloader
```

---

## Quick Reference

### Start Testing (Full Flow)

```bash
# 1. Start services
docker-compose -f docker-compose.test.yml up -d

# 2. Apply migrations
export DATABASE_URL="postgresql://test_user:test_password@localhost:5432/autoloader_test"
alembic upgrade head

# 3. Run in PyCharm
# Right-click test → Debug 'pytest in test_phase1...'
```

### Verify Infrastructure

```bash
# PostgreSQL
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test -c "SELECT 1"

# MinIO
curl http://localhost:9000/minio/health/live

# Spark
curl -I http://localhost:4040

# All services
docker-compose -f docker-compose.test.yml ps
```

### Check Test Results

```bash
# View processed files
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test -c "SELECT file_path, status, records_ingested FROM processed_files;"

# View runs
docker exec autoloader-test-postgres psql -U test_user -d autoloader_test -c "SELECT id, status, files_processed, duration_seconds FROM runs;"

# MinIO buckets
docker exec autoloader-test-minio mc ls minio/
```

---

## Tips for Effective Debugging

### 1. Use Conditional Breakpoints

Right-click breakpoint → **Edit Breakpoint** → Add condition:
```python
file_path.endswith('batch_1.json')  # Only break for specific file
status == "FAILED"                   # Only break on failures
```

### 2. Evaluate Expressions

While paused, select any expression → Right-click → **Evaluate Expression**

Useful expressions:
```python
len(processed_files)
[f.file_path for f in processed_files]
run.status
ingestion.__dict__
```

### 3. Log Output

The `-s` flag shows all print statements. Add temporary debug prints:
```python
print(f"DEBUG: Processing file {file_path}")
print(f"DEBUG: Database has {len(processed_files)} files")
```

### 4. Database Inspection

Add breakpoints after database operations and inspect:
```python
# In debug console
test_db_postgres.query(ProcessedFile).all()
test_db_postgres.query(Run).filter(Run.id == run.id).first()
```

---

## Additional Resources

- **Project README**: `../README.md`
- **Test README**: `../tests/README.md`
- **Phase 1 Implementation Guide**: `./batch-processing/phase1-s3-implementation-guide.md`
- **Phase 1 Summary**: `./batch-processing/phase1-implementation-summary.md`

## Support

If you encounter issues:

1. Check docker-compose logs: `docker-compose -f docker-compose.test.yml logs`
2. Verify all prerequisites are met
3. Try complete cleanup and restart: `docker-compose -f docker-compose.test.yml down -v && docker-compose -f docker-compose.test.yml up -d`
4. Open an issue or contact the development team

---

**Happy Testing!** 🎉
