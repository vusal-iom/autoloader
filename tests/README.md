# IOMETE Autoloader Tests

This directory contains the test suite for IOMETE Autoloader.

## Test Structure

```
tests/
├── conftest.py                          # Shared fixtures (database, API client)
├── e2e/                                 # End-to-end integration tests
│   ├── conftest.py                      # E2E fixtures (MinIO, Spark)
│   └── test_basic_s3_json_ingestion.py  # Happy path test
└── README.md                            # This file
```

## Test Categories

### E2E (End-to-End) Tests

Tests that validate the complete workflow using **real services**:
- ✅ MinIO (S3-compatible storage)
- ✅ PostgreSQL (database)
- ✅ Spark Connect (data processing)

**Location:** `tests/e2e/`

**Markers:**
- `@pytest.mark.e2e` - All E2E tests
- `@pytest.mark.requires_spark` - Requires Spark Connect
- `@pytest.mark.requires_minio` - Requires MinIO

## Running Tests

### Prerequisites

1. **Start test services:**
   ```bash
   docker-compose -f docker-compose.test.yml up -d
   ```

2. **Wait for services to be ready** (~1-2 minutes for Spark):
   ```bash
   # Check service health
   docker-compose -f docker-compose.test.yml ps

   # Wait for Spark UI to be accessible
   curl http://localhost:4040

   # Wait for MinIO to be accessible
   curl http://localhost:9000/minio/health/live
   ```

3. **Install test dependencies:**
   ```bash
   pip install -e .
   pip install pytest pytest-asyncio httpx
   ```

### Run All E2E Tests

```bash
pytest tests/e2e/ -v
```

### Run Specific Test

```bash
pytest tests/e2e/test_basic_s3_json_ingestion.py::TestBasicS3JsonIngestion::test_happy_path_s3_json_ingestion -v
```

### Run with Markers

```bash
# Run only E2E tests
pytest -m e2e -v

# Run only tests that require Spark
pytest -m requires_spark -v

# Skip E2E tests (run unit tests only)
pytest -m "not e2e" -v
```

### Run with Coverage

```bash
pytest tests/e2e/ --cov=app --cov-report=html -v
```

### Run with Debug Output

```bash
pytest tests/e2e/ -v -s --log-cli-level=DEBUG
```

## Test Environment

Tests use configuration from `.env.test`:

```bash
# MinIO Configuration
TEST_MINIO_ENDPOINT=http://localhost:9000
TEST_MINIO_ACCESS_KEY=minioadmin
TEST_MINIO_SECRET_KEY=minioadmin

# Spark Connect Configuration
TEST_SPARK_CONNECT_URL=sc://localhost:15002

# Database Configuration (optional)
# TEST_DATABASE_URL=postgresql://test_user:test_password@localhost:5432/autoloader_test

# Test Configuration
TEST_TENANT_ID=test-tenant-001
TEST_CLUSTER_ID=test-cluster-001
TEST_DATA_NUM_FILES=3
TEST_DATA_RECORDS_PER_FILE=1000
```

## Fixtures

### Shared Fixtures (tests/conftest.py)

- **`test_db`** - Fresh database session for each test
- **`api_client`** - FastAPI TestClient with DB override
- **`ensure_services_ready`** - Health checks for all services

### E2E Fixtures (tests/e2e/conftest.py)

- **`minio_client`** - Boto3 S3 client for MinIO
- **`test_bucket`** - Creates/cleans up test bucket per test
- **`sample_json_files`** - Uploads 3 JSON files with 3000 records
- **`spark_session`** - Spark Connect session for verification
- **`test_tenant_id`** - Test tenant ID
- **`test_cluster_id`** - Test cluster ID

## E2E Test: Basic S3 JSON Ingestion

**Test:** `test_basic_s3_json_ingestion.py`

**What it tests:**
1. ✅ Create ingestion configuration via API
2. ✅ Trigger manual run
3. ✅ Poll for completion (max 3 minutes)
4. ✅ Verify run metrics (3 files, 3000 records)
5. ✅ Query Iceberg table via Spark to verify data
6. ✅ Verify run history shows completed run

**Expected duration:** 2-5 minutes

**Requirements:**
- MinIO running on localhost:9000
- Spark Connect running on localhost:15002
- PostgreSQL running on localhost:5432

## Troubleshooting

### Services not ready

**Error:**
```
RuntimeError: MinIO not ready. Please start services
```

**Solution:**
```bash
docker-compose -f docker-compose.test.yml up -d
docker-compose -f docker-compose.test.yml ps
```

### Spark takes too long to start

**Error:**
```
RuntimeError: Spark Connect not ready
```

**Solution:**
Spark can take 1-2 minutes to start. Check logs:
```bash
docker-compose -f docker-compose.test.yml logs spark-connect
```

Wait for this message:
```
SparkConnectServer: Started SparkConnectServer on port 15002
```

### Database connection failed

**Error:**
```
sqlalchemy.exc.OperationalError: could not connect to server
```

**Solution:**
```bash
# Check PostgreSQL is running
docker-compose -f docker-compose.test.yml ps postgres

# Check logs
docker-compose -f docker-compose.test.yml logs postgres
```

### Test timeout

**Error:**
```
pytest.fail: Run did not complete within 180s
```

**Solution:**
- Check Spark logs for errors
- Increase timeout in test if needed
- Verify MinIO has test files

## CI/CD Integration

### GitHub Actions Example

```yaml
name: E2E Tests

on: [push, pull_request]

jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Start services
        run: docker-compose -f docker-compose.test.yml up -d

      - name: Wait for services
        run: |
          timeout 120 bash -c 'until curl -f http://localhost:4040; do sleep 2; done'

      - name: Run E2E tests
        run: pytest tests/e2e/ -v --junitxml=junit.xml

      - name: Cleanup
        if: always()
        run: docker-compose -f docker-compose.test.yml down -v
```

## Writing New Tests

### 1. Create test file in tests/e2e/

```python
import pytest

@pytest.mark.e2e
@pytest.mark.requires_spark
class TestMyFeature:
    def test_something(self, api_client, test_bucket):
        # Use API only, no direct DB access
        response = api_client.get("/api/v1/...")
        assert response.status_code == 200
```

### 2. Use existing fixtures

```python
def test_with_fixtures(
    api_client,        # FastAPI test client
    minio_client,      # Boto3 S3 client
    test_bucket,       # Auto-created bucket
    sample_json_files, # Pre-uploaded test files
    spark_session      # Spark Connect session
):
    # Your test here
    pass
```

### 3. Follow CLAUDE.md guidelines

From `CLAUDE.md`:
- ✅ **API-only interactions** - No direct database manipulation
- ✅ **Real systems** - Use MinIO, PostgreSQL, Spark from Docker
- ✅ **Single test focus** - One test case at a time
- ✅ **Test data in MinIO** - Acceptable to pre-create files
- ✅ **Outcome validation via API** - All verifications through endpoints

## Test Data

### Sample JSON Record

```json
{
  "id": 0,
  "timestamp": "2025-01-05T10:30:00.000000Z",
  "user_id": "user-0",
  "event_type": "login",
  "value": 0.0
}
```

### Test Files

- **Num files:** 3 (configurable via `TEST_DATA_NUM_FILES`)
- **Records per file:** 1000 (configurable via `TEST_DATA_RECORDS_PER_FILE`)
- **Total records:** 3000
- **Format:** Newline-delimited JSON

## Next Steps

1. **Add more E2E tests:**
   - Incremental load (only new files)
   - Schema evolution detection
   - Failed file handling
   - CSV format ingestion
   - Scheduled ingestion

2. **Add unit tests:**
   - Service layer tests
   - Repository tests
   - Model tests

3. **Add integration tests:**
   - API endpoint tests (mocked Spark)
   - Database integration tests
