# Testing Quick Start Guide

Get up and running with tests in 5 minutes!

## Prerequisites

- Python 3.9+
- Docker (for MinIO and PostgreSQL)
- Make (optional, for convenient commands)

## Quick Setup (3 steps)

### 1. Install Dependencies

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

### 2. Start Test Infrastructure

```bash
# Using Make (recommended)
make setup-test

# OR using docker-compose directly
docker-compose -f docker-compose.test.yml up -d
```

This starts:
- **MinIO** on port 9000 (S3-compatible storage)
- **MinIO Console** on port 9001 (http://localhost:9001)
- **PostgreSQL** on port 5432 (optional, SQLite used by default)

### 3. Set Environment Variables

```bash
# Copy example env file
cp .env.test.example .env.test

# Load environment variables
export $(cat .env.test | xargs)

# On Windows (PowerShell):
# Get-Content .env.test | ForEach-Object { $var = $_.Split('='); [Environment]::SetEnvironmentVariable($var[0], $var[1]) }
```

## Run Tests

### Option 1: Using Make (easiest)

```bash
# Run all tests (excluding slow tests)
make test

# Run only E2E tests
make test-e2e

# Run with coverage report
make test-cov
```

### Option 2: Using pytest directly

```bash
# Run all tests
pytest

# Run only E2E tests (without Spark)
pytest -m "e2e and not requires_spark"

# Run specific test file
pytest tests/e2e/test_basic_s3_json_ingestion.py

# Run with verbose output
pytest -v -s
```

## What Tests Are Available?

### E2E Test: Basic S3 JSON Ingestion

**Test ID:** E2E-01
**File:** `tests/e2e/test_basic_s3_json_ingestion.py`

**Tests the following scenarios:**
1. ✅ Create ingestion configuration
2. ✅ Activate ingestion
3. ✅ Trigger manual run
4. ✅ Complete ingestion flow (with mocked Spark)
5. ✅ Complete ingestion flow (with real Spark - optional)
6. ✅ List ingestions
7. ✅ Get ingestion details
8. ✅ Delete ingestion
9. ✅ Pause and resume ingestion
10. ✅ Run history pagination

### Run Individual Tests

```bash
# Test 1: Create ingestion
pytest tests/e2e/test_basic_s3_json_ingestion.py::TestBasicS3JsonIngestion::test_create_ingestion -v

# Test 4: Complete flow with mock
pytest tests/e2e/test_basic_s3_json_ingestion.py::TestBasicS3JsonIngestion::test_complete_ingestion_flow_with_mock -v

# Test 5: Complete flow with real Spark (requires Spark Connect)
pytest tests/e2e/test_basic_s3_json_ingestion.py::TestBasicS3JsonIngestion::test_complete_ingestion_flow_with_real_spark -v
```

## Understanding Test Output

### Successful Test Run
```
tests/e2e/test_basic_s3_json_ingestion.py::TestBasicS3JsonIngestion::test_create_ingestion PASSED [100%]

===================== 1 passed in 0.45s =====================
```

### Test with Fixtures
```
2025-11-02 10:00:00 [INFO] Creating test database...
2025-11-02 10:00:01 [INFO] Setting up S3 client...
2025-11-02 10:00:02 [INFO] Uploading test data...
tests/e2e/test_basic_s3_json_ingestion.py::TestBasicS3JsonIngestion::test_complete_ingestion_flow_with_mock PASSED
2025-11-02 10:00:15 [INFO] Cleaning up test resources...
```

### Coverage Report
```
---------- coverage: platform darwin, python 3.9.18 -----------
Name                                 Stmts   Miss  Cover   Missing
------------------------------------------------------------------
app/api/v1/ingestions.py               145     12    92%   156-163
app/services/ingestion_service.py       89     25    72%   45-52, 78-85
------------------------------------------------------------------
TOTAL                                  1234    123    90%

Coverage HTML report: htmlcov/index.html
```

## Test Markers (Selective Execution)

Tests are marked for selective execution:

```bash
# Unit tests only (fast)
pytest -m unit

# Integration tests only
pytest -m integration

# E2E tests only
pytest -m e2e

# Exclude slow tests
pytest -m "not slow"

# Exclude tests requiring Spark
pytest -m "not requires_spark"

# Exclude tests requiring MinIO
pytest -m "not requires_minio"
```

## Common Issues & Solutions

### Issue 1: "Connection refused" to MinIO

**Solution:**
```bash
# Check MinIO is running
docker ps | grep minio

# Restart MinIO if needed
make teardown-test
make setup-test

# Or manually
docker-compose -f docker-compose.test.yml restart minio
```

### Issue 2: "ModuleNotFoundError"

**Solution:**
```bash
# Ensure virtual environment is activated
source .venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Issue 3: Tests are slow

**Solution:**
```bash
# Skip slow tests
pytest -m "not slow"

# Run only fast unit tests
make test-fast

# Use pytest-xdist for parallel execution
pip install pytest-xdist
pytest -n auto
```

### Issue 4: Spark Connect tests fail

**Solution:**
```bash
# Skip Spark-dependent tests (most common during development)
pytest -m "not requires_spark"

# Or set TEST_SPARK_CONNECT_URL if you have Spark available
export TEST_SPARK_CONNECT_URL=sc://your-spark-host:15002
```

## Next Steps

1. **Run your first test:**
   ```bash
   make setup-test
   make test
   ```

2. **View coverage report:**
   ```bash
   make test-cov
   open htmlcov/index.html  # macOS
   # Or navigate to htmlcov/index.html in your browser
   ```

3. **Read detailed documentation:**
   - [tests/README.md](tests/README.md) - Comprehensive test documentation
   - [docs/test-cases/e2e-01-basic-s3-json-ingestion.md](docs/test-cases/e2e-01-basic-s3-json-ingestion.md) - Test case details

4. **Write your own tests:**
   - Use existing fixtures from `tests/conftest.py`
   - Follow examples in `tests/e2e/test_basic_s3_json_ingestion.py`
   - Add new fixtures for your specific needs

## Cleanup

When you're done testing:

```bash
# Stop test infrastructure
make teardown-test

# Clean up generated files
make clean

# Deactivate virtual environment
deactivate
```

## CI/CD Integration

Tests are designed to run in CI/CD pipelines:

```bash
# CI/CD friendly command (no Spark, no slow tests)
pytest -m "not requires_spark and not slow" --cov=app --cov-report=xml
```

See `tests/README.md` for GitHub Actions integration example.

## Getting Help

- **Documentation:** See [tests/README.md](tests/README.md)
- **Test Cases:** See [docs/test-cases/](docs/test-cases/)
- **Issues:** Open an issue in the repository
- **Makefile help:** Run `make help`

Happy Testing! 🚀
