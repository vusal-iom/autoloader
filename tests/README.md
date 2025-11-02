# IOMETE Autoloader Tests

Comprehensive test suite for the IOMETE Autoloader project, including unit tests, integration tests, and end-to-end tests.

## Table of Contents

- [Test Structure](#test-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running Tests](#running-tests)
- [Test Categories](#test-categories)
- [Configuration](#configuration)
- [Writing Tests](#writing-tests)
- [CI/CD Integration](#cicd-integration)
- [Troubleshooting](#troubleshooting)

## Test Structure

```
tests/
├── __init__.py                           # Test package init
├── README.md                             # This file
├── conftest.py                           # Shared pytest fixtures
├── fixtures/                             # Test utilities and helpers
│   ├── __init__.py
│   └── data_generator.py                 # Test data generation utilities
├── e2e/                                  # End-to-end tests
│   ├── __init__.py
│   └── test_basic_s3_json_ingestion.py   # E2E-01: Basic S3 JSON ingestion
├── integration/                          # Integration tests (TODO)
│   └── __init__.py
└── unit/                                 # Unit tests (TODO)
    └── __init__.py
```

## Prerequisites

### Required

- **Python 3.9+**
- **pip** or **poetry** for dependency management
- **Docker** (for running MinIO and PostgreSQL containers)

### Optional (for full E2E tests)

- **Apache Spark 3.5.0+** with Spark Connect enabled
- **MinIO** or **AWS S3** access for storage testing
- **PostgreSQL** (or use SQLite for local testing)

## Setup

### 1. Install Dependencies

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

### 2. Start Test Infrastructure

#### Option A: Using Docker Compose (Recommended)

Create a `docker-compose.test.yml`:

```yaml
version: '3.8'

services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 3s
      retries: 3

  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: autoloader_test
      POSTGRES_USER: test_user
      POSTGRES_PASSWORD: test_password
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U test_user"]
      interval: 5s
      timeout: 3s
      retries: 3
```

Start services:

```bash
docker-compose -f docker-compose.test.yml up -d
```

#### Option B: Manual Setup

**Start MinIO:**
```bash
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

### 3. Configure Environment Variables

Create a `.env.test` file:

```bash
# Test MinIO Configuration
TEST_MINIO_ENDPOINT=http://localhost:9000
TEST_MINIO_ACCESS_KEY=minioadmin
TEST_MINIO_SECRET_KEY=minioadmin

# Spark Connect (optional - for full E2E tests)
TEST_SPARK_CONNECT_URL=sc://localhost:15002
TEST_SPARK_CONNECT_TOKEN=your-token-here

# Database (optional - SQLite used by default)
TEST_DATABASE_URL=sqlite:///./test.db
```

Load environment:
```bash
export $(cat .env.test | xargs)
```

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test Categories

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only E2E tests
pytest -m e2e

# Run tests NOT marked as slow
pytest -m "not slow"

# Run tests that don't require Spark
pytest -m "not requires_spark"
```

### Run Specific Test File

```bash
pytest tests/e2e/test_basic_s3_json_ingestion.py
```

### Run Specific Test Function

```bash
pytest tests/e2e/test_basic_s3_json_ingestion.py::TestBasicS3JsonIngestion::test_create_ingestion
```

### Run with Coverage

```bash
pytest --cov=app --cov-report=html
```

View coverage report:
```bash
open htmlcov/index.html  # On macOS
```

### Run with Verbose Output

```bash
pytest -v -s
```

### Run in Parallel (faster)

```bash
pip install pytest-xdist
pytest -n auto
```

## Test Categories

### Unit Tests (`@pytest.mark.unit`)

Test individual functions and classes in isolation.

**Location:** `tests/unit/`

**Characteristics:**
- Fast execution (< 1 second per test)
- No external dependencies
- Use mocks and stubs
- High code coverage

**Example:**
```bash
pytest -m unit
```

### Integration Tests (`@pytest.mark.integration`)

Test integration between components (API, database, services).

**Location:** `tests/integration/`

**Characteristics:**
- Moderate execution time (1-5 seconds per test)
- Use real database (test instance)
- Mock external services (Spark, S3)
- Test API endpoints with database

**Example:**
```bash
pytest -m integration
```

### End-to-End Tests (`@pytest.mark.e2e`)

Test complete workflows from API to data storage.

**Location:** `tests/e2e/`

**Characteristics:**
- Slow execution (10-120 seconds per test)
- Use real infrastructure (MinIO, optionally Spark)
- Test full user workflows
- Validate data integrity

**Example:**
```bash
pytest -m e2e
```

## Configuration

### pytest.ini

Configuration file for pytest settings, markers, and options.

**Key settings:**
- Test discovery patterns
- Coverage options
- Custom markers
- Timeout settings
- Log configuration

### conftest.py

Shared fixtures available to all tests.

**Available Fixtures:**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `test_config` | session | Global test configuration |
| `test_db` | function | Fresh test database for each test |
| `api_client` | function | FastAPI test client |
| `s3_client` | function | Boto3 S3 client (MinIO) |
| `test_bucket` | function | S3 bucket with cleanup |
| `test_data_s3` | function | Pre-populated test data in S3 |
| `sample_ingestion_config` | function | Sample ingestion configuration |
| `mock_spark_executor` | function | Mocked Spark executor |
| `spark_connect_available` | session | Check if Spark Connect is available |

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TEST_MINIO_ENDPOINT` | Yes | `http://localhost:9000` | MinIO endpoint URL |
| `TEST_MINIO_ACCESS_KEY` | Yes | `minioadmin` | MinIO access key |
| `TEST_MINIO_SECRET_KEY` | Yes | `minioadmin` | MinIO secret key |
| `TEST_SPARK_CONNECT_URL` | No | None | Spark Connect URL for real tests |
| `TEST_SPARK_CONNECT_TOKEN` | No | None | Spark Connect authentication token |
| `TEST_DATABASE_URL` | No | SQLite | Database URL for testing |

## Writing Tests

### Test Naming Convention

- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

### Example Unit Test

```python
import pytest
from app.services.cost_estimator import CostEstimator

@pytest.mark.unit
def test_estimate_compute_cost():
    estimator = CostEstimator()
    cost = estimator.estimate_compute_cost(
        duration_hours=1.0,
        cluster_dbus=4,
        runs_per_month=30
    )
    assert cost > 0
    assert cost == 1.0 * 4 * 0.40 * 30  # Expected calculation
```

### Example Integration Test

```python
import pytest
from fastapi.testclient import TestClient

@pytest.mark.integration
def test_create_ingestion_api(api_client: TestClient, sample_ingestion_config):
    response = api_client.post(
        "/api/v1/ingestions",
        json=sample_ingestion_config
    )
    assert response.status_code == 201
    assert "id" in response.json()
```

### Example E2E Test

```python
import pytest
from fastapi.testclient import TestClient

@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.requires_minio
def test_complete_ingestion_flow(
    api_client: TestClient,
    sample_ingestion_config,
    test_data_s3
):
    # Create ingestion
    response = api_client.post("/api/v1/ingestions", json=sample_ingestion_config)
    ingestion_id = response.json()["id"]

    # Trigger run
    run_response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
    run_id = run_response.json()["run_id"]

    # Poll for completion
    # ... (see full test for details)

    # Verify results
    assert run_data["status"] == "COMPLETED"
    assert run_data["records_ingested"] == 3000
```

### Using Fixtures

```python
@pytest.fixture
def my_custom_fixture(test_db):
    """Custom fixture that depends on test_db"""
    # Setup
    data = create_test_data(test_db)
    yield data
    # Teardown
    cleanup_test_data(test_db, data)

def test_with_custom_fixture(my_custom_fixture):
    # Use the fixture
    assert my_custom_fixture is not None
```

## CI/CD Integration

### GitHub Actions Example

Create `.github/workflows/test.yml`:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      minio:
        image: minio/minio
        ports:
          - 9000:9000
        env:
          MINIO_ROOT_USER: minioadmin
          MINIO_ROOT_PASSWORD: minioadmin
        options: >-
          --health-cmd "curl -f http://localhost:9000/minio/health/live"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 3

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt

      - name: Run tests
        env:
          TEST_MINIO_ENDPOINT: http://localhost:9000
          TEST_MINIO_ACCESS_KEY: minioadmin
          TEST_MINIO_SECRET_KEY: minioadmin
        run: |
          pytest -m "not requires_spark" --cov=app --cov-report=xml

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
```

## Troubleshooting

### MinIO Connection Issues

**Problem:** Tests fail with "Connection refused" to MinIO

**Solutions:**
1. Ensure MinIO is running: `docker ps | grep minio`
2. Check MinIO is accessible: `curl http://localhost:9000/minio/health/live`
3. Verify environment variables are set correctly
4. Try restarting MinIO: `docker restart <minio-container-id>`

### Database Lock Issues

**Problem:** "database is locked" errors with SQLite

**Solutions:**
1. Tests are not cleaning up properly - check for unclosed sessions
2. Use function-scoped `test_db` fixture to get fresh database per test
3. Consider using PostgreSQL for parallel test execution

### Spark Connect Issues

**Problem:** Tests requiring Spark Connect fail or timeout

**Solutions:**
1. Check if `TEST_SPARK_CONNECT_URL` is set and accessible
2. Skip Spark tests: `pytest -m "not requires_spark"`
3. Verify Spark cluster is running with Spark Connect enabled
4. Check Spark Connect port (default 15002) is accessible

### Slow Test Execution

**Problem:** Tests take too long to run

**Solutions:**
1. Run only fast tests: `pytest -m "not slow"`
2. Use pytest-xdist for parallel execution: `pytest -n auto`
3. Skip E2E tests during development: `pytest -m "not e2e"`
4. Use mocked fixtures instead of real services

### Import Errors

**Problem:** `ModuleNotFoundError` when running tests

**Solutions:**
1. Ensure virtual environment is activated
2. Install dependencies: `pip install -r requirements.txt`
3. Check PYTHONPATH includes project root
4. Run tests from project root directory

## Test Coverage Goals

- **Unit Tests:** > 80% code coverage
- **Integration Tests:** Cover all API endpoints
- **E2E Tests:** Cover critical user workflows

Current coverage can be viewed with:
```bash
pytest --cov=app --cov-report=term-missing
```

## Related Documentation

- [Test Case Documentation](../docs/test-cases/e2e-01-basic-s3-json-ingestion.md)
- [Project README](../README.md)
- [Architecture Documentation](../docs/ingestion-prd-using-connect.md)

## Contributing

When adding new tests:

1. Follow the naming conventions
2. Use appropriate markers (`@pytest.mark.unit`, etc.)
3. Add docstrings describing what the test validates
4. Use existing fixtures when possible
5. Clean up resources in teardown
6. Add test documentation to `docs/test-cases/` for E2E tests

## Questions or Issues?

Please open an issue in the project repository or contact the development team.
