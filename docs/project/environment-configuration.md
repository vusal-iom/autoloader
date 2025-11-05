# Environment Configuration Guide

This document describes all environment configuration files in the IOMETE Autoloader project, their purpose, and how they are used.

## Overview

The project uses multiple environment files to manage configuration across different contexts:

| File | Purpose | Version Control | Required |
|------|---------|----------------|----------|
| `.env.example` | Template for production/development configuration | Committed | No (template only) |
| `.env` | Actual production/development configuration | **Ignored** (gitignored) | Yes for running app |
| `.env.test.example` | Template for test environment configuration | Committed | No (template only) |
| `.env.test` | Actual test environment configuration | **Ignored** (gitignored) | Yes for running tests |

## Configuration Loading

All environment files are loaded via [Pydantic BaseSettings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) in `app/config.py`:

- **Main application**: Loads `.env` file (line 45 in `app/config.py`)
- **Tests**: Load `.env.test` file (via `os.getenv()` in `tests/conftest.py`)

### Settings Class

```python
class Settings(BaseSettings):
    # ... configuration fields ...

    class Config:
        env_file = ".env"
        case_sensitive = False
```

Configuration is cached using `@lru_cache()` for performance.

---

## 1. `.env.example` - Production/Development Template

**Location**: `/Users/vusaldadalov/Documents/IOMETE/github.com/autoloader/.env.example`

**Purpose**: Template for main application configuration. Copy to `.env` and customize for your environment.

**Setup**:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Configuration Sections

#### Application Settings
```bash
APP_NAME=IOMETE Autoloader
APP_VERSION=1.0.0
DEBUG=false
```

- `APP_NAME`: Application display name
- `APP_VERSION`: Current version
- `DEBUG`: Enable debug mode (use `true` for development)

#### Database Configuration
```bash
# Development (SQLite)
DATABASE_URL=sqlite:///./autoloader.db

# Production (PostgreSQL)
# DATABASE_URL=postgresql://user:password@localhost:5432/autoloader
```

- **Development**: Uses SQLite for simplicity
- **Production**: Should use PostgreSQL for reliability
- Format: Standard SQLAlchemy database URL

#### Security Settings
```bash
SECRET_KEY=change-this-in-production
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

- `SECRET_KEY`: **CRITICAL** - Must be changed in production
  - Generate with: `openssl rand -hex 32`
- `ALGORITHM`: JWT signing algorithm (default: HS256)
- `ACCESS_TOKEN_EXPIRE_MINUTES`: Token expiration time

#### Spark Connect Configuration
```bash
SPARK_CONNECT_DEFAULT_PORT=15002
TEST_SPARK_CONNECT_TOKEN=
SPARK_SESSION_POOL_SIZE=10
SPARK_SESSION_IDLE_TIMEOUT=1800
```

- `SPARK_CONNECT_DEFAULT_PORT`: Default port for Spark Connect (15002)
- `TEST_SPARK_CONNECT_TOKEN`: Optional token for local development
- `SPARK_SESSION_POOL_SIZE`: Maximum number of pooled sessions
- `SPARK_SESSION_IDLE_TIMEOUT`: Timeout in seconds (default: 30 minutes)

#### Ingestion Settings
```bash
# Development (local filesystem)
# CHECKPOINT_BASE_PATH=/tmp/iomete-autoloader/checkpoints

# Production (S3)
CHECKPOINT_BASE_PATH=s3://iomete-checkpoints/

MAX_PREVIEW_FILES=10
MAX_PREVIEW_ROWS=100
```

- `CHECKPOINT_BASE_PATH`: Where Spark Auto Loader stores checkpoints
  - Development: Local filesystem path
  - Production: S3/Azure/GCS path
- `MAX_PREVIEW_FILES`: Maximum files to preview in test mode
- `MAX_PREVIEW_ROWS`: Maximum rows to preview

#### Cost Estimation
```bash
COST_PER_DBU_HOUR=0.40
STORAGE_COST_PER_GB=0.023
LIST_OPERATION_COST_PER_1000=0.005
```

- `COST_PER_DBU_HOUR`: Cost per Databricks Unit per hour ($)
- `STORAGE_COST_PER_GB`: Storage cost per GB ($)
- `LIST_OPERATION_COST_PER_1000`: Cost per 1000 list operations ($)

Used by `app/services/cost_estimator.py` to estimate monthly costs.

#### Scheduler Settings
```bash
SCHEDULER_CHECK_INTERVAL=60
```

- `SCHEDULER_CHECK_INTERVAL`: How often (in seconds) the scheduler checks for pending jobs

#### Monitoring Settings
```bash
METRICS_RETENTION_DAYS=30
```

- `METRICS_RETENTION_DAYS`: How long to retain run history and metrics

### Usage in Code

All settings are accessed via the `get_settings()` function:

```python
from app.config import get_settings

settings = get_settings()
print(settings.database_url)
print(settings.checkpoint_base_path)
```

Referenced in:
- `app/main.py` - Application startup
- `app/database.py` - Database connection
- `app/services/cost_estimator.py` - Cost calculations
- `app/spark/connect_client.py` - Spark Connect setup

---

## 2. `.env.test.example` - Test Environment Template

**Location**: `/Users/vusaldadalov/Documents/IOMETE/github.com/autoloader/.env.test.example`

**Purpose**: Template for test environment configuration. Copy to `.env.test` for running tests.

**Setup**:
```bash
cp .env.test.example .env.test
# Edit .env.test if needed (usually defaults are fine)
```

### Configuration Sections

#### MinIO Configuration (S3-Compatible Storage)
```bash
TEST_MINIO_ENDPOINT=http://localhost:9000
TEST_MINIO_ACCESS_KEY=minioadmin
TEST_MINIO_SECRET_KEY=minioadmin
```

- Used for S3-compatible object storage in tests
- Requires MinIO running via `docker-compose.test.yml`
- Default credentials: `minioadmin/minioadmin`

#### Spark Connect Configuration
```bash
TEST_SPARK_CONNECT_URL=
TEST_SPARK_CONNECT_TOKEN=
```

- **Optional**: Only needed for full end-to-end tests with real Spark
- Leave empty to run tests with mocked Spark executor
- Set to `sc://localhost:15002` for real Spark Connect testing

#### Database Configuration
```bash
# TEST_DATABASE_URL=postgresql://test_user:test_password@localhost:5432/autoloader_test
```

- **Optional**: By default, tests use in-memory SQLite
- Uncomment to use PostgreSQL for integration tests
- Requires PostgreSQL running via `docker-compose.test.yml`

#### Test Configuration
```bash
TEST_TENANT_ID=test-tenant-001
TEST_CLUSTER_ID=test-cluster-001
```

- `TEST_TENANT_ID`: Default tenant ID for tests
- `TEST_CLUSTER_ID`: Default cluster ID for tests

#### Test Data Configuration
```bash
TEST_DATA_NUM_FILES=3
TEST_DATA_RECORDS_PER_FILE=1000
```

- `TEST_DATA_NUM_FILES`: Number of test files to generate
- `TEST_DATA_RECORDS_PER_FILE`: Records per generated file

#### Logging
```bash
LOG_LEVEL=DEBUG
```

- `LOG_LEVEL`: Logging level for tests (DEBUG, INFO, WARNING, ERROR)

### Usage in Tests

Environment variables are accessed directly in test fixtures via `os.getenv()`:

```python
# In tests/conftest.py
@pytest.fixture(scope="session")
def test_config() -> Dict[str, Any]:
    return {
        "spark_connect_url": os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002"),
        "spark_connect_token": os.getenv("TEST_SPARK_CONNECT_TOKEN", "test-token"),
    }
```

Referenced in:
- `tests/conftest.py` - Test fixtures and configuration (lines 33-34, 89-92)
- `tests/e2e/test_basic_s3_json_ingestion.py` - End-to-end integration tests
- `scripts/test_spark_connect.py` - Spark Connect connectivity tests

### Loading Test Environment

To manually load test environment variables:

```bash
export $(cat .env.test | xargs)
```

---

## 3. `.env.test` - Actual Test Configuration

**Location**: `/Users/vusaldadalov/Documents/IOMETE/github.com/autoloader/.env.test`

**Purpose**: Actual test configuration (identical to `.env.test.example` with potential customizations).

**Key Differences from Example**:
```bash
# TEST_SPARK_CONNECT_URL is set in the actual file
TEST_SPARK_CONNECT_URL=sc://localhost:15002
```

This file exists in the repository but is **gitignored** (line 60 in `.gitignore`). The committed version appears to be the same as the example, but local changes won't be tracked.

---

## Environment File Hierarchy

### Git Tracking

From `.gitignore`:

```bash
# Environment files (NOT tracked)
.env
.env.local
.env.test
.env.production

# Template files (TRACKED)
# .env.example
# .env.test.example
```

### File Relationships

```
.env.example  ────copy───▶  .env  ────loaded by───▶  app/config.py
(template)                  (local)                  (Pydantic Settings)

.env.test.example ─copy───▶ .env.test ──loaded by──▶ tests/conftest.py
(template)                  (local)                   (os.getenv)
```

---

## Quick Setup Guide

### For Development

```bash
# 1. Copy template
cp .env.example .env

# 2. Update critical settings
# Edit .env:
#   - Set SECRET_KEY (openssl rand -hex 32)
#   - Set DATABASE_URL if using PostgreSQL
#   - Set CHECKPOINT_BASE_PATH for your environment

# 3. Run application
uvicorn app.main:app --reload
```

### For Testing

```bash
# 1. Copy template
cp .env.test.example .env.test

# 2. (Optional) Customize test settings
# Usually defaults are fine

# 3. Start test infrastructure
docker-compose -f docker-compose.test.yml up -d

# 4. Run tests
pytest
```

---

## Security Considerations

### Secrets Management

- **Never commit** `.env` files with actual credentials
- **Always change** `SECRET_KEY` in production
- **Use environment-specific** credentials (dev/staging/prod)
- Consider using secret management tools (AWS Secrets Manager, Azure Key Vault, etc.) in production

### Sensitive Variables

The following variables contain sensitive information:

- `SECRET_KEY` - JWT signing key
- `DATABASE_URL` - Database credentials
- `TEST_SPARK_CONNECT_TOKEN` - Spark authentication token
- `TEST_MINIO_ACCESS_KEY` / `TEST_MINIO_SECRET_KEY` - Object storage credentials

### Production Recommendations

1. Use a dedicated `.env.production` file (gitignored)
2. Generate strong `SECRET_KEY`: `openssl rand -hex 32`
3. Use PostgreSQL instead of SQLite
4. Store checkpoints in cloud storage (S3/Azure/GCS)
5. Enable HTTPS and proper authentication
6. Rotate credentials regularly

---

## Troubleshooting

### Environment Not Loading

**Problem**: Settings not applying

**Solutions**:
- Verify `.env` file exists in project root
- Check file permissions (must be readable)
- Ensure no syntax errors (no quotes around values)
- Restart application to reload settings

### Database Connection Errors

**Problem**: `sqlite3.OperationalError` or PostgreSQL connection errors

**Solutions**:
- Verify `DATABASE_URL` format
- For PostgreSQL: ensure database exists and credentials are correct
- For SQLite: ensure write permissions in project directory

### Spark Connect Failures in Tests

**Problem**: Tests fail with Spark Connect errors

**Solutions**:
- Leave `TEST_SPARK_CONNECT_URL` empty to use mocked executor
- Ensure Spark Connect server is running if using real Spark
- Verify `TEST_SPARK_CONNECT_TOKEN` if authentication is required

### MinIO Connection Errors

**Problem**: S3 tests fail with connection errors

**Solutions**:
- Start MinIO: `docker-compose -f docker-compose.test.yml up -d`
- Verify `TEST_MINIO_ENDPOINT` is correct (default: http://localhost:9000)
- Check MinIO credentials match defaults

---

## References

- **Configuration Implementation**: `app/config.py`
- **Test Fixtures**: `tests/conftest.py`
- **Setup Guide**: `README.md` (lines 95-99)
- **Test Setup**: `tests/README.md` (lines 122-142)
- **Pydantic Settings Docs**: https://docs.pydantic.dev/latest/concepts/pydantic_settings/
