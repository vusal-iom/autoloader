# Configuration Management in IOMETE Autoloader

This document explains how configuration and settings are managed in the IOMETE Autoloader project using Pydantic Settings.

## Table of Contents

1. [Overview](#overview)
2. [Settings Architecture](#settings-architecture)
3. [Environment Variables](#environment-variables)
4. [Configuration Sources](#configuration-sources)
5. [Usage Patterns](#usage-patterns)
6. [Environment-Specific Configuration](#environment-specific-configuration)
7. [Best Practices](#best-practices)

## Overview

The project uses **Pydantic Settings** (`pydantic-settings`) for type-safe configuration management with support for:

- Environment variables
- `.env` files
- Default values
- Type validation
- Case-insensitive matching

**Key File:** `app/config.py`

## Settings Architecture

### The Settings Class

All application configuration is centralized in the `Settings` class:

```python
# app/config.py
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    """Application settings."""

    # Application
    app_name: str = "IOMETE Autoloader"
    app_version: str = "1.0.0"
    debug: bool = False

    # Database
    database_url: str = "sqlite:///./autoloader.db"

    # Security
    secret_key: str = "change-this-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = False

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
```

### Key Components

#### 1. BaseSettings

Inheriting from `BaseSettings` enables:
- Automatic environment variable reading
- `.env` file support
- Type conversion and validation
- Field-level configuration

#### 2. Field Definitions

Each setting has:
- **Type annotation** (`str`, `int`, `bool`, `float`)
- **Default value** (used if not provided via environment)

```python
app_name: str = "IOMETE Autoloader"  # Default value
debug: bool = False                   # Default: False
database_url: str = "sqlite:///./autoloader.db"  # Default DB
```

#### 3. Config Class

```python
class Config:
    env_file = ".env"          # Read from .env file
    case_sensitive = False     # APP_NAME, app_name, App_Name all work
```

#### 4. Singleton Pattern

```python
@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
```

**Why `@lru_cache()`?**
- Creates settings instance once
- Reuses same instance for all calls
- Prevents re-reading environment on every access
- Thread-safe singleton

## Environment Variables

### Variable Naming

Pydantic Settings maps Python field names to environment variables:

```python
class Settings(BaseSettings):
    app_name: str = "IOMETE Autoloader"
    database_url: str = "sqlite:///./autoloader.db"
    spark_connect_default_port: int = 15002
```

**Environment Variable Names (case-insensitive):**
- `APP_NAME` or `app_name` or `App_Name`
- `DATABASE_URL` or `database_url`
- `SPARK_CONNECT_DEFAULT_PORT` or `spark_connect_default_port`

### Convention

**Recommended:** Use UPPERCASE with underscores:
```bash
APP_NAME=MyAutoloader
DATABASE_URL=postgresql://user:pass@localhost/autoloader
SPARK_CONNECT_DEFAULT_PORT=15002
```

### Type Conversion

Pydantic automatically converts environment variable strings to the correct type:

```python
# Environment variables are always strings
# export DEBUG=true
# export ACCESS_TOKEN_EXPIRE_MINUTES=60
# export COST_PER_DBU_HOUR=0.75

class Settings(BaseSettings):
    debug: bool = False                        # "true" → True
    access_token_expire_minutes: int = 30      # "60" → 60
    cost_per_dbu_hour: float = 0.40           # "0.75" → 0.75
```

**Supported conversions:**
- `"true"`, `"1"`, `"yes"` → `True`
- `"false"`, `"0"`, `"no"` → `False`
- `"123"` → `123` (int)
- `"3.14"` → `3.14` (float)

## Configuration Sources

Settings are loaded from multiple sources in order of precedence (highest to lowest):

### 1. Environment Variables (Highest Priority)

```bash
export DATABASE_URL=postgresql://prod-server/db
export DEBUG=false
```

### 2. .env File

Create a `.env` file in the project root:

```bash
# .env
APP_NAME=IOMETE Autoloader
DATABASE_URL=postgresql://user:password@localhost:5432/autoloader
SECRET_KEY=your-secret-key-here
DEBUG=true

# Spark Connect
SPARK_CONNECT_DEFAULT_PORT=15002
TEST_SPARK_CONNECT_TOKEN=your-token

# Cost Estimation
COST_PER_DBU_HOUR=0.40
STORAGE_COST_PER_GB=0.023
```

### 3. Default Values (Lowest Priority)

Defined in the `Settings` class:

```python
class Settings(BaseSettings):
    app_name: str = "IOMETE Autoloader"  # Used if not in env or .env
    debug: bool = False
```

### Precedence Example

```python
# In Settings class
debug: bool = False  # Default

# In .env file
DEBUG=true

# In environment
export DEBUG=false

# Result: False (environment wins)
```

**Order:**
1. Check environment variables
2. Check `.env` file
3. Use default value
4. Raise error if required field missing

## Usage Patterns

### Pattern 1: Import and Use Directly

```python
# app/main.py
from app.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
)
```

### Pattern 2: Dependency Injection (FastAPI)

```python
from fastapi import Depends
from app.config import Settings, get_settings

@app.get("/info")
async def get_info(settings: Settings = Depends(get_settings)):
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "debug": settings.debug,
    }
```

**Benefits:**
- Testable (can inject mock settings)
- Follows FastAPI patterns
- Automatic caching via `@lru_cache()`

### Pattern 3: In Services

```python
# app/services/cost_estimator.py
from app.config import get_settings

class CostEstimator:
    def __init__(self):
        self.settings = get_settings()

    def calculate_compute_cost(self, duration_hours: float, dbm_count: int):
        return (
            duration_hours *
            dbm_count *
            self.settings.cost_per_dbu_hour
        )
```

### Pattern 4: Helper Functions

```python
# app/config.py
def get_spark_connect_url(cluster_id: str) -> str:
    """Get Spark Connect URL for a cluster."""
    # TODO: Implement cluster lookup
    return "sc://localhost:15002"

def get_spark_connect_credentials(cluster_id: str) -> tuple[str, str]:
    """Get Spark Connect URL and token."""
    settings = get_settings()
    url = get_spark_connect_url(cluster_id)
    token = settings.test_spark_connect_token or ""
    return url, token
```

**Usage:**

```python
from app.config import get_spark_connect_credentials

url, token = get_spark_connect_credentials("cluster-123")
spark_client = SparkConnectClient(url, token)
```

## Environment-Specific Configuration

### Development (.env)

```bash
# .env (for local development)
APP_NAME=Autoloader Dev
DATABASE_URL=sqlite:///./autoloader.db
DEBUG=true
SECRET_KEY=dev-secret-key

# Local Spark Connect
SPARK_CONNECT_DEFAULT_PORT=15002
TEST_SPARK_CONNECT_TOKEN=

# Checkpoints on local filesystem
CHECKPOINT_BASE_PATH=/tmp/iomete-autoloader/checkpoints
```

### Production (Environment Variables)

```bash
# Production environment (Kubernetes/Docker)
export APP_NAME="IOMETE Autoloader Production"
export DATABASE_URL="postgresql://autoloader:secure-pwd@db-server:5432/autoloader_prod"
export DEBUG=false
export SECRET_KEY="$PRODUCTION_SECRET_KEY"

# Production Spark
export SPARK_CONNECT_DEFAULT_PORT=15002
export SPARK_SESSION_POOL_SIZE=50
export SPARK_SESSION_IDLE_TIMEOUT=3600

# S3 Checkpoints
export CHECKPOINT_BASE_PATH="s3://iomete-production-checkpoints/"

# Cost settings
export COST_PER_DBU_HOUR=0.40
export STORAGE_COST_PER_GB=0.023
```

### Testing

```python
# tests/conftest.py
import pytest
from app.config import Settings

@pytest.fixture
def test_settings():
    """Override settings for tests."""
    return Settings(
        database_url="sqlite:///:memory:",
        debug=True,
        secret_key="test-secret",
        test_spark_connect_token="test-token",
    )

@pytest.fixture
def app(test_settings):
    """Create test app with test settings."""
    from app.main import app
    app.dependency_overrides[get_settings] = lambda: test_settings
    yield app
    app.dependency_overrides.clear()
```

## Configuration Categories

### Application Settings

```python
app_name: str = "IOMETE Autoloader"
app_version: str = "1.0.0"
debug: bool = False
```

**Environment Variables:**
```bash
APP_NAME=MyApp
APP_VERSION=2.0.0
DEBUG=true
```

### Database Configuration

```python
database_url: str = "sqlite:///./autoloader.db"
```

**Examples:**

```bash
# SQLite (development)
DATABASE_URL=sqlite:///./autoloader.db

# PostgreSQL (production)
DATABASE_URL=postgresql://user:password@localhost:5432/autoloader

# With connection pool settings
DATABASE_URL=postgresql://user:pass@host:5432/db?pool_size=20&max_overflow=0
```

### Security Settings

```python
secret_key: str = "change-this-in-production"
algorithm: str = "HS256"
access_token_expire_minutes: int = 30
```

**Production Example:**

```bash
SECRET_KEY=$(openssl rand -hex 32)
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=60
```

### Spark Connect Settings

```python
spark_connect_default_port: int = 15002
test_spark_connect_token: str = ""
spark_session_pool_size: int = 10
spark_session_idle_timeout: int = 1800  # 30 minutes
```

**Configuration:**

```bash
SPARK_CONNECT_DEFAULT_PORT=15002
TEST_SPARK_CONNECT_TOKEN=your-test-token
SPARK_SESSION_POOL_SIZE=50
SPARK_SESSION_IDLE_TIMEOUT=3600
```

### Ingestion Settings

```python
checkpoint_base_path: str = "s3://iomete-checkpoints/"
max_preview_files: int = 10
max_preview_rows: int = 100
```

**Environment-specific:**

```bash
# Development
CHECKPOINT_BASE_PATH=/tmp/checkpoints

# Production
CHECKPOINT_BASE_PATH=s3://prod-checkpoints/
MAX_PREVIEW_FILES=50
MAX_PREVIEW_ROWS=500
```

### Cost Estimation Settings

```python
cost_per_dbu_hour: float = 0.40
storage_cost_per_gb: float = 0.023
list_operation_cost_per_1000: float = 0.005
```

**Customization:**

```bash
COST_PER_DBU_HOUR=0.50
STORAGE_COST_PER_GB=0.025
LIST_OPERATION_COST_PER_1000=0.006
```

### Scheduler Settings

```python
scheduler_check_interval: int = 60  # seconds
```

**Configuration:**

```bash
SCHEDULER_CHECK_INTERVAL=30  # Check every 30 seconds
```

### Monitoring Settings

```python
metrics_retention_days: int = 30
```

**Configuration:**

```bash
METRICS_RETENTION_DAYS=90  # Keep metrics for 90 days
```

## Best Practices

### 1. Never Commit Secrets

```bash
# .gitignore
.env
.env.local
.env.production
*.key
credentials.json
```

Create `.env.example` instead:

```bash
# .env.example
APP_NAME=IOMETE Autoloader
DATABASE_URL=sqlite:///./autoloader.db
SECRET_KEY=change-this-in-production
DEBUG=false

# ... all config keys with safe defaults
```

### 2. Use Type Hints

```python
# Good
database_url: str = "sqlite:///./autoloader.db"
debug: bool = False
port: int = 8000

# Avoid
database_url = "sqlite:///./autoloader.db"  # No type hint
```

### 3. Provide Sensible Defaults

```python
# Good - works out of the box for development
database_url: str = "sqlite:///./autoloader.db"
checkpoint_base_path: str = "/tmp/iomete-autoloader/checkpoints"

# Bad - requires configuration to run
database_url: str  # No default, must be provided
```

### 4. Document Configuration

```python
class Settings(BaseSettings):
    """
    Application settings.

    Configuration can be provided via:
    1. Environment variables (highest priority)
    2. .env file
    3. Default values (lowest priority)

    See .env.example for all available settings.
    """

    # Spark Connect
    spark_connect_default_port: int = 15002  # Spark Connect default port
    test_spark_connect_token: str = ""       # For testing: leave empty or set token
```

### 5. Group Related Settings

```python
class Settings(BaseSettings):
    # Application
    app_name: str = "IOMETE Autoloader"
    app_version: str = "1.0.0"
    debug: bool = False

    # Database
    database_url: str = "sqlite:///./autoloader.db"

    # Security
    secret_key: str = "change-this-in-production"
    algorithm: str = "HS256"

    # Cost Estimation
    cost_per_dbu_hour: float = 0.40
    storage_cost_per_gb: float = 0.023
```

### 6. Validate Complex Settings

```python
from pydantic import field_validator

class Settings(BaseSettings):
    database_url: str = "sqlite:///./autoloader.db"

    @field_validator('database_url')
    def validate_database_url(cls, v):
        if not v.startswith(('sqlite:', 'postgresql:')):
            raise ValueError('Database must be SQLite or PostgreSQL')
        return v
```

### 7. Environment-Specific Validation

```python
class Settings(BaseSettings):
    debug: bool = False
    secret_key: str = "change-this-in-production"

    @field_validator('secret_key')
    def validate_secret_in_production(cls, v, values):
        if not values.get('debug') and v == "change-this-in-production":
            raise ValueError('Must set SECRET_KEY in production')
        return v
```

## Common Patterns

### Pattern: Multiple Environment Files

```python
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    class Config:
        # Load environment-specific file
        env_file = f".env.{os.getenv('ENVIRONMENT', 'dev')}"
        case_sensitive = False
```

**Usage:**

```bash
# Load .env.dev
export ENVIRONMENT=dev
python run.py

# Load .env.prod
export ENVIRONMENT=prod
python run.py
```

### Pattern: Required Settings

```python
from pydantic import Field

class Settings(BaseSettings):
    # No default = required
    database_url: str = Field(..., env='DATABASE_URL')
    secret_key: str = Field(..., env='SECRET_KEY')

    # Optional with default
    debug: bool = False
```

### Pattern: Computed Settings

```python
class Settings(BaseSettings):
    database_host: str = "localhost"
    database_port: int = 5432
    database_name: str = "autoloader"
    database_user: str = "user"
    database_password: str = "password"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.database_user}:{self.database_password}@"
            f"{self.database_host}:{self.database_port}/{self.database_name}"
        )
```

## Troubleshooting

### Settings Not Loading from .env

**Check:**
1. `.env` file in project root (same directory as where you run the app)
2. `env_file = ".env"` in `Config` class
3. No syntax errors in `.env` (no spaces around `=`)

```bash
# Good
DEBUG=true
DATABASE_URL=sqlite:///./autoloader.db

# Bad
DEBUG = true  # Spaces around =
```

### Type Conversion Errors

```bash
# Wrong
DEBUG=True  # Capital T won't work

# Right
DEBUG=true  # or DEBUG=1 or DEBUG=yes
```

### Case Sensitivity Issues

```python
class Config:
    case_sensitive = False  # Make sure this is set
```

```bash
# All of these work with case_sensitive=False
DEBUG=true
debug=true
DeBuG=true
```

## Security Considerations

### 1. Secret Management

**Development:**
```bash
# .env (not committed)
SECRET_KEY=dev-secret-key-1234567890
```

**Production:**
```bash
# Use environment variables from secrets manager
export SECRET_KEY=$(aws secretsmanager get-secret-value ...)
```

### 2. Database Credentials

**Never commit:**
```bash
# .env (in .gitignore)
DATABASE_URL=postgresql://user:password@host/db
```

**Production:**
```bash
# Inject from secrets
export DATABASE_URL=$DATABASE_CONNECTION_STRING
```

### 3. Audit Sensitive Defaults

```python
# Bad - insecure default in production
secret_key: str = "default-secret"

# Good - forces configuration
secret_key: str = "change-this-in-production"  # Clear warning
```

## Related Documentation

- [Pydantic Usage](./pydantic-usage.md) - How Pydantic is used for API schemas
- [Pydantic Settings Docs](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) - Official documentation

## Key Files Reference

- **`app/config.py`** - Settings class and helper functions
- **`.env.example`** - Example configuration (committed)
- **`.env`** - Local configuration (not committed)
- **`app/main.py`** - Settings usage in FastAPI app
- **`app/services/cost_estimator.py`** - Example service using settings
