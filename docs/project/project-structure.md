# IOMETE Autoloader - Project Structure

This document explains how the IOMETE Autoloader project is organized, the purpose of each directory and file, and how components interact.

## Table of Contents

1. [Overview](#overview)
2. [Project Layout](#project-layout)
3. [Directory Structure](#directory-structure)
4. [Layered Architecture](#layered-architecture)
5. [Key Files](#key-files)
6. [Module Organization](#module-organization)
7. [Data Flow](#data-flow)
8. [Naming Conventions](#naming-conventions)
9. [Finding Things](#finding-things)

## Overview

The project follows a **layered architecture** pattern with clear separation of concerns:

```
┌─────────────────────────────────────┐
│         API Layer (FastAPI)         │  HTTP endpoints, request/response
├─────────────────────────────────────┤
│      Service Layer (Business)       │  Business logic, orchestration
├─────────────────────────────────────┤
│    Repository Layer (Data Access)   │  Database operations
├─────────────────────────────────────┤
│      Database (SQLAlchemy ORM)      │  PostgreSQL/SQLite
└─────────────────────────────────────┘

         ┌──────────────────┐
         │  Spark Connect   │  Parallel: Remote Spark execution
         └──────────────────┘
```

**Design Principles:**
- Clean separation between layers
- Dependency injection for testability
- Repository pattern for data access
- Service layer for business logic
- Pydantic for validation at API boundaries

## Project Layout

```
autoloader/
├── app/                    # Main application code
│   ├── api/               # API endpoints (FastAPI routes)
│   ├── models/            # Data models (SQLAlchemy + Pydantic)
│   ├── repositories/      # Data access layer
│   ├── services/          # Business logic layer
│   ├── spark/             # Spark Connect integration
│   ├── config.py          # Application settings
│   ├── database.py        # Database setup
│   └── main.py            # FastAPI application entry point
│
├── tests/                 # Test suite
│   ├── e2e/              # End-to-end integration tests
│   └── fixtures/         # Test data and utilities
│
├── docs/                  # Documentation
│   ├── project/          # Technical documentation
│   ├── batch-processing/ # Implementation guides
│   └── mocks/            # UI mockups
│
├── alembic/              # Database migrations
│   └── versions/         # Migration scripts
│
├── scripts/              # Utility scripts
├── run.py                # Application runner
├── requirements.txt      # Python dependencies
├── alembic.ini          # Alembic configuration
└── pytest.ini           # Pytest configuration
```

## Directory Structure

### `/app` - Main Application

The core application code organized by architectural layer.

```
app/
├── __init__.py           # Package initialization
├── main.py              # FastAPI app setup, CORS, routers
├── config.py            # Settings and configuration
├── database.py          # Database session management
│
├── api/                 # API Layer
│   ├── __init__.py
│   └── v1/              # API version 1
│       ├── __init__.py
│       ├── ingestions.py    # Ingestion CRUD endpoints
│       ├── runs.py          # Run history endpoints
│       └── clusters.py      # Cluster management endpoints
│
├── models/              # Data Models
│   ├── __init__.py
│   ├── domain.py        # SQLAlchemy ORM models (database)
│   └── schemas.py       # Pydantic models (API validation)
│
├── repositories/        # Data Access Layer
│   ├── __init__.py
│   ├── ingestion_repository.py      # Ingestion data access
│   ├── run_repository.py            # Run history data access
│   └── processed_file_repository.py # File tracking data access
│
├── services/            # Business Logic Layer
│   ├── __init__.py
│   ├── ingestion_service.py       # Ingestion management
│   ├── batch_orchestrator.py      # Batch processing coordinator
│   ├── batch_file_processor.py    # File processing logic
│   ├── file_discovery_service.py  # Cloud file discovery
│   ├── file_state_service.py      # File state tracking
│   ├── spark_service.py           # Spark operations wrapper
│   └── cost_estimator.py          # Cost calculation
│
└── spark/               # Spark Integration
    ├── __init__.py
    ├── connect_client.py    # Spark Connect client
    ├── executor.py          # Ingestion execution
    └── session_manager.py   # Session pooling (TODO)
```

### `/app/api` - API Layer

**Purpose:** HTTP endpoints and request/response handling

**Structure:**
- Versioned APIs (`v1`, `v2`, etc.)
- Each file represents a resource (ingestions, runs, clusters)
- Uses Pydantic schemas for validation
- Delegates business logic to services

**Example:** `app/api/v1/ingestions.py:24-40`
```python
@router.post("", response_model=IngestionResponse, status_code=status.HTTP_201_CREATED)
async def create_ingestion(
    ingestion: IngestionCreate,  # Pydantic validates request
    service: IngestionService = Depends(get_ingestion_service),  # DI
    current_user: str = "user_xyz",  # TODO: Add auth dependency
):
    """Create a new ingestion configuration."""
    return service.create_ingestion(ingestion, current_user)
```

**Key Files:**
- `ingestions.py` - Ingestion CRUD, run, pause, resume, test, cost estimate
- `runs.py` - Run history, retry
- `clusters.py` - Cluster listing, testing

### `/app/models` - Data Models

**Purpose:** Define data structures for database and API

**Two types of models:**

#### 1. Domain Models (`domain.py`)
SQLAlchemy ORM models for database persistence

```python
class Ingestion(Base):
    __tablename__ = "ingestions"

    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    status = Column(String, nullable=False)
    # ... database fields
```

**Location:** `app/models/domain.py`

**Purpose:**
- Database table definitions
- Relationships between tables
- Database constraints

#### 2. API Schemas (`schemas.py`)
Pydantic models for API validation

```python
class IngestionCreate(BaseModel):
    """Create ingestion request."""
    name: str
    cluster_id: str
    source: SourceConfig
    # ... API fields
```

**Location:** `app/models/schemas.py`

**Purpose:**
- Request validation
- Response serialization
- Type safety
- Auto-generated API docs

**See:** [Pydantic Usage](./pydantic-usage.md) for detailed documentation

### `/app/repositories` - Data Access Layer

**Purpose:** Abstract database operations

**Pattern:**
- One repository per entity (Ingestion, Run, ProcessedFile)
- All database queries go through repositories
- Services never use SQLAlchemy directly

**Example:** `app/repositories/ingestion_repository.py`
```python
class IngestionRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, ingestion: Ingestion) -> Ingestion:
        self.db.add(ingestion)
        self.db.commit()
        self.db.refresh(ingestion)
        return ingestion

    def get_by_id(self, ingestion_id: str) -> Optional[Ingestion]:
        return self.db.query(Ingestion).filter(Ingestion.id == ingestion_id).first()
```

**Benefits:**
- Testable (can mock repositories)
- Reusable queries
- Centralized data access logic
- Easier to optimize queries

**Key Files:**
- `ingestion_repository.py` - Ingestion CRUD operations
- `run_repository.py` - Run history queries
- `processed_file_repository.py` - File state tracking

### `/app/services` - Business Logic Layer

**Purpose:** Implement business logic and orchestration

**Responsibilities:**
- Coordinate between multiple repositories
- Implement business rules
- Handle complex workflows
- Interact with external systems (Spark, cloud storage)

**Example:** `app/services/ingestion_service.py`
```python
class IngestionService:
    def __init__(self, db: Session):
        self.db = db
        self.ingestion_repo = IngestionRepository(db)
        self.run_repo = RunRepository(db)

    def create_ingestion(self, data: IngestionCreate, user_id: str) -> IngestionResponse:
        # Business logic: validate, create checkpoint, save
        checkpoint = self._generate_checkpoint_location(data)
        ingestion = self.ingestion_repo.create(...)
        return IngestionResponse.from_orm(ingestion)
```

**Services by Function:**

| Service | Purpose | Key Methods |
|---------|---------|-------------|
| `ingestion_service.py` | Ingestion lifecycle management | create, update, delete, run, pause, resume |
| `batch_orchestrator.py` | Coordinates batch processing | orchestrate_batch_run |
| `batch_file_processor.py` | Processes files in batches | process_batch |
| `file_discovery_service.py` | Discovers files in cloud storage | discover_new_files |
| `file_state_service.py` | Tracks file processing state | mark_as_processed, get_new_files |
| `spark_service.py` | Wrapper for Spark operations | execute_ingestion |
| `cost_estimator.py` | Calculates cost estimates | estimate_monthly_cost |

### `/app/spark` - Spark Connect Integration

**Purpose:** Interface with Apache Spark via Spark Connect

**Structure:**
```
spark/
├── connect_client.py    # Low-level Spark Connect client
├── executor.py          # High-level ingestion executor
└── session_manager.py   # Session pooling (TODO)
```

**Key Component:** `connect_client.py`

Provides Spark Connect operations:
- `connect()` - Establish connection
- `read_stream()` - Create Auto Loader DataFrame
- `write_stream()` - Write to Iceberg table
- `preview_files()` - Sample data preview
- `test_connection()` - Health check

**Example Usage:**
```python
client = SparkConnectClient(url, token)
client.connect(source_config)

df = client.read_stream(
    source_path="s3://bucket/path",
    source_type="s3",
    format_type="json",
)

client.write_stream(
    df=df,
    catalog="lakehouse",
    database="raw",
    table="events",
    checkpoint_location="s3://checkpoints/ingestion-123",
)
```

**See:** `app/spark/connect_client.py` for implementation

### `/tests` - Test Suite

**Purpose:** Automated testing

```
tests/
├── conftest.py          # Pytest configuration and fixtures
├── e2e/                 # End-to-end integration tests
│   ├── test_basic_s3_json_ingestion.py
│   └── test_phase1_batch_processing_e2e.py
└── fixtures/            # Test utilities and data generators
    └── data_generator.py
```

**Test Types:**

1. **E2E Tests** (`tests/e2e/`)
   - Full integration tests
   - Require Spark Connect and S3/MinIO
   - Test complete workflows

2. **Fixtures** (`tests/fixtures/`)
   - Shared test data
   - Data generators
   - Mock objects

**See:** [Running Integration Tests in PyCharm](./running-integration-tests-in-pycharm.md)

### `/docs` - Documentation

**Purpose:** Project documentation

```
docs/
├── project/                    # Technical documentation
│   ├── README.md              # Documentation index
│   ├── pydantic-usage.md      # Pydantic patterns
│   ├── configuration-management.md
│   ├── project-structure.md   # This file
│   └── alembic-guide.md
│
├── batch-processing/           # Implementation guides
│   ├── phase1-implementation-summary.md
│   └── batch-processing-implementation-guide.md
│
├── ingestion-prd-v1.md        # Product requirements
├── autoloaderv1.md            # UI/UX mockups
├── mocks/                      # HTML mockups
└── test-cases/                 # Test case documentation
```

### `/alembic` - Database Migrations

**Purpose:** Database schema version control

```
alembic/
├── versions/           # Migration scripts
│   └── 67371aeb5ae0_initial_migration_with_processed_files.py
├── env.py             # Alembic environment
└── script.py.mako     # Migration template
```

**See:** [Alembic Guide](./alembic-guide.md) for usage

### `/scripts` - Utility Scripts

**Purpose:** Development and testing utilities

```
scripts/
└── test_spark_connect.py    # Test Spark Connect connectivity
```

## Layered Architecture

### Layer Responsibilities

```
┌──────────────────────────────────────────────────┐
│  API Layer (app/api/v1/)                         │
│  - Route requests to services                    │
│  - Validate input (Pydantic)                     │
│  - Serialize responses                           │
│  - Handle HTTP concerns (status codes, headers)  │
└──────────────────────────────────────────────────┘
                      ↓
┌──────────────────────────────────────────────────┐
│  Service Layer (app/services/)                   │
│  - Business logic                                │
│  - Coordinate multiple repositories              │
│  - External system integration (Spark, S3)       │
│  - Transaction management                        │
└──────────────────────────────────────────────────┘
                      ↓
┌──────────────────────────────────────────────────┐
│  Repository Layer (app/repositories/)            │
│  - Database queries                              │
│  - CRUD operations                               │
│  - Data access abstraction                       │
└──────────────────────────────────────────────────┘
                      ↓
┌──────────────────────────────────────────────────┐
│  Database (SQLAlchemy ORM)                       │
│  - PostgreSQL (production)                       │
│  - SQLite (development)                          │
└──────────────────────────────────────────────────┘
```

### Dependency Flow

**Rule:** Higher layers depend on lower layers, never the reverse

```python
# ✅ Good - API depends on Service
from app.services.ingestion_service import IngestionService

@router.post("")
async def create(service: IngestionService = Depends(...)):
    return service.create_ingestion(...)

# ✅ Good - Service depends on Repository
class IngestionService:
    def __init__(self, db: Session):
        self.repo = IngestionRepository(db)

# ❌ Bad - Repository should NOT depend on Service
class IngestionRepository:
    def __init__(self, service: IngestionService):  # Wrong!
        pass
```

### Cross-Cutting Concerns

Some modules are used across all layers:

- **`app/config.py`** - Configuration (all layers)
- **`app/models/`** - Data models (all layers)
- **`app/database.py`** - DB session (repository layer + main.py)

## Key Files

### Application Entry Points

| File | Purpose | When to Use |
|------|---------|-------------|
| `run.py` | Development server | `python run.py` |
| `app/main.py` | FastAPI application | Production (uvicorn) |

### Configuration Files

| File | Purpose | Documentation |
|------|---------|---------------|
| `app/config.py` | Settings class | [Configuration Management](./configuration-management.md) |
| `.env.example` | Config template | Copy to `.env` |
| `alembic.ini` | Migration config | [Alembic Guide](./alembic-guide.md) |
| `pytest.ini` | Test configuration | - |
| `requirements.txt` | Python dependencies | - |

### Core Application Files

| File | Purpose | Key Content |
|------|---------|-------------|
| `app/main.py` | FastAPI app setup | CORS, routes, startup/shutdown |
| `app/database.py` | DB session management | `get_db()`, `init_db()` |
| `app/config.py` | Settings | All configuration parameters |
| `app/models/domain.py` | Database models | Ingestion, Run, SchemaVersion, ProcessedFile |
| `app/models/schemas.py` | API schemas | All Pydantic models |

## Module Organization

### Import Patterns

**Absolute imports from project root:**
```python
# ✅ Good - absolute imports
from app.models.domain import Ingestion
from app.repositories.ingestion_repository import IngestionRepository
from app.services.ingestion_service import IngestionService
from app.config import get_settings

# ❌ Avoid - relative imports
from ..models.domain import Ingestion
from .repository import IngestionRepository
```

### Package Initialization

Each package has an `__init__.py`:

```python
# app/models/__init__.py
"""Data models package."""

# app/api/__init__.py
"""API routes package."""

# Empty __init__.py files are fine for Python 3.3+
```

### Dependency Injection

**Pattern used throughout:**

```python
# API Layer
def get_ingestion_service(db: Session = Depends(get_db)) -> IngestionService:
    return IngestionService(db)

@router.post("")
async def create(service: IngestionService = Depends(get_ingestion_service)):
    pass

# Service Layer
class IngestionService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = IngestionRepository(db)
```

**Benefits:**
- Testable (inject mocks)
- Loose coupling
- Clear dependencies

## Data Flow

### Request Flow (Create Ingestion)

```
1. HTTP POST /api/v1/ingestions
   ↓
2. FastAPI validates request body against IngestionCreate schema
   ↓
3. API endpoint (app/api/v1/ingestions.py:create_ingestion)
   ↓
4. Injects IngestionService via dependency injection
   ↓
5. Service (app/services/ingestion_service.py:create_ingestion)
   - Validates business rules
   - Generates checkpoint location
   - Creates domain model
   ↓
6. Repository (app/repositories/ingestion_repository.py:create)
   - Inserts into database
   - Commits transaction
   ↓
7. Service converts ORM model to Pydantic schema
   ↓
8. API serializes IngestionResponse to JSON
   ↓
9. HTTP 201 CREATED response
```

### Batch Processing Flow

```
1. Scheduler triggers run
   ↓
2. BatchOrchestrator.orchestrate_batch_run()
   ↓
3. FileDiscoveryService.discover_new_files()
   - Lists S3/Azure/GCS files
   ↓
4. FileStateService.get_new_files()
   - Filters already processed files
   ↓
5. BatchFileProcessor.process_batch()
   - Creates DataFrame with Spark
   - Writes to Iceberg table
   ↓
6. FileStateService.mark_files_as_processed()
   - Updates processed_files table
   ↓
7. RunRepository.create()
   - Records run metrics
```

## Naming Conventions

### Files

| Type | Convention | Example |
|------|------------|---------|
| Modules | `snake_case.py` | `ingestion_service.py` |
| Tests | `test_*.py` | `test_ingestion_service.py` |
| Migrations | `{rev}_{description}.py` | `67371aeb5ae0_initial_migration.py` |

### Classes

| Type | Convention | Example |
|------|------------|---------|
| Models (ORM) | `PascalCase` | `Ingestion`, `Run` |
| Schemas | `PascalCase` + suffix | `IngestionCreate`, `IngestionResponse` |
| Services | `PascalCase` + `Service` | `IngestionService` |
| Repositories | `PascalCase` + `Repository` | `IngestionRepository` |
| Enums | `PascalCase` | `IngestionStatus`, `RunStatus` |

### Functions

| Type | Convention | Example |
|------|------------|---------|
| Functions | `snake_case` | `create_ingestion()` |
| Private methods | `_snake_case` | `_generate_checkpoint()` |
| Async functions | `async snake_case` | `async def create_ingestion()` |

### Variables

| Type | Convention | Example |
|------|------------|---------|
| Variables | `snake_case` | `ingestion_id`, `checkpoint_location` |
| Constants | `UPPER_SNAKE_CASE` | `COST_PER_DBU_HOUR` |
| Private attributes | `_snake_case` | `self._db` |

## Finding Things

### "Where do I find...?"

| What | Where | File |
|------|-------|------|
| API endpoints | `app/api/v1/` | `ingestions.py`, `runs.py`, `clusters.py` |
| Database models | `app/models/` | `domain.py` |
| API schemas | `app/models/` | `schemas.py` |
| Business logic | `app/services/` | `*_service.py` |
| Database queries | `app/repositories/` | `*_repository.py` |
| Spark operations | `app/spark/` | `connect_client.py` |
| Configuration | `app/` | `config.py` |
| Database setup | `app/` | `database.py` |
| FastAPI app | `app/` | `main.py` |
| Migrations | `alembic/versions/` | `*.py` |
| Tests | `tests/e2e/` | `test_*.py` |

### "How do I...?"

| Task | Location | Reference |
|------|----------|-----------|
| Add a new endpoint | `app/api/v1/` | See `ingestions.py` |
| Add business logic | `app/services/` | See `ingestion_service.py` |
| Add database query | `app/repositories/` | See `ingestion_repository.py` |
| Add database table | Create migration | [Alembic Guide](./alembic-guide.md) |
| Add configuration | `app/config.py` | [Configuration Management](./configuration-management.md) |
| Add validation | `app/models/schemas.py` | [Pydantic Usage](./pydantic-usage.md) |
| Run tests | `pytest tests/` | [Running Tests](./running-integration-tests-in-pycharm.md) |

## Architecture Decisions

### Why Layered Architecture?

**Benefits:**
- Clear separation of concerns
- Testable components
- Easier to maintain
- Scalable structure

**Trade-offs:**
- More files/boilerplate
- Learning curve for new developers

### Why Repository Pattern?

**Benefits:**
- Abstract data access
- Easy to test (mock repositories)
- Centralized query logic
- Can swap databases

**Trade-offs:**
- Additional abstraction layer
- More code for simple CRUD

### Why Spark Connect?

**Benefits:**
- No job submission complexity
- Better observability
- Simplified architecture
- Direct DataFrame operations

**Trade-offs:**
- Requires Spark 3.4+
- Network latency

**See:** `docs/ingestion-prd-using-connect.md`

## Package Dependencies

### Core Dependencies

```
FastAPI 0.109.0        # Web framework
SQLAlchemy 2.0.25      # ORM
Pydantic 2.5.3         # Validation
Alembic 1.13.1         # Migrations
PySpark 3.5.0          # Spark Connect
```

**See:** `requirements.txt` for full list

### Dependency Graph

```
main.py
 ├── api/v1/ingestions.py
 │   ├── services/ingestion_service.py
 │   │   ├── repositories/ingestion_repository.py
 │   │   │   └── models/domain.py
 │   │   ├── spark/connect_client.py
 │   │   └── services/cost_estimator.py
 │   ├── models/schemas.py
 │   └── database.py
 └── config.py
```

## Development Workflow

### Adding a New Feature

1. **Plan** - Design API, data model, business logic
2. **Database** - Create migration if needed (`alembic revision`)
3. **Models** - Add/update domain models and schemas
4. **Repository** - Add data access methods
5. **Service** - Implement business logic
6. **API** - Add endpoint
7. **Test** - Write integration tests
8. **Document** - Update relevant docs

### Example: Adding "Pause Ingestion" Feature

```
1. Design API endpoint: POST /api/v1/ingestions/{id}/pause
2. No DB changes needed (status field exists)
3. Update schemas.py: IngestionUpdate allows status change
4. Add repository method: ingestion_repository.update_status()
5. Add service method: ingestion_service.pause_ingestion()
6. Add API endpoint: api/v1/ingestions.py:pause_ingestion()
7. Write test: tests/e2e/test_pause_ingestion.py
```

## Related Documentation

- [Pydantic Usage](./pydantic-usage.md) - API schemas and validation
- [Configuration Management](./configuration-management.md) - Settings and config
- [Alembic Guide](./alembic-guide.md) - Database migrations
- [Running Tests](./running-integration-tests-in-pycharm.md) - Testing guide

## Troubleshooting

### "Where should this code go?"

**Decision Tree:**

```
Is it an HTTP endpoint?
  → app/api/v1/

Is it business logic?
  → app/services/

Is it a database query?
  → app/repositories/

Is it a data structure?
  → app/models/ (domain.py or schemas.py)

Is it Spark-related?
  → app/spark/

Is it configuration?
  → app/config.py

Is it a utility?
  → scripts/ or create app/utils/
```

### Common Mistakes

**❌ Don't:**
- Put business logic in API endpoints
- Put database queries in services
- Import services in repositories
- Use relative imports

**✅ Do:**
- Follow the layered architecture
- Use dependency injection
- Use absolute imports
- Keep layers separate

## Summary

The IOMETE Autoloader follows a clean layered architecture:

- **`app/api/`** - HTTP endpoints and routing
- **`app/services/`** - Business logic and orchestration
- **`app/repositories/`** - Data access abstraction
- **`app/models/`** - Data structures (ORM + Pydantic)
- **`app/spark/`** - Spark Connect integration
- **`tests/`** - Automated testing
- **`docs/`** - Documentation
- **`alembic/`** - Database migrations

Each layer has a specific responsibility, and dependencies flow downward from API → Service → Repository → Database.
