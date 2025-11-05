# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

IOMETE Autoloader is a zero-code, UI-driven (and API) data ingestion system that enables users to automatically load files from cloud storage (AWS S3, Azure Blob, GCS) into Apache Iceberg tables without writing Spark code.

**Key Features:**
- Scheduled batch ingestion (hourly, daily, custom cron) using Spark's availableNow trigger
- Multi-cloud support (S3, Azure Blob, GCS)
- Auto schema inference and evolution detection
- Cost estimation and transparency
- Preview/test mode before activation
- Real-time monitoring with run history
- UI configuration wizard
- Resource-efficient: cluster only used during scheduled execution windows


## User Request Context

### Original Problem Statement

For the jobs I'm writing, I import data from AWS S3 into our data lake. I have AWS, Snyk just dump their output there for now.
Right now, I write custom Spark code to list, fetch, and import these. This is quite a bit of work to properly test and even debug Spark.
What I'd really like is **a simple scheduled ingestion**: check an S3 bucket, Azure Blob, GCS, or FTP hourly, daily, or on whatever schedule, and automatically load new files into a table — no Spark code or extra effort from the user.
As Shafi mentioned: making this easy for such a common use case could boost usage in existing customers.
Fuad did point out we have a **marketplace streaming Job** for this, but this would be up and running all the time — while most of my files come in daily or less frequently — so a scheduled approach would save resources and simplify things.
Even then, the job makes the UX clunkier than just a simple UI to configure a file ingestion (which underneath can be a job).

*Note: This represents one example use case. IOMETE Autoloader is designed for generalized file ingestion across various sources and scenarios.*

## Tech Stack

**Backend:**
- FastAPI 0.109.0 - Web framework
- SQLAlchemy 2.0.25 - ORM
- Pydantic 2.5.3 - Data validation
- Alembic 1.13.1 - Database migrations
- Uvicorn 0.27.0 - ASGI server

**Spark Integration:**
- Apache PySpark with Spark Connect 3.5.0
- Apache Iceberg for destination tables

**Database:**
- PostgreSQL (production, e2e integration tests) with psycopg2
- SQLite (development, unit tests)

**Cloud SDKs:**
- AWS: boto3 1.34.34
- Azure: azure-storage-blob 12.19.0
- GCP: google-cloud-storage 2.14.0

**Security:**
- python-jose with cryptography
- passlib with bcrypt

**Utilities:**
- croniter 2.0.1 - Cron expression parsing
- httpx 0.26.0 - HTTP client
- python-dotenv 1.0.0 - Environment config
- prometheus-client 0.19.0 - Metrics

**Testing:**
- pytest 7.4.4, pytest-asyncio 0.23.3, pytest-cov 4.1.0

## Project Structure

```
app/
├── main.py                           # FastAPI application entry point
├── config.py                         # Settings management (Pydantic BaseSettings)
├── database.py                       # Database setup and session management
│
├── api/v1/                          # API endpoints (versioned)
│   ├── ingestions.py                # Ingestion CRUD + run/pause/resume
│   ├── runs.py                      # Run history and retry
│   └── clusters.py                  # Cluster management and testing
│
├── models/
│   ├── domain.py                    # SQLAlchemy ORM models (Ingestion, Run, SchemaVersion)
│   └── schemas.py                   # Pydantic schemas for API requests/responses
│
├── services/                        # Business logic layer
│   ├── ingestion_service.py         # Ingestion management
│   ├── spark_service.py             # Spark operations wrapper
│   ├── cost_estimator.py            # Cost calculation service
│   ├── file_discovery_service.py    # File discovery and listing from cloud storage
│   ├── file_state_service.py        # File state tracking (processed/pending)
│   ├── batch_file_processor.py      # Batch file processing logic
│   └── batch_orchestrator.py        # Orchestrates batch ingestion runs
│
├── repositories/                    # Data access layer
│   ├── ingestion_repository.py      # Ingestion data access
│   ├── run_repository.py            # Run history data access
│   └── processed_file_repository.py # Processed file tracking data access
│
└── spark/                           # Spark Connect integration
    ├── connect_client.py            # Spark Connect client
    ├── executor.py                  # Ingestion executor
    └── session_manager.py           # Session pooling
```


## Architecture Patterns

**Layered Architecture:**
```
API Layer (FastAPI routes)
    ↓
Service Layer (Business logic)
    ↓
Repository Layer (Data access)
    ↓
Database (SQLAlchemy ORM)

Parallel: Spark Connect (Remote execution)
```

**Separation of Concerns:**
- **Domain models** (SQLAlchemy) for database persistence
- **API schemas** (Pydantic) for request/response validation
- **Services** for business logic
- **Repositories** for data access abstraction
- **Dependency injection** for database sessions

**Key Design Decisions:**
- Spark Connect instead of job submission for simplified architecture and better observability
- Repository pattern for clean data access
- Enums for type safety (IngestionStatus, SourceType, FormatType, etc.)
- Encrypted credentials stored as JSON
- Checkpoint location auto-generated per ingestion

## Configuration (app/config.py)

**Environment Variables:**
- `APP_NAME`, `VERSION`, `DEBUG`
- `DATABASE_URL` (default: SQLite, production: PostgreSQL)
- `SECRET_KEY`, `ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES`
- `SPARK_CONNECT_DEFAULT_PORT`: 15002
- `SPARK_SESSION_POOL_SIZE`, `SPARK_SESSION_IDLE_TIMEOUT`
- `CHECKPOINT_BASE_PATH`: /tmp/iomete-autoloader/checkpoints
- `PREVIEW_MAX_FILES`, `PREVIEW_MAX_ROWS`
- Cost estimation rates
- Scheduler intervals

## Development Guidelines

**When Working with This Codebase:**

1. **Follow the Repository Pattern:** Always access data through repositories, not directly via SQLAlchemy
2. **Use Dependency Injection:** Database sessions should be injected via `Depends(get_db)`
3. **Respect Layer Boundaries:** API → Service → Repository → Database
4. **Use Type Hints:** All functions should have proper type annotations
5. **Pydantic for API:** Use schemas.py models for request/response validation
6. **SQLAlchemy for DB:** Use domain.py models for database operations
7. **Async Where Appropriate:** API endpoints are async, repository methods are sync
8. **Error Handling:** Use HTTPException with appropriate status codes
9. **Testing:** Write tests in pytest with fixtures for database sessions
10. **TODOs:** Check for TODO comments indicating incomplete implementations

**Important Patterns:**
- Tenant isolation via `tenant_id` filtering
- Credentials stored encrypted in JSON format
- Status transitions: DRAFT → ACTIVE → PAUSED/ERROR
- Async endpoints returning 202 ACCEPTED for long-running operations
- Preview mode for testing configurations before activation

## Documentation

See documentation under `docs/batch-processing/`:
- `batch-processing-implementation-guide.md` - Comprehensive implementation guide
- `phase1-s3-implementation-guide.md` - Phase 1 S3 implementation details

## End to end (e2e) integration tests

These tests are intended to be **end-to-end**.

1. **API-only interactions**
   * Do **not** manipulate the database directly.
   * All operations and verifications must be performed **through the API**.

2. **Test data setup**
   * It’s acceptable to create **test files in MinIO** before running the test.
   * However, all outcome validation must happen via **API responses**, not by inspecting the database.

3. **Single test focus**
   * Each time, work on **only one test case**.
   * Avoid handling multiple test methods at once — it makes reviews harder for users.

4. **Collaborative workflow**
   * When writing a new test or making a significant change, **first show the proposed changes** to the user.
   * **Discuss and confirm** that the approach is correct before applying the modifications.
   * **Never make changes directly** without prior agreement.

5. **Test execution**
   * Use **pytest** to run and verify that each test actually works as expected.