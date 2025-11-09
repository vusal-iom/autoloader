# Integration Testing Strategy

## Overview

This document defines the integration testing approach for IOMETE Autoloader, positioned between unit tests and end-to-end tests to validate component interactions while maintaining test speed and reliability.

## Test Classification

### End-to-End Tests (Existing)
- **Scope**: Full user workflows from API request to final state
- **Mocking**: None - all real components
- **Database**: Real (PostgreSQL in test environment)
- **External Systems**: Real (Spark Connect, Prefect, Cloud Storage)
- **Purpose**: Validate complete business scenarios
- **Speed**: Slow (minutes)
- **Location**: `tests/e2e/`

### Integration Tests (Proposed)
- **Scope**: Specific component interactions and integration points
- **Mocking**: Strategic - mock non-critical or expensive external calls
- **Database**: Real (PostgreSQL/SQLite test database)
- **External Systems**: Selective mocking based on test focus
- **Purpose**: Validate layer boundaries and external integrations
- **Speed**: Medium (seconds to tens of seconds)
- **Location**: `tests/integration/`

### Unit Tests (Future)
- **Scope**: Individual functions and classes
- **Mocking**: Heavy - mock all dependencies
- **Database**: Mocked
- **External Systems**: All mocked
- **Purpose**: Validate business logic in isolation
- **Speed**: Fast (milliseconds)

## Integration Test Principles

### What to Test
1. **Layer Interactions**: API → Service → Repository → Database
2. **External System Integration**: Prefect deployment creation, Spark Connect session management
3. **State Transitions**: Ingestion status changes, run lifecycle
4. **Data Persistence**: Database writes and reads across layers
5. **Authentication & Authorization**: Real middleware and security checks
6. **Schema Validation**: Pydantic models and database constraints

### What to Mock
1. **Expensive Operations**:
   - Cloud storage file listing (thousands of files)
   - Large file downloads
   - Actual Spark data processing

2. **External Service Variability**:
   - Cloud provider APIs (S3, Azure Blob, GCS)
   - Time-dependent operations (for deterministic tests)

3. **Non-Critical Dependencies**:
   - When testing Prefect integration, mock Spark
   - When testing Spark integration, mock Prefect
   - When testing database operations, mock both

### What NOT to Mock
1. **FastAPI application and routing**
2. **Database layer** (use test database instead)
3. **SQLAlchemy ORM operations**
4. **Pydantic validation**
5. **Core business logic in services**
6. **The component under test**

## Test Structure

### Directory Organization

```
tests/
├── e2e/                                    # Existing end-to-end tests
│   ├── test_s3_ingestion_flow.py
│   └── ...
│
├── integration/                            # New integration tests
│   ├── conftest.py                        # Shared fixtures
│   ├── test_ingestion_prefect_integration.py
│   ├── test_run_spark_integration.py
│   ├── test_schema_evolution.py
│   ├── test_file_state_tracking.py
│   └── test_cost_estimation.py
│
└── unit/                                   # Future unit tests
    └── ...
```

### Fixture Strategy

```python
# tests/integration/conftest.py

@pytest.fixture
def test_db():
    """Real test database with clean state"""
    # Setup test database
    # Run migrations
    # Yield session
    # Teardown and cleanup

@pytest.fixture
def api_client(test_db):
    """TestClient with real database"""
    # Return FastAPI TestClient

@pytest.fixture
def mock_prefect_client():
    """Mocked Prefect client for tests not focusing on Prefect"""
    # Return MagicMock with common operations

@pytest.fixture
def mock_spark_connect():
    """Mocked Spark Connect for tests not focusing on Spark"""
    # Return MagicMock

@pytest.fixture
def mock_s3_client():
    """Mocked S3 client with predictable responses"""
    # Return boto3 mock

@pytest.fixture
def sample_files_metadata():
    """Sample file metadata for testing without real cloud storage"""
    # Return list of file metadata dicts
```

## Proposed Integration Test Scenarios

### 1. Ingestion Creation with Prefect Deployment Validation

**Test Focus**: Verify that creating an ingestion via API correctly creates database records AND triggers Prefect deployment creation.

**What's Real**:
- FastAPI endpoint: `POST /api/v1/ingestions`
- Service layer: `ingestion_service.py`
- Repository layer: `ingestion_repository.py`
- Database: Real test database
- Prefect client: Real or partially real

**What's Mocked**:
- Cloud storage validation (S3 bucket access check)
- File discovery/listing

**Test Flow**:
```python
def test_create_ingestion_creates_prefect_deployment(api_client, mock_s3_client):
    """
    GIVEN: Valid ingestion configuration
    WHEN: POST /api/v1/ingestions is called
    THEN:
        - Ingestion record exists in database with DRAFT status
        - Prefect deployment is created with correct schedule
        - Deployment name matches ingestion ID
        - Deployment tags include tenant_id and ingestion_id
    """
```

**Assertions**:
- Database has ingestion record
- Ingestion status is DRAFT
- Prefect deployment exists (query Prefect API or check mock calls)
- Deployment schedule matches cron expression
- Checkpoint path is correctly generated

**Value**: Validates critical integration between API layer and Prefect orchestration without full file processing.

---

### 2. Run Execution with File State Tracking

**Test Focus**: Verify that triggering a run processes files and correctly tracks state in the database.

**What's Real**:
- FastAPI endpoint: `POST /api/v1/ingestions/{id}/run`
- Batch orchestrator: `batch_orchestrator.py`
- File state service: `file_state_service.py`
- Repository layer: All repositories
- Database: Real test database

**What's Mocked**:
- Cloud storage file listing (return predefined sample files)
- Spark Connect execution (simulate successful processing)
- Actual file content reading

**Test Flow**:
```python
def test_run_execution_tracks_file_states(api_client, mock_s3_client, mock_spark_connect, sample_files_metadata):
    """
    GIVEN: Active ingestion with sample files in source
    WHEN: Run is triggered via API
    THEN:
        - Run record created with IN_PROGRESS status
        - Files discovered and marked as PENDING
        - Mock Spark processes files successfully
        - Files marked as PROCESSED with timestamps
        - Run status updated to COMPLETED
        - Run statistics (files_processed, bytes_processed) are accurate
    """
```

**Assertions**:
- Run record exists with correct status transitions
- Processed files tracked in `processed_files` table
- File states transition: PENDING → PROCESSING → PROCESSED
- Statistics match expected values
- No files marked as FAILED

**Value**: Validates the orchestration layer and state management without expensive Spark operations.

---

### 3. Schema Evolution Detection and Versioning

**Test Focus**: Verify that schema changes in source files are detected and tracked across runs.

**What's Real**:
- Service layer: `batch_orchestrator.py`, `spark_service.py`
- Repository layer: `ingestion_repository.py`
- Database: Real test database with schema_versions table

**What's Mocked**:
- Cloud storage (return files with different schemas)
- Spark schema inference (return predefined schemas)

**Test Flow**:
```python
def test_schema_evolution_creates_new_version(api_client, mock_s3_client, mock_spark_connect):
    """
    GIVEN: Ingestion with existing schema version
    WHEN: New files with additional columns are discovered
    THEN:
        - New schema version created in database
        - Version number incremented
        - Schema changes logged (added columns, type changes)
        - Ingestion config updated to use new schema
        - Alert/notification triggered (if configured)
    """
```

**Assertions**:
- Multiple schema versions exist for same ingestion
- Version numbers sequential
- Schema differences correctly captured
- Ingestion references latest schema version

**Value**: Tests critical data quality feature without full Spark execution.

---

### 4. Cost Estimation Accuracy

**Test Focus**: Verify cost estimation calculations are accurate based on file metadata and processing parameters.

**What's Real**:
- Service layer: `cost_estimator.py`
- Repository layer: For fetching ingestion config
- Database: Real test database

**What's Mocked**:
- File discovery (return controlled file sizes and counts)
- Cloud storage API calls

**Test Flow**:
```python
def test_cost_estimation_calculation_accuracy(api_client, mock_s3_client, sample_files_metadata):
    """
    GIVEN: Ingestion configuration with known cluster costs
    WHEN: Cost estimation is requested
    THEN:
        - File count and total size calculated correctly
        - Processing time estimated based on throughput rates
        - Cluster cost calculated (time * hourly rate)
        - Storage cost estimated
        - Total cost within expected range
        - Cost breakdown includes all components
    """
```

**Assertions**:
- Estimated file count matches mock data
- Estimated total size matches mock data
- Processing time formula correct (based on throughput config)
- Cost components (compute, storage, transfer) calculated
- Total cost = sum of components

**Value**: Validates business logic for cost transparency without cloud API costs.

---

### 5. Error Handling and Run Retry Logic

**Test Focus**: Verify that failed runs can be retried and errors are properly tracked.

**What's Real**:
- FastAPI endpoints: `POST /api/v1/runs/{id}/retry`
- Service layer: Run retry logic
- Repository layer: Run updates
- Database: Real test database

**What's Mocked**:
- Spark Connect (simulate failures and success on retry)
- Cloud storage

**Test Flow**:
```python
def test_failed_run_retry_with_error_tracking(api_client, mock_s3_client, mock_spark_connect):
    """
    GIVEN: Run that failed with specific error
    WHEN: Retry is triggered via API
    THEN:
        - Original run status remains FAILED
        - New run record created linked to original
        - Retry counter incremented
        - Error message preserved in original run
        - New run processes successfully
        - File state correctly restored for retry
        - Only unprocessed files reprocessed
    """
```

**Assertions**:
- Failed run record unchanged
- New run created with retry reference
- Retry count accurate
- File state handling correct (skip already processed)
- Error logging comprehensive

**Value**: Tests critical resilience feature and error handling paths.

---

## Test Database Strategy

### Option 1: SQLite (Fast, Isolated)
- **Pros**: Fast setup/teardown, no external dependencies
- **Cons**: May have dialect differences from PostgreSQL
- **Use case**: When testing pure business logic

### Option 2: PostgreSQL (Production-Like)
- **Pros**: Exact production behavior, tests PostgreSQL-specific features
- **Cons**: Slower, requires PostgreSQL running
- **Use case**: When testing database-specific features (JSON columns, constraints)

### Recommendation
- Use PostgreSQL for integration tests (consistency with e2e and production)
- Each test gets isolated schema or database
- Cleanup after each test

## Mocking Strategy

### Preferred Mocking Tools
1. **unittest.mock** (standard library)
2. **pytest-mock** (pytest plugin)
3. **responses** (for HTTP mocking if needed)

### Mocking Patterns

```python
# Mock at the right boundary
@pytest.fixture
def mock_s3_client(monkeypatch):
    """Mock S3 client at the service boundary"""
    mock_client = MagicMock()
    mock_client.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'file1.json', 'Size': 1024, 'LastModified': datetime.now()},
            {'Key': 'file2.json', 'Size': 2048, 'LastModified': datetime.now()},
        ]
    }

    # Patch where it's used, not where it's defined
    monkeypatch.setattr('app.services.file_discovery_service.boto3.client', lambda *args, **kwargs: mock_client)
    return mock_client
```

## Running Integration Tests

### Pytest Configuration

```ini
# pytest.ini or pyproject.toml
[tool.pytest.ini_options]
markers = [
    "integration: Integration tests (deselect with '-m \"not integration\"')",
    "e2e: End-to-end tests",
    "slow: Slow tests",
]
```

### Command Examples

```bash
# Run only integration tests
pytest tests/integration -v

# Run integration tests with coverage
pytest tests/integration --cov=app --cov-report=html

# Run specific integration test
pytest tests/integration/test_ingestion_prefect_integration.py -v

# Skip integration tests (for CI quick checks)
pytest -m "not integration"

# Run both integration and e2e
pytest tests/integration tests/e2e -v
```

## Success Criteria

An integration test should:
1. **Run in isolation**: No dependencies on other tests
2. **Be deterministic**: Same input = same output every time
3. **Be fast**: Complete in seconds, not minutes
4. **Test one integration point**: Focus on specific component interaction
5. **Have clear assertions**: Test specific, measurable outcomes
6. **Clean up after itself**: No leftover database state
7. **Fail clearly**: Error messages indicate what went wrong

## Next Steps

### Phase 1: Setup (This Session)
1. Create `tests/integration/` directory structure
2. Set up `conftest.py` with shared fixtures
3. Document mocking patterns
4. Implement first integration test (#1: Ingestion + Prefect)

### Phase 2: Core Tests
1. Implement remaining 4 integration tests
2. Validate test performance (should be <5s each)
3. Add to CI pipeline

### Phase 3: Expansion
1. Identify additional integration test scenarios
2. Add tests for edge cases
3. Monitor test stability and flakiness

### Phase 4: Documentation
1. Update README with testing strategy
2. Document how to run tests locally
3. Create testing best practices guide

## Questions for Discussion

1. **Database Choice**: SQLite for speed or PostgreSQL for accuracy?
2. **Prefect Testing**: Use real Prefect instance or mock client?
3. **Spark Testing**: Mock all Spark calls or use Spark Connect in test mode?
4. **Test Data**: Fixtures vs. factories vs. seed data?
5. **CI Integration**: Run integration tests on every commit or only on PR?

---

## Comprehensive Test Coverage Matrix

Based on analysis of the actual implementation in `app/`, here is a complete catalog of potential tests organized by type.

### Legend
- **Priority**: H (High), M (Medium), L (Low)
- **Complexity**: Simple, Medium, Complex
- **Test Type**: Unit, Integration, E2E

---

### API Layer Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 1 | Integration | `POST /ingestions` | Create ingestion with valid config | FastAPI, Services, Repositories, Database | S3 client, Spark Connect | H | Medium |
| 2 | Integration | `POST /ingestions` | Create ingestion creates Prefect deployment | FastAPI, Services, Repositories, Database, Prefect client | S3 client, Spark Connect | H | Medium |
| 3 | Integration | `POST /ingestions` | Create ingestion generates checkpoint path correctly | FastAPI, Services, Repositories, Database | S3, Spark, Prefect | M | Simple |
| 4 | Unit | `POST /ingestions` | Validation rejects invalid source_path | Pydantic validation | All services | M | Simple |
| 5 | Unit | `POST /ingestions` | Validation rejects unsupported source_type (Azure/GCS) | Pydantic validation | All services | M | Simple |
| 6 | Integration | `GET /ingestions` | List ingestions filters by tenant_id | FastAPI, Services, Repositories, Database | - | H | Simple |
| 7 | Integration | `GET /ingestions/{id}` | Get single ingestion returns 404 for non-existent | FastAPI, Services, Repositories, Database | - | M | Simple |
| 8 | Integration | `PUT /ingestions/{id}` | Update schedule updates Prefect deployment | FastAPI, Services, Repositories, Database, Prefect | S3, Spark | H | Medium |
| 9 | Integration | `PUT /ingestions/{id}` | Update rejects changes to source_path (restricted field) | FastAPI, Services, Repositories, Database | All external | M | Simple |
| 10 | Integration | `DELETE /ingestions/{id}` | Delete ingestion with cascade deletes processed_files | FastAPI, Services, Repositories, Database | Prefect, Spark | H | Medium |
| 11 | Integration | `DELETE /ingestions/{id}` | Delete ingestion with delete_table=true drops Iceberg table | FastAPI, Services, Repositories, Database, Spark | S3, Prefect | M | Medium |
| 12 | Integration | `POST /ingestions/{id}/run` | Manual run creates Prefect flow run | FastAPI, Services, Repositories, Database, Prefect | S3, Spark | H | Medium |
| 13 | Integration | `POST /ingestions/{id}/pause` | Pause ingestion updates status and preserves checkpoint | FastAPI, Services, Repositories, Database | All external | M | Simple |
| 14 | Integration | `POST /ingestions/{id}/resume` | Resume ingestion updates status to ACTIVE | FastAPI, Services, Repositories, Database | All external | M | Simple |
| 15 | Integration | `DELETE /ingestions/{id}/processed-files` | Clear processed files enables re-ingestion | FastAPI, Services, Repositories, Database | All external | M | Simple |
| 16 | Integration | `POST /ingestions/test` | Preview returns inferred schema and sample data | FastAPI, Services, S3 client (mocked files) | Spark (return mocked schema) | H | Medium |
| 17 | Integration | `POST /ingestions/estimate-cost` | Cost estimation calculates accurate breakdown | FastAPI, CostEstimator, S3 (mocked) | All external | M | Medium |
| 18 | Integration | `GET /ingestions/{id}/runs` | List runs filters by date range (since parameter) | FastAPI, Services, Repositories, Database | - | M | Simple |
| 19 | Integration | `POST /ingestions/{id}/runs/{run_id}/retry` | Retry failed run creates new run record | FastAPI, Services, Repositories, Database, Prefect | S3, Spark | H | Medium |
| 20 | Integration | `POST /ingestions/{id}/refresh/full` | Full refresh drops table + clears files + triggers run | FastAPI, RefreshService, Repositories, Database, Spark, Prefect | S3 | H | Complex |
| 21 | Integration | `POST /ingestions/{id}/refresh/new-only` | New-only refresh drops table + keeps file history | FastAPI, RefreshService, Repositories, Database, Spark | S3, Prefect | H | Complex |
| 22 | Integration | `POST /ingestions/{id}/refresh/*` | Dry-run returns preview without executing operations | FastAPI, RefreshService, Repositories, Database | All external | M | Medium |
| 23 | Unit | `POST /ingestions/{id}/refresh/*` | Refresh requires confirm=true or returns 400 | Pydantic validation | All services | M | Simple |

---

### Service Layer Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 24 | Unit | `IngestionService` | create_ingestion generates unique checkpoint path | Service logic | Repositories, Spark, Prefect, S3 | H | Simple |
| 25 | Integration | `IngestionService` | create_ingestion saves to DB via repository | Service, Repository, Database | Spark, Prefect, S3 | H | Medium |
| 26 | Integration | `IngestionService` | create_ingestion creates Prefect deployment when scheduled | Service, Repository, Database, Prefect | Spark, S3 | H | Medium |
| 27 | Unit | `IngestionService` | list_ingestions filters by tenant_id | Service logic | Repository (return mock data) | M | Simple |
| 28 | Integration | `IngestionService` | delete_ingestion calls Spark drop_table when delete_table=true | Service, SparkService | Repository, Prefect | M | Medium |
| 29 | Unit | `FileDiscoveryService` | list_files handles S3 pagination correctly | Service logic with mocked S3 | S3 client (paginated responses) | H | Medium |
| 30 | Unit | `FileDiscoveryService` | list_files filters by modified date (since parameter) | Service logic | S3 client | M | Medium |
| 31 | Unit | `FileDiscoveryService` | list_files respects max_files limit | Service logic | S3 client | M | Simple |
| 32 | Unit | `FileDiscoveryService` | list_files raises ValueError for Azure/GCS (Phase 1) | Service validation | - | M | Simple |
| 33 | Unit | `FileDiscoveryService` | list_files handles S3 ClientError gracefully | Service error handling | S3 client (raises exception) | H | Medium |
| 34 | Integration | `FileStateService` | lock_file_for_processing is atomic (concurrent workers) | Service, Repository, Database | - | H | Complex |
| 35 | Integration | `FileStateService` | lock_file_for_processing updates file metadata (size, etag) | Service, Repository, Database | - | M | Simple |
| 36 | Integration | `FileStateService` | get_processed_files returns only SUCCESS/SKIPPED by default | Service, Repository, Database | - | M | Simple |
| 37 | Integration | `FileStateService` | get_failed_files filters by max_retries | Service, Repository, Database | - | M | Simple |
| 38 | Integration | `FileStateService` | clear_processed_files deletes all records for ingestion | Service, Repository, Database | - | H | Simple |
| 39 | Unit | `BatchFileProcessor` | process_files calls _process_single_file for each file | Service logic | Spark, FileStateService | M | Medium |
| 40 | Integration | `BatchFileProcessor` | process_files locks files atomically before processing | Service, FileStateService, Repository, Database | Spark | H | Medium |
| 41 | Integration | `BatchFileProcessor` | process_files marks files as SUCCESS with metrics | Service, FileStateService, Repository, Database | Spark (return mock metrics) | H | Medium |
| 42 | Integration | `BatchFileProcessor` | process_files marks files as FAILED on Spark error | Service, FileStateService, Repository, Database | Spark (raise exception) | H | Medium |
| 43 | Integration | `BatchFileProcessor` | process_files returns accurate metrics summary | Service, FileStateService, Repository, Database | Spark | M | Medium |
| 44 | Unit | `CostEstimator` | estimate calculates compute cost correctly | Service logic | File discovery | M | Simple |
| 45 | Unit | `CostEstimator` | estimate calculates runs_per_month from schedule frequency | Service logic | File discovery | M | Simple |
| 46 | Unit | `CostEstimator` | estimate adjusts processing time based on format (JSON vs Parquet) | Service logic | File discovery | M | Medium |
| 47 | Integration | `SparkService` | test_connection succeeds with valid Spark Connect URL | Service, Spark Connect | - | H | Medium |
| 48 | Integration | `SparkService` | test_connection returns error with invalid credentials | Service, Spark Connect | - | M | Medium |
| 49 | Integration | `SparkService` | drop_table executes DROP TABLE via Spark | Service, Spark Connect | - | M | Medium |
| 50 | Integration | `PrefectService` | initialize connects to Prefect server successfully | Service, Prefect API | - | H | Medium |
| 51 | Integration | `PrefectService` | create_deployment creates deployment with correct schedule | Service, Prefect API | - | H | Medium |
| 52 | Integration | `PrefectService` | create_deployment sets correct work queue based on size | Service, Prefect API | - | M | Medium |
| 53 | Integration | `PrefectService` | create_deployment includes correct tags (tenant, source) | Service, Prefect API | - | M | Simple |
| 54 | Integration | `PrefectService` | update_deployment_schedule updates existing deployment | Service, Prefect API | - | M | Medium |
| 55 | Integration | `PrefectService` | trigger_flow_run returns flow run ID | Service, Prefect API | - | H | Medium |
| 56 | Integration | `PrefectService` | delete_deployment removes deployment from Prefect | Service, Prefect API | - | M | Simple |
| 57 | Integration | `RefreshService` | refresh validates confirm=true before execution | Service, Repository, Database | Spark, FileState, Ingestion | M | Simple |
| 58 | Integration | `RefreshService` | refresh full mode clears processed files | Service, FileStateService, Repository, Database | Spark, Prefect | H | Medium |
| 59 | Integration | `RefreshService` | refresh new_only mode keeps processed file history | Service, FileStateService, Repository, Database | Spark, Prefect | H | Medium |
| 60 | Integration | `RefreshService` | refresh dry_run returns preview without executing | Service, Repository, Database | All external | M | Medium |
| 61 | Integration | `RefreshService` | refresh with auto_run=true triggers run after cleanup | Service, IngestionService, Repository, Database | Spark, Prefect | H | Medium |

---

### Repository Layer Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 62 | Integration | `IngestionRepository` | create inserts ingestion into database | Repository, Database | - | H | Simple |
| 63 | Integration | `IngestionRepository` | get_by_id returns ingestion or None | Repository, Database | - | H | Simple |
| 64 | Integration | `IngestionRepository` | get_by_tenant filters by tenant_id correctly | Repository, Database | - | H | Simple |
| 65 | Integration | `IngestionRepository` | get_active_ingestions returns only ACTIVE status | Repository, Database | - | M | Simple |
| 66 | Integration | `IngestionRepository` | update persists changes and updates updated_at timestamp | Repository, Database | - | M | Simple |
| 67 | Integration | `IngestionRepository` | delete removes ingestion from database | Repository, Database | - | M | Simple |
| 68 | Integration | `IngestionRepository` | update_status transitions ingestion status | Repository, Database | - | M | Simple |
| 69 | Integration | `IngestionRepository` | update_last_run updates run tracking fields | Repository, Database | - | M | Simple |
| 70 | Integration | `RunRepository` | create inserts run into database | Repository, Database | - | H | Simple |
| 71 | Integration | `RunRepository` | get_runs_by_ingestion filters by date range | Repository, Database | - | M | Simple |
| 72 | Integration | `RunRepository` | get_runs_by_ingestion respects limit parameter | Repository, Database | - | M | Simple |
| 73 | Integration | `RunRepository` | update_status calculates duration_seconds when ended_at provided | Repository, Database | - | M | Simple |
| 74 | Integration | `RunRepository` | update_metrics updates run metrics fields | Repository, Database | - | M | Simple |
| 75 | Integration | `ProcessedFileRepository` | lock_file_for_processing is atomic (no duplicate locks) | Repository, Database | - | H | Complex |
| 76 | Integration | `ProcessedFileRepository` | lock_file_for_processing creates new record if not exists | Repository, Database | - | H | Medium |
| 77 | Integration | `ProcessedFileRepository` | lock_file_for_processing updates existing record | Repository, Database | - | H | Medium |
| 78 | Integration | `ProcessedFileRepository` | get_processed_file_paths returns set for O(1) lookup | Repository, Database | - | M | Simple |
| 79 | Integration | `ProcessedFileRepository` | get_failed_files filters by status and retry_count | Repository, Database | - | M | Simple |
| 80 | Integration | `ProcessedFileRepository` | get_stale_processing_files finds timed-out files | Repository, Database | - | M | Medium |
| 81 | Integration | `ProcessedFileRepository` | mark_success updates status and metrics | Repository, Database | - | H | Simple |
| 82 | Integration | `ProcessedFileRepository` | mark_failed increments retry_count | Repository, Database | - | H | Simple |
| 83 | Integration | `ProcessedFileRepository` | mark_skipped updates status with reason | Repository, Database | - | M | Simple |
| 84 | Integration | `ProcessedFileRepository` | delete_by_ingestion removes all files for ingestion | Repository, Database | - | M | Simple |

---

### Spark Integration Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 85 | Integration | `SparkConnectClient` | connect establishes session with remote Spark Connect | Spark Connect | - | H | Medium |
| 86 | Integration | `SparkConnectClient` | connect reuses existing session if active | Spark Connect | - | M | Medium |
| 87 | Integration | `SparkConnectClient` | test_connection returns version and status | Spark Connect | - | H | Simple |
| 88 | Integration | `SparkConnectClient` | read_stream creates streaming DataFrame with cloudFiles | Spark Connect | - | H | Complex |
| 89 | Integration | `SparkConnectClient` | read_stream applies format_options correctly | Spark Connect | - | M | Medium |
| 90 | Integration | `SparkConnectClient` | write_stream writes to Iceberg with availableNow trigger | Spark Connect | - | H | Complex |
| 91 | Integration | `SparkConnectClient` | write_stream respects partition_columns | Spark Connect | - | M | Medium |
| 92 | Integration | `IngestionExecutor` | execute runs full ingestion workflow | Executor, Spark Connect, SessionPool | - | H | Complex |
| 93 | Integration | `IngestionExecutor` | execute returns metrics from StreamingQuery | Executor, Spark Connect | - | M | Medium |
| 94 | Integration | `IngestionExecutor` | execute returns client to pool after completion | Executor, SessionPool | Spark Connect (mock) | M | Medium |
| 95 | Integration | `IngestionExecutor` | preview returns schema and sample data | Executor, Spark Connect | - | H | Medium |
| 96 | Unit | `SessionPool` | get_client reuses existing client from pool | SessionPool logic | SparkConnectClient (mock) | M | Simple |
| 97 | Unit | `SessionPool` | get_client creates new client if pool empty | SessionPool logic | SparkConnectClient (mock) | M | Simple |
| 98 | Unit | `SessionPool` | return_client adds client back to pool | SessionPool logic | SparkConnectClient (mock) | M | Simple |
| 99 | Unit | `SessionPool` | return_client closes client if pool full | SessionPool logic | SparkConnectClient (mock) | M | Simple |
| 100 | Unit | `SessionPool` | cleanup_idle_sessions removes expired clients | SessionPool logic | SparkConnectClient (mock) | M | Medium |

---

### Prefect Flow Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 101 | Integration | `run_ingestion_flow` | Flow orchestrates complete ingestion workflow | Prefect flow, Tasks, Database | Spark, S3 | H | Complex |
| 102 | Integration | `run_ingestion_flow` | Flow returns NO_NEW_FILES when no files discovered | Prefect flow, Tasks, Database | S3 (empty), Spark | M | Medium |
| 103 | Integration | `run_ingestion_flow` | Flow calls fail_run_record_task on error | Prefect flow, Tasks, Database | S3, Spark (error) | H | Medium |
| 104 | Integration | `discover_files_task` | Task discovers files from S3 via FileDiscoveryService | Task, FileDiscoveryService, Database | S3 (mocked files) | H | Medium |
| 105 | Integration | `discover_files_task` | Task retries on S3 ClientError | Task, FileDiscoveryService | S3 (error then success) | M | Medium |
| 106 | Integration | `check_file_state_task` | Task returns processed file paths | Task, FileStateService, Database | - | M | Simple |
| 107 | Integration | `process_files_task` | Task processes files via BatchFileProcessor | Task, BatchFileProcessor, Database | Spark (mocked), S3 | H | Complex |
| 108 | Integration | `process_files_task` | Task respects timeout_seconds configuration | Task, BatchFileProcessor | Spark (slow), S3 | M | Medium |
| 109 | Integration | `create_run_record_task` | Task creates Run with RUNNING status | Task, RunRepository, Database | - | H | Simple |
| 110 | Integration | `complete_run_record_task` | Task updates Run with SUCCESS and metrics | Task, RunRepository, Database | - | H | Simple |
| 111 | Integration | `fail_run_record_task` | Task updates Run with FAILED and error message | Task, RunRepository, Database | - | H | Simple |

---

### Cross-Layer Integration Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 112 | Integration | API → Service → Repo | Create ingestion persists to DB and returns response | API, Services, Repositories, Database | Spark, Prefect, S3 | H | Medium |
| 113 | Integration | API → Service → Prefect | Create ingestion creates Prefect deployment | API, Services, Repositories, Database, Prefect | Spark, S3 | H | Medium |
| 114 | Integration | API → Service → Spark | Delete ingestion drops table via Spark | API, Services, Repositories, Database, Spark | S3, Prefect | M | Medium |
| 115 | Integration | Prefect → Service → Repo | Manual run creates flow run and Run record | Prefect, Services, Repositories, Database | Spark, S3 | H | Medium |
| 116 | Integration | Service → Repo → DB | FileStateService locks file atomically in DB | Services, Repositories, Database | - | H | Complex |
| 117 | Integration | Service → Spark → DB | BatchFileProcessor processes files and updates DB | Services, Spark, Repositories, Database | S3 | H | Complex |
| 118 | Integration | API → Service → DB | Refresh full mode clears files and triggers run | API, RefreshService, Services, Repositories, Database | Spark, Prefect | H | Complex |
| 119 | Integration | Service → S3 → DB | FileDiscoveryService discovers files and tracks state | Services, S3, Repositories, Database | Spark | M | Medium |
| 120 | Integration | Prefect Flow → All | Complete run workflow from discovery to completion | Prefect, All Services, Repositories, Database | Spark (mocked execution), S3 (mocked files) | H | Complex |

---

### End-to-End Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 121 | E2E | Full ingestion | User creates ingestion → scheduled run → files processed | ALL | NONE | H | Complex |
| 122 | E2E | Full ingestion | S3 files ingested into Iceberg table end-to-end | ALL | NONE | H | Complex |
| 123 | E2E | Schema evolution | New columns detected and tracked across runs | ALL | NONE | M | Complex |
| 124 | E2E | Error recovery | Failed run retried and succeeds | ALL | NONE | H | Complex |
| 125 | E2E | Refresh flow | Full refresh clears state and re-ingests all files | ALL | NONE | M | Complex |
| 126 | E2E | Pause/Resume | Ingestion paused, files accumulate, resume processes backlog | ALL | NONE | M | Complex |
| 127 | E2E | Multi-format | JSON, CSV, Parquet files ingested to different tables | ALL | NONE | M | Complex |
| 128 | E2E | Partitioning | Partitioned table created and data distributed correctly | ALL | NONE | L | Complex |
| 129 | E2E | Z-ordering | Z-ordered table optimized for query performance | ALL | NONE | L | Complex |
| 130 | E2E | Cost tracking | Estimated vs actual cost comparison across runs | ALL | NONE | L | Complex |

---

### Data Model & Validation Tests

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 131 | Unit | `IngestionCreate` | Pydantic validates required fields | Validation | - | M | Simple |
| 132 | Unit | `IngestionCreate` | Pydantic rejects invalid source_type | Validation | - | M | Simple |
| 133 | Unit | `IngestionCreate` | Pydantic rejects invalid format_type | Validation | - | M | Simple |
| 134 | Unit | `IngestionUpdate` | Pydantic allows only mutable fields | Validation | - | M | Simple |
| 135 | Unit | `RefreshRequest` | Pydantic validates confirm is boolean | Validation | - | M | Simple |
| 136 | Integration | `Ingestion` model | SQLAlchemy cascade deletes processed_files | ORM, Database | - | H | Medium |
| 137 | Integration | `Ingestion` model | SQLAlchemy updates updated_at on change | ORM, Database | - | M | Simple |
| 138 | Integration | `ProcessedFile` model | Indexes improve query performance | ORM, Database | - | M | Medium |
| 139 | Unit | `RunMetrics` property | Property aggregates metrics correctly | Model logic | - | M | Simple |

---

### Error Handling & Edge Cases

| # | Test Type | Component | Test Scenario | What's Real | What's Mocked | Priority | Complexity |
|---|-----------|-----------|---------------|-------------|---------------|----------|------------|
| 140 | Integration | `FileDiscoveryService` | S3 NoCredentialsError handled gracefully | Service | S3 (error) | H | Medium |
| 141 | Integration | `FileDiscoveryService` | S3 pagination handles large file lists (>1000 files) | Service | S3 (paginated) | M | Medium |
| 142 | Integration | `SparkConnectClient` | Connection timeout handled with retry | Spark Connect | - | H | Medium |
| 143 | Integration | `SparkConnectClient` | Invalid credentials return clear error | Spark Connect | - | M | Medium |
| 144 | Integration | `PrefectService` | Prefect API unavailable at startup logged as warning | Service | Prefect (unavailable) | M | Medium |
| 145 | Integration | `BatchFileProcessor` | Partial file processing failure doesn't stop batch | Service, Database | Spark (partial error), S3 | H | Complex |
| 146 | Integration | `FileStateService` | Concurrent lock attempts handled atomically | Service, Database | - | H | Complex |
| 147 | Integration | `ProcessedFileRepository` | Stale processing files detected and recovered | Repository, Database | - | M | Medium |
| 148 | Integration | API | Invalid tenant_id returns 404/403 | API, Services, Database | - | M | Simple |
| 149 | Integration | API | Missing required fields returns 422 validation error | API validation | - | M | Simple |
| 150 | Integration | `RefreshService` | Refresh without confirm=true returns 400 error | Service, API | - | M | Simple |

---

## Test Priority Summary

### High Priority (Must Implement First)
- Core API endpoint tests (create, run, delete ingestion)
- Prefect integration (deployment creation, flow runs)
- File state tracking (lock mechanism, status transitions)
- Database persistence (repositories with real DB)
- Spark Connect integration (connection, read/write)
- Error handling (S3 errors, Spark failures, retry logic)

**Estimated Count**: ~40 tests

### Medium Priority (Implement in Phase 2)
- Additional API endpoints (pause, resume, update)
- Service layer business logic (cost estimation, refresh operations)
- Repository edge cases (pagination, filtering)
- Prefect tasks (individual task testing)
- Session pooling and resource management

**Estimated Count**: ~60 tests

### Low Priority (Nice to Have)
- Advanced features (partitioning, z-ordering)
- Performance tests (large file sets, stress testing)
- Cost tracking accuracy over time
- Schema evolution edge cases

**Estimated Count**: ~20 tests

---

## Recommended Implementation Order

### Session 1: Foundation (5-8 tests)
1. Test #62: IngestionRepository.create (Integration)
2. Test #1: POST /ingestions basic creation (Integration)
3. Test #2: POST /ingestions creates Prefect deployment (Integration)
4. Test #75: ProcessedFileRepository.lock_file_for_processing atomicity (Integration)
5. Test #105: File discovery with S3 mock (Integration)

### Session 2: Run Workflow (6-8 tests)
6. Test #109: create_run_record_task (Integration)
7. Test #40: BatchFileProcessor locks files before processing (Integration)
8. Test #41: BatchFileProcessor marks files SUCCESS (Integration)
9. Test #42: BatchFileProcessor marks files FAILED (Integration)
10. Test #110: complete_run_record_task (Integration)
11. Test #101: run_ingestion_flow orchestration (Integration)

### Session 3: Refresh & Error Handling (6-8 tests)
12. Test #20: Full refresh workflow (Integration)
13. Test #21: New-only refresh workflow (Integration)
14. Test #140: S3 error handling (Integration)
15. Test #145: Partial failure handling (Integration)
16. Test #19: Retry failed run (Integration)

### Session 4: Additional Coverage (Ongoing)
- Service layer unit tests
- Additional API endpoint tests
- Edge cases and error scenarios
- E2E tests for complete workflows

---

## References

- Existing E2E Tests: `tests/e2e/`
- FastAPI Testing: https://fastapi.tiangolo.com/tutorial/testing/
- Pytest Fixtures: https://docs.pytest.org/en/stable/fixture.html
- SQLAlchemy Testing: https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites
