# Testing Strategy: E2E vs Unit Tests

**Document Version:** 1.0
**Created:** 2025-01-05
**Status:** Active
**Owner:** Engineering Team

## Purpose

This document defines the testing strategy for IOMETE Autoloader, specifically addressing the critical question: **What needs end-to-end (e2e) testing vs unit/integration testing?**

The goal is to balance comprehensive test coverage with maintainability, speed, and cost.

---

## Testing Pyramid

```
        /\
       /  \        E2E Tests (5-7 tests)
      /____\       - Slow, expensive, brittle
     /      \      - Real infrastructure (Spark, S3, Iceberg)
    /        \     - Integration points only
   /__________\    - 10-15 minutes total
  /            \
 /              \  Unit/Integration Tests (100s of tests)
/________________\ - Fast, cheap, stable
                   - Mocked dependencies
                   - Business logic focus
                   - <1 minute total
```

**Key Principle:** Use the most lightweight test that can validate the behavior.

---

## E2E Tests: What Belongs Here

### Decision Criteria

An e2e test is justified when **ALL** of these are true:

1. **Complex integration point** - Multiple real systems must interact (Spark + S3 + Iceberg)
2. **Stateful behavior** - State persists across runs (checkpoints, file tracking)
3. **Cannot be mocked reliably** - External system behavior is too complex to simulate
4. **High business risk** - Failure would be catastrophic in production

### Approved E2E Test Suite

| Test ID | Name | Status | Duration | Justification |
|---------|------|--------|----------|---------------|
| E2E-01 | Basic S3 JSON Ingestion | ✅ Implemented | ~2 min | **Core integration point.** Validates entire pipeline: API → Spark Connect → S3 → Iceberg. Cannot mock Spark's actual file reading and Iceberg table creation. |
| E2E-02 | Incremental Load | ✅ Implemented | ~3 min | **Spark checkpoint is stateful and opaque.** Must verify checkpoint persists across runs and prevents re-processing. Cannot mock Spark's internal checkpoint mechanism. |
| E2E-03 | Schema Evolution | ✅ Implemented | ~3 min | **Iceberg schema evolution is complex.** Must verify Iceberg ALTER TABLE, column mapping, and backward compatibility work correctly. Cannot mock Iceberg's versioning system. |
| E2E-04 | Critical Failure Recovery | 📋 Planned | ~3 min | **Distributed system resilience.** Test Spark crash + recovery to ensure checkpoint-based recovery works. Limited to 1-2 critical failure scenarios only. |
| E2E-05 | Azure Blob Storage | 🤔 Optional | ~2 min | **Only if storage behavior differs significantly from S3.** If Azure uses same Spark code path, skip this and use unit tests for config validation. |
| E2E-06 | Large File Performance | 🤔 Optional | ~5 min | **Performance baseline only.** Separate benchmark suite, not required for CI/CD. Run manually or nightly. |

**Total E2E Duration:** ~10-15 minutes (acceptable for CI/CD)

### What Should NOT Be E2E

❌ **Error Handling** (E2E-03 from original plan)
- **Why not:** Most error scenarios are business logic, not integration issues
- **Alternative:** Unit tests with mocked exceptions (100x faster)
- **E2E coverage:** 1-2 critical failure scenarios only (e.g., Spark crash)

❌ **CSV Format Ingestion** (E2E-05 from original plan)
- **Why not:** CSV uses same Spark code path as JSON, just different options
- **Already covered by:** E2E-01 proves Spark integration works
- **Alternative:** Unit test config validation and format option mapping

❌ **Scheduled/Cron Ingestion** (E2E-06 from original plan)
- **Why not:** Cron parsing is pure logic, scheduler is mockable
- **Alternative:** Unit test cron calculation, mock clock for scheduler tests
- **Already covered by:** E2E-01 proves run execution works

❌ **All Edge Cases and Error Scenarios**
- **Why not:** Combinatorial explosion (100s of scenarios)
- **Alternative:** Unit tests for business logic, 1-2 e2e for critical failures

---

## Unit/Integration Tests: What Belongs Here

### Decision Criteria

Use unit/integration tests when:

1. **Pure business logic** - No external system dependencies
2. **Mockable dependencies** - External calls can be simulated
3. **Fast feedback needed** - Developers need instant validation
4. **High variation** - Many edge cases and scenarios

### Test Coverage by Layer

#### 1. Service Layer (`app/services/`)

**`ingestion_service.py`**
```python
✅ Credential validation (mock S3 client)
✅ Configuration validation (Pydantic schemas)
✅ Status transitions (DRAFT → ACTIVE → PAUSED)
✅ Error message formatting
✅ Retry logic (mock transient failures)
```

**`spark_service.py`**
```python
✅ Connection pool management (mock Spark sessions)
✅ Query building (don't execute)
✅ Exception mapping (Spark exceptions → domain errors)
✅ Session cleanup logic
```

**`cost_estimator.py`**
```python
✅ Cost calculation formulas
✅ Rate configuration
✅ Edge cases (zero files, huge files)
```

**`file_discovery_service.py`**
```python
✅ File pattern matching
✅ S3 pagination (mock boto3)
✅ File filtering logic
✅ Error handling (access denied, bucket not found)
```

**`file_state_service.py`**
```python
✅ File state transitions
✅ Duplicate detection
✅ State queries
```

**`batch_orchestrator.py`**
```python
✅ Run orchestration logic
✅ Metrics aggregation
✅ Checkpoint coordination (mock checkpoint reads)
✅ Error rollback logic
```

#### 2. Repository Layer (`app/repositories/`)

**All repositories:**
```python
✅ CRUD operations (use SQLite in-memory)
✅ Query filters (tenant isolation, status filters)
✅ Pagination
✅ Soft deletes
✅ Constraint validation
```

**Database:** Use in-memory SQLite for speed, PostgreSQL for dialect-specific features.

#### 3. API Layer (`app/api/v1/`)

**All endpoints:**
```python
✅ Request validation (Pydantic)
✅ Response formatting
✅ HTTP status codes
✅ Error responses (404, 400, 500)
✅ Authentication/authorization (if implemented)
✅ Pagination
```

**Tool:** Use FastAPI `TestClient` (no real HTTP server needed)

#### 4. Models (`app/models/`)

```python
✅ Schema validation (Pydantic)
✅ Enum values
✅ Default values
✅ Field constraints (min/max, regex)
```

### Example Unit Test Structure

```python
# tests/unit/services/test_ingestion_service.py

@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client"""
    with patch('boto3.client') as mock:
        yield mock

@pytest.fixture
def mock_spark_service():
    """Mock Spark service"""
    with patch('app.services.spark_service.SparkService') as mock:
        yield mock

class TestIngestionService:
    """Unit tests for IngestionService - no real infrastructure"""

    def test_validate_s3_credentials_invalid(self):
        """Test credential validation without calling real S3"""
        service = IngestionService()

        with pytest.raises(ValidationError) as exc:
            service.validate_credentials({
                "aws_access_key_id": "",  # Empty key
                "aws_secret_access_key": "secret"
            })

        assert "access_key_id is required" in str(exc.value)

    def test_retry_on_transient_failure(self, mock_spark_service):
        """Test retry logic without real Spark"""
        mock_spark = mock_spark_service.return_value
        mock_spark.execute.side_effect = [
            ConnectionError("Connection timeout"),  # First attempt fails
            {"status": "success"}  # Second attempt succeeds
        ]

        service = IngestionService(spark_service=mock_spark)
        result = service.execute_with_retry(max_retries=2)

        assert result["status"] == "success"
        assert mock_spark.execute.call_count == 2

    def test_malformed_json_error_handling(self, mock_spark_service):
        """Test error handling for parse failures"""
        mock_spark = mock_spark_service.return_value
        mock_spark.execute.side_effect = SparkParseException("Malformed JSON at line 42")

        service = IngestionService(spark_service=mock_spark)

        with pytest.raises(IngestionError) as exc:
            service.execute_ingestion(ingestion_id="test-123")

        assert "Malformed JSON" in str(exc.value)
        assert exc.value.line_number == 42

    def test_schema_mismatch_when_evolution_disabled(self, mock_spark_service):
        """Test schema validation before execution"""
        mock_spark = mock_spark_service.return_value
        mock_spark.infer_schema.return_value = {
            "fields": ["id", "name", "new_field"]  # Schema has changed
        }

        service = IngestionService(spark_service=mock_spark)
        ingestion = Ingestion(
            id="test-123",
            schema_evolution_enabled=False,
            expected_schema={"fields": ["id", "name"]}
        )

        with pytest.raises(SchemaMismatchError):
            service.validate_schema(ingestion)
```

### Example Integration Test (DB Only)

```python
# tests/integration/repositories/test_ingestion_repository.py

@pytest.fixture
def db_session():
    """In-memory SQLite for fast tests"""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

class TestIngestionRepository:
    """Integration tests with real database (SQLite in-memory)"""

    def test_create_and_retrieve_ingestion(self, db_session):
        """Test CRUD operations"""
        repo = IngestionRepository(db_session)

        ingestion = repo.create(
            tenant_id="tenant-1",
            name="Test Ingestion",
            source_type="s3",
            status="draft"
        )

        assert ingestion.id is not None

        retrieved = repo.get_by_id(ingestion.id)
        assert retrieved.name == "Test Ingestion"

    def test_tenant_isolation(self, db_session):
        """Test tenant filtering"""
        repo = IngestionRepository(db_session)

        repo.create(tenant_id="tenant-1", name="Tenant 1 Ingestion")
        repo.create(tenant_id="tenant-2", name="Tenant 2 Ingestion")

        tenant1_ingestions = repo.list_by_tenant("tenant-1")
        assert len(tenant1_ingestions) == 1
        assert tenant1_ingestions[0].name == "Tenant 1 Ingestion"
```

---

## Error Handling Test Strategy

### Original Plan (E2E-03: All Error Scenarios)

This was **too ambitious** for e2e testing:
- Invalid credentials
- Malformed files
- Schema mismatches
- Network issues
- Partial failures
- Retries

**Problem:** This would require 20+ e2e tests, taking 30+ minutes.

### Revised Strategy

**90% Unit Tests + 10% E2E**

#### Unit Tests (Fast, Comprehensive)

```python
# tests/unit/services/test_error_handling.py

class TestErrorHandling:
    """Test all error scenarios with mocked dependencies"""

    # Credential errors
    def test_invalid_s3_credentials_error()
    def test_expired_credentials_error()
    def test_insufficient_permissions_error()

    # File format errors
    def test_malformed_json_handling()
    def test_invalid_csv_delimiter()
    def test_empty_file_handling()
    def test_corrupt_file_handling()

    # Schema errors
    def test_schema_mismatch_when_evolution_disabled()
    def test_type_mismatch_handling()
    def test_missing_required_field()

    # Network errors
    def test_connection_timeout_retry()
    def test_dns_resolution_failure()
    def test_throttling_backoff()

    # Partial failures
    def test_some_files_succeed_some_fail()
    def test_metrics_reflect_partial_success()

    # Retry logic
    def test_transient_error_retry()
    def test_permanent_error_no_retry()
    def test_max_retries_exceeded()
```

**Coverage:** ~20-30 tests, <10 seconds total

#### E2E Tests (Critical Scenarios Only)

```python
# tests/e2e/test_failure_recovery.py

class TestFailureRecovery:
    """E2E test for critical failure scenarios with real infrastructure"""

    def test_spark_connect_crash_and_recovery(self):
        """
        Test resilience when Spark crashes mid-ingestion.

        Steps:
        1. Start ingestion with 5 files
        2. Kill Spark Connect after 2 files processed
        3. Restart Spark Connect
        4. Trigger retry
        5. Verify only remaining 3 files processed (checkpoint works)
        6. Verify total 5000 records, no duplicates
        """
        # This MUST be e2e because:
        # - Checkpoint persistence is Spark-internal
        # - Recovery logic depends on real Spark state
        # - Cannot mock distributed system failure
```

**Coverage:** 1-2 tests, ~5 minutes total

---

## CSV and Scheduled Ingestion Strategy

### CSV Format (E2E-05 - REJECTED for E2E)

**Why not e2e:**
- CSV uses same Spark DataFrameReader as JSON
- Already proven by E2E-01 that Spark integration works
- Only difference is format options (delimiter, header, quote)

**Alternative: Unit tests**

```python
# tests/unit/services/test_format_handling.py

class TestFormatHandling:
    """Test format configuration without Spark execution"""

    def test_csv_format_options():
        config = CSVFormatConfig(
            delimiter=",",
            header=True,
            quote='"',
            escape="\\"
        )

        spark_options = config.to_spark_options()

        assert spark_options["sep"] == ","
        assert spark_options["header"] == "true"

    def test_csv_custom_delimiter():
        config = CSVFormatConfig(delimiter="|")
        assert config.to_spark_options()["sep"] == "|"

    def test_invalid_csv_config():
        with pytest.raises(ValidationError):
            CSVFormatConfig(delimiter="")  # Empty delimiter
```

### Scheduled Ingestion (E2E-06 - REJECTED for E2E)

**Why not e2e:**
- Cron parsing is pure logic (`croniter` library)
- Scheduler triggering is mockable
- Execution logic already tested by E2E-01

**Alternative: Unit tests**

```python
# tests/unit/services/test_scheduler.py

class TestScheduler:
    """Test scheduling logic without real time"""

    @freeze_time("2025-01-05 10:00:00")
    def test_cron_next_run_calculation():
        scheduler = Scheduler()
        ingestion = Ingestion(cron="0 */6 * * *")  # Every 6 hours

        next_run = scheduler.calculate_next_run(ingestion)

        assert next_run == datetime(2025, 1, 5, 16, 0, 0)

    @patch('app.api.v1.ingestions.trigger_run')
    def test_scheduler_triggers_run(mock_trigger):
        scheduler = Scheduler()
        ingestion = Ingestion(id="test-123", cron="* * * * *")

        scheduler.process_due_ingestions()

        mock_trigger.assert_called_once_with("test-123")

    def test_backfill_skipped_runs():
        scheduler = Scheduler()
        ingestion = Ingestion(
            cron="0 0 * * *",  # Daily
            last_run=datetime(2025, 1, 1),
            backfill_enabled=True
        )

        runs = scheduler.calculate_backfill_runs(
            ingestion,
            until=datetime(2025, 1, 5)
        )

        assert len(runs) == 4  # Jan 2, 3, 4, 5
```

---

## Test Execution Strategy

### Local Development

```bash
# Fast feedback loop - run constantly
make test-unit  # <1 second

# Pre-commit validation
make test-integration  # ~5 seconds (DB tests)

# Pre-push validation
make test-e2e  # ~10-15 minutes (full e2e suite)
```

### CI/CD Pipeline

```yaml
# .github/workflows/ci.yml

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/unit/
      - timeout: 2 minutes
      - coverage: 80% minimum

  integration-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
    steps:
      - run: pytest tests/integration/
      - timeout: 5 minutes

  e2e-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
      minio:
        image: minio/minio
      spark:
        image: apache/spark:3.5.0
    steps:
      - run: docker-compose -f docker-compose.test.yml up -d
      - run: pytest tests/e2e/
      - timeout: 20 minutes
      - run-on: [main, release/*]  # Only on important branches
```

### Coverage Targets

| Test Type | Coverage Goal | Reason |
|-----------|---------------|--------|
| Unit | 80-90% | Comprehensive business logic coverage |
| Integration | 70-80% | Database operations and API endpoints |
| E2E | 5-10 critical paths | Integration points only |

**Overall:** Aim for 80%+ code coverage with majority from unit tests.

---

## Benefits of This Strategy

### Speed
- **Unit tests:** <1 second (instant feedback)
- **Integration tests:** ~5 seconds (acceptable)
- **E2E tests:** ~10-15 minutes (tolerable for CI/CD)

### Cost
- **Unit tests:** Zero infrastructure cost
- **Integration tests:** Minimal (in-memory DB)
- **E2E tests:** Docker containers only when needed

### Reliability
- **Unit tests:** No flakiness (no network, no timing issues)
- **Integration tests:** Minimal flakiness (local DB only)
- **E2E tests:** Some flakiness acceptable (real systems)

### Maintainability
- **Unit tests:** Easy to debug (isolated failures)
- **Integration tests:** Moderate (DB state issues)
- **E2E tests:** Harder to debug (multi-system issues)

### Coverage
- **100s of edge cases** tested via unit tests
- **Critical integration points** validated via e2e tests
- **Best of both worlds:** Comprehensive + fast

---

## Anti-Patterns to Avoid

### ❌ E2E-First Testing

**Don't do this:**
```python
# tests/e2e/test_every_single_error.py
def test_invalid_credentials_e2e():
    # Start Spark, MinIO, PostgreSQL
    # Create ingestion with bad creds
    # Wait for failure
    # Verify error message
    # Takes 2 minutes
```

**Do this instead:**
```python
# tests/unit/services/test_ingestion_service.py
def test_invalid_credentials(mock_s3):
    mock_s3.side_effect = ClientError("Invalid credentials")
    with pytest.raises(CredentialError):
        service.validate_credentials(...)
    # Takes 0.001 seconds
```

### ❌ Testing Implementation Details

**Don't test:** "Spark uses DataFrameReader with option X"
**Do test:** "System correctly ingests CSV files with custom delimiter"

### ❌ Duplicate Coverage

**Don't:** Test same behavior in unit AND e2e tests
**Do:** Unit test for logic, e2e test for integration only

### ❌ Slow Unit Tests

**Don't:** Make real HTTP calls, use real databases, sleep()
**Do:** Mock everything, use in-memory fixtures, freeze time

---

## Migration Plan

### Current State (After Schema Evolution)
- ✅ 3 e2e tests implemented
- ❓ Unknown unit test coverage
- ❓ Unknown integration test coverage

### Phase 1: Establish Unit Test Foundation (1 week)
```
[ ] Set up unit test structure (tests/unit/)
[ ] Add fixtures for mocking (conftest.py)
[ ] Write unit tests for services/ (20-30 tests)
[ ] Write unit tests for repositories/ (10-15 tests)
[ ] Write unit tests for api/ (15-20 tests)
[ ] Target: 70%+ unit test coverage
```

### Phase 2: Add Integration Tests (3 days)
```
[ ] Set up integration test structure (tests/integration/)
[ ] Configure in-memory SQLite fixtures
[ ] Write repository integration tests (10 tests)
[ ] Write API integration tests (10 tests)
[ ] Target: 80%+ combined coverage
```

### Phase 3: Selective E2E Expansion (2 days)
```
[ ] Add E2E-04: Spark crash recovery (1 test)
[ ] Evaluate need for Azure Blob e2e test
[ ] Document e2e test maintenance runbook
[ ] Final e2e suite: 4-5 tests, <15 minutes
```

### Phase 4: CI/CD Integration (1 day)
```
[ ] Configure GitHub Actions for all test levels
[ ] Set up coverage reporting
[ ] Add pre-commit hooks for unit tests
[ ] Document testing workflow in CONTRIBUTING.md
```

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-01-05 | Limit e2e tests to 5-7 critical scenarios | Balance coverage with speed and cost |
| 2025-01-05 | Reject E2E-03 (all error scenarios) | 90% of errors testable via unit tests |
| 2025-01-05 | Reject E2E-05 (CSV format) | Same Spark code path as JSON, already covered |
| 2025-01-05 | Reject E2E-06 (scheduled ingestion) | Scheduler logic is pure, mockable |
| 2025-01-05 | Add E2E-04 (failure recovery) | Critical resilience scenario, cannot mock |

---

## References

- Test Pyramid: https://martinfowler.com/articles/practical-test-pyramid.html
- Testing Best Practices: https://testingjavascript.com/ (concepts apply to Python)
- Spark Testing: https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#testing

---

## Approval

**Strategy Approved By:** _________________
**Date:** _________________
**Next Review:** After Phase 1 completion

---

**Document Status:** ACTIVE
**Last Updated:** 2025-01-05
**Owner:** Engineering Team
