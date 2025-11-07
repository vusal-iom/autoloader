# Phase 2 Test Coverage Plan

**Status:** Planning
**Date:** 2025-11-07
**Phase 2 Commit:** 252c529

## Overview

This document outlines comprehensive test coverage for Phase 2 Prefect integration changes. The tests are categorized by layer and type, with clear objectives and implementation guidance.

## What Changed in Phase 2

### Core Changes
1. **PrefectService** (`app/services/prefect_service.py`) - New service for Prefect API management
2. **IngestionService** (`app/services/ingestion_service.py`) - Converted to async, integrated with Prefect
3. **API Endpoints** (`app/api/v1/ingestions.py`) - Updated to await async service methods
4. **Application Startup** (`app/main.py`) - Added Prefect initialization hook
5. **Prefect Flows** (`app/prefect/flows/`) - New Prefect flow definitions
6. **Prefect Tasks** (`app/prefect/tasks/`) - Task implementations for flows

### Key Behaviors to Test
- ✅ Prefect deployment lifecycle (create/update/pause/resume/delete)
- ✅ Graceful degradation when Prefect unavailable
- ✅ Dual execution path (Prefect vs BatchOrchestrator)
- ✅ Schedule synchronization between database and Prefect
- ✅ Cron expression generation from schedule config
- ✅ Flow run triggering and tracking

---

## Test Structure

### Test Levels
1. **Unit Tests** - Test individual functions/methods in isolation
2. **Integration Tests** - Test service interactions with mocked Prefect API
3. **E2E Tests** - Test complete workflows with real Prefect server

---

## 1. Unit Tests

### 1.1 PrefectService Unit Tests
**File:** `tests/unit/services/test_prefect_service.py`

#### Test Suite: Cron Expression Generation

```python
class TestCronExpressionGeneration:
    """Test cron expression building from schedule config"""

    def test_hourly_schedule(self):
        """Hourly frequency generates '0 * * * *'"""

    def test_daily_schedule_default_time(self):
        """Daily with no time defaults to '0 0 * * *' (midnight)"""

    def test_daily_schedule_with_time(self):
        """Daily with time '14:30' generates '30 14 * * *'"""

    def test_weekly_schedule(self):
        """Weekly generates '0 0 * * 0' (Sunday midnight by default)"""

    def test_custom_cron_expression(self):
        """Custom frequency uses provided cron expression"""

    def test_invalid_time_format_fallback(self):
        """Invalid time format (e.g., '25:99') falls back to '00:00'"""

    def test_missing_frequency_returns_none(self):
        """None frequency returns None schedule"""
```

**Objectives:**
- Verify correct cron expression for all frequency types
- Ensure time parsing handles edge cases
- Validate fallback behavior for invalid input

#### Test Suite: Work Queue Routing

```python
class TestWorkQueueRouting:
    """Test work queue determination logic"""

    def test_default_work_queue(self):
        """All ingestions use default queue (current implementation)"""

    def test_future_resource_based_routing(self):
        """TODO: Test routing based on cluster size, source type, etc."""
```

**Objectives:**
- Document current behavior (default queue)
- Placeholder for future resource-based routing

#### Test Suite: Schedule Object Construction

```python
class TestScheduleConstruction:
    """Test Prefect CronSchedule construction"""

    def test_schedule_with_timezone(self):
        """Schedule respects timezone setting"""

    def test_schedule_without_frequency(self):
        """Returns None when no frequency specified"""
```

---

### 1.2 IngestionService Unit Tests
**File:** `tests/unit/services/test_ingestion_service_async.py`

#### Test Suite: Async Method Signatures

```python
class TestAsyncConversion:
    """Verify all methods are properly async"""

    async def test_create_ingestion_is_async(self):
        """create_ingestion is async and awaitable"""

    async def test_update_ingestion_is_async(self):
        """update_ingestion is async and awaitable"""

    async def test_delete_ingestion_is_async(self):
        """delete_ingestion is async and awaitable"""

    async def test_trigger_manual_run_is_async(self):
        """trigger_manual_run is async and awaitable"""

    async def test_pause_ingestion_is_async(self):
        """pause_ingestion is async and awaitable"""

    async def test_resume_ingestion_is_async(self):
        """resume_ingestion is async and awaitable"""
```

**Objectives:**
- Ensure proper async/await usage
- Prevent synchronous blocking calls

---

## 2. Integration Tests

### 2.1 PrefectService Integration Tests
**File:** `tests/integration/services/test_prefect_service_integration.py`

#### Test Suite: Deployment Lifecycle (Mocked Prefect API)

```python
@pytest.mark.integration
class TestDeploymentLifecycle:
    """Test deployment CRUD operations with mocked Prefect client"""

    async def test_create_deployment_success(self, mock_prefect_client, sample_ingestion):
        """Successfully creates deployment with correct parameters"""
        # Mock: prefect client.create_deployment() returns deployment object
        # Verify: deployment_id returned, correct schedule/tags/work_queue

    async def test_create_deployment_without_schedule(self, mock_prefect_client):
        """Creates deployment with schedule=None when no frequency"""

    async def test_update_deployment_schedule(self, mock_prefect_client):
        """Updates deployment schedule when ingestion schedule changes"""

    async def test_pause_deployment(self, mock_prefect_client):
        """Sets deployment paused state to True"""

    async def test_resume_deployment(self, mock_prefect_client):
        """Sets deployment paused state to False"""

    async def test_delete_deployment(self, mock_prefect_client):
        """Deletes deployment successfully"""

    async def test_delete_nonexistent_deployment(self, mock_prefect_client):
        """Handles ObjectNotFound gracefully (no exception)"""

    async def test_trigger_deployment(self, mock_prefect_client):
        """Triggers flow run and returns flow_run_id"""
```

**Objectives:**
- Verify Prefect API called with correct parameters
- Test error handling and fallback behavior
- Ensure graceful degradation

#### Test Suite: Error Handling

```python
class TestErrorHandling:
    """Test error scenarios and recovery"""

    async def test_prefect_api_timeout(self, mock_prefect_client):
        """Handles API timeout gracefully"""

    async def test_prefect_api_connection_error(self, mock_prefect_client):
        """Handles connection refused error"""

    async def test_invalid_deployment_id(self, mock_prefect_client):
        """Handles invalid UUID format"""

    async def test_prefect_server_5xx_error(self, mock_prefect_client):
        """Handles server errors with appropriate logging"""
```

**Objectives:**
- Verify robust error handling
- Ensure system continues functioning when Prefect unavailable

---

### 2.2 IngestionService Integration Tests
**File:** `tests/integration/services/test_ingestion_service_prefect.py`

#### Test Suite: Create Ingestion with Prefect

```python
@pytest.mark.integration
class TestCreateIngestionWithPrefect:
    """Test ingestion creation triggers Prefect deployment"""

    async def test_create_with_schedule_creates_deployment(self, db_session, mock_prefect):
        """Creating ingestion with schedule creates Prefect deployment"""
        # Setup: Mock PrefectService.create_deployment
        # Action: Create ingestion with schedule
        # Verify: deployment_id stored in ingestion record

    async def test_create_without_schedule_no_deployment(self, db_session, mock_prefect):
        """Creating ingestion without schedule skips deployment"""

    async def test_create_deployment_failure_continues(self, db_session, mock_prefect):
        """Ingestion still created even if deployment fails"""
        # Mock: PrefectService.create_deployment raises exception
        # Verify: Ingestion created, warning logged, no exception raised
```

**Objectives:**
- Verify deployment creation integrated into ingestion lifecycle
- Ensure non-blocking behavior on Prefect failures

#### Test Suite: Update Ingestion Schedule

```python
class TestUpdateIngestionSchedule:
    """Test schedule updates propagate to Prefect"""

    async def test_update_schedule_updates_deployment(self, db_session, mock_prefect):
        """Changing schedule updates Prefect deployment"""

    async def test_update_non_schedule_field_no_deployment_update(self, db_session, mock_prefect):
        """Updating alert_recipients doesn't trigger deployment update"""

    async def test_update_schedule_on_ingestion_without_deployment(self, db_session, mock_prefect):
        """Updating schedule when no deployment_id is graceful"""
```

**Objectives:**
- Verify selective update propagation
- Test idempotency

#### Test Suite: Delete Ingestion

```python
class TestDeleteIngestion:
    """Test deletion removes Prefect deployment"""

    async def test_delete_removes_deployment(self, db_session, mock_prefect):
        """Deleting ingestion deletes Prefect deployment first"""

    async def test_delete_when_deployment_already_gone(self, db_session, mock_prefect):
        """Handles case where deployment was manually deleted"""
```

**Objectives:**
- Verify cleanup cascade
- Handle already-deleted deployments

#### Test Suite: Manual Trigger with Dual Path

```python
class TestManualTriggerDualPath:
    """Test dual execution path: Prefect vs BatchOrchestrator"""

    async def test_trigger_with_prefect_deployment(self, db_session, mock_prefect, mock_orchestrator):
        """Triggers via Prefect when deployment exists"""
        # Setup: Ingestion with prefect_deployment_id
        # Mock: PrefectService.trigger_deployment returns flow_run_id
        # Verify: Returns {"method": "prefect", "flow_run_id": ...}
        # Verify: BatchOrchestrator NOT called

    async def test_trigger_without_prefect_deployment(self, db_session, mock_prefect, mock_orchestrator):
        """Falls back to BatchOrchestrator when no deployment"""
        # Setup: Ingestion without prefect_deployment_id
        # Verify: BatchOrchestrator.run_ingestion called
        # Verify: Returns {"method": "direct", "run_id": ...}

    async def test_trigger_prefect_failure_fallback(self, db_session, mock_prefect, mock_orchestrator):
        """Falls back to BatchOrchestrator when Prefect trigger fails"""
        # Setup: Ingestion with deployment
        # Mock: PrefectService.trigger_deployment raises exception
        # Verify: Warning logged, falls back to orchestrator
        # Verify: Returns {"method": "direct", "run_id": ...}
```

**Objectives:**
- **CRITICAL:** Verify dual execution path works correctly
- Ensure fallback behavior is seamless
- Test primary/fallback routing logic

#### Test Suite: Pause/Resume

```python
class TestPauseResume:
    """Test pause/resume propagates to Prefect"""

    async def test_pause_pauses_deployment(self, db_session, mock_prefect):
        """Pausing ingestion pauses Prefect deployment"""

    async def test_resume_resumes_deployment(self, db_session, mock_prefect):
        """Resuming ingestion resumes Prefect deployment"""
```

---

### 2.3 API Integration Tests
**File:** `tests/integration/api/test_ingestions_api_async.py`

#### Test Suite: API Async Behavior

```python
@pytest.mark.integration
class TestAPIAsyncBehavior:
    """Test API endpoints properly await service calls"""

    async def test_create_endpoint_awaits_service(self, api_client, mock_ingestion_service):
        """POST /ingestions awaits create_ingestion"""

    async def test_update_endpoint_awaits_service(self, api_client, mock_ingestion_service):
        """PUT /ingestions/{id} awaits update_ingestion"""

    async def test_delete_endpoint_awaits_service(self, api_client, mock_ingestion_service):
        """DELETE /ingestions/{id} awaits delete_ingestion"""

    async def test_trigger_endpoint_awaits_service(self, api_client, mock_ingestion_service):
        """POST /ingestions/{id}/run awaits trigger_manual_run"""
```

**Objectives:**
- Verify proper async/await propagation through API layer
- Ensure no blocking calls in async endpoints

#### Test Suite: Response Format Changes

```python
class TestTriggerResponseFormat:
    """Test new trigger response format"""

    async def test_trigger_prefect_response(self, api_client, mock_service):
        """Trigger via Prefect returns flow_run_id"""
        # Mock: service returns {"method": "prefect", "flow_run_id": "..."}
        # Verify: Response includes flow_run_id

    async def test_trigger_direct_response(self, api_client, mock_service):
        """Trigger via direct returns run_id"""
        # Mock: service returns {"method": "direct", "run_id": "..."}
        # Verify: Response includes run_id
```

---

## 3. E2E Tests

### 3.1 Prefect E2E Tests
**File:** `tests/e2e/test_prefect_workflows.py`

#### Test Suite: Complete Prefect Workflow

```python
@pytest.mark.e2e
@pytest.mark.requires_prefect
class TestPrefectWorkflows:
    """E2E tests with real Prefect server"""

    async def test_complete_lifecycle_with_prefect(
        self,
        api_client,
        prefect_server,  # Fixture that ensures Prefect server running
        minio_client,
        test_bucket,
        spark_session
    ):
        """
        Complete lifecycle test:
        1. Create ingestion with schedule via API
        2. Verify deployment created in Prefect
        3. Trigger manual run via API
        4. Verify flow run in Prefect
        5. Verify data in Iceberg table
        6. Update schedule via API
        7. Verify deployment schedule updated
        8. Delete ingestion via API
        9. Verify deployment deleted in Prefect
        """

    async def test_scheduled_run_execution(self, api_client, prefect_server):
        """
        Test scheduled execution (not just manual):
        1. Create ingestion with very frequent schedule (every 1 minute)
        2. Wait for scheduled run to trigger
        3. Verify run completed successfully
        """

    async def test_pause_resume_workflow(self, api_client, prefect_server):
        """
        Test pause/resume:
        1. Create active ingestion with schedule
        2. Verify deployment is active
        3. Pause via API
        4. Verify deployment paused in Prefect
        5. Resume via API
        6. Verify deployment active again
        """
```

**Objectives:**
- **CRITICAL:** Test complete workflows with real Prefect server
- Verify end-to-end integration
- Catch issues that unit/integration tests miss

#### Test Suite: Prefect Deployment Verification

```python
class TestPrefectDeploymentVerification:
    """Verify deployments are correctly configured in Prefect"""

    async def test_deployment_tags_correct(self, api_client, prefect_api):
        """Deployment has correct tags (tenant, source type, etc.)"""

    async def test_deployment_schedule_matches_ingestion(self, api_client, prefect_api):
        """Deployment schedule matches ingestion configuration"""

    async def test_deployment_work_queue_correct(self, api_client, prefect_api):
        """Deployment assigned to correct work queue"""

    async def test_deployment_parameters_correct(self, api_client, prefect_api):
        """Deployment parameters include ingestion_id"""
```

---

### 3.2 Graceful Degradation E2E Tests
**File:** `tests/e2e/test_prefect_degradation.py`

#### Test Suite: Prefect Unavailable Scenarios

```python
@pytest.mark.e2e
class TestPrefectDegradation:
    """Test system behavior when Prefect unavailable"""

    async def test_startup_without_prefect(self, api_client_no_prefect):
        """
        Application starts successfully when Prefect unreachable:
        1. Start app with PREFECT_API_URL pointing to nonexistent server
        2. Verify app starts (no crash)
        3. Verify health endpoint shows prefect: "disconnected"
        """

    async def test_create_ingestion_without_prefect(self, api_client_no_prefect):
        """
        Can create ingestion when Prefect unavailable:
        1. Create ingestion with schedule
        2. Verify ingestion created in database
        3. Verify warning logged
        4. Verify prefect_deployment_id is NULL
        """

    async def test_manual_trigger_without_prefect(self, api_client_no_prefect):
        """
        Manual trigger falls back to BatchOrchestrator:
        1. Create ingestion (no Prefect)
        2. Trigger manual run
        3. Verify run completes via BatchOrchestrator
        4. Verify response indicates "direct" method
        """

    async def test_update_schedule_without_prefect(self, api_client_no_prefect):
        """
        Can update schedule when Prefect unavailable:
        1. Create ingestion
        2. Update schedule
        3. Verify update succeeds (no exception)
        4. Verify warning logged
        """
```

**Objectives:**
- **CRITICAL:** Verify graceful degradation works as documented
- Ensure system remains functional without Prefect
- Test user experience when Prefect unavailable

---

### 3.3 Existing E2E Tests (Already Implemented)
**File:** `tests/e2e/test_prefect_ingestion.py` ✅

#### Already Covered:
- ✅ Basic Prefect flow execution (`test_prefect_basic_ingestion`)
- ✅ Data verification in Iceberg table
- ✅ Run metrics validation

---

## 4. Special Test Scenarios

### 4.1 Concurrency Tests
**File:** `tests/integration/test_prefect_concurrency.py`

```python
class TestConcurrency:
    """Test concurrent operations"""

    async def test_concurrent_deployment_creation(self):
        """Multiple ingestions created concurrently don't conflict"""

    async def test_concurrent_manual_triggers(self):
        """Multiple manual triggers of same ingestion handled correctly"""
```

---

### 4.2 Schedule Edge Cases
**File:** `tests/unit/services/test_prefect_schedule_edge_cases.py`

```python
class TestScheduleEdgeCases:
    """Test edge cases in schedule configuration"""

    def test_timezone_handling(self):
        """Various IANA timezones handled correctly"""

    def test_daylight_saving_time(self):
        """Schedule respects DST transitions"""

    def test_leap_year_february_29(self):
        """Daily schedule on Feb 29 handled correctly"""
```

---

## 5. Test Fixtures & Mocks

### 5.1 Required Fixtures

```python
# tests/conftest.py or tests/integration/conftest.py

@pytest.fixture
async def mock_prefect_client():
    """Mock Prefect API client for integration tests"""

@pytest.fixture
async def mock_prefect_service():
    """Mock PrefectService with pre-configured responses"""

@pytest.fixture
def sample_ingestion():
    """Sample ingestion domain model for testing"""

@pytest.fixture
async def prefect_server(docker_services):
    """Ensure Prefect server running for E2E tests (docker-compose)"""

@pytest.fixture
async def api_client_no_prefect(monkeypatch):
    """API client with PREFECT_API_URL pointing to nonexistent server"""
```

---

## 6. Test Execution Strategy

### 6.1 Test Pyramid

```
        E2E (5%)
       /        \
  Integration (20%)
     /            \
   Unit (75%)
```

**Unit Tests:** Fast, isolated, no external dependencies
**Integration Tests:** Mocked Prefect API, real database
**E2E Tests:** Real Prefect server (docker-compose), real Spark, real MinIO

### 6.2 CI/CD Pipeline Stages

**Stage 1: Unit Tests (fast)**
```bash
pytest tests/unit/ -v
```

**Stage 2: Integration Tests**
```bash
pytest tests/integration/ -v
```

**Stage 3: E2E Tests**
```bash
docker-compose -f docker-compose.test.yml up -d
pytest tests/e2e/ -v --markers=e2e
```

### 6.3 Markers

```python
@pytest.mark.unit            # Fast, isolated
@pytest.mark.integration     # Mocked dependencies
@pytest.mark.e2e             # Real services
@pytest.mark.requires_prefect  # Needs Prefect server
@pytest.mark.requires_spark    # Needs Spark Connect
@pytest.mark.requires_minio    # Needs MinIO
```

---

## 7. Coverage Goals

### Minimum Coverage Targets
- **PrefectService:** 90%+ coverage
- **IngestionService (Prefect integration):** 85%+ coverage
- **API Endpoints:** 95%+ coverage
- **Prefect flows/tasks:** 70%+ coverage (harder to unit test)

### Critical Paths (Must Have 100% Coverage)
- ✅ Dual execution path (Prefect vs BatchOrchestrator)
- ✅ Graceful degradation when Prefect unavailable
- ✅ Deployment lifecycle (create/update/delete)
- ✅ Schedule synchronization

---

## 8. Implementation Priority

### Phase 1 (High Priority) - Core Functionality
1. ✅ Unit tests for cron expression generation
2. ✅ Integration tests for dual execution path
3. ✅ E2E test for graceful degradation
4. ✅ Integration tests for deployment lifecycle

### Phase 2 (Medium Priority) - Error Handling
5. ✅ Integration tests for error scenarios
6. ✅ E2E tests for Prefect unavailable
7. ✅ Unit tests for edge cases

### Phase 3 (Nice to Have) - Advanced Scenarios
8. ⏳ Concurrency tests
9. ⏳ Schedule edge cases (DST, timezones)
10. ⏳ Performance tests

---

## 9. Running Tests Locally

### Setup
```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Start test infrastructure
docker-compose -f docker-compose.test.yml up -d

# Wait for services (especially Spark ~2 min)
docker-compose -f docker-compose.test.yml logs -f spark-connect
```

### Run Unit Tests
```bash
pytest tests/unit/services/test_prefect_service.py -v
```

### Run Integration Tests
```bash
# Requires database
pytest tests/integration/services/test_prefect_service_integration.py -v
```

### Run E2E Tests
```bash
# Requires all services (MinIO, Spark, PostgreSQL, Prefect)
pytest tests/e2e/test_prefect_workflows.py -v
```

### Run All Phase 2 Tests
```bash
pytest -v -m "prefect or async"
```

---

## 10. Test Maintenance

### When to Update Tests
- ✅ Adding new schedule frequency types
- ✅ Changing deployment naming convention
- ✅ Modifying error handling behavior
- ✅ Adding resource-based work queue routing
- ✅ Changing API response formats

### Test Review Checklist
- [ ] All critical paths covered
- [ ] Error scenarios tested
- [ ] Edge cases documented
- [ ] Mocks properly isolated
- [ ] E2E tests use real services
- [ ] Test names clearly describe behavior
- [ ] Assertions include helpful messages
- [ ] Cleanup in fixtures/teardown

---

## 11. Known Gaps (To Address)

1. **Prefect Flow Tests:** Limited coverage of `app/prefect/flows/` and `app/prefect/tasks/`
   - **Reason:** Harder to unit test Prefect flows/tasks
   - **Solution:** Focus on E2E tests + integration tests with mocked tasks

2. **Scheduled Run Testing:** Testing actual scheduled runs (not just manual) is slow
   - **Solution:** Use very short intervals (1 minute) or mock time

3. **Multi-tenant Testing:** Phase 2 doesn't fully test tenant isolation with Prefect
   - **Future:** Add tests when multi-tenant auth implemented

4. **Performance Testing:** No load tests for concurrent Prefect operations
   - **Future:** Add after functional tests complete

---

## 12. Success Criteria

Phase 2 testing is complete when:
- ✅ All High Priority tests implemented (Section 8, Phase 1)
- ✅ Coverage targets met (Section 7)
- ✅ CI/CD pipeline passes all stages
- ✅ E2E test for graceful degradation passes
- ✅ Dual execution path verified in integration tests
- ✅ No regressions in existing E2E tests (`test_basic_ingestion.py`, etc.)

---

## 13. Documentation References

- **Implementation Guide:** `docs/prefect-migration/PHASE2_COMPLETE.md`
- **Testing Guide:** `TESTING_PREFECT.md`
- **E2E Test Rules:** `tests/e2e/CLAUDE.md`
- **Existing Tests:** `tests/e2e/test_prefect_ingestion.py`

---

## Summary

This test coverage plan provides:
- **Comprehensive coverage** across unit, integration, and E2E levels
- **Clear objectives** for each test suite
- **Prioritized implementation** plan
- **Specific test scenarios** with expected outcomes
- **Guidance on fixtures and mocks**
- **Coverage goals and success criteria**

**Key Focus Areas:**
1. **Dual execution path** (Prefect vs BatchOrchestrator)
2. **Graceful degradation** (system works without Prefect)
3. **Schedule synchronization** (database ↔️ Prefect)
4. **Error handling** (robust against Prefect failures)

**Next Steps:**
1. Review this plan with team
2. Start with Phase 1 high-priority tests
3. Implement fixtures and mocks first
4. Write tests incrementally, verifying coverage
5. Update CI/CD pipeline to run new tests
