# Fallback Code Cleanup - Single Path Enforcement

**Date:** 2025-11-08
**Status:** ✅ Complete
**Commit:** [To be added]

## Decision

Remove all fallback logic to BatchOrchestrator. Enforce **single execution path via Prefect only**.

**Rationale:**
- Direct migration to Prefect without incremental transition
- No need for backward compatibility
- Simpler codebase with single execution path
- Fail-fast behavior ensures proper monitoring and alerting

## Changes Made

### 1. `create_ingestion()` - Fail Fast on Prefect Unavailable

**Before:**
```python
# Create Prefect deployment if scheduling enabled
if ingestion.schedule_frequency:
    try:
        prefect = await get_prefect_service()
        deployment_id = await prefect.create_deployment(ingestion)
        ...
    except Exception as e:
        logger.error(f"❌ Failed to create Prefect deployment: {e}")
        logger.warning("Ingestion created but scheduling unavailable")
        # Don't fail the request - ingestion can still be triggered manually
```

**After:**
```python
# Create Prefect deployment (required for execution)
if ingestion.schedule_frequency:
    prefect = await get_prefect_service()

    if not prefect.initialized:
        # Rollback ingestion creation
        self.ingestion_repo.delete(ingestion_id)
        raise HTTPException(
            status_code=503,
            detail="Cannot create ingestion: Orchestration service unavailable"
        )

    deployment_id = await prefect.create_deployment(ingestion)
    ...
```

**Impact:**
- ✅ Ingestion creation fails if Prefect unavailable
- ✅ No orphaned ingestions without deployments
- ✅ Clear error message to user

---

### 2. `trigger_manual_run()` - Remove Fallback to BatchOrchestrator

**Before:**
```python
async def trigger_manual_run(self, ingestion_id: str) -> dict:
    """
    If ingestion has a Prefect deployment, triggers via Prefect.
    Otherwise, falls back to direct BatchOrchestrator execution.
    """
    # Primary path: Trigger via Prefect deployment
    if ingestion.prefect_deployment_id:
        try:
            prefect = await get_prefect_service()
            flow_run_id = await prefect.trigger_deployment(...)
            return {"method": "prefect", "flow_run_id": flow_run_id}
        except Exception as e:
            logger.error(f"❌ Failed to trigger Prefect flow: {e}")
            logger.warning("Falling back to direct orchestrator execution")
            # Fall through to fallback path

    # Fallback path: Direct BatchOrchestrator execution
    logger.info(f"Triggering ingestion via BatchOrchestrator (fallback)")
    orchestrator = BatchOrchestrator(self.db)
    run = orchestrator.run_ingestion(ingestion)
    return {"method": "direct", "run_id": run.id}
```

**After:**
```python
async def trigger_manual_run(self, ingestion_id: str) -> dict:
    """
    Trigger a manual ingestion run via Prefect.

    Raises:
        ValueError: If ingestion has no deployment
        HTTPException: If Prefect service unavailable (503)
    """
    # ... validation ...

    # Require Prefect deployment
    if not ingestion.prefect_deployment_id:
        raise ValueError(
            f"Ingestion {ingestion_id} does not have a deployment. "
            "Please configure a schedule to enable execution."
        )

    # Trigger via Prefect (single execution path)
    prefect = await get_prefect_service()

    if not prefect.initialized:
        raise HTTPException(
            status_code=503,
            detail="Orchestration service unavailable. Please try again later."
        )

    flow_run_id = await prefect.trigger_deployment(...)
    return {"method": "prefect", "flow_run_id": flow_run_id}
```

**Impact:**
- ✅ Single execution path - always via Prefect
- ✅ Clear error if no deployment configured
- ✅ Fail fast if Prefect unavailable
- ❌ **BREAKING:** Ingestions without deployments cannot be triggered

---

### 3. `update_ingestion()` - Fail Fast on Schedule Update

**Before:**
```python
# Update Prefect deployment schedule if changed
if schedule_changed and ingestion.prefect_deployment_id:
    try:
        prefect = await get_prefect_service()
        await prefect.update_deployment_schedule(...)
        logger.info(f"✅ Updated Prefect deployment schedule")
    except Exception as e:
        logger.error(f"❌ Failed to update Prefect deployment: {e}")
        # Don't fail the request - schedule update will be retried
```

**After:**
```python
# Update Prefect deployment schedule if changed
if schedule_changed and ingestion.prefect_deployment_id:
    prefect = await get_prefect_service()

    if not prefect.initialized:
        raise HTTPException(
            status_code=503,
            detail="Cannot update schedule: Orchestration service unavailable"
        )

    await prefect.update_deployment_schedule(...)
    logger.info(f"✅ Updated Prefect deployment schedule")
```

**Impact:**
- ✅ Schedule updates fail immediately if Prefect unavailable
- ✅ Prevents DB/Prefect state divergence

---

### 4. `pause_ingestion()` - Fail Fast

**Before:**
```python
# Pause Prefect deployment
if ingestion.prefect_deployment_id:
    try:
        prefect = await get_prefect_service()
        await prefect.pause_deployment(deployment_id)
        logger.info(f"✅ Paused Prefect deployment")
    except Exception as e:
        logger.error(f"❌ Failed to pause Prefect deployment: {e}")

result = self.ingestion_repo.update_status(ingestion_id, IngestionStatus.PAUSED)
```

**After:**
```python
# Pause Prefect deployment
if ingestion.prefect_deployment_id:
    prefect = await get_prefect_service()

    if not prefect.initialized:
        raise HTTPException(
            status_code=503,
            detail="Cannot pause ingestion: Orchestration service unavailable"
        )

    await prefect.pause_deployment(deployment_id)
    logger.info(f"✅ Paused Prefect deployment")

result = self.ingestion_repo.update_status(ingestion_id, IngestionStatus.PAUSED)
```

**Impact:**
- ✅ Prevents inconsistent state (DB paused but Prefect active)
- ✅ Clear error message

---

### 5. `resume_ingestion()` - Fail Fast

**Same pattern as pause_ingestion** - fail if Prefect unavailable.

**Impact:**
- ✅ Prevents inconsistent state (DB active but Prefect paused)

---

### 6. `delete_ingestion()` - Kept Graceful Degradation

**No changes made** - Delete operation continues even if Prefect deployment deletion fails.

**Rationale:**
- Deletion is a cleanup operation
- Better to have orphaned Prefect deployment than inability to delete ingestion
- Orphaned deployment won't execute (ingestion record gone, flow will fail)
- Admin can manually clean up Prefect deployments if needed

---

## What Was NOT Changed

### BatchOrchestrator Class
- ✅ **KEPT** - Still used by Prefect flows
- Prefect flows call `BatchOrchestrator.run_ingestion()` internally
- This is correct - BatchOrchestrator is the business logic layer
- Only removed direct API → BatchOrchestrator path

### Architecture After Cleanup
```
Before (Dual Path):
API → IngestionService → [Try Prefect] → [Fallback to BatchOrchestrator]
                                    ↓
                            Prefect Flow → BatchOrchestrator

After (Single Path):
API → IngestionService → Prefect ONLY → Prefect Flow → BatchOrchestrator
```

---

## Breaking Changes

### For API Clients

1. **Ingestion creation fails if Prefect down:**
   - Before: Ingestion created, warning logged
   - After: 503 error, ingestion not created

2. **Manual trigger requires deployment:**
   - Before: Could trigger via fallback even without deployment
   - After: ValueError if no deployment configured

3. **Operations fail fast when Prefect unavailable:**
   - Before: Operations continued, warnings logged
   - After: 503 errors for create/update/pause/resume/trigger

### Migration Path

**Existing ingestions without deployments:**
- Cannot be triggered until schedule is configured
- Need to run migration script to create deployments
- See Phase 3 migration plan

---

## Testing Impact

### Tests to Update

1. ❌ **Remove:** `test_prefect_degradation.py` - No longer relevant
2. ❌ **Remove:** Fallback tests in integration suite
3. ✅ **Keep:** E2E tests (`test_prefect_workflows.py`) - These test the correct path

### New Test Scenarios

1. **Test 503 errors when Prefect unavailable:**
   ```python
   def test_create_ingestion_prefect_unavailable_fails():
       # Mock: PrefectService.initialized = False
       # Assert: HTTPException 503 raised
   ```

2. **Test trigger requires deployment:**
   ```python
   def test_trigger_without_deployment_fails():
       # Create ingestion without schedule
       # Assert: ValueError when triggering
   ```

---

## Operational Considerations

### Monitoring

**Critical Alerts:**
- Prefect server availability (must be monitored)
- Failed ingestion creations (503 errors)
- Failed manual triggers (503 errors)

**Prefect Server SLA:**
- Must maintain high availability (99.9%+)
- Fast failover/recovery required
- Health checks critical

### Deployment Strategy

**Phase 3 Requirements:**
1. Deploy highly-available Prefect infrastructure FIRST
2. Verify Prefect health before cutover
3. Have rollback plan ready
4. Monitor error rates closely

---

## File Changes

**Modified:**
- `app/services/ingestion_service.py` - Removed all fallback logic

**Lines Removed:** ~40 lines of fallback code
**Lines Added:** ~30 lines of fail-fast checks

**No Changes Required:**
- `app/services/batch_orchestrator.py` - Still used by Prefect flows
- `app/prefect/flows/run_ingestion.py` - Correctly uses BatchOrchestrator
- API endpoints - Already correctly calling async service methods

---

## Verification

```bash
# Syntax check
python -m py_compile app/services/ingestion_service.py
# ✅ Passed

# Search for remaining fallback code
grep -r "fallback\|Fall through\|BatchOrchestrator" app/services/ingestion_service.py
# ✅ No matches (BatchOrchestrator import removed)

# Run existing tests
pytest tests/e2e/test_prefect_workflows.py -v
# ⏳ Should pass (testing correct Prefect path)
```

---

## Next Steps

1. **Run Tests:** Verify E2E tests still pass
2. **Update Tests:** Remove degradation tests
3. **Phase 3:** Deploy Prefect infrastructure to Kubernetes
4. **Migration:** Create script to add deployments to existing ingestions
5. **Monitoring:** Set up Prefect availability alerts

---

## Summary

**Goal:** Single execution path via Prefect only ✅

**Changes:**
- ✅ Removed fallback to BatchOrchestrator in trigger
- ✅ Fail fast when Prefect unavailable (create/update/pause/resume)
- ✅ Enforce deployment required for execution
- ✅ Kept BatchOrchestrator (used by Prefect flows)
- ✅ Kept graceful degradation for delete (cleanup operation)

**Result:**
- Simpler codebase
- Clear error messages
- Fail-fast behavior
- Ready for Phase 3 deployment

**Status:** ✅ **CLEANUP COMPLETE**
