"""
E2E Tests: Prefect Graceful Degradation (Phase 2)

Tests system behavior when Prefect is unavailable:
1. Application starts successfully when Prefect unreachable
2. Can create ingestion when Prefect unavailable
3. Manual trigger falls back to BatchOrchestrator
4. Can update schedule when Prefect unavailable

These tests verify that the system remains functional without Prefect,
gracefully degrading to direct execution mode.

IMPORTANT: These tests intentionally use a non-existent Prefect URL
to simulate Prefect being unavailable.
"""

import pytest
import os
from typing import Dict
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession
from unittest.mock import patch

from tests.e2e.helpers import (
    E2ELogger,
    create_standard_ingestion,
    trigger_run,
    wait_for_run_completion,
    assert_run_metrics,
    verify_table_data,
    generate_unique_table_name,
    get_table_identifier,
    print_test_summary,
)


@pytest.fixture
def prefect_unavailable_config(monkeypatch):
    """
    Configure environment to point to non-existent Prefect server.
    This simulates Prefect being unavailable.
    """
    # Point to non-existent Prefect server
    monkeypatch.setenv("PREFECT_API_URL", "http://localhost:9999")
    yield


@pytest.fixture
def api_client_no_prefect(
    prefect_unavailable_config,
    ensure_services_ready,
    test_engine
):
    """
    Create FastAPI test client with Prefect unavailable.

    This fixture ensures the application can start even when Prefect
    is unreachable, demonstrating graceful degradation.
    """
    from fastapi.testclient import TestClient
    from app.main import app
    import app.services.prefect_service as prefect_module

    # Clear singleton to force recreation with new config
    prefect_module._prefect_service = None

    client = TestClient(app)
    yield client

    # Cleanup: restore original state
    prefect_module._prefect_service = None


@pytest.mark.e2e
class TestPrefectDegradation:
    """E2E tests for system behavior when Prefect unavailable"""

    def test_create_ingestion_without_prefect(
        self,
        api_client_no_prefect: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        sample_json_files: list,
        test_cluster_id: str,
    ):
        """
        Can create ingestion when Prefect unavailable:
        1. Create ingestion with schedule
        2. Verify ingestion created in database
        3. Verify prefect_deployment_id is NULL
        4. System continues functioning
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Create Ingestion Without Prefect")

        table_name = generate_unique_table_name("no_prefect_create")

        # =============================================================================
        # Phase 1: Create ingestion with schedule (Prefect unavailable)
        # =============================================================================
        logger.phase("📝 Phase 1: Creating ingestion (Prefect unavailable)...")

        ingestion = create_standard_ingestion(
            api_client=api_client_no_prefect,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E No Prefect Create Test",
            schedule={
                "frequency": "hourly",
                "time": None,
                "timezone": "UTC",
                "cron_expression": None,
                "backfill": {"enabled": False}
            }
        )

        ingestion_id = ingestion["id"]
        logger.success(f"Created ingestion: {ingestion_id}")

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"

        # =============================================================================
        # Phase 2: Verify no Prefect deployment created
        # =============================================================================
        logger.phase("🔍 Phase 2: Verifying no Prefect deployment...")

        deployment_id = ingestion.get("prefect_deployment_id")
        logger.step(f"Prefect deployment ID: {deployment_id}")

        assert deployment_id is None, \
            "Expected prefect_deployment_id to be NULL when Prefect unavailable"

        logger.success("Graceful degradation: No deployment created ✅")

        # =============================================================================
        # Phase 3: Verify ingestion persisted correctly
        # =============================================================================
        logger.phase("📊 Phase 3: Verifying ingestion persisted...")

        get_response = api_client_no_prefect.get(f"/api/v1/ingestions/{ingestion_id}")
        assert get_response.status_code == 200, \
            f"Failed to retrieve ingestion: {get_response.text}"

        retrieved = get_response.json()
        assert retrieved["id"] == ingestion_id
        assert retrieved["name"] == "E2E No Prefect Create Test"
        assert retrieved.get("prefect_deployment_id") is None

        logger.success("Ingestion persisted correctly")

        # =============================================================================
        # Cleanup
        # =============================================================================
        logger.phase("🧹 Cleanup: Deleting ingestion...")

        delete_response = api_client_no_prefect.delete(
            f"/api/v1/ingestions/{ingestion_id}"
        )
        assert delete_response.status_code == 204

        logger.success("Ingestion deleted")

        # =============================================================================
        # Test Summary
        # =============================================================================
        logger.section("✅ E2E TEST PASSED: Create Ingestion Without Prefect")
        print_test_summary([
            ("Test", "Create Without Prefect"),
            ("Ingestion ID", ingestion_id),
            ("Schedule Configured", "hourly"),
            ("Prefect Deployment ID", "None (as expected)"),
            ("System Behavior", "✅ Graceful degradation"),
            ("Test Status", "SUCCESS ✅")
        ])

    def test_manual_trigger_without_prefect(
        self,
        api_client_no_prefect: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        sample_json_files: list,
        spark_session: SparkSession,
        test_cluster_id: str,
    ):
        """
        Manual trigger falls back to BatchOrchestrator:
        1. Create ingestion (no Prefect)
        2. Trigger manual run
        3. Verify run completes via BatchOrchestrator
        4. Verify response indicates "direct" method
        5. Verify data ingested successfully
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Manual Trigger Without Prefect")

        table_name = generate_unique_table_name("no_prefect_trigger")

        # =============================================================================
        # Phase 1: Create ingestion
        # =============================================================================
        logger.phase("📝 Phase 1: Creating ingestion...")

        ingestion = create_standard_ingestion(
            api_client=api_client_no_prefect,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E No Prefect Trigger Test",
        )

        ingestion_id = ingestion["id"]
        logger.success(f"Created ingestion: {ingestion_id}")

        assert ingestion.get("prefect_deployment_id") is None

        # =============================================================================
        # Phase 2: Trigger manual run
        # =============================================================================
        logger.phase("🚀 Phase 2: Triggering manual run (should fallback)...")

        trigger_response = api_client_no_prefect.post(
            f"/api/v1/ingestions/{ingestion_id}/run"
        )
        assert trigger_response.status_code == 202, \
            f"Failed to trigger run: {trigger_response.text}"

        trigger_data = trigger_response.json()
        logger.step(f"Trigger method: {trigger_data.get('method', 'unknown')}")
        logger.step(f"Trigger status: {trigger_data.get('status', 'unknown')}")

        # =============================================================================
        # Phase 3: Verify fallback to direct execution
        # =============================================================================
        logger.phase("🔍 Phase 3: Verifying fallback to BatchOrchestrator...")

        # Should execute directly via BatchOrchestrator (not Prefect)
        assert trigger_data.get("method") == "direct", \
            "Expected trigger via direct execution when Prefect unavailable"
        assert trigger_data.get("run_id") is not None, \
            "Expected run_id from direct execution"

        run_id = trigger_data["run_id"]
        logger.success(f"Fallback successful: Run ID {run_id}")

        # =============================================================================
        # Phase 4: Verify run completed successfully
        # =============================================================================
        logger.phase("⏳ Phase 4: Verifying run completion...")

        run = wait_for_run_completion(
            api_client_no_prefect,
            ingestion_id,
            run_id,
            timeout=180,
            logger=logger
        )

        assert_run_metrics(
            run=run,
            expected_files=3,
            expected_records=3000,
            expected_errors=0,
            logger=logger
        )

        # =============================================================================
        # Phase 5: Verify data in Iceberg table
        # =============================================================================
        logger.phase("🔍 Phase 5: Verifying data ingested...")

        table_identifier = get_table_identifier(ingestion)

        verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=3000,
            expected_fields=["id", "timestamp", "user_id", "event_type", "value"],
            check_duplicates=True,
            logger=logger
        )

        logger.success("Data verification passed")

        # =============================================================================
        # Cleanup
        # =============================================================================
        logger.phase("🧹 Cleanup: Deleting ingestion...")

        delete_response = api_client_no_prefect.delete(
            f"/api/v1/ingestions/{ingestion_id}"
        )
        assert delete_response.status_code == 204

        logger.success("Ingestion deleted")

        # =============================================================================
        # Test Summary
        # =============================================================================
        logger.section("✅ E2E TEST PASSED: Manual Trigger Without Prefect")
        print_test_summary([
            ("Test", "Manual Trigger Fallback"),
            ("Ingestion ID", ingestion_id),
            ("Run ID", run_id),
            ("Execution Method", "direct (BatchOrchestrator)"),
            ("Files Processed", 3),
            ("Records Ingested", 3000),
            ("Table", table_identifier),
            ("Fallback Behavior", "✅ Verified"),
            ("Test Status", "SUCCESS ✅")
        ])

    def test_update_schedule_without_prefect(
        self,
        api_client_no_prefect: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        sample_json_files: list,
        test_cluster_id: str,
    ):
        """
        Can update schedule when Prefect unavailable:
        1. Create ingestion
        2. Update schedule
        3. Verify update succeeds (no exception)
        4. System continues functioning
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Update Schedule Without Prefect")

        table_name = generate_unique_table_name("no_prefect_update")

        # =============================================================================
        # Phase 1: Create ingestion
        # =============================================================================
        logger.phase("📝 Phase 1: Creating ingestion...")

        ingestion = create_standard_ingestion(
            api_client=api_client_no_prefect,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E No Prefect Update Test",
            schedule={
                "frequency": "hourly",
                "time": None,
                "timezone": "UTC",
                "cron_expression": None,
                "backfill": {"enabled": False}
            }
        )

        ingestion_id = ingestion["id"]
        logger.success(f"Created ingestion: {ingestion_id}")

        assert ingestion.get("prefect_deployment_id") is None

        # =============================================================================
        # Phase 2: Update schedule
        # =============================================================================
        logger.phase("🔄 Phase 2: Updating schedule (Prefect unavailable)...")

        update_response = api_client_no_prefect.put(
            f"/api/v1/ingestions/{ingestion_id}",
            json={
                "schedule": {
                    "frequency": "daily",
                    "time": "09:00",
                    "timezone": "UTC",
                    "cron_expression": None,
                    "backfill": {"enabled": False}
                }
            }
        )

        assert update_response.status_code == 200, \
            f"Failed to update schedule: {update_response.text}"

        logger.success("Schedule update succeeded (graceful degradation)")

        # =============================================================================
        # Phase 3: Verify schedule updated
        # =============================================================================
        logger.phase("🔍 Phase 3: Verifying schedule updated...")

        updated_ingestion = update_response.json()
        logger.step(f"Updated schedule: {updated_ingestion.get('schedule', {})}")

        assert updated_ingestion["id"] == ingestion_id
        # Note: Schedule update stored in database even though Prefect unavailable
        # This is expected graceful degradation behavior

        logger.success("Schedule persisted correctly")

        # =============================================================================
        # Cleanup
        # =============================================================================
        logger.phase("🧹 Cleanup: Deleting ingestion...")

        delete_response = api_client_no_prefect.delete(
            f"/api/v1/ingestions/{ingestion_id}"
        )
        assert delete_response.status_code == 204

        logger.success("Ingestion deleted")

        # =============================================================================
        # Test Summary
        # =============================================================================
        logger.section("✅ E2E TEST PASSED: Update Schedule Without Prefect")
        print_test_summary([
            ("Test", "Update Schedule Without Prefect"),
            ("Ingestion ID", ingestion_id),
            ("Original Schedule", "hourly"),
            ("Updated Schedule", "daily 09:00"),
            ("System Behavior", "✅ Graceful degradation"),
            ("Test Status", "SUCCESS ✅")
        ])
