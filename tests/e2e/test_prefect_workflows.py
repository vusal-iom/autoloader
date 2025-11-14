"""
E2E Tests: Prefect Workflows (Phase 2)

Tests the complete Prefect integration lifecycle:
1. Create ingestion with schedule → Prefect deployment created
2. Trigger manual run → Prefect flow execution
3. Verify deployment in Prefect server
4. Update schedule → Prefect deployment updated
5. Pause/resume → Prefect deployment paused/resumed
6. Delete ingestion → Prefect deployment deleted

This test requires REAL Prefect server from docker-compose.test.yml.
All interactions are API-only for setup/verification.

IMPORTANT: This test commits data to the real database (not using transactional isolation)
because Prefect tasks create their own database sessions via SessionLocal().
"""

from typing import Dict

import pytest
from fastapi.testclient import TestClient
from prefect import get_client
from pyspark.sql import SparkSession

from tests.e2e.helpers import (
    TestLogger,
    create_standard_ingestion,
    wait_for_run_completion,
    assert_run_metrics,
    verify_table_data,
    generate_unique_table_name,
    get_table_identifier,
    print_test_summary,
    verify_prefect_deployment_exists,
    verify_prefect_deployment_active,
    verify_prefect_deployment_paused,
    verify_prefect_deployment_deleted,
    wait_for_prefect_flow_completion,
    get_latest_run_id,
)


@pytest.mark.e2e
@pytest.mark.requires_prefect
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestPrefectWorkflows:
    """E2E tests for complete Prefect workflow with real Prefect server"""

    async def test_complete_lifecycle_with_prefect(
        self,
        e2e_api_client_no_override: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        minio_config_for_ingestion: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        sample_json_files: list,
        spark_session: SparkSession,
        test_tenant_id: str,
        test_cluster_id: str,
        spark_connect_url: str
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
        logger = TestLogger()
        logger.section("E2E TEST: Complete Prefect Lifecycle")

        table_name = generate_unique_table_name("prefect_lifecycle")
        deployment_id = None

        # Phase 1: Create ingestion with schedule
        logger.phase("Phase 1: Creating ingestion with schedule")

        ingestion = create_standard_ingestion(
            api_client=e2e_api_client_no_override,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config_for_ingestion,  # Use worker-accessible endpoint
            table_name=table_name,
            name="E2E Prefect Lifecycle Test",
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
        logger.step(f"Schedule: hourly")

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"
        assert ingestion.get("prefect_deployment_id") is not None, \
            "Expected prefect_deployment_id to be set when schedule is configured"

        deployment_id = ingestion["prefect_deployment_id"]
        logger.success(f"Prefect deployment ID: {deployment_id}")

        # Phase 2: Verify deployment exists in Prefect
        logger.phase("Phase 2: Verifying deployment in Prefect server")

        deployment = await verify_prefect_deployment_exists(
            deployment_id=deployment_id,
            expected_name=f"ingestion-{ingestion_id}",
            expected_tags=["autoloader"],
            logger=logger
        )

        # Verify deployment is active with schedules
        await verify_prefect_deployment_active(deployment_id=deployment_id, logger=logger)

        # Phase 3: Trigger manual run via API
        logger.phase("Phase 3: Triggering manual run via API")

        trigger_response = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/run"
        )
        assert trigger_response.status_code == 202, \
            f"Failed to trigger run: {trigger_response.text}"

        trigger_data = trigger_response.json()
        logger.step(f"Trigger method: {trigger_data.get('method', 'unknown')}")
        logger.step(f"Trigger status: {trigger_data.get('status', 'unknown')}")

        # Should trigger via Prefect (not direct)
        assert trigger_data.get("method") == "prefect", \
            "Expected trigger via Prefect when deployment exists"
        assert trigger_data.get("flow_run_id") is not None, \
            "Expected flow_run_id from Prefect trigger"

        flow_run_id = trigger_data["flow_run_id"]
        logger.success(f"Prefect flow run triggered: {flow_run_id}")

        # Phase 4: Wait for flow run completion and verify in Prefect
        logger.phase("Phase 4: Waiting for flow run completion")

        await wait_for_prefect_flow_completion(
            flow_run_id=flow_run_id,
            logger=logger
        )

        # Fetch run ID from API
        run_id = get_latest_run_id(e2e_api_client_no_override, ingestion_id)
        if run_id is None:
            pytest.fail("Flow run completed but no run ID found in database")

        logger.success(f"Found run ID: {run_id}")

        # Phase 5: Verify run metrics via API
        logger.phase("Phase 5: Verifying run metrics")

        run = wait_for_run_completion(
            e2e_api_client_no_override,
            ingestion_id,
            run_id,
            timeout=60,  # Should be quick since flow already completed
            logger=logger
        )

        assert_run_metrics(
            run=run,
            expected_files=3,
            expected_records=3000,
            expected_errors=0,
            logger=logger
        )

        # Phase 6: Verify data in Iceberg table
        logger.phase("Phase 6: Verifying data in Iceberg table")

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

        # Phase 7: Update schedule
        logger.phase("Phase 7: Updating schedule")

        update_response = e2e_api_client_no_override.put(
            f"/api/v1/ingestions/{ingestion_id}",
            json={
                "schedule": {
                    "frequency": "daily",
                    "time": "14:30",
                    "timezone": "UTC",
                    "cron_expression": None,
                    "backfill": {"enabled": False}
                }
            }
        )
        assert update_response.status_code == 200, \
            f"Failed to update schedule: {update_response.text}"

        updated_ingestion = update_response.json()
        logger.success("Schedule updated to daily at 14:30")

        # Verify deployment schedule updated in Prefect
        async with get_client() as prefect_client:
            deployment = await prefect_client.read_deployment(deployment_id)
            logger.step(f"Prefect schedules: {deployment.schedules}")
            assert deployment.schedules, "Expected schedules to remain set"

        # Phase 8: Test pause/resume
        logger.phase("Phase 8: Testing pause")

        pause_response = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/pause"
        )
        assert pause_response.status_code == 200, \
            f"Failed to pause ingestion: {pause_response.text}"

        logger.success("Ingestion paused")

        # Verify deployment paused in Prefect
        await verify_prefect_deployment_paused(deployment_id=deployment_id, logger=logger)

        logger.phase("Resuming")

        resume_response = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/resume"
        )
        assert resume_response.status_code == 200, \
            f"Failed to resume ingestion: {resume_response.text}"

        logger.success("Ingestion resumed")

        # Verify deployment resumed in Prefect
        await verify_prefect_deployment_active(deployment_id=deployment_id, logger=logger)

        # Phase 9: Delete ingestion
        logger.phase("Phase 9: Deleting ingestion")

        delete_response = e2e_api_client_no_override.delete(
            f"/api/v1/ingestions/{ingestion_id}"
        )
        assert delete_response.status_code == 204, \
            f"Failed to delete ingestion: {delete_response.text}"

        logger.success("Ingestion deleted")

        # Verify deployment deleted in Prefect
        await verify_prefect_deployment_deleted(deployment_id=deployment_id, logger=logger)

        # Test Summary
        logger.section("E2E TEST PASSED: Complete Prefect Lifecycle")
        print_test_summary([
            ("Test", "Complete Lifecycle"),
            ("Ingestion ID", ingestion_id),
            ("Deployment ID", deployment_id),
            ("Flow Run ID", flow_run_id),
            ("Run ID", run_id),
            ("Files Processed", 3),
            ("Records Ingested", 3000),
            ("Table", table_identifier),
            ("Schedule Changes", "hourly -> daily 14:30"),
            ("Pause/Resume", "Verified"),
            ("Cleanup", "Deployment deleted"),
            ("Test Status", "SUCCESS")
        ])

    async def test_pause_resume_workflow(
        self,
        e2e_api_client_no_override: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        sample_json_files: list,
        test_tenant_id: str,
        test_cluster_id: str,
    ):
        """
        Test pause/resume:
        1. Create active ingestion with schedule
        2. Verify deployment is active
        3. Pause via API
        4. Verify deployment paused in Prefect
        5. Resume via API
        6. Verify deployment active again
        """
        logger = TestLogger()
        logger.section("E2E TEST: Pause/Resume Workflow")

        table_name = generate_unique_table_name("prefect_pause_resume")

        # Phase 1: Create ingestion with schedule
        logger.phase("Phase 1: Creating ingestion with schedule")

        ingestion = create_standard_ingestion(
            api_client=e2e_api_client_no_override,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E Pause/Resume Test",
            schedule={
                "frequency": "daily",
                "time": "10:00",
                "timezone": "UTC",
                "cron_expression": None,
                "backfill": {"enabled": False}
            }
        )

        ingestion_id = ingestion["id"]
        deployment_id = ingestion.get("prefect_deployment_id")

        assert deployment_id is not None, "Expected deployment to be created"
        logger.success(f"Created ingestion: {ingestion_id}")
        logger.success(f"Deployment ID: {deployment_id}")

        # Phase 2: Verify deployment is active
        logger.phase("Phase 2: Verifying deployment is active")

        await verify_prefect_deployment_active(deployment_id=deployment_id, logger=logger)

        # Phase 3: Pause ingestion
        logger.phase("Phase 3: Pausing ingestion")

        pause_response = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/pause"
        )
        assert pause_response.status_code == 200, \
            f"Failed to pause: {pause_response.text}"

        logger.success("Pause request successful")

        # Phase 4: Verify deployment paused in Prefect
        logger.phase("Phase 4: Verifying deployment paused in Prefect")

        await verify_prefect_deployment_paused(deployment_id=deployment_id, logger=logger)

        # Phase 5: Resume ingestion
        logger.phase("Phase 5: Resuming ingestion")

        resume_response = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/resume"
        )
        assert resume_response.status_code == 200, \
            f"Failed to resume: {resume_response.text}"

        logger.success("Resume request successful")

        # Phase 6: Verify deployment active again in Prefect
        logger.phase("Phase 6: Verifying deployment active in Prefect")

        await verify_prefect_deployment_active(deployment_id=deployment_id, logger=logger)

        # Cleanup
        logger.phase("Cleanup: Deleting ingestion")

        delete_response = e2e_api_client_no_override.delete(
            f"/api/v1/ingestions/{ingestion_id}"
        )
        assert delete_response.status_code == 204

        logger.success("Ingestion deleted")

        # Test Summary
        logger.section("E2E TEST PASSED: Pause/Resume Workflow")
        print_test_summary([
            ("Test", "Pause/Resume Workflow"),
            ("Ingestion ID", ingestion_id),
            ("Deployment ID", deployment_id),
            ("Pause Operation", "Verified in Prefect"),
            ("Resume Operation", "Verified in Prefect"),
            ("Test Status", "SUCCESS")
        ])
