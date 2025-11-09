"""
E2E Test: Incremental Load (E2E-02)

Tests the incremental file processing workflow:
1. Create ingestion configuration for S3 JSON files
2. Upload 3 JSON files and trigger first run
3. Verify first run processes 3 files, 3000 records
4. Upload 2 additional JSON files
5. Trigger second run
6. Verify second run processes only 2 NEW files (total 5000 records, not 6000)
7. Verify no duplicate data
8. Verify run history and metrics accuracy

This test validates the core value proposition:
- File state tracking prevents re-processing
- Checkpoint management works correctly
- Metrics are accurate across multiple runs
- No data duplication occurs

This test uses REAL services from docker-compose.test.yml:
- MinIO (S3-compatible storage) on localhost:9000
- Spark Connect on localhost:15002
- PostgreSQL on localhost:5432

All interactions are API-only (no direct database manipulation).
"""

import time
import pytest
from typing import Dict
from fastapi.testclient import TestClient
from prefect import get_client
from pyspark.sql import SparkSession

from .helpers import (
    E2ELogger,
    create_standard_ingestion,
    wait_for_run_completion,
    assert_run_metrics,
    verify_table_data,
    upload_json_files,
    generate_unique_table_name,
    get_table_identifier,
    print_test_summary,
    verify_prefect_deployment_exists,
    verify_prefect_deployment_active,
    verify_prefect_deployment_deleted,
)

@pytest.mark.e2e
@pytest.mark.requires_prefect
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestIncrementalLoadPrefect:
    """E2E test for incremental file processing across multiple runs with Prefect integration"""

    async def test_incremental_processing_with_prefect(
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
        Test incremental file processing across multiple runs with Prefect.

        Success criteria:
        - Ingestion with schedule → Prefect deployment created
        - First run via Prefect: 3 files processed, 3000 records
        - Second run via Prefect: Only 2 NEW files processed, total 5000 records (not 6000)
        - No duplicate data based on ID field
        - Accurate metrics for both runs
        - All 5 files marked as processed
        - Deployment deleted on cleanup
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Incremental Load with Prefect")

        table_name = generate_unique_table_name("prefect_incremental")
        deployment_id = None

        # =============================================================================
        # Phase 1: Create ingestion with schedule
        # =============================================================================
        logger.phase("📝 Phase 1: Creating ingestion with schedule...")

        ingestion = create_standard_ingestion(
            api_client=e2e_api_client_no_override,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config_for_ingestion,
            table_name=table_name,
            name="E2E Prefect Incremental Load",
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
        assert ingestion.get("prefect_deployment_id") is not None, \
            "Expected prefect_deployment_id to be set when schedule is configured"

        deployment_id = ingestion["prefect_deployment_id"]
        logger.success(f"Prefect deployment ID: {deployment_id}")

        # =============================================================================
        # Phase 2: Verify deployment exists in Prefect
        # =============================================================================
        logger.phase("🔍 Phase 2: Verifying deployment in Prefect server...")

        deployment = await verify_prefect_deployment_exists(
            deployment_id=deployment_id,
            expected_name=f"ingestion-{ingestion_id}",
            expected_tags=["autoloader"],
            logger=logger
        )

        await verify_prefect_deployment_active(deployment_id=deployment_id, logger=logger)

        # =============================================================================
        # Phase 3: First Run - Trigger via Prefect
        # =============================================================================
        logger.section("📦 PHASE 3: Initial Ingestion via Prefect (3 files)")
        logger.step(f"Initial files already uploaded: {len(sample_json_files)} files", always=True)

        logger.phase("🚀 Triggering first run via Prefect...")

        trigger_response = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/run"
        )
        assert trigger_response.status_code == 202, \
            f"Failed to trigger run: {trigger_response.text}"

        trigger_data = trigger_response.json()
        assert trigger_data.get("method") == "prefect", \
            "Expected trigger via Prefect when deployment exists"
        assert trigger_data.get("flow_run_id") is not None, \
            "Expected flow_run_id from Prefect trigger"

        flow_run_id_1 = trigger_data["flow_run_id"]
        logger.success(f"Prefect flow run 1 triggered: {flow_run_id_1}")

        # =============================================================================
        # Phase 4: Wait for first flow run completion
        # =============================================================================
        logger.phase("⏳ Phase 4: Waiting for first flow run completion...")

        max_wait_time = 180
        poll_interval = 3
        start_time = time.time()
        run1_id = None

        async with get_client() as prefect_client:
            while time.time() - start_time < max_wait_time:
                try:
                    flow_run = await prefect_client.read_flow_run(flow_run_id_1)
                    state_name = flow_run.state.name if flow_run.state else "unknown"

                    if logger.verbose:
                        elapsed = int(time.time() - start_time)
                        print(f"  ⏱️  {elapsed}s - Prefect state: {state_name}")

                    if state_name in ["Completed", "COMPLETED"]:
                        logger.success(f"Flow run 1 completed successfully")
                        break
                    elif state_name in ["Failed", "FAILED", "Crashed", "CRASHED"]:
                        pytest.fail(f"Flow run 1 failed with state: {state_name}")

                    time.sleep(poll_interval)

                except Exception as e:
                    pytest.fail(f"Error checking flow run status: {e}")

            if run1_id is None:
                logger.step("Fetching run ID from API...")
                runs_response = e2e_api_client_no_override.get(
                    f"/api/v1/ingestions/{ingestion_id}/runs"
                )
                assert runs_response.status_code == 200
                runs = runs_response.json()
                if runs:
                    run1_id = runs[0]["id"]
                    logger.success(f"Found run 1 ID: {run1_id}")

        if run1_id is None:
            pytest.fail(f"Flow run did not complete within {max_wait_time}s")

        # =============================================================================
        # Phase 5: Verify first run metrics
        # =============================================================================
        logger.phase("📊 Phase 5: Verifying first run metrics...")

        run1 = wait_for_run_completion(
            e2e_api_client_no_override,
            ingestion_id,
            run1_id,
            timeout=60,
            logger=logger
        )

        assert_run_metrics(
            run=run1,
            expected_files=3,
            expected_records=3000,
            expected_errors=0,
            logger=logger
        )
        logger.success("First run metrics verified")

        # =============================================================================
        # Phase 6: Verify data after first run
        # =============================================================================
        logger.phase("🔍 Phase 6: Verifying data after first run...")
        table_identifier = get_table_identifier(ingestion)

        verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=3000,
            expected_fields=["id", "timestamp", "user_id", "event_type", "value"],
            check_duplicates=True,
            logger=logger
        )
        logger.success("Data verification after first run passed")

        # =============================================================================
        # Phase 7: Upload additional files
        # =============================================================================
        logger.section("📦 PHASE 7: Add New Files (2 more files)")

        logger.phase("📤 Uploading 2 additional files...")
        new_files = upload_json_files(
            minio_client=minio_client,
            test_bucket=test_bucket,
            start_file_idx=3,
            num_files=2,
            records_per_file=1000,
            logger=logger
        )
        logger.success(f"Uploaded {len(new_files)} additional files")
        logger.step(f"Total files in bucket: {len(sample_json_files) + len(new_files)}", always=True)

        # =============================================================================
        # Phase 8: Second Run - Trigger via Prefect (Incremental)
        # =============================================================================
        logger.section("📦 PHASE 8: Second Ingestion Run via Prefect (Incremental)")

        logger.phase("🚀 Triggering second run via Prefect...")

        trigger_response_2 = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/run"
        )
        assert trigger_response_2.status_code == 202

        trigger_data_2 = trigger_response_2.json()
        flow_run_id_2 = trigger_data_2["flow_run_id"]
        logger.success(f"Prefect flow run 2 triggered: {flow_run_id_2}")

        # =============================================================================
        # Phase 9: Wait for second flow run completion
        # =============================================================================
        logger.phase("⏳ Phase 9: Waiting for second flow run completion...")

        start_time = time.time()
        run2_id = None

        async with get_client() as prefect_client:
            while time.time() - start_time < max_wait_time:
                try:
                    flow_run = await prefect_client.read_flow_run(flow_run_id_2)
                    state_name = flow_run.state.name if flow_run.state else "unknown"

                    if logger.verbose:
                        elapsed = int(time.time() - start_time)
                        print(f"  ⏱️  {elapsed}s - Prefect state: {state_name}")

                    if state_name in ["Completed", "COMPLETED"]:
                        logger.success(f"Flow run 2 completed successfully")
                        break
                    elif state_name in ["Failed", "FAILED", "Crashed", "CRASHED"]:
                        pytest.fail(f"Flow run 2 failed with state: {state_name}")

                    time.sleep(poll_interval)

                except Exception as e:
                    pytest.fail(f"Error checking flow run status: {e}")

            if run2_id is None:
                logger.step("Fetching run ID from API...")
                runs_response = e2e_api_client_no_override.get(
                    f"/api/v1/ingestions/{ingestion_id}/runs"
                )
                assert runs_response.status_code == 200
                runs = runs_response.json()
                if len(runs) >= 2:
                    # Get the latest run (should be run 2)
                    # Runs are ordered by created_at DESC (newest first)
                    for run in runs:
                        if run["id"] != run1_id:
                            run2_id = run["id"]
                            break
                    logger.success(f"Found run 2 ID: {run2_id}")

        if run2_id is None:
            pytest.fail(f"Flow run 2 did not complete within {max_wait_time}s")

        # =============================================================================
        # Phase 10: Verify second run metrics (CRITICAL - Incremental)
        # =============================================================================
        logger.phase("📊 Phase 10: Verifying second run metrics (CRITICAL - Incremental Load)...")

        run2 = wait_for_run_completion(
            e2e_api_client_no_override,
            ingestion_id,
            run2_id,
            timeout=60,
            logger=logger
        )

        assert_run_metrics(
            run=run2,
            expected_files=2,  # CRITICAL: Only NEW files
            expected_records=2000,  # CRITICAL: Only NEW records
            expected_errors=0,
            logger=logger
        )
        logger.success("Second run metrics verified - INCREMENTAL LOAD WORKING!", always=True)

        # =============================================================================
        # Phase 11: Verify total data (no duplicates)
        # =============================================================================
        logger.phase("🔍 Phase 11: Verifying total data (no duplicates)...")

        # Refresh table metadata to see latest Iceberg snapshots
        spark_session.sql(f"REFRESH TABLE {table_identifier}")

        df = spark_session.table(table_identifier)
        total_record_count = df.count()
        distinct_count = df.select("id").distinct().count()

        logger.step(f"Total records: {total_record_count}", always=True)
        logger.step(f"Distinct IDs: {distinct_count}", always=True)

        # CRITICAL: Total should be 5000 (3000 + 2000), NOT 6000
        assert total_record_count == 5000, \
            f"Expected 5000 total records (3000 + 2000), got {total_record_count}. " \
            f"If you got 6000, it means files were re-processed (FAILED)."

        # CRITICAL: No duplicate IDs
        assert distinct_count == total_record_count, \
            f"Found duplicate IDs: {total_record_count - distinct_count} duplicates. " \
            f"This means data was ingested twice (FAILED)."

        logger.success("Total data verification passed - NO DUPLICATES!", always=True)

        # =============================================================================
        # Phase 12: Verify run history
        # =============================================================================
        logger.phase("📜 Phase 12: Verifying run history...")
        response = e2e_api_client_no_override.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert response.status_code == 200

        runs = response.json()
        assert len(runs) >= 2, "Expected at least 2 runs in history"

        run1_in_history = next((r for r in runs if r["id"] == run1_id), None)
        run2_in_history = next((r for r in runs if r["id"] == run2_id), None)

        assert run1_in_history is not None, f"Run 1 {run1_id} not found in history"
        assert run2_in_history is not None, f"Run 2 {run2_id} not found in history"

        logger.success("Run history verified")

        # =============================================================================
        # Phase 13: Delete ingestion and verify deployment cleanup
        # =============================================================================
        logger.phase("🗑️  Phase 13: Deleting ingestion...")

        delete_response = e2e_api_client_no_override.delete(
            f"/api/v1/ingestions/{ingestion_id}"
        )
        assert delete_response.status_code == 204, \
            f"Failed to delete ingestion: {delete_response.text}"

        logger.success("Ingestion deleted")

        # Verify deployment deleted in Prefect
        await verify_prefect_deployment_deleted(deployment_id=deployment_id, logger=logger)

        # =============================================================================
        # Test Summary
        # =============================================================================
        logger.section("✅ E2E TEST PASSED: Incremental Load with Prefect")
        print_test_summary([
            ("Test", "Incremental Load with Prefect"),
            ("Ingestion ID", ingestion_id),
            ("Deployment ID", deployment_id),
            ("Flow Run 1 ID", flow_run_id_1),
            ("Flow Run 2 ID", flow_run_id_2),
            ("Run 1 ID", run1_id),
            ("Run 2 ID", run2_id),
            ("Run 1", "3 files, 3000 records"),
            ("Run 2", "2 files, 2000 records (INCREMENTAL)"),
            ("Total", "5 files, 5000 records (NO DUPLICATES)"),
            ("Table", table_identifier),
            ("Cleanup", "✅ Deployment deleted"),
            ("Status", "SUCCESS ✅")
        ], footer_message="🎉 INCREMENTAL LOAD + PREFECT VALIDATED!")
