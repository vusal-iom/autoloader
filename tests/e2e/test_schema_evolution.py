"""
E2E Test: Schema Evolution (E2E-03) - Prefect Integration

Tests the schema evolution workflow with Prefect:
1. Create ingestion with schedule and schema evolution enabled → Prefect deployment created
2. Upload 3 JSON files with base schema (5 fields)
3. Trigger first run via Prefect and verify 3000 records with 5 columns
4. Upload 2 JSON files with evolved schema (7 fields - adding region, metadata)
5. Trigger second run via Prefect
6. Verify schema evolution detected and applied (7 columns in table)
7. Verify backward compatibility (old records have NULL for new fields)
8. Verify all 5000 records are queryable with full schema
9. Delete ingestion → Prefect deployment deleted

This test validates:
- Automatic schema detection on new files
- Schema evolution in Iceberg tables
- Backward compatibility of existing data
- No data loss during schema changes
- Incremental processing still works with schema changes
- Prefect integration works end-to-end

This test requires REAL Prefect server from docker-compose.test.yml.
All interactions are API-only for setup/verification.

IMPORTANT: This test commits data to the real database (not using transactional isolation)
because Prefect tasks create their own database sessions via SessionLocal().
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
    trigger_run,
    wait_for_run_completion,
    assert_run_metrics,
    verify_table_data,
    verify_schema_evolution,
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
class TestSchemaEvolutionPrefect:
    """E2E test for schema evolution across multiple runs with Prefect integration"""

    async def test_schema_evolution_with_prefect(
        self,
        e2e_api_client_no_override: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        minio_config_for_ingestion: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        spark_session: SparkSession,
        test_tenant_id: str,
        test_cluster_id: str,
        spark_connect_url: str
    ):
        """
        Test schema evolution with Prefect integration.

        Success criteria:
        - Ingestion with schedule → Prefect deployment created
        - First run via Prefect: 3 files, 3000 records, 5 columns
        - Second run via Prefect: 2 files, 2000 records, schema evolved to 7 columns
        - Old records: Have NULL for new fields (backward compatibility)
        - New records: Have values for all 7 fields
        - All 5000 records queryable
        - No data loss
        - Deployment deleted on cleanup
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Schema Evolution with Prefect")

        table_name = generate_unique_table_name("prefect_schema_evolution")
        deployment_id = None

        # =============================================================================
        # Phase 1: Create ingestion with schedule and evolution enabled
        # =============================================================================
        logger.phase("📝 Phase 1: Creating ingestion with schedule and evolution enabled...")

        ingestion = create_standard_ingestion(
            api_client=e2e_api_client_no_override,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config_for_ingestion,
            table_name=table_name,
            name="E2E Prefect Schema Evolution",
            evolution_enabled=True,  # CRITICAL: Enable schema evolution
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
        logger.step(f"Schema Evolution: {ingestion['format']['schema']['evolution_enabled']}", always=True)

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"
        assert ingestion["format"]["schema"]["evolution_enabled"] is True
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
        # Phase 3: Upload files with BASE schema
        # =============================================================================
        logger.section("📦 PHASE 3: Initial Load (Base Schema - 5 fields)")

        logger.phase("📤 Uploading 3 files with BASE schema (5 fields)...")
        base_files = upload_json_files(
            minio_client=minio_client,
            test_bucket=test_bucket,
            start_file_idx=0,
            num_files=3,
            records_per_file=1000,
            schema_type="base",  # 5 fields
            logger=logger
        )
        logger.success(f"Uploaded {len(base_files)} files with BASE schema")
        logger.step("Fields: id, timestamp, user_id, event_type, value", always=True)

        # =============================================================================
        # Phase 4: First Run - Trigger via Prefect
        # =============================================================================
        logger.phase("🚀 Phase 4: Triggering first run via Prefect...")

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
        # Phase 5: Wait for first flow run completion
        # =============================================================================
        logger.phase("⏳ Phase 5: Waiting for first flow run completion...")

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
        # Phase 6: Verify first run metrics
        # =============================================================================
        logger.phase("📊 Phase 6: Verifying first run metrics...")

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
        # Phase 7: Verify initial schema (5 fields)
        # =============================================================================
        logger.phase("🔍 Phase 7: Verifying initial schema (5 fields)...")
        table_identifier = get_table_identifier(ingestion)

        # Refresh table metadata to see latest Iceberg snapshots
        spark_session.sql(f"REFRESH TABLE {table_identifier}")

        df = verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=3000,
            expected_fields=["id", "timestamp", "user_id", "event_type", "value"],
            unexpected_fields=["region", "metadata"],  # Should NOT exist yet
            check_duplicates=True,
            logger=logger
        )
        logger.success("Initial schema verified (5 base fields only)")

        # =============================================================================
        # Phase 8: Upload files with EVOLVED schema
        # =============================================================================
        logger.section("📦 PHASE 8: Schema Evolution (Add region, metadata)")

        logger.phase("📤 Uploading 2 files with EVOLVED schema (7 fields)...")
        evolved_files = upload_json_files(
            minio_client=minio_client,
            test_bucket=test_bucket,
            start_file_idx=3,
            num_files=2,
            records_per_file=1000,
            schema_type="evolved",  # 7 fields
            logger=logger
        )
        logger.success(f"Uploaded {len(evolved_files)} files with EVOLVED schema")
        logger.step("NEW fields: region, metadata", always=True)

        # =============================================================================
        # Phase 9: Second Run - Trigger via Prefect (with evolved schema)
        # =============================================================================
        logger.phase("🚀 Phase 9: Triggering second run via Prefect...")

        trigger_response_2 = e2e_api_client_no_override.post(
            f"/api/v1/ingestions/{ingestion_id}/run"
        )
        assert trigger_response_2.status_code == 202

        trigger_data_2 = trigger_response_2.json()
        flow_run_id_2 = trigger_data_2["flow_run_id"]
        logger.success(f"Prefect flow run 2 triggered: {flow_run_id_2}")

        # =============================================================================
        # Phase 10: Wait for second flow run completion
        # =============================================================================
        logger.phase("⏳ Phase 10: Waiting for second flow run completion...")

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
        # Phase 11: Verify second run metrics
        # =============================================================================
        logger.phase("📊 Phase 11: Verifying second run metrics...")

        run2 = wait_for_run_completion(
            e2e_api_client_no_override,
            ingestion_id,
            run2_id,
            timeout=60,
            logger=logger
        )

        assert_run_metrics(
            run=run2,
            expected_files=2,
            expected_records=2000,
            expected_errors=0,
            logger=logger
        )
        logger.success("Second run metrics verified")

        # =============================================================================
        # Phase 12: Verify schema evolution (7 fields)
        # =============================================================================
        logger.phase("🔍 Phase 12: Verifying schema evolution (7 fields)...")

        # Refresh table metadata to see latest Iceberg snapshots
        spark_session.sql(f"REFRESH TABLE {table_identifier}")

        df = verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=5000,
            expected_fields=["id", "timestamp", "user_id", "event_type", "value", "region", "metadata"],
            check_duplicates=True,
            logger=logger
        )
        logger.success("Schema evolution verified - Table now has 7 fields!", always=True)

        # =============================================================================
        # Phase 13: Verify backward and forward compatibility
        # =============================================================================
        logger.phase("🔍 Phase 13: Verifying backward and forward compatibility...")

        verify_schema_evolution(
            spark_session=spark_session,
            table_identifier=table_identifier,
            old_record_filter="id < 3000",
            new_record_filter="id >= 3000",
            old_count=3000,
            new_count=2000,
            new_fields=["region", "metadata"],
            logger=logger
        )

        # =============================================================================
        # Phase 14: Verify all data is queryable
        # =============================================================================
        logger.phase("🔍 Phase 14: Verifying all data is queryable...")

        result = df.groupBy("event_type").count().collect()
        logger.step(f"Event type distribution: {len(result)} types", always=True)

        region_counts = df.filter("region IS NOT NULL").groupBy("region").count().collect()
        logger.step(f"Region distribution: {len(region_counts)} regions (new records only)", always=True)

        logger.success("All data is queryable - Schema evolution successful!")

        # =============================================================================
        # Phase 15: Delete ingestion and verify deployment cleanup
        # =============================================================================
        logger.phase("🗑️  Phase 15: Deleting ingestion...")

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
        logger.section("✅ E2E TEST PASSED: Schema Evolution with Prefect")
        print_test_summary([
            ("Test", "Schema Evolution with Prefect"),
            ("Ingestion ID", ingestion_id),
            ("Deployment ID", deployment_id),
            ("Flow Run 1 ID", flow_run_id_1),
            ("Flow Run 2 ID", flow_run_id_2),
            ("Run 1 ID", run1_id),
            ("Run 2 ID", run2_id),
            ("Run 1", "3 files, 3000 records, 5 fields"),
            ("Run 2", "2 files, 2000 records, SCHEMA EVOLVED to 7 fields"),
            ("Total", "5 files, 5000 records"),
            ("Backward Compatibility", "✅ (old records have NULL for new fields)"),
            ("Forward Compatibility", "✅ (new records have all fields)"),
            ("Table", table_identifier),
            ("Cleanup", "✅ Deployment deleted"),
            ("Status", "SUCCESS ✅")
        ], footer_message="🎉 SCHEMA EVOLUTION + PREFECT VALIDATED!")
