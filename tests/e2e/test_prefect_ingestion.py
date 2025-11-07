"""
E2E Test: Prefect-based Ingestion Flow

Tests the complete Prefect workflow:
1. Create ingestion configuration via API
2. Upload test JSON files to MinIO
3. Run Prefect flow directly (run_ingestion_flow)
4. Verify run status and metrics via API
5. Verify data in Iceberg table via Spark

This test uses REAL services from docker-compose.test.yml:
- MinIO (S3-compatible storage) on localhost:9000
- Spark Connect on localhost:15002
- PostgreSQL on localhost:5432

All interactions are API-only for setup/verification.
The Prefect flow is executed directly to simulate scheduled runs.

IMPORTANT: This test commits data to the real database (not using transactional isolation)
because Prefect tasks create their own database sessions via SessionLocal().
"""

import pytest
from typing import Dict
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession

from app.prefect.flows.run_ingestion import run_ingestion_flow
from tests.e2e.helpers import E2ELogger, create_standard_ingestion, get_run, assert_run_metrics, verify_table_data, \
    generate_unique_table_name, get_table_identifier, print_test_summary


@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestPrefectIngestion:
    """E2E test for Prefect-based ingestion flow"""

    def test_prefect_basic_ingestion(
        self,
        e2e_api_client_no_override: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        sample_json_files: list,
        spark_session: SparkSession,
        test_tenant_id: str,
        test_cluster_id: str,
        spark_connect_url: str
    ):
        """
        Test complete Prefect ingestion workflow.

        Steps:
        1. Create ingestion configuration via API
        2. Test data (3 JSON files with 1000 records each) already uploaded by fixture
        3. Run Prefect flow directly
        4. Verify run status and metrics via API
        5. Query Iceberg table to verify data
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Prefect-based Ingestion Flow")

        table_name = generate_unique_table_name("prefect_test_table")

        logger.phase("📝 Creating ingestion configuration...")

        ingestion = create_standard_ingestion(
            api_client=e2e_api_client_no_override,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E Test Prefect Ingestion"
        )

        ingestion_id = ingestion["id"]

        logger.success(f"Created ingestion: {ingestion_id}")
        logger.step(f"Source: {ingestion['source']['path']}")
        logger.step(f"Destination: {get_table_identifier(ingestion)}")

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"
        assert ingestion["name"] == "E2E Test Prefect Ingestion"

        logger.phase("🚀 Running Prefect flow...")

        # NOTE: This test intentionally deviates from the standard E2E pattern by calling
        # run_ingestion_flow() directly instead of using the API trigger endpoint.
        # This is necessary to test the Prefect flow execution path itself, which is used
        # by the scheduler. The API endpoint is tested in test_basic_ingestion.py.
        result = run_ingestion_flow(
            ingestion_id=ingestion_id,
            trigger="manual"
        )

        logger.success(f"Prefect flow completed")
        logger.step(f"Status: {result['status']}")
        logger.step(f"Run ID: {result['run_id']}")
        logger.step(f"Files Processed: {result['files_processed']}")

        assert result["status"] == "SUCCESS"
        assert result["run_id"] is not None
        assert result["files_processed"] == 3

        run_id = result["run_id"]

        logger.phase("📊 Verifying run metrics via API...")

        # Get run details from API
        run = get_run(e2e_api_client_no_override, ingestion_id, run_id)

        assert_run_metrics(
            run=run,
            expected_files=3,
            expected_records=3000,
            expected_errors=0,
            logger=logger
        )

        logger.success("All metrics verified")

        logger.phase("🔍 Verifying data in Iceberg table...")

        table_identifier = get_table_identifier(ingestion)

        df = verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=3000,
            expected_fields=["id", "timestamp", "user_id", "event_type", "value"],
            check_duplicates=True,
            logger=logger
        )

        sample_data = df.limit(5).collect()
        assert len(sample_data) == 5, "Expected 5 sample records"

        logger.success("Data verification passed")

        logger.phase("📜 Verifying run history via API...")

        response = e2e_api_client_no_override.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert response.status_code == 200

        runs = response.json()
        logger.step(f"Total runs: {len(runs)}")

        assert len(runs) >= 1, "Expected at least 1 run in history"

        our_run = next((r for r in runs if r["id"] == run_id), None)
        assert our_run is not None, f"Run {run_id} not found in history"

        logger.success(f"Run found in history: {our_run['id']}")

        logger.section("✅ E2E TEST PASSED: Prefect-based Ingestion Flow")
        print_test_summary([
            ("Ingestion ID", ingestion_id),
            ("Run ID", run_id),
            ("Files Processed", 3),
            ("Records Ingested", 3000),
            ("Table", table_identifier),
            ("Prefect Status", result['status']),
            ("Test Status", "SUCCESS ✅")
        ])
