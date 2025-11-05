"""
E2E Test: Basic S3 JSON Ingestion (Happy Path)

Tests the complete workflow:
1. Create ingestion configuration for S3 JSON files
2. Trigger manual run
3. Poll until completion
4. Verify data in Iceberg table via Spark
5. Verify run history

This test uses REAL services from docker-compose.test.yml:
- MinIO (S3-compatible storage) on localhost:9000
- Spark Connect on localhost:15002
- PostgreSQL on localhost:5432

All interactions are API-only (no direct database manipulation).
"""

import pytest
from typing import Dict
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession

from .helpers import (
    E2ELogger,
    create_standard_ingestion,
    trigger_run,
    wait_for_run_completion,
    assert_run_metrics,
    verify_table_data,
    generate_unique_table_name,
    get_table_identifier
)


@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestBasicS3JsonIngestion:
    """E2E test for basic S3 JSON ingestion - Happy Path"""

    def test_happy_path_s3_json_ingestion(
        self,
        api_client: TestClient,
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
        Test complete S3 JSON ingestion workflow.

        Steps:
        1. Create ingestion configuration
        2. Trigger manual run
        3. Poll for completion (max 3 minutes)
        4. Verify run metrics (3 files, 3000 records)
        5. Query Iceberg table to verify data
        6. Verify run history
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Basic S3 JSON Ingestion - Happy Path")

        # Generate unique table name
        table_name = generate_unique_table_name("e2e_test_table")

        # ====================================================================
        # STEP 1: Create Ingestion Configuration
        # ====================================================================
        logger.phase("📝 Creating ingestion configuration...")

        ingestion = create_standard_ingestion(
            api_client=api_client,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E Test S3 JSON Ingestion"
        )

        ingestion_id = ingestion["id"]

        logger.success(f"Created ingestion: {ingestion_id}")
        logger.step(f"Source: {ingestion['source']['path']}")
        logger.step(f"Destination: {get_table_identifier(ingestion)}")

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"
        assert ingestion["name"] == "E2E Test S3 JSON Ingestion"

        # ====================================================================
        # STEP 2: Trigger Manual Run
        # ====================================================================
        logger.phase("🚀 Triggering manual run...")

        run_id = trigger_run(api_client, ingestion_id)

        logger.success(f"Triggered run: {run_id}")

        # ====================================================================
        # STEP 3: Poll for Completion
        # ====================================================================
        logger.phase("⏳ Polling for completion...")

        run = wait_for_run_completion(
            api_client=api_client,
            ingestion_id=ingestion_id,
            run_id=run_id,
            timeout=180,
            logger=logger
        )

        # ====================================================================
        # STEP 4: Verify Run Metrics
        # ====================================================================
        logger.phase("📊 Verifying run metrics...")

        assert_run_metrics(
            run=run,
            expected_files=3,
            expected_records=3000,
            expected_errors=0,
            logger=logger
        )

        logger.success("All metrics verified")

        # ====================================================================
        # STEP 5: Verify Data in Iceberg Table
        # ====================================================================
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

        # Verify data is queryable
        sample_data = df.limit(5).collect()
        assert len(sample_data) == 5, "Expected 5 sample records"

        logger.success("Data verification passed")

        # ====================================================================
        # STEP 6: Verify Run History
        # ====================================================================
        logger.phase("📜 Verifying run history...")

        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert response.status_code == 200

        runs = response.json()
        logger.step(f"Total runs: {len(runs)}")

        assert len(runs) >= 1, "Expected at least 1 run in history"

        # Find our run
        our_run = next((r for r in runs if r["id"] == run_id), None)
        assert our_run is not None, f"Run {run_id} not found in history"

        logger.success(f"Run found in history: {our_run['id']}")

        # ====================================================================
        # TEST COMPLETE
        # ====================================================================
        logger.section("✅ E2E TEST PASSED: Basic S3 JSON Ingestion - Happy Path")
        print(f"\nSummary:")
        print(f"  - Ingestion ID: {ingestion_id}")
        print(f"  - Run ID: {run_id}")
        print(f"  - Files Processed: 3")
        print(f"  - Records Ingested: 3000")
        print(f"  - Table: {table_identifier}")
        print(f"  - Status: SUCCESS ✅")
        print("="*80 + "\n")
