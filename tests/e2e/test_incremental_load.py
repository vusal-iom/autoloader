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
    upload_json_files,
    generate_unique_table_name,
    get_table_identifier
)


@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestIncrementalLoad:
    """E2E test for incremental file processing across multiple runs"""

    def test_incremental_load_s3_json(
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
        Test incremental file processing across multiple runs.

        Success criteria:
        - First run: 3 files processed, 3000 records
        - Second run: Only 2 NEW files processed, total 5000 records (not 6000)
        - No duplicate data based on ID field
        - Accurate metrics for both runs
        - All 5 files marked as processed
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Incremental Load (E2E-02)")

        table_name = generate_unique_table_name("e2e_incremental_test")

        logger.phase("📝 Creating ingestion configuration...")

        ingestion = create_standard_ingestion(
            api_client=api_client,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E Test Incremental Load"
        )

        ingestion_id = ingestion["id"]
        logger.success(f"Created ingestion: {ingestion_id}")

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"

        logger.section("📦 PHASE 1: Initial Ingestion (3 files)")
        logger.step(f"Initial files already uploaded: {len(sample_json_files)} files", always=True)

        logger.phase("🚀 Triggering first run...")
        run1_id = trigger_run(api_client, ingestion_id)
        logger.success(f"Triggered run 1: {run1_id}")

        logger.phase("⏳ Waiting for first run completion...")
        run1 = wait_for_run_completion(
            api_client=api_client,
            ingestion_id=ingestion_id,
            run_id=run1_id,
            logger=logger
        )

        logger.phase("📊 Verifying first run metrics...")
        assert_run_metrics(
            run=run1,
            expected_files=3,
            expected_records=3000,
            logger=logger
        )
        logger.success("First run metrics verified")

        logger.phase("🔍 Verifying data after first run...")
        table_identifier = get_table_identifier(ingestion)

        verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=3000,
            check_duplicates=True,
            logger=logger
        )
        logger.success("Data verification after first run passed")

        logger.section("📦 PHASE 2: Add New Files (2 more files)")

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

        logger.section("📦 PHASE 3: Second Ingestion Run (Incremental)")

        logger.phase("🚀 Triggering second run (incremental)...")
        run2_id = trigger_run(api_client, ingestion_id)
        logger.success(f"Triggered run 2: {run2_id}")

        logger.phase("⏳ Waiting for second run completion...")
        run2 = wait_for_run_completion(
            api_client=api_client,
            ingestion_id=ingestion_id,
            run_id=run2_id,
            logger=logger
        )

        logger.phase("📊 Verifying second run metrics (CRITICAL - Incremental Load)...")
        assert_run_metrics(
            run=run2,
            expected_files=2,  # CRITICAL: Only NEW files
            expected_records=2000,  # CRITICAL: Only NEW records
            logger=logger
        )
        logger.success("Second run metrics verified - INCREMENTAL LOAD WORKING!", always=True)

        logger.phase("🔍 Verifying total data (no duplicates)...")
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

        logger.phase("📜 Verifying run history...")
        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert response.status_code == 200

        runs = response.json()
        assert len(runs) >= 2, "Expected at least 2 runs in history"

        run1_in_history = next((r for r in runs if r["id"] == run1_id), None)
        run2_in_history = next((r for r in runs if r["id"] == run2_id), None)

        assert run1_in_history is not None, f"Run 1 {run1_id} not found in history"
        assert run2_in_history is not None, f"Run 2 {run2_id} not found in history"

        logger.success("Run history verified")

        logger.section("✅ E2E TEST PASSED: Incremental Load (E2E-02)")
        print(f"\nSummary:")
        print(f"  - Ingestion ID: {ingestion_id}")
        print(f"  - Run 1 ID: {run1_id}")
        print(f"  - Run 2 ID: {run2_id}")
        print(f"  - Run 1: 3 files, 3000 records")
        print(f"  - Run 2: 2 files, 2000 records (INCREMENTAL)")
        print(f"  - Total: 5 files, 5000 records (NO DUPLICATES)")
        print(f"  - Table: {table_identifier}")
        print(f"  - Status: SUCCESS ✅")
        print(f"\n🎉 INCREMENTAL LOAD FEATURE VALIDATED!")
        print("="*80 + "\n")
