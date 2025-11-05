"""
E2E Test: Schema Evolution (E2E-03)

Tests the schema evolution workflow:
1. Create ingestion configuration with schema evolution enabled
2. Upload 3 JSON files with base schema (5 fields)
3. Trigger first run and verify 3000 records with 5 columns
4. Upload 2 JSON files with evolved schema (7 fields - adding region, metadata)
5. Trigger second run
6. Verify schema evolution detected and applied (7 columns in table)
7. Verify backward compatibility (old records have NULL for new fields)
8. Verify all 5000 records are queryable with full schema

This test validates:
- Automatic schema detection on new files
- Schema evolution in Iceberg tables
- Backward compatibility of existing data
- No data loss during schema changes
- Incremental processing still works with schema changes

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
    verify_schema_evolution,
    upload_json_files,
    generate_unique_table_name,
    get_table_identifier,
    print_test_summary
)


@pytest.mark.e2e
@pytest.mark.requires_spark
@pytest.mark.requires_minio
class TestSchemaEvolution:
    """E2E test for schema evolution across multiple runs"""

    def test_schema_evolution_s3_json(
        self,
        api_client: TestClient,
        minio_client,
        minio_config: Dict[str, str],
        test_bucket: str,
        lakehouse_bucket: str,
        spark_session: SparkSession,
        test_tenant_id: str,
        test_cluster_id: str,
        spark_connect_url: str
    ):
        """
        Test schema evolution with automatic detection and Iceberg schema updates.

        Success criteria:
        - First run: 3 files, 3000 records, 5 columns
        - Second run: 2 files, 2000 records, schema evolved to 7 columns
        - Old records: Have NULL for new fields (backward compatibility)
        - New records: Have values for all 7 fields
        - All 5000 records queryable
        - No data loss
        """
        logger = E2ELogger()
        logger.section("🧪 E2E TEST: Schema Evolution (E2E-03)")

        table_name = generate_unique_table_name("e2e_schema_evolution")

        logger.phase("📝 Creating ingestion configuration (evolution enabled)...")

        ingestion = create_standard_ingestion(
            api_client=api_client,
            cluster_id=test_cluster_id,
            test_bucket=test_bucket,
            minio_config=minio_config,
            table_name=table_name,
            name="E2E Test Schema Evolution",
            evolution_enabled=True  # CRITICAL: Enable schema evolution
        )

        ingestion_id = ingestion["id"]
        logger.success(f"Created ingestion: {ingestion_id}")
        logger.step(f"Schema Evolution: {ingestion['format']['schema']['evolution_enabled']}", always=True)

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"
        assert ingestion["format"]["schema"]["evolution_enabled"] is True

        logger.section("📦 PHASE 1: Initial Load (Base Schema - 5 fields)")

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

        logger.phase("🔍 Verifying initial schema (5 fields)...")
        table_identifier = get_table_identifier(ingestion)

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

        logger.section("📦 PHASE 2: Schema Evolution (Add region, metadata)")

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

        logger.phase("🚀 Triggering second run (with evolved schema)...")
        run2_id = trigger_run(api_client, ingestion_id)
        logger.success(f"Triggered run 2: {run2_id}")

        logger.phase("⏳ Waiting for second run completion...")
        run2 = wait_for_run_completion(
            api_client=api_client,
            ingestion_id=ingestion_id,
            run_id=run2_id,
            logger=logger
        )

        logger.phase("📊 Verifying second run metrics...")
        assert_run_metrics(
            run=run2,
            expected_files=2,
            expected_records=2000,
            logger=logger
        )
        logger.success("Second run metrics verified")

        logger.phase("🔍 Verifying schema evolution (7 fields)...")
        df = verify_table_data(
            spark_session=spark_session,
            table_identifier=table_identifier,
            expected_count=5000,
            expected_fields=["id", "timestamp", "user_id", "event_type", "value", "region", "metadata"],
            check_duplicates=True,
            logger=logger
        )
        logger.success("Schema evolution verified - Table now has 7 fields!", always=True)

        logger.phase("🔍 Verifying backward and forward compatibility...")
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

        logger.phase("🔍 Verifying all data is queryable...")

        result = df.groupBy("event_type").count().collect()
        logger.step(f"Event type distribution: {len(result)} types", always=True)

        region_counts = df.filter("region IS NOT NULL").groupBy("region").count().collect()
        logger.step(f"Region distribution: {len(region_counts)} regions (new records only)", always=True)

        logger.success("All data is queryable - Schema evolution successful!")

        logger.section("✅ E2E TEST PASSED: Schema Evolution (E2E-03)")
        print_test_summary([
            ("Ingestion ID", ingestion_id),
            ("Run 1 ID", run1_id),
            ("Run 2 ID", run2_id),
            ("Run 1", "3 files, 3000 records, 5 fields"),
            ("Run 2", "2 files, 2000 records, SCHEMA EVOLVED to 7 fields"),
            ("Total", "5 files, 5000 records"),
            ("Backward Compatibility", "✅ (old records have NULL for new fields)"),
            ("Forward Compatibility", "✅ (new records have all fields)"),
            ("Table", table_identifier),
            ("Status", "SUCCESS ✅")
        ], footer_message="🎉 SCHEMA EVOLUTION FEATURE VALIDATED!")
