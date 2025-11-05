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
import time
import json
from typing import Dict, Any, List
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession
from datetime import datetime, timezone


def create_base_schema_files(
    minio_client,
    test_bucket: str,
    start_file_idx: int,
    num_files: int,
    records_per_file: int = 1000
) -> List[Dict[str, any]]:
    """
    Create JSON files with BASE schema (5 fields).

    Base schema:
    - id: int
    - timestamp: string (ISO format)
    - user_id: string
    - event_type: string
    - value: float

    Args:
        minio_client: Boto3 S3 client
        test_bucket: Target bucket name
        start_file_idx: Starting index for file numbering
        num_files: Number of files to create
        records_per_file: Number of records per file

    Returns:
        List of file metadata dicts
    """
    files_created = []

    for file_idx in range(start_file_idx, start_file_idx + num_files):
        records = []
        for record_idx in range(records_per_file):
            record_id = file_idx * records_per_file + record_idx
            record = {
                "id": record_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user_id": f"user-{record_id % 100}",
                "event_type": ["login", "pageview", "click", "purchase"][record_id % 4],
                "value": record_id * 10.5
            }
            records.append(record)

        # Convert to newline-delimited JSON
        file_content = "\n".join([json.dumps(record) for record in records])

        # Upload to MinIO
        key = f"data/batch_{file_idx}.json"
        minio_client.put_object(
            Bucket=test_bucket,
            Key=key,
            Body=file_content.encode('utf-8')
        )

        file_path = f"s3://{test_bucket}/{key}"
        file_size = len(file_content.encode('utf-8'))

        files_created.append({
            "path": file_path,
            "key": key,
            "size": file_size,
            "records": len(records)
        })

        print(f"  ✅ Uploaded BASE schema: {key} ({len(records)} records, 5 fields)")

    return files_created


def create_evolved_schema_files(
    minio_client,
    test_bucket: str,
    start_file_idx: int,
    num_files: int,
    records_per_file: int = 1000
) -> List[Dict[str, any]]:
    """
    Create JSON files with EVOLVED schema (7 fields).

    Evolved schema (adds 2 new fields):
    - id: int
    - timestamp: string (ISO format)
    - user_id: string
    - event_type: string
    - value: float
    - region: string (NEW)
    - metadata: object (NEW)

    Args:
        minio_client: Boto3 S3 client
        test_bucket: Target bucket name
        start_file_idx: Starting index for file numbering
        num_files: Number of files to create
        records_per_file: Number of records per file

    Returns:
        List of file metadata dicts
    """
    files_created = []
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

    for file_idx in range(start_file_idx, start_file_idx + num_files):
        records = []
        for record_idx in range(records_per_file):
            record_id = file_idx * records_per_file + record_idx
            record = {
                "id": record_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user_id": f"user-{record_id % 100}",
                "event_type": ["login", "pageview", "click", "purchase"][record_id % 4],
                "value": record_id * 10.5,
                # NEW FIELDS
                "region": regions[record_id % len(regions)],
                "metadata": {
                    "browser": ["chrome", "firefox", "safari"][record_id % 3],
                    "version": f"{100 + (record_id % 50)}.0",
                    "device": ["desktop", "mobile", "tablet"][record_id % 3]
                }
            }
            records.append(record)

        # Convert to newline-delimited JSON
        file_content = "\n".join([json.dumps(record) for record in records])

        # Upload to MinIO
        key = f"data/batch_{file_idx}.json"
        minio_client.put_object(
            Bucket=test_bucket,
            Key=key,
            Body=file_content.encode('utf-8')
        )

        file_path = f"s3://{test_bucket}/{key}"
        file_size = len(file_content.encode('utf-8'))

        files_created.append({
            "path": file_path,
            "key": key,
            "size": file_size,
            "records": len(records)
        })

        print(f"  ✅ Uploaded EVOLVED schema: {key} ({len(records)} records, 7 fields)")

    return files_created


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
        print("\n" + "="*80)
        print("🧪 E2E TEST: Schema Evolution (E2E-03)")
        print("="*80)

        # Generate unique table name to avoid test pollution
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        table_name = f"e2e_schema_evolution_{timestamp}"

        # ========================================================================
        # STEP 1: Create Ingestion Configuration (Evolution ENABLED)
        # ========================================================================
        print("\n📝 STEP 1: Creating ingestion configuration (evolution enabled)...")

        ingestion_payload = {
            "name": "E2E Test Schema Evolution",
            "cluster_id": test_cluster_id,
            "source": {
                "type": "s3",
                "path": f"s3://{test_bucket}/data/",
                "file_pattern": "*.json",
                "credentials": {
                    "aws_access_key_id": minio_config["aws_access_key_id"],
                    "aws_secret_access_key": minio_config["aws_secret_access_key"],
                    "endpoint_url": minio_config["endpoint_url"]
                }
            },
            "format": {
                "type": "json",
                "options": {
                    "multiline": False
                },
                "schema": {
                    "inference": "auto",
                    "evolution_enabled": True  # CRITICAL: Enable schema evolution
                }
            },
            "destination": {
                "catalog": "test_catalog",
                "database": "test_db",
                "table": table_name,
                "write_mode": "append",
                "partitioning": {
                    "enabled": False,
                    "columns": []
                },
                "optimization": {
                    "z_ordering_enabled": False,
                    "z_ordering_columns": []
                }
            },
            "schedule": {
                "frequency": None,  # Manual trigger only
                "time": None,
                "timezone": "UTC",
                "cron_expression": None,
                "backfill": {
                    "enabled": False
                }
            },
            "quality": {
                "row_count_threshold": None,
                "alerts_enabled": False,
                "alert_recipients": []
            }
        }

        response = api_client.post("/api/v1/ingestions", json=ingestion_payload)
        assert response.status_code == 201, f"Failed to create ingestion: {response.text}"

        ingestion = response.json()
        ingestion_id = ingestion["id"]

        print(f"  ✅ Created ingestion: {ingestion_id}")
        print(f"     Name: {ingestion['name']}")
        print(f"     Status: {ingestion['status']}")
        print(f"     Schema Evolution: {ingestion['format']['schema']['evolution_enabled']}")
        print(f"     Destination: {ingestion['destination']['catalog']}.{ingestion['destination']['database']}.{ingestion['destination']['table']}")

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"
        assert ingestion["format"]["schema"]["evolution_enabled"] is True

        # ========================================================================
        # PHASE 1: Initial Load (Base Schema - 5 fields)
        # ========================================================================
        print("\n" + "="*80)
        print("📦 PHASE 1: Initial Load (Base Schema - 5 fields)")
        print("="*80)

        print("\n📤 STEP 2: Uploading 3 files with BASE schema...")
        base_files = create_base_schema_files(
            minio_client=minio_client,
            test_bucket=test_bucket,
            start_file_idx=0,
            num_files=3,
            records_per_file=1000
        )

        print(f"\n  ✅ Uploaded {len(base_files)} files with BASE schema (5 fields)")
        print(f"     Fields: id, timestamp, user_id, event_type, value")

        # ========================================================================
        # STEP 3: Trigger First Run
        # ========================================================================
        print("\n🚀 STEP 3: Triggering first run...")

        response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
        assert response.status_code == 202, f"Failed to trigger run: {response.text}"

        run1_response = response.json()
        run1_id = run1_response["run_id"]

        print(f"  ✅ Triggered run 1: {run1_id}")
        print(f"     Status: {run1_response['status']}")

        assert run1_id is not None
        assert run1_response["status"] in ["accepted", "running"]

        # ========================================================================
        # STEP 4: Poll for First Run Completion
        # ========================================================================
        print("\n⏳ STEP 4: Polling for first run completion...")

        max_wait_seconds = 180  # 3 minutes
        poll_interval = 2  # seconds
        start_time = time.time()

        run1_status = None

        while time.time() - start_time < max_wait_seconds:
            response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run1_id}")
            assert response.status_code == 200, f"Failed to get run status: {response.text}"

            run1 = response.json()
            run1_status = run1["status"]

            print(f"  ⏱️  {int(time.time() - start_time)}s - Status: {run1_status}", end="")

            if run1_status in ["success", "completed"]:
                print(" ✅")
                break
            elif run1_status == "failed":
                print(" ❌")
                pytest.fail(f"Run 1 failed: {run1.get('errors', [])}")
            else:
                print()
                time.sleep(poll_interval)

        if run1_status not in ["success", "completed"]:
            pytest.fail(f"Run 1 did not complete within {max_wait_seconds}s. Last status: {run1_status}")

        print(f"\n  ✅ Run 1 completed successfully in {int(time.time() - start_time)}s")

        # ========================================================================
        # STEP 5: Verify First Run Metrics
        # ========================================================================
        print("\n📊 STEP 5: Verifying first run metrics...")

        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run1_id}")
        assert response.status_code == 200

        run1 = response.json()

        print(f"  Run ID: {run1['id']}")
        print(f"  Status: {run1['status']}")
        print(f"  Files Processed: {run1.get('metrics', {}).get('files_processed', 0)}")
        print(f"  Records Ingested: {run1.get('metrics', {}).get('records_ingested', 0)}")

        metrics1 = run1.get("metrics", {})
        assert metrics1.get("files_processed", 0) == 3, "Expected 3 files processed in first run"
        assert metrics1.get("records_ingested", 0) == 3000, "Expected 3000 records ingested in first run"

        errors = run1.get('errors') or []
        assert len(errors) == 0, "Expected no errors in first run"

        print("  ✅ First run metrics verified")

        # ========================================================================
        # STEP 6: Verify Initial Schema (5 fields)
        # ========================================================================
        print("\n🔍 STEP 6: Verifying initial schema (5 fields)...")

        table_identifier = f"{ingestion['destination']['catalog']}.{ingestion['destination']['database']}.{ingestion['destination']['table']}"

        try:
            df = spark_session.table(table_identifier)
            record_count = df.count()
            schema = df.schema
            field_names = [field.name for field in schema.fields]

            print(f"  Records in table: {record_count}")
            print(f"  Schema fields ({len(field_names)}): {field_names}")

            # Assertions
            assert record_count == 3000, f"Expected 3000 records after first run, got {record_count}"

            # Verify base schema fields
            expected_base_fields = ["id", "timestamp", "user_id", "event_type", "value"]
            for field in expected_base_fields:
                assert field in field_names, f"Missing base field: {field}"

            # Should NOT have evolved fields yet
            assert "region" not in field_names, "Should not have 'region' field yet"
            assert "metadata" not in field_names, "Should not have 'metadata' field yet"

            print("  ✅ Initial schema verified (5 base fields only)")

        except Exception as e:
            pytest.fail(f"Failed to verify initial schema: {e}")

        # ========================================================================
        # PHASE 2: Schema Evolution (Add 2 new fields)
        # ========================================================================
        print("\n" + "="*80)
        print("📦 PHASE 2: Schema Evolution (Add region, metadata)")
        print("="*80)

        print("\n📤 STEP 7: Uploading 2 files with EVOLVED schema...")
        evolved_files = create_evolved_schema_files(
            minio_client=minio_client,
            test_bucket=test_bucket,
            start_file_idx=3,
            num_files=2,
            records_per_file=1000
        )

        print(f"\n  ✅ Uploaded {len(evolved_files)} files with EVOLVED schema (7 fields)")
        print(f"     Fields: id, timestamp, user_id, event_type, value, region, metadata")
        print(f"     NEW fields: region, metadata")

        # ========================================================================
        # STEP 8: Trigger Second Run
        # ========================================================================
        print("\n🚀 STEP 8: Triggering second run (with evolved schema)...")

        response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
        assert response.status_code == 202, f"Failed to trigger second run: {response.text}"

        run2_response = response.json()
        run2_id = run2_response["run_id"]

        print(f"  ✅ Triggered run 2: {run2_id}")
        print(f"     Status: {run2_response['status']}")

        assert run2_id is not None
        assert run2_response["status"] in ["accepted", "running"]

        # ========================================================================
        # STEP 9: Poll for Second Run Completion
        # ========================================================================
        print("\n⏳ STEP 9: Polling for second run completion...")

        start_time = time.time()
        run2_status = None

        while time.time() - start_time < max_wait_seconds:
            response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run2_id}")
            assert response.status_code == 200, f"Failed to get run status: {response.text}"

            run2 = response.json()
            run2_status = run2["status"]

            print(f"  ⏱️  {int(time.time() - start_time)}s - Status: {run2_status}", end="")

            if run2_status in ["success", "completed"]:
                print(" ✅")
                break
            elif run2_status == "failed":
                print(" ❌")
                pytest.fail(f"Run 2 failed: {run2.get('errors', [])}")
            else:
                print()
                time.sleep(poll_interval)

        if run2_status not in ["success", "completed"]:
            pytest.fail(f"Run 2 did not complete within {max_wait_seconds}s. Last status: {run2_status}")

        print(f"\n  ✅ Run 2 completed successfully in {int(time.time() - start_time)}s")

        # ========================================================================
        # STEP 10: Verify Second Run Metrics
        # ========================================================================
        print("\n📊 STEP 10: Verifying second run metrics...")

        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run2_id}")
        assert response.status_code == 200

        run2 = response.json()

        print(f"  Run ID: {run2['id']}")
        print(f"  Status: {run2['status']}")
        print(f"  Files Processed: {run2.get('metrics', {}).get('files_processed', 0)}")
        print(f"  Records Ingested: {run2.get('metrics', {}).get('records_ingested', 0)}")

        metrics2 = run2.get("metrics", {})
        assert metrics2.get("files_processed", 0) == 2, "Expected 2 files processed in second run"
        assert metrics2.get("records_ingested", 0) == 2000, "Expected 2000 records ingested in second run"

        errors = run2.get('errors') or []
        assert len(errors) == 0, "Expected no errors in second run"

        print("  ✅ Second run metrics verified")

        # ========================================================================
        # STEP 11: Verify Schema Evolution (7 fields)
        # ========================================================================
        print("\n🔍 STEP 11: Verifying schema evolution (7 fields)...")

        try:
            df = spark_session.table(table_identifier)
            total_record_count = df.count()
            schema = df.schema
            field_names = [field.name for field in schema.fields]

            print(f"  Total records in table: {total_record_count}")
            print(f"  Schema fields ({len(field_names)}): {field_names}")

            # CRITICAL: Should have 7 fields now
            expected_all_fields = ["id", "timestamp", "user_id", "event_type", "value", "region", "metadata"]

            assert total_record_count == 5000, f"Expected 5000 total records, got {total_record_count}"

            # Verify all fields present
            for field in expected_all_fields:
                assert field in field_names, f"Missing field after evolution: {field}"

            print("  ✅ Schema evolution verified - Table now has 7 fields!")
            print(f"     Base fields (5): id, timestamp, user_id, event_type, value")
            print(f"     NEW fields (2): region, metadata")

        except Exception as e:
            pytest.fail(f"Failed to verify schema evolution: {e}")

        # ========================================================================
        # STEP 12: Verify Backward Compatibility (Old records with NULL)
        # ========================================================================
        print("\n🔍 STEP 12: Verifying backward compatibility...")

        try:
            # Query old records (id < 3000)
            old_records_df = df.filter("id < 3000")
            old_count = old_records_df.count()

            # Check NULL counts for new fields in old records
            old_null_region = old_records_df.filter("region IS NULL").count()
            old_null_metadata = old_records_df.filter("metadata IS NULL").count()

            print(f"  Old records (id < 3000): {old_count}")
            print(f"  Old records with NULL region: {old_null_region}")
            print(f"  Old records with NULL metadata: {old_null_metadata}")

            # CRITICAL: All old records should have NULL for new fields
            assert old_count == 3000, f"Expected 3000 old records, got {old_count}"
            assert old_null_region == 3000, f"Expected all old records to have NULL region, got {old_null_region}"
            assert old_null_metadata == 3000, f"Expected all old records to have NULL metadata, got {old_null_metadata}"

            print("  ✅ Backward compatibility verified - Old records have NULL for new fields")

        except Exception as e:
            pytest.fail(f"Failed to verify backward compatibility: {e}")

        # ========================================================================
        # STEP 13: Verify New Records (With values for all fields)
        # ========================================================================
        print("\n🔍 STEP 13: Verifying new records have evolved fields...")

        try:
            # Query new records (id >= 3000)
            new_records_df = df.filter("id >= 3000")
            new_count = new_records_df.count()

            # Check non-NULL counts for new fields in new records
            new_non_null_region = new_records_df.filter("region IS NOT NULL").count()
            new_non_null_metadata = new_records_df.filter("metadata IS NOT NULL").count()

            # Sample a few new records
            sample_new = new_records_df.limit(3).collect()

            print(f"  New records (id >= 3000): {new_count}")
            print(f"  New records with region values: {new_non_null_region}")
            print(f"  New records with metadata values: {new_non_null_metadata}")

            # CRITICAL: All new records should have values for new fields
            assert new_count == 2000, f"Expected 2000 new records, got {new_count}"
            assert new_non_null_region == 2000, f"Expected all new records to have region, got {new_non_null_region}"
            assert new_non_null_metadata == 2000, f"Expected all new records to have metadata, got {new_non_null_metadata}"

            # Verify sample data
            for row in sample_new:
                assert row['region'] is not None, "New record missing region"
                assert row['metadata'] is not None, "New record missing metadata"
                print(f"     Sample: id={row['id']}, region={row['region']}, metadata={row['metadata']}")

            print("  ✅ New records verified - All have values for evolved fields")

        except Exception as e:
            pytest.fail(f"Failed to verify new records: {e}")

        # ========================================================================
        # STEP 14: Verify All Data Queryable
        # ========================================================================
        print("\n🔍 STEP 14: Verifying all data is queryable...")

        try:
            # Complex query combining old and new data
            result = df.groupBy("event_type").count().collect()

            print(f"  Event type distribution across all {total_record_count} records:")
            for row in result:
                print(f"     {row['event_type']}: {row['count']}")

            # Query with new fields
            region_counts = df.filter("region IS NOT NULL").groupBy("region").count().collect()

            print(f"  Region distribution (new records only):")
            for row in region_counts:
                print(f"     {row['region']}: {row['count']}")

            print("  ✅ All data is queryable - Schema evolution successful!")

        except Exception as e:
            pytest.fail(f"Failed to query evolved data: {e}")

        # ========================================================================
        # TEST COMPLETE
        # ========================================================================
        print("\n" + "="*80)
        print("✅ E2E TEST PASSED: Schema Evolution (E2E-03)")
        print("="*80)
        print(f"\nSummary:")
        print(f"  - Ingestion ID: {ingestion_id}")
        print(f"  - Run 1 ID: {run1_id}")
        print(f"  - Run 2 ID: {run2_id}")
        print(f"  - Run 1: 3 files, 3000 records, 5 fields")
        print(f"  - Run 2: 2 files, 2000 records, SCHEMA EVOLVED to 7 fields")
        print(f"  - Total: 5 files, 5000 records")
        print(f"  - Schema: {len(field_names)} fields")
        print(f"  - Backward Compatibility: ✅ (old records have NULL for new fields)")
        print(f"  - Forward Compatibility: ✅ (new records have all fields)")
        print(f"  - Table: {table_identifier}")
        print(f"  - Status: SUCCESS ✅")
        print(f"\n🎉 SCHEMA EVOLUTION FEATURE VALIDATED!")
        print("="*80 + "\n")
