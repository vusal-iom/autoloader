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
import time
import json
from typing import Dict, Any, List
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession
from datetime import datetime, timezone


def upload_additional_files(
    minio_client,
    test_bucket: str,
    start_file_idx: int,
    num_files: int,
    records_per_file: int = 1000
) -> List[Dict[str, any]]:
    """
    Helper function to upload additional JSON files to MinIO mid-test.

    Args:
        minio_client: Boto3 S3 client
        test_bucket: Target bucket name
        start_file_idx: Starting index for file numbering
        num_files: Number of files to create
        records_per_file: Number of records per file

    Returns:
        List of file metadata dicts with keys: path, size, key, records
    """
    files_created = []

    for file_idx in range(start_file_idx, start_file_idx + num_files):
        # Generate data for this file
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

        print(f"  ✅ Uploaded: {key} ({len(records)} records, {file_size} bytes)")

    return files_created


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
        print("\n" + "="*80)
        print("🧪 E2E TEST: Incremental Load (E2E-02)")
        print("="*80)

        # Generate unique table name to avoid test pollution
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        table_name = f"e2e_incremental_test_{timestamp}"

        # ========================================================================
        # STEP 1: Create Ingestion Configuration
        # ========================================================================
        print("\n📝 STEP 1: Creating ingestion configuration...")

        ingestion_payload = {
            "name": "E2E Test Incremental Load",
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
                    "evolution_enabled": True
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
        print(f"     Source: {ingestion['source']['path']}")
        print(f"     Destination: {ingestion['destination']['catalog']}.{ingestion['destination']['database']}.{ingestion['destination']['table']}")

        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"

        # ========================================================================
        # PHASE 1: Initial Ingestion (3 files)
        # ========================================================================
        print("\n" + "="*80)
        print("📦 PHASE 1: Initial Ingestion (3 files)")
        print("="*80)

        # Files already uploaded by sample_json_files fixture
        print(f"\n✅ Initial files already uploaded: {len(sample_json_files)} files")

        # ========================================================================
        # STEP 2: Trigger First Run
        # ========================================================================
        print("\n🚀 STEP 2: Triggering first run...")

        response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
        assert response.status_code == 202, f"Failed to trigger run: {response.text}"

        run1_response = response.json()
        run1_id = run1_response["run_id"]

        print(f"  ✅ Triggered run 1: {run1_id}")
        print(f"     Status: {run1_response['status']}")

        assert run1_id is not None
        assert run1_response["status"] in ["accepted", "running"]

        # ========================================================================
        # STEP 3: Poll for First Run Completion
        # ========================================================================
        print("\n⏳ STEP 3: Polling for first run completion...")

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
        # STEP 4: Verify First Run Metrics
        # ========================================================================
        print("\n📊 STEP 4: Verifying first run metrics...")

        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run1_id}")
        assert response.status_code == 200

        run1 = response.json()

        print(f"  Run ID: {run1['id']}")
        print(f"  Status: {run1['status']}")
        print(f"  Started: {run1['started_at']}")
        print(f"  Ended: {run1['ended_at']}")
        print(f"  Duration: {run1.get('metrics', {}).get('duration_seconds', 0)}s")
        print(f"  Files Processed: {run1.get('metrics', {}).get('files_processed', 0)}")
        print(f"  Records Ingested: {run1.get('metrics', {}).get('records_ingested', 0)}")
        print(f"  Bytes Read: {run1.get('metrics', {}).get('bytes_read', 0)}")
        errors = run1.get('errors') or []
        print(f"  Errors: {len(errors)}")

        # Assertions for first run
        assert run1["status"] in ["success", "completed"]
        assert run1["started_at"] is not None
        assert run1["ended_at"] is not None

        metrics1 = run1.get("metrics", {})
        assert metrics1.get("files_processed", 0) == 3, "Expected 3 files processed in first run"
        assert metrics1.get("records_ingested", 0) == 3000, "Expected 3000 records ingested in first run"
        errors = run1.get('errors') or []
        assert len(errors) == 0, "Expected no errors in first run"

        print("  ✅ First run metrics verified")

        # ========================================================================
        # STEP 5: Verify Data After First Run
        # ========================================================================
        print("\n🔍 STEP 5: Verifying data after first run...")

        table_identifier = f"{ingestion['destination']['catalog']}.{ingestion['destination']['database']}.{ingestion['destination']['table']}"

        try:
            df = spark_session.table(table_identifier)
            record_count = df.count()
            print(f"  Records in table: {record_count}")

            schema = df.schema
            print(f"  Schema fields: {[field.name for field in schema.fields]}")

            distinct_count = df.select("id").distinct().count()
            print(f"  Distinct IDs: {distinct_count}")

            # Assertions
            assert record_count == 3000, f"Expected 3000 records after first run, got {record_count}"
            assert distinct_count == record_count, f"Found duplicate IDs after first run: {record_count - distinct_count} duplicates"

            print("  ✅ Data verification after first run passed")

        except Exception as e:
            pytest.fail(f"Failed to verify Iceberg table after first run: {e}")

        # ========================================================================
        # PHASE 2: Add New Files
        # ========================================================================
        print("\n" + "="*80)
        print("📦 PHASE 2: Add New Files (2 more files)")
        print("="*80)

        print("\n📤 STEP 6: Uploading 2 additional files...")

        # Upload 2 more files (indices 3 and 4)
        new_files = upload_additional_files(
            minio_client=minio_client,
            test_bucket=test_bucket,
            start_file_idx=3,
            num_files=2,
            records_per_file=1000
        )

        print(f"\n  ✅ Uploaded {len(new_files)} additional files")
        print(f"     Total files in bucket: {len(sample_json_files) + len(new_files)}")

        # ========================================================================
        # PHASE 3: Second Ingestion Run (Incremental)
        # ========================================================================
        print("\n" + "="*80)
        print("📦 PHASE 3: Second Ingestion Run (Incremental)")
        print("="*80)

        # ========================================================================
        # STEP 7: Trigger Second Run
        # ========================================================================
        print("\n🚀 STEP 7: Triggering second run (incremental)...")

        response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
        assert response.status_code == 202, f"Failed to trigger second run: {response.text}"

        run2_response = response.json()
        run2_id = run2_response["run_id"]

        print(f"  ✅ Triggered run 2: {run2_id}")
        print(f"     Status: {run2_response['status']}")

        assert run2_id is not None
        assert run2_response["status"] in ["accepted", "running"]

        # ========================================================================
        # STEP 8: Poll for Second Run Completion
        # ========================================================================
        print("\n⏳ STEP 8: Polling for second run completion...")

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
        # STEP 9: Verify Second Run Metrics (CRITICAL)
        # ========================================================================
        print("\n📊 STEP 9: Verifying second run metrics (CRITICAL - Incremental Load)...")

        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run2_id}")
        assert response.status_code == 200

        run2 = response.json()

        print(f"  Run ID: {run2['id']}")
        print(f"  Status: {run2['status']}")
        print(f"  Started: {run2['started_at']}")
        print(f"  Ended: {run2['ended_at']}")
        print(f"  Duration: {run2.get('metrics', {}).get('duration_seconds', 0)}s")
        print(f"  Files Processed: {run2.get('metrics', {}).get('files_processed', 0)}")
        print(f"  Records Ingested: {run2.get('metrics', {}).get('records_ingested', 0)}")
        print(f"  Bytes Read: {run2.get('metrics', {}).get('bytes_read', 0)}")
        errors = run2.get('errors') or []
        print(f"  Errors: {len(errors)}")

        # CRITICAL Assertions for second run - must process only NEW files
        assert run2["status"] in ["success", "completed"]
        assert run2["started_at"] is not None
        assert run2["ended_at"] is not None

        metrics2 = run2.get("metrics", {})

        # CRITICAL: Second run should process ONLY 2 new files, not all 5
        assert metrics2.get("files_processed", 0) == 2, \
            f"Expected 2 files processed in second run (incremental), got {metrics2.get('files_processed', 0)}"

        # CRITICAL: Second run should ingest ONLY 2000 new records
        assert metrics2.get("records_ingested", 0) == 2000, \
            f"Expected 2000 records ingested in second run (incremental), got {metrics2.get('records_ingested', 0)}"

        errors = run2.get('errors') or []
        assert len(errors) == 0, "Expected no errors in second run"

        print("  ✅ Second run metrics verified - INCREMENTAL LOAD WORKING!")

        # ========================================================================
        # STEP 10: Verify Total Data (No Duplicates)
        # ========================================================================
        print("\n🔍 STEP 10: Verifying total data (no duplicates)...")

        try:
            df = spark_session.table(table_identifier)
            total_record_count = df.count()
            print(f"  Total records in table: {total_record_count}")

            distinct_count = df.select("id").distinct().count()
            print(f"  Distinct IDs: {distinct_count}")

            # CRITICAL: Total should be 5000 (3000 + 2000), NOT 6000
            assert total_record_count == 5000, \
                f"Expected 5000 total records (3000 + 2000), got {total_record_count}. " \
                f"If you got 6000, it means files were re-processed (FAILED)."

            # CRITICAL: No duplicate IDs
            assert distinct_count == total_record_count, \
                f"Found duplicate IDs: {total_record_count - distinct_count} duplicates. " \
                f"This means data was ingested twice (FAILED)."

            print("  ✅ Total data verification passed - NO DUPLICATES!")

        except Exception as e:
            pytest.fail(f"Failed to verify Iceberg table after second run: {e}")

        # ========================================================================
        # STEP 11: Verify Run History
        # ========================================================================
        print("\n📜 STEP 11: Verifying run history...")

        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert response.status_code == 200

        runs = response.json()

        print(f"  Total runs: {len(runs)}")

        # Assertions
        assert len(runs) >= 2, "Expected at least 2 runs in history"

        # Verify both runs are in history
        run1_in_history = next((r for r in runs if r["id"] == run1_id), None)
        run2_in_history = next((r for r in runs if r["id"] == run2_id), None)

        assert run1_in_history is not None, f"Run 1 {run1_id} not found in history"
        assert run2_in_history is not None, f"Run 2 {run2_id} not found in history"

        print(f"\n  Run 1:")
        print(f"    Status: {run1_in_history['status']}")
        print(f"    Files Processed: {run1_in_history.get('metrics', {}).get('files_processed', 0)}")
        print(f"    Records Ingested: {run1_in_history.get('metrics', {}).get('records_ingested', 0)}")

        print(f"\n  Run 2:")
        print(f"    Status: {run2_in_history['status']}")
        print(f"    Files Processed: {run2_in_history.get('metrics', {}).get('files_processed', 0)}")
        print(f"    Records Ingested: {run2_in_history.get('metrics', {}).get('records_ingested', 0)}")

        print("\n  ✅ Run history verified")

        # ========================================================================
        # TEST COMPLETE
        # ========================================================================
        print("\n" + "="*80)
        print("✅ E2E TEST PASSED: Incremental Load (E2E-02)")
        print("="*80)
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
