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
import time
from typing import Dict, Any
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession


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
        print("\n" + "="*80)
        print("🧪 E2E TEST: Basic S3 JSON Ingestion - Happy Path")
        print("="*80)

        # ========================================================================
        # STEP 1: Create Ingestion Configuration
        # ========================================================================
        print("\n📝 STEP 1: Creating ingestion configuration...")

        ingestion_payload = {
            "name": "E2E Test S3 JSON Ingestion",
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
                "catalog": "local",
                "database": "test_db",
                "table": "e2e_test_table",
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

        # Verify response
        assert response.status_code == 201, f"Failed to create ingestion: {response.text}"

        ingestion = response.json()
        ingestion_id = ingestion["id"]

        print(f"  ✅ Created ingestion: {ingestion_id}")
        print(f"     Name: {ingestion['name']}")
        print(f"     Status: {ingestion['status']}")
        print(f"     Source: {ingestion['source']['path']}")
        print(f"     Destination: {ingestion['destination']['catalog']}.{ingestion['destination']['database']}.{ingestion['destination']['table']}")

        # Assertions
        assert ingestion["id"] is not None
        assert ingestion["status"] == "draft"
        assert ingestion["name"] == "E2E Test S3 JSON Ingestion"

        # ========================================================================
        # STEP 2: Trigger Manual Run
        # ========================================================================
        print("\n🚀 STEP 2: Triggering manual run...")

        response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")

        # Verify response
        assert response.status_code == 202, f"Failed to trigger run: {response.text}"

        run_response = response.json()
        run_id = run_response["run_id"]

        print(f"  ✅ Triggered run: {run_id}")
        print(f"     Status: {run_response['status']}")

        # Assertions
        assert run_id is not None
        assert run_response["status"] in ["accepted", "running"]

        # ========================================================================
        # STEP 3: Poll for Completion
        # ========================================================================
        print("\n⏳ STEP 3: Polling for completion...")

        max_wait_seconds = 180  # 3 minutes
        poll_interval = 2  # seconds
        start_time = time.time()

        run_status = None

        while time.time() - start_time < max_wait_seconds:
            # Get run status
            response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run_id}")

            assert response.status_code == 200, f"Failed to get run status: {response.text}"

            run = response.json()
            run_status = run["status"]

            print(f"  ⏱️  {int(time.time() - start_time)}s - Status: {run_status}", end="")

            if run_status in ["success", "completed"]:
                print(" ✅")
                break
            elif run_status == "failed":
                print(" ❌")
                pytest.fail(f"Run failed: {run.get('errors', [])}")
            else:
                print()
                time.sleep(poll_interval)

        # Check if completed
        if run_status not in ["success", "completed"]:
            pytest.fail(f"Run did not complete within {max_wait_seconds}s. Last status: {run_status}")

        print(f"\n  ✅ Run completed successfully in {int(time.time() - start_time)}s")

        # ========================================================================
        # STEP 4: Verify Run Metrics
        # ========================================================================
        print("\n📊 STEP 4: Verifying run metrics...")

        # Get final run details
        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run_id}")
        assert response.status_code == 200

        run = response.json()

        print(f"  Run ID: {run['id']}")
        print(f"  Status: {run['status']}")
        print(f"  Started: {run['started_at']}")
        print(f"  Ended: {run['ended_at']}")
        print(f"  Duration: {run.get('metrics', {}).get('duration_seconds', 0)}s")
        print(f"  Files Processed: {run.get('metrics', {}).get('files_processed', 0)}")
        print(f"  Records Ingested: {run.get('metrics', {}).get('records_ingested', 0)}")
        print(f"  Bytes Read: {run.get('metrics', {}).get('bytes_read', 0)}")
        print(f"  Errors: {len(run.get('errors', []))}")

        # Assertions
        assert run["status"] in ["success", "completed"]
        assert run["started_at"] is not None
        assert run["ended_at"] is not None

        metrics = run.get("metrics", {})
        assert metrics.get("files_processed", 0) == 3, "Expected 3 files processed"
        assert metrics.get("records_ingested", 0) == 3000, "Expected 3000 records ingested"
        assert len(run.get("errors", [])) == 0, "Expected no errors"

        print("  ✅ All metrics verified")

        # ========================================================================
        # STEP 5: Verify Data in Iceberg Table
        # ========================================================================
        print("\n🔍 STEP 5: Verifying data in Iceberg table...")

        table_identifier = f"{ingestion['destination']['catalog']}.{ingestion['destination']['database']}.{ingestion['destination']['table']}"

        try:
            # Query table
            df = spark_session.table(table_identifier)

            # Get record count
            record_count = df.count()
            print(f"  Records in table: {record_count}")

            # Get schema
            schema = df.schema
            print(f"  Schema fields: {[field.name for field in schema.fields]}")

            # Get sample data
            sample_data = df.limit(5).collect()
            print(f"  Sample records: {len(sample_data)}")

            # Check for duplicates
            distinct_count = df.select("id").distinct().count()
            print(f"  Distinct IDs: {distinct_count}")

            # Assertions
            assert record_count == 3000, f"Expected 3000 records, got {record_count}"

            # Verify schema has expected columns
            field_names = [field.name for field in schema.fields]
            expected_fields = ["id", "timestamp", "user_id", "event_type", "value"]

            for field in expected_fields:
                assert field in field_names, f"Missing field: {field}"

            # Verify no duplicates
            assert distinct_count == record_count, f"Found duplicate IDs: {record_count - distinct_count} duplicates"

            # Verify data is queryable
            assert len(sample_data) == 5, "Expected 5 sample records"

            print("  ✅ Data verification passed")

        except Exception as e:
            pytest.fail(f"Failed to verify Iceberg table: {e}")

        # ========================================================================
        # STEP 6: Verify Run History
        # ========================================================================
        print("\n📜 STEP 6: Verifying run history...")

        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert response.status_code == 200

        runs = response.json()

        print(f"  Total runs: {len(runs)}")

        # Assertions
        assert len(runs) >= 1, "Expected at least 1 run in history"

        # Find our run
        our_run = next((r for r in runs if r["id"] == run_id), None)
        assert our_run is not None, f"Run {run_id} not found in history"

        print(f"  ✅ Run found in history: {our_run['id']}")
        print(f"     Status: {our_run['status']}")
        print(f"     Files Processed: {our_run.get('metrics', {}).get('files_processed', 0)}")

        # ========================================================================
        # TEST COMPLETE
        # ========================================================================
        print("\n" + "="*80)
        print("✅ E2E TEST PASSED: Basic S3 JSON Ingestion - Happy Path")
        print("="*80)
        print(f"\nSummary:")
        print(f"  - Ingestion ID: {ingestion_id}")
        print(f"  - Run ID: {run_id}")
        print(f"  - Files Processed: 3")
        print(f"  - Records Ingested: 3000")
        print(f"  - Duration: {int(time.time() - start_time)}s")
        print(f"  - Table: {table_identifier}")
        print(f"  - Status: SUCCESS ✅")
        print("="*80 + "\n")
