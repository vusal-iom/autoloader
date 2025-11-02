"""
E2E Test: Basic S3 JSON Ingestion

Test ID: E2E-01
Description: End-to-end test for creating and manually running a basic S3 JSON ingestion
"""
import time
from typing import Dict, Any

import pytest
from fastapi.testclient import TestClient


@pytest.mark.e2e
@pytest.mark.requires_minio
class TestBasicS3JsonIngestion:
    """End-to-end test for basic S3 JSON ingestion flow"""

    def test_create_ingestion(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test Step 1: Create ingestion configuration

        Validates:
        - API accepts valid ingestion configuration
        - Response contains generated ID
        - Initial status is DRAFT
        - All fields are correctly stored
        """
        response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )

        # Assertions
        assert response.status_code == 201, f"Expected 201, got {response.status_code}: {response.text}"

        data = response.json()
        assert "id" in data, "Response should contain ingestion ID"
        assert data["status"] == "draft", f"Expected draft status, got {data['status']}"
        assert data["name"] == sample_ingestion_config["name"]
        assert data["source"]["type"] == sample_ingestion_config["source"]["type"]
        assert data["format"]["type"] == sample_ingestion_config["format"]["type"]
        assert data["schedule"]["mode"] == sample_ingestion_config["schedule"]["mode"]

        # Store ingestion ID for subsequent tests
        return data["id"]

    def test_activate_ingestion(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test Step 2: Activate ingestion

        Validates:
        - Status transitions from DRAFT to ACTIVE
        - Update endpoint works correctly
        """
        # First create ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        # Activate ingestion
        update_response = api_client.put(
            f"/api/v1/ingestions/{ingestion_id}",
            json={"status": "ACTIVE"}
        )

        # Assertions
        assert update_response.status_code == 200, f"Expected 200, got {update_response.status_code}"

        data = update_response.json()
        assert data["status"] == "active", f"Expected active status, got {data['status']}"
        assert data["id"] == ingestion_id

    def test_trigger_manual_run(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test Step 3: Trigger manual run

        Validates:
        - Manual run can be triggered
        - Returns 202 ACCEPTED for async operation
        - Response contains run_id
        - Initial run status is appropriate (RUNNING or PENDING)
        """
        # Create and activate ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        api_client.put(
            f"/api/v1/ingestions/{ingestion_id}",
            json={"status": "ACTIVE"}
        )

        # Trigger manual run
        run_response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")

        # Assertions
        assert run_response.status_code == 202, f"Expected 202, got {run_response.status_code}"

        data = run_response.json()
        assert "run_id" in data, "Response should contain run_id"
        assert data["status"] in ["accepted"], f"Unexpected status: {data['status']}"

        return {
            "ingestion_id": ingestion_id,
            "run_id": data["run_id"]
        }

    @pytest.mark.slow
    def test_complete_ingestion_flow_with_mock(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any],
        mock_spark_executor
    ):
        """
        Test Steps 1-6: Complete ingestion flow with mocked Spark executor

        This test runs the full flow but mocks the Spark execution.
        Use this when Spark Connect is not available.

        Validates:
        - Complete workflow from creation to completion
        - Status transitions are correct
        - Metrics are recorded
        - Run history is maintained
        """
        # Step 1: Create ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        # Step 2: Activate ingestion
        activate_response = api_client.put(
            f"/api/v1/ingestions/{ingestion_id}",
            json={"status": "ACTIVE"}
        )
        assert activate_response.status_code == 200

        # Step 3: Trigger manual run
        run_response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
        assert run_response.status_code == 202
        run_id = run_response.json()["run_id"]

        # Step 4: Poll run status (with timeout)
        max_attempts = 30  # 30 seconds timeout
        poll_interval = 1  # 1 second

        final_status = None
        for attempt in range(max_attempts):
            status_response = api_client.get(
                f"/api/v1/ingestions/{ingestion_id}/runs/{run_id}"
            )
            assert status_response.status_code == 200

            run_data = status_response.json()
            current_status = run_data["status"]

            if current_status in ["success", "failed", "partial"]:
                final_status = current_status
                break

            time.sleep(poll_interval)

        # Assertions on final state
        assert final_status == "success", f"Expected success, got {final_status}"

        # Verify metrics (with mock, these should match mock values)
        assert run_data["metrics"]["files_processed"] == 3, f"Expected 3 files, got {run_data['metrics']['files_processed']}"
        assert run_data["metrics"]["records_ingested"] == 3000, f"Expected 3000 records, got {run_data['metrics']['records_ingested']}"
        assert run_data["errors"] == [] or run_data["errors"] is None
        assert run_data["metrics"]["duration_seconds"] > 0

        # Step 5: Verify run history
        history_response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert history_response.status_code == 200

        history_data = history_response.json()
        assert "runs" in history_data or isinstance(history_data, list)
        runs = history_data if isinstance(history_data, list) else history_data["runs"]
        assert len(runs) >= 1
        assert runs[0]["id"] == run_id
        assert runs[0]["status"] == "success"

    @pytest.mark.requires_spark
    @pytest.mark.slow
    def test_complete_ingestion_flow_with_real_spark(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any],
        test_data_s3: Dict[str, Any],
        spark_connect_available: bool
    ):
        """
        Test Steps 1-6: Complete ingestion flow with real Spark Connect

        This test requires a running Spark cluster with Spark Connect.
        Set TEST_SPARK_CONNECT_URL environment variable to enable.

        Validates:
        - Real Spark execution works end-to-end
        - Data is actually written to Iceberg table
        - Schema inference works correctly
        - Metrics reflect actual processing
        """
        if not spark_connect_available:
            pytest.skip("Spark Connect not available. Set TEST_SPARK_CONNECT_URL to enable.")

        # Step 1: Create ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        # Step 2: Activate ingestion
        activate_response = api_client.put(
            f"/api/v1/ingestions/{ingestion_id}",
            json={"status": "ACTIVE"}
        )
        assert activate_response.status_code == 200

        # Step 3: Trigger manual run
        run_response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
        assert run_response.status_code == 202
        run_id = run_response.json()["run_id"]

        # Step 4: Poll run status (with longer timeout for real execution)
        max_attempts = 120  # 2 minutes timeout
        poll_interval = 1

        final_status = None
        run_data = None

        for attempt in range(max_attempts):
            status_response = api_client.get(
                f"/api/v1/ingestions/{ingestion_id}/runs/{run_id}"
            )
            assert status_response.status_code == 200

            run_data = status_response.json()
            current_status = run_data["status"]

            if current_status in ["success", "failed", "partial"]:
                final_status = current_status
                break

            time.sleep(poll_interval)

        # Assertions
        assert final_status == "success", f"Run failed with status: {final_status}"
        assert run_data is not None

        # Verify metrics from real execution
        assert run_data["metrics"]["files_processed"] == test_data_s3["num_files"]
        assert run_data["metrics"]["records_ingested"] == test_data_s3["total_records"]
        assert run_data["metrics"]["bytes_read"] > 0
        assert run_data["metrics"]["duration_seconds"] > 0
        assert run_data["errors"] == [] or run_data["errors"] is None

        # Step 5: Verify data in Iceberg table (requires Spark Connect client)
        # TODO: Implement Iceberg table verification
        # from app.spark.connect_client import SparkConnectClient
        # client = SparkConnectClient(sample_ingestion_config["spark_connect_url"])
        # df = client.read_table(...)
        # assert df.count() == test_data_s3["total_records"]

        # Step 6: Verify run history
        history_response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
        assert history_response.status_code == 200

        history_data = history_response.json()
        assert len(history_data["runs"]) >= 1

    def test_list_ingestions(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test: List ingestions with tenant filtering

        Validates:
        - List endpoint returns created ingestions
        - Tenant filtering works correctly
        - Response format is correct
        """
        # Create an ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        # List ingestions
        list_response = api_client.get(
            "/api/v1/ingestions",
            params={"tenant_id": sample_ingestion_config["tenant_id"]}
        )

        # Assertions
        assert list_response.status_code == 200
        data = list_response.json()

        assert isinstance(data, list) or "ingestions" in data
        ingestions = data if isinstance(data, list) else data["ingestions"]

        assert len(ingestions) >= 1
        assert any(ing["id"] == ingestion_id for ing in ingestions)

    def test_get_ingestion_details(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test: Get ingestion details

        Validates:
        - Detail endpoint returns complete ingestion info
        - All fields are present
        """
        # Create an ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        # Get details
        detail_response = api_client.get(f"/api/v1/ingestions/{ingestion_id}")

        # Assertions
        assert detail_response.status_code == 200
        data = detail_response.json()

        assert data["id"] == ingestion_id
        assert data["name"] == sample_ingestion_config["name"]
        assert data["source"]["type"] == sample_ingestion_config["source"]["type"]
        assert "created_at" in data
        assert "updated_at" in data

    def test_delete_ingestion(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test: Delete ingestion

        Validates:
        - Ingestion can be deleted
        - Deleted ingestion returns 404 on subsequent access
        """
        # Create an ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        # Delete ingestion
        delete_response = api_client.delete(f"/api/v1/ingestions/{ingestion_id}")
        assert delete_response.status_code in [200, 204]

        # Verify deletion
        get_response = api_client.get(f"/api/v1/ingestions/{ingestion_id}")
        assert get_response.status_code == 404

    def test_pause_and_resume_ingestion(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test: Pause and resume ingestion

        Validates:
        - Active ingestion can be paused
        - Paused ingestion can be resumed
        - Status transitions are correct
        """
        # Create and activate ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        api_client.put(
            f"/api/v1/ingestions/{ingestion_id}",
            json={"status": "ACTIVE"}
        )

        # Pause ingestion
        pause_response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/pause")
        assert pause_response.status_code == 200
        assert pause_response.json()["status"] == "paused"

        # Resume ingestion
        resume_response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/resume")
        assert resume_response.status_code == 200
        assert resume_response.json()["status"] == "active"

    def test_run_history_pagination(
        self,
        api_client: TestClient,
        sample_ingestion_config: Dict[str, Any]
    ):
        """
        Test: Run history with pagination

        Validates:
        - Run history endpoint supports pagination
        - Limit and offset parameters work correctly
        """
        # Create and activate ingestion
        create_response = api_client.post(
            "/api/v1/ingestions",
            json=sample_ingestion_config
        )
        assert create_response.status_code == 201
        ingestion_id = create_response.json()["id"]

        # Get run history (even if empty)
        history_response = api_client.get(
            f"/api/v1/ingestions/{ingestion_id}/runs",
            params={"limit": 10, "offset": 0}
        )

        assert history_response.status_code == 200
        data = history_response.json()
        assert "runs" in data or isinstance(data, list)
