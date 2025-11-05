"""
Integration tests for Refresh API endpoints.

Tests the complete request/response cycle including:
- Endpoint routing
- Request validation
- Service integration
- Response formatting
- Error handling
"""

import pytest
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from datetime import datetime

from app.models.domain import Ingestion, IngestionStatus


@pytest.fixture(scope="function")
def sample_ingestion_in_db(test_db):
    """Create a sample ingestion in the test database."""
    import uuid
    from app.repositories.ingestion_repository import IngestionRepository

    repo = IngestionRepository(test_db)

    # Use unique ID for each test to avoid conflicts
    unique_id = f"ing-test-{uuid.uuid4().hex[:8]}"

    ingestion = Ingestion(
        id=unique_id,
        tenant_id="tenant-1",
        name="Test Ingestion",
        status=IngestionStatus.ACTIVE,
        cluster_id="cluster-1",
        # Source
        source_type="s3",
        source_path="s3://test-bucket/data/",
        source_file_pattern="*.json",
        # Format
        format_type="json",
        schema_inference="auto",
        schema_evolution_enabled=True,
        # Destination
        destination_catalog="catalog",
        destination_database="database",
        destination_table="test_table",
        write_mode="append",
        # Schedule
        schedule_frequency="daily",
        # Metadata
        checkpoint_location=f"/tmp/checkpoints/{unique_id}",
        created_by="test-user"
    )

    created = repo.create(ingestion)
    test_db.commit()

    yield created

    # Cleanup after test
    try:
        repo.delete(unique_id)
        test_db.commit()
    except Exception:
        pass


class TestFullRefreshEndpoint:
    """Test /api/v1/ingestions/{id}/refresh/full endpoint."""

    @patch('app.services.refresh_service.SparkService')
    @patch('app.services.refresh_service.IngestionService')
    def test_full_refresh_success(
        self,
        mock_ingestion_service_class,
        mock_spark_service_class,
        api_client,
        sample_ingestion_in_db
    ):
        """Test successful full refresh via API."""
        # Setup mocks
        mock_spark = Mock()
        mock_spark.drop_table.return_value = {
            "success": True,
            "table_info": {"existed": True}
        }
        mock_spark_service_class.return_value = mock_spark

        mock_ingestion_service = Mock()
        mock_run = Mock()
        mock_run.id = "run-test456"
        mock_ingestion_service.trigger_manual_run.return_value = mock_run
        mock_ingestion_service_class.return_value = mock_ingestion_service

        # Make request
        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={
                "confirm": True,
                "auto_run": True,
                "dry_run": False
            }
        )

        # Assertions
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["mode"] == "full"
        assert data["ingestion_id"] == sample_ingestion_in_db.id
        assert "operations" in data
        assert "impact" in data

    def test_full_refresh_missing_confirmation(self, api_client, sample_ingestion_in_db):
        """Test that confirmation is required."""
        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={
                "confirm": False
            }
        )

        # Pydantic validator returns 422 for validation errors
        assert response.status_code == 422
        detail = response.json()["detail"]
        # Should contain validation error about confirm
        assert any("confirm" in str(err).lower() for err in detail)

    @patch('app.services.refresh_service.SparkService')
    def test_full_refresh_dry_run(
        self,
        mock_spark_service_class,
        api_client,
        sample_ingestion_in_db
    ):
        """Test dry run mode."""
        # Setup mocks
        mock_spark = Mock()
        mock_spark_service_class.return_value = mock_spark

        # Make request
        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={
                "confirm": True,
                "dry_run": True
            }
        )

        # Assertions
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "dry_run"
        assert "would_perform" in data
        assert "next_steps" in data

        # Verify no actual operations performed
        mock_spark.drop_table.assert_not_called()

    def test_full_refresh_ingestion_not_found(self, api_client):
        """Test error when ingestion doesn't exist."""
        response = api_client.post(
            "/api/v1/ingestions/ing-nonexistent/refresh/full",
            json={
                "confirm": True
            }
        )

        assert response.status_code == 400
        assert "not found" in response.json()["detail"].lower()

    def test_full_refresh_invalid_request_body(self, api_client, sample_ingestion_in_db):
        """Test validation of request body."""
        # Missing required 'confirm' field
        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={}
        )

        assert response.status_code == 422  # Pydantic validation error


class TestNewOnlyRefreshEndpoint:
    """Test /api/v1/ingestions/{id}/refresh/new-only endpoint."""

    @patch('app.services.refresh_service.SparkService')
    @patch('app.services.refresh_service.IngestionService')
    def test_new_only_refresh_success(
        self,
        mock_ingestion_service_class,
        mock_spark_service_class,
        api_client,
        sample_ingestion_in_db
    ):
        """Test successful new-only refresh via API."""
        # Setup mocks
        mock_spark = Mock()
        mock_spark.drop_table.return_value = {
            "success": True,
            "table_info": {"existed": True}
        }
        mock_spark_service_class.return_value = mock_spark

        mock_ingestion_service = Mock()
        mock_run = Mock()
        mock_run.id = "run-test789"
        mock_ingestion_service.trigger_manual_run.return_value = mock_run
        mock_ingestion_service_class.return_value = mock_ingestion_service

        # Make request
        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/new-only",
            json={
                "confirm": True,
                "auto_run": True
            }
        )

        # Assertions
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["mode"] == "new_only"
        assert data["ingestion_id"] == sample_ingestion_in_db.id
        assert "operations" in data

        # Verify file history NOT cleared (key difference from full refresh)
        # In new-only mode, we only drop table and trigger run
        assert len([op for op in data["operations"] if op["operation"] == "processed_files_cleared"]) == 0

    def test_new_only_refresh_without_auto_run(
        self,
        api_client,
        sample_ingestion_in_db
    ):
        """Test new-only refresh without triggering run."""
        with patch('app.services.refresh_service.SparkService') as mock_spark_class:
            mock_spark = Mock()
            mock_spark.drop_table.return_value = {
                "success": True,
                "table_info": {"existed": True}
            }
            mock_spark_class.return_value = mock_spark

            response = api_client.post(
                f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/new-only",
                json={
                    "confirm": True,
                    "auto_run": False
                }
            )

            assert response.status_code == 202
            data = response.json()
            assert data["run_id"] is None
            assert len([op for op in data["operations"] if op["operation"] == "run_triggered"]) == 0

    @patch('app.services.refresh_service.SparkService')
    def test_new_only_refresh_dry_run(
        self,
        mock_spark_service_class,
        api_client,
        sample_ingestion_in_db
    ):
        """Test dry run mode for new-only refresh."""
        mock_spark = Mock()
        mock_spark_service_class.return_value = mock_spark

        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/new-only",
            json={
                "confirm": True,
                "dry_run": True
            }
        )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "dry_run"
        assert "would_perform" in data
        # Verify it shows only 2 operations (table drop + run), not file clear
        assert len(data["would_perform"]) == 2

    def test_new_only_refresh_missing_confirmation(self, api_client, sample_ingestion_in_db):
        """Test that confirmation is required."""
        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/new-only",
            json={
                "confirm": False
            }
        )

        # Pydantic validator returns 422 for validation errors
        assert response.status_code == 422
        detail = response.json()["detail"]
        # Should contain validation error about confirm
        assert any("confirm" in str(err).lower() for err in detail)


class TestErrorHandling:
    """Test error handling in API endpoints."""

    @patch('app.services.refresh_service.SparkService')
    def test_full_refresh_service_error(
        self,
        mock_spark_service_class,
        api_client,
        sample_ingestion_in_db
    ):
        """Test handling of service-level errors."""
        # Setup mock to raise exception
        mock_spark = Mock()
        mock_spark.drop_table.side_effect = Exception("Spark connection failed")
        mock_spark_service_class.return_value = mock_spark

        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={
                "confirm": True
            }
        )

        # Should return partial_failure, not 500 error
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "partial_failure"
        # Error details should be in operations and notes
        assert any(op["status"] == "failed" for op in data["operations"])
        assert len(data["notes"]) > 0
        assert "Spark connection failed" in str(data)

    def test_invalid_ingestion_id_format(self, api_client):
        """Test with malformed ingestion ID."""
        response = api_client.post(
            "/api/v1/ingestions/invalid-id-format/refresh/full",
            json={
                "confirm": True
            }
        )

        # Should handle gracefully (not found)
        assert response.status_code == 400


class TestResponseFormat:
    """Test response format compliance with PRD."""

    @patch('app.services.refresh_service.SparkService')
    @patch('app.services.refresh_service.IngestionService')
    def test_response_has_required_fields(
        self,
        mock_ingestion_service_class,
        mock_spark_service_class,
        api_client,
        sample_ingestion_in_db
    ):
        """Test that response contains all required fields per PRD."""
        # Setup mocks
        mock_spark = Mock()
        mock_spark.drop_table.return_value = {
            "success": True,
            "table_info": {"existed": True}
        }
        mock_spark_service_class.return_value = mock_spark

        mock_ingestion_service = Mock()
        mock_run = Mock()
        mock_run.id = "run-test"
        mock_ingestion_service.trigger_manual_run.return_value = mock_run
        mock_ingestion_service_class.return_value = mock_ingestion_service

        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={
                "confirm": True
            }
        )

        assert response.status_code == 202
        data = response.json()

        # Required fields per PRD
        required_fields = [
            "status",
            "message",
            "ingestion_id",
            "timestamp",
            "mode",
            "operations",
            "impact",
            "warnings",
            "notes"
        ]

        for field in required_fields:
            assert field in data, f"Missing required field: {field}"

        # Verify impact structure
        impact = data["impact"]
        impact_required = [
            "files_to_process",
            "estimated_data_size_gb",
            "estimated_cost_usd",
            "estimated_duration_minutes"
        ]
        for field in impact_required:
            assert field in impact, f"Missing impact field: {field}"

    @patch('app.services.refresh_service.SparkService')
    def test_dry_run_response_has_next_steps(
        self,
        mock_spark_service_class,
        api_client,
        sample_ingestion_in_db
    ):
        """Test that dry run response includes next_steps."""
        mock_spark = Mock()
        mock_spark_service_class.return_value = mock_spark

        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={
                "confirm": True,
                "dry_run": True
            }
        )

        data = response.json()
        assert "next_steps" in data
        assert "to_proceed" in data["next_steps"]
        assert "to_cancel" in data["next_steps"]
        assert f"{sample_ingestion_in_db.id}" in data["next_steps"]["to_proceed"]


class TestRequestValidation:
    """Test request validation per PRD schemas."""

    def test_confirm_field_validation(self, api_client, sample_ingestion_in_db):
        """Test that confirm field must be true."""
        # Test with confirm=false
        response = api_client.post(
            f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
            json={
                "confirm": False
            }
        )
        # Pydantic validation returns 422
        assert response.status_code == 422

    def test_optional_fields_defaults(
        self,
        api_client,
        sample_ingestion_in_db
    ):
        """Test that optional fields have correct defaults."""
        with patch('app.services.refresh_service.SparkService') as mock_spark_class:
            with patch('app.services.refresh_service.IngestionService') as mock_ing_class:
                mock_spark = Mock()
                mock_spark.drop_table.return_value = {"success": True, "table_info": {"existed": True}}
                mock_spark_class.return_value = mock_spark

                mock_ing = Mock()
                mock_run = Mock()
                mock_run.id = "run-test"
                mock_ing.trigger_manual_run.return_value = mock_run
                mock_ing_class.return_value = mock_ing

                # Only provide confirm, let others default
                response = api_client.post(
                    f"/api/v1/ingestions/{sample_ingestion_in_db.id}/refresh/full",
                    json={
                        "confirm": True
                    }
                )

                assert response.status_code == 202
                # auto_run defaults to True, so run should be triggered
                data = response.json()
                assert data["run_id"] is not None
