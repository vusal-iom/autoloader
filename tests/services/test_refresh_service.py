"""
Unit tests for RefreshService.

Tests cover:
- Full refresh operations
- New-only refresh operations
- Dry run mode
- Confirmation validation
- Error handling
- Impact estimation
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from sqlalchemy.orm import Session

from app.services.refresh_service import RefreshService
from app.models.domain import Ingestion, IngestionStatus, Run, RunStatus


@pytest.fixture
def mock_db():
    """Mock database session."""
    return Mock(spec=Session)


@pytest.fixture
def sample_ingestion():
    """Create a sample ingestion for testing."""
    ingestion = Mock(spec=Ingestion)
    ingestion.id = "ing-test123"
    ingestion.cluster_id = "cluster-1"
    ingestion.destination_catalog = "catalog"
    ingestion.destination_database = "database"
    ingestion.destination_table = "test_table"
    ingestion.status = IngestionStatus.ACTIVE
    return ingestion


@pytest.fixture
def sample_run():
    """Create a sample run for testing."""
    run = Mock(spec=Run)
    run.id = "run-test456"
    run.status = RunStatus.RUNNING
    run.started_at = datetime.utcnow()
    return run


@pytest.fixture
def refresh_service(mock_db):
    """Create RefreshService with mocked dependencies."""
    # Mock all dependent services
    spark_service = Mock()
    file_state_service = Mock()
    ingestion_service = Mock()
    ingestion_repo = Mock()

    service = RefreshService(
        db=mock_db,
        spark_service=spark_service,
        file_state_service=file_state_service,
        ingestion_service=ingestion_service,
        ingestion_repo=ingestion_repo
    )

    return service


class TestRefreshServiceValidation:
    """Test input validation and error handling."""

    def test_refresh_requires_confirmation(self, refresh_service):
        """Test that confirmation is required."""
        with pytest.raises(ValueError, match="Confirmation required"):
            refresh_service.refresh(
                ingestion_id="ing-test123",
                confirm=False
            )

    def test_refresh_ingestion_not_found(self, refresh_service):
        """Test error when ingestion doesn't exist."""
        refresh_service.ingestion_repo.get_by_id.return_value = None

        with pytest.raises(ValueError, match="Ingestion not found"):
            refresh_service.refresh(
                ingestion_id="ing-nonexistent",
                confirm=True
            )


class TestFullRefresh:
    """Test full refresh operations."""

    def test_full_refresh_success(self, refresh_service, sample_ingestion, sample_run):
        """Test successful full refresh."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.spark.drop_table.return_value = {
            "success": True,
            "table_name": "catalog.database.test_table",
            "table_info": {"existed": True}
        }
        refresh_service.file_state.get_processed_files.return_value = {"file1.json", "file2.json"}
        refresh_service.file_state.clear_processed_files.return_value = 2
        refresh_service.ingestion_service.trigger_manual_run.return_value = sample_run

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="full",
            auto_run=True,
            dry_run=False
        )

        # Assertions
        assert result["status"] == "accepted"
        assert result["mode"] == "full"
        assert result["run_id"] == "run-test456"
        assert len(result["operations"]) == 3  # table_dropped, files_cleared, run_triggered
        assert all(op["status"] == "success" for op in result["operations"])

        # Verify calls
        refresh_service.spark.drop_table.assert_called_once_with(
            "cluster-1",
            "catalog.database.test_table"
        )
        refresh_service.file_state.clear_processed_files.assert_called_once_with("ing-test123")
        refresh_service.ingestion_service.trigger_manual_run.assert_called_once_with("ing-test123")

    def test_full_refresh_without_auto_run(self, refresh_service, sample_ingestion):
        """Test full refresh without triggering run."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.spark.drop_table.return_value = {
            "success": True,
            "table_info": {"existed": True}
        }
        refresh_service.file_state.get_processed_files.return_value = set()
        refresh_service.file_state.clear_processed_files.return_value = 0

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="full",
            auto_run=False,
            dry_run=False
        )

        # Assertions
        assert result["status"] == "accepted"
        assert result["run_id"] is None
        assert len(result["operations"]) == 2  # Only table_dropped and files_cleared
        refresh_service.ingestion_service.trigger_manual_run.assert_not_called()

    def test_full_refresh_dry_run(self, refresh_service, sample_ingestion):
        """Test full refresh in dry run mode."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.file_state.get_processed_files.return_value = {"file1.json", "file2.json"}

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="full",
            dry_run=True
        )

        # Assertions
        assert result["status"] == "dry_run"
        assert result["mode"] == "full"
        assert "would_perform" in result
        assert len(result["would_perform"]) == 3  # All operations listed
        assert "next_steps" in result
        assert "⚠️" in "\n".join(result["notes"])  # Contains warnings

        # Verify no actual operations performed
        refresh_service.spark.drop_table.assert_not_called()
        refresh_service.file_state.clear_processed_files.assert_not_called()
        refresh_service.ingestion_service.trigger_manual_run.assert_not_called()


class TestNewOnlyRefresh:
    """Test new-only refresh operations."""

    def test_new_only_refresh_success(self, refresh_service, sample_ingestion, sample_run):
        """Test successful new-only refresh."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.spark.drop_table.return_value = {
            "success": True,
            "table_info": {"existed": True}
        }
        refresh_service.file_state.get_processed_files.return_value = {"file1.json", "file2.json"}
        refresh_service.ingestion_service.trigger_manual_run.return_value = sample_run

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="new_only",
            auto_run=True,
            dry_run=False
        )

        # Assertions
        assert result["status"] == "accepted"
        assert result["mode"] == "new_only"
        assert result["run_id"] == "run-test456"
        assert len(result["operations"]) == 2  # Only table_dropped and run_triggered

        # Verify calls
        refresh_service.spark.drop_table.assert_called_once()
        refresh_service.file_state.clear_processed_files.assert_not_called()  # Key difference!
        refresh_service.ingestion_service.trigger_manual_run.assert_called_once()

    def test_new_only_refresh_dry_run(self, refresh_service, sample_ingestion):
        """Test new-only refresh in dry run mode."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.file_state.get_processed_files.return_value = {"file1.json", "file2.json"}

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="new_only",
            dry_run=True
        )

        # Assertions
        assert result["status"] == "dry_run"
        assert result["mode"] == "new_only"
        assert "would_perform" in result
        assert len(result["would_perform"]) == 2  # Only table_dropped and run_triggered
        assert "ℹ️" in "\n".join(result["notes"])  # Contains info notes

        # Verify no actual operations performed
        refresh_service.spark.drop_table.assert_not_called()
        refresh_service.ingestion_service.trigger_manual_run.assert_not_called()


class TestErrorHandling:
    """Test error handling in refresh operations."""

    def test_table_drop_failure(self, refresh_service, sample_ingestion):
        """Test handling of table drop failure."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.spark.drop_table.side_effect = Exception("Spark connection error")
        refresh_service.file_state.get_processed_files.return_value = set()

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="full",
            dry_run=False
        )

        # Assertions
        assert result["status"] == "partial_failure"
        assert len(result["operations"]) == 1
        assert result["operations"][0]["status"] == "failed"
        assert result["operations"][0]["error"] == "Spark connection error"
        assert "error" in result

        # Verify subsequent operations not called
        refresh_service.file_state.clear_processed_files.assert_not_called()
        refresh_service.ingestion_service.trigger_manual_run.assert_not_called()

    def test_file_clear_failure(self, refresh_service, sample_ingestion):
        """Test handling of file clear failure."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.spark.drop_table.return_value = {"success": True, "table_info": {"existed": True}}
        refresh_service.file_state.get_processed_files.return_value = {"file1.json"}
        refresh_service.file_state.clear_processed_files.side_effect = Exception("Database error")

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="full",
            dry_run=False
        )

        # Assertions
        assert result["status"] == "partial_failure"
        assert len(result["operations"]) == 2
        assert result["operations"][0]["status"] == "success"  # Table dropped
        assert result["operations"][1]["status"] == "failed"   # File clear failed
        assert "error" in result

        # Verify run not triggered
        refresh_service.ingestion_service.trigger_manual_run.assert_not_called()

    def test_run_trigger_failure(self, refresh_service, sample_ingestion):
        """Test handling of run trigger failure."""
        # Setup mocks
        refresh_service.ingestion_repo.get_by_id.return_value = sample_ingestion
        refresh_service.spark.drop_table.return_value = {"success": True, "table_info": {"existed": True}}
        refresh_service.file_state.get_processed_files.return_value = set()
        refresh_service.file_state.clear_processed_files.return_value = 0
        refresh_service.ingestion_service.trigger_manual_run.side_effect = Exception("Cluster not available")

        # Execute
        result = refresh_service.refresh(
            ingestion_id="ing-test123",
            confirm=True,
            mode="full",
            auto_run=True,
            dry_run=False
        )

        # Assertions
        assert result["status"] == "partial_failure"
        assert len(result["operations"]) == 3
        assert result["operations"][0]["status"] == "success"  # Table dropped
        assert result["operations"][1]["status"] == "success"  # Files cleared
        assert result["operations"][2]["status"] == "failed"   # Run trigger failed
        assert "recovery" in result
        assert "Manually trigger run" in result["recovery"]["action"]


class TestImpactEstimation:
    """Test impact estimation logic."""

    def test_full_refresh_impact_estimation(self, refresh_service, sample_ingestion):
        """Test impact estimation for full refresh."""
        # Setup
        refresh_service.file_state.get_processed_files.return_value = {
            f"file{i}.json" for i in range(100)
        }

        # Execute
        impact = refresh_service._estimate_full_refresh_impact(sample_ingestion)

        # Assertions
        assert "files_to_process" in impact
        assert "estimated_data_size_gb" in impact
        assert "estimated_cost_usd" in impact
        assert "estimated_duration_minutes" in impact
        assert impact["files_to_process"] == 100
        assert impact["files_skipped"] == 0
        assert impact["estimated_data_size_gb"] > 0
        assert impact["estimated_cost_usd"] > 0

    def test_incremental_refresh_impact_estimation(self, refresh_service, sample_ingestion):
        """Test impact estimation for incremental refresh."""
        # Setup
        refresh_service.file_state.get_processed_files.return_value = {
            f"file{i}.json" for i in range(100)
        }

        # Execute
        impact = refresh_service._estimate_incremental_refresh_impact(sample_ingestion)

        # Assertions
        assert "files_to_process" in impact
        assert "files_skipped" in impact
        assert "estimated_data_size_gb" in impact
        assert "estimated_cost_usd" in impact
        assert impact["files_skipped"] == 100
        assert impact["files_to_process"] < impact["files_skipped"]  # New files < processed files


class TestHelperMethods:
    """Test helper methods."""

    def test_get_table_fqn(self, refresh_service, sample_ingestion):
        """Test table FQN generation."""
        fqn = refresh_service._get_table_fqn(sample_ingestion)
        assert fqn == "catalog.database.test_table"

    def test_build_warnings_high_cost(self, refresh_service):
        """Test warning generation for high-cost operations."""
        impact = {
            "files_to_process": 1000,
            "estimated_data_size_gb": 500.0,
            "estimated_cost_usd": 150.0,
            "estimated_duration_minutes": 120
        }

        warnings = refresh_service._build_warnings(impact, "full")

        assert len(warnings) > 0
        assert any("1000 files" in w for w in warnings)
        assert any("$150" in w for w in warnings)

    def test_build_recovery_instructions(self, refresh_service):
        """Test recovery instructions for partial failures."""
        operations = [
            {"operation": "table_dropped", "status": "success"},
            {"operation": "processed_files_cleared", "status": "success"},
            {"operation": "run_triggered", "status": "failed"}
        ]

        recovery = refresh_service._build_recovery_instructions(operations, "ing-test123")

        assert "run failed" in recovery["message"].lower()
        assert "POST /api/v1/ingestions/ing-test123/run" in recovery["action"]
        assert "state" in recovery
