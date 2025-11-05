"""
Unit tests for RefreshService with real file discovery integration.

Tests the integration of FileDiscoveryService into refresh impact estimation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
from app.services.refresh_service import RefreshService
from app.services.file_discovery_service import FileMetadata
from app.models.domain import Ingestion, IngestionStatus


@pytest.fixture
def sample_ingestion():
    """Create a sample ingestion for testing."""
    ingestion = Mock(spec=Ingestion)
    ingestion.id = "test-ing-123"
    ingestion.tenant_id = "tenant-1"
    ingestion.name = "Test Ingestion"
    ingestion.status = IngestionStatus.ACTIVE

    # Source configuration
    ingestion.source_type = "s3"
    ingestion.source_path = "s3://test-bucket/data/"
    ingestion.source_credentials = {
        "aws_access_key_id": "test-key",
        "aws_secret_access_key": "test-secret"
    }

    # Destination configuration
    ingestion.destination_catalog = "lakehouse"
    ingestion.destination_database = "raw"
    ingestion.destination_table = "events"
    ingestion.cluster_id = "cluster-1"

    return ingestion


@pytest.fixture
def mock_file_metadata():
    """Create mock file metadata for testing."""
    def create_files(count: int, size_mb: int = 100):
        """Helper to create multiple file metadata objects."""
        from datetime import timedelta
        files = []
        base_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        for i in range(count):
            file = FileMetadata(
                path=f"s3a://test-bucket/data/file_{i:04d}.json",
                size=size_mb * 1024 * 1024,  # Convert MB to bytes
                modified_at=base_time + timedelta(hours=i),  # Increment by hours instead
                etag=f"etag-{i}"
            )
            files.append(file)

        return files

    return create_files


class TestRefreshServiceFileDiscovery:
    """Test refresh service with real file discovery."""

    def test_full_refresh_impact_with_real_discovery(self, sample_ingestion, mock_file_metadata):
        """Test full refresh impact estimation with actual file discovery."""
        # Arrange
        db = Mock()
        spark_service = Mock()
        file_state_service = Mock()
        ingestion_service = Mock()
        ingestion_repo = Mock()

        # Mock: No processed files yet
        file_state_service.get_processed_files.return_value = set()

        # Mock: Discovery returns 50 files, each 100MB
        discovered_files = mock_file_metadata(50, 100)

        service = RefreshService(
            db=db,
            spark_service=spark_service,
            file_state_service=file_state_service,
            ingestion_service=ingestion_service,
            ingestion_repo=ingestion_repo
        )

        # Patch file discovery
        with patch.object(service, '_discover_files', return_value=discovered_files):
            # Act
            impact = service._estimate_full_refresh_impact(sample_ingestion)

        # Assert
        assert impact["files_to_process"] == 50
        assert impact["files_skipped"] == 0
        assert impact["estimated_data_size_gb"] == pytest.approx(4.88, abs=0.1)  # 50 * 100MB = ~4.88GB
        assert impact["estimated_cost_usd"] == pytest.approx(1.22, abs=0.1)  # ~4.88GB * $0.25/GB
        assert impact["estimated_duration_minutes"] >= 5  # At least 5 minutes

        # Verify date range is included
        assert "oldest_file_date" in impact
        assert "newest_file_date" in impact

    def test_incremental_refresh_impact_with_real_discovery(self, sample_ingestion, mock_file_metadata):
        """Test incremental refresh impact with actual file discovery."""
        # Arrange
        db = Mock()
        spark_service = Mock()
        file_state_service = Mock()
        ingestion_service = Mock()
        ingestion_repo = Mock()

        # Mock: 40 files already processed
        processed_paths = {f"s3a://test-bucket/data/file_{i:04d}.json" for i in range(40)}
        file_state_service.get_processed_files.return_value = processed_paths

        # Mock: Discovery returns 50 total files (40 old + 10 new)
        all_files = mock_file_metadata(50, 100)

        service = RefreshService(
            db=db,
            spark_service=spark_service,
            file_state_service=file_state_service,
            ingestion_service=ingestion_service,
            ingestion_repo=ingestion_repo
        )

        # Patch file discovery
        with patch.object(service, '_discover_files', return_value=all_files):
            # Act
            impact = service._estimate_incremental_refresh_impact(sample_ingestion)

        # Assert
        assert impact["files_to_process"] == 10  # Only new files
        assert impact["files_skipped"] == 40  # Already processed
        assert impact["estimated_data_size_gb"] == pytest.approx(0.95, abs=0.1)  # 10 * 100MB = ~0.95GB
        assert impact["estimated_cost_usd"] == pytest.approx(0.24, abs=0.05)  # ~0.95GB * $0.25/GB
        assert impact["estimated_duration_minutes"] >= 5

        # Verify file state was checked
        file_state_service.get_processed_files.assert_called_once_with(sample_ingestion.id)

    def test_incremental_refresh_no_new_files(self, sample_ingestion, mock_file_metadata):
        """Test incremental refresh when all files are already processed."""
        # Arrange
        db = Mock()
        file_state_service = Mock()

        # Mock: All files already processed
        all_files = mock_file_metadata(30, 100)
        processed_paths = {f.path for f in all_files}
        file_state_service.get_processed_files.return_value = processed_paths

        service = RefreshService(
            db=db,
            file_state_service=file_state_service
        )

        # Patch file discovery
        with patch.object(service, '_discover_files', return_value=all_files):
            # Act
            impact = service._estimate_incremental_refresh_impact(sample_ingestion)

        # Assert
        assert impact["files_to_process"] == 0  # No new files
        assert impact["files_skipped"] == 30
        assert impact["estimated_data_size_gb"] == 0.0
        assert impact["estimated_cost_usd"] == 0.0

    def test_full_refresh_large_files(self, sample_ingestion, mock_file_metadata):
        """Test full refresh with large files."""
        # Arrange
        db = Mock()
        file_state_service = Mock()
        file_state_service.get_processed_files.return_value = set()

        # Mock: 10 files, each 500MB
        discovered_files = mock_file_metadata(10, 500)

        service = RefreshService(db=db, file_state_service=file_state_service)

        # Patch file discovery
        with patch.object(service, '_discover_files', return_value=discovered_files):
            # Act
            impact = service._estimate_full_refresh_impact(sample_ingestion)

        # Assert
        assert impact["files_to_process"] == 10
        assert impact["estimated_data_size_gb"] == pytest.approx(4.88, abs=0.1)  # 10 * 500MB = ~4.88GB
        assert impact["estimated_cost_usd"] == pytest.approx(1.22, abs=0.05)

    def test_discovery_failure_fallback(self, sample_ingestion):
        """Test that service falls back gracefully when discovery fails."""
        # Arrange
        db = Mock()
        file_state_service = Mock()
        file_state_service.get_processed_files.return_value = set()

        service = RefreshService(db=db, file_state_service=file_state_service)

        # Patch file discovery to raise exception
        with patch.object(service, '_discover_files', side_effect=Exception("S3 connection failed")):
            # Act
            impact = service._estimate_full_refresh_impact(sample_ingestion)

        # Assert - should fall back to estimation
        assert impact["files_to_process"] > 0  # Should have some default estimate
        assert impact["estimated_data_size_gb"] > 0
        assert "estimated" in str(impact).lower()  # Indicates this is an estimate

    def test_discover_files_integration(self, sample_ingestion):
        """Test the _discover_files helper method."""
        # Arrange
        db = Mock()
        service = RefreshService(db=db)

        # Mock FileDiscoveryService
        mock_discovery_service = Mock()
        mock_files = [
            FileMetadata(
                path="s3a://bucket/file1.json",
                size=1024 * 1024,  # 1MB
                modified_at=datetime.now(timezone.utc),
                etag="etag1"
            )
        ]
        mock_discovery_service.discover_files_from_path.return_value = mock_files

        # Patch FileDiscoveryService instantiation
        with patch('app.services.refresh_service.FileDiscoveryService', return_value=mock_discovery_service):
            # Act
            files = service._discover_files(sample_ingestion)

        # Assert
        assert len(files) == 1
        assert files[0].path == "s3a://bucket/file1.json"
        assert files[0].size == 1024 * 1024

        # Verify discovery service was called correctly
        mock_discovery_service.discover_files_from_path.assert_called_once()

    def test_file_date_range_in_impact(self, sample_ingestion, mock_file_metadata):
        """Test that impact includes file date ranges."""
        # Arrange
        db = Mock()
        file_state_service = Mock()
        file_state_service.get_processed_files.return_value = set()

        # Mock: Files spanning multiple days
        discovered_files = mock_file_metadata(30, 100)

        service = RefreshService(db=db, file_state_service=file_state_service)

        # Patch file discovery
        with patch.object(service, '_discover_files', return_value=discovered_files):
            # Act
            impact = service._estimate_full_refresh_impact(sample_ingestion)

        # Assert - should include date range info
        assert "oldest_file_date" in impact
        assert "newest_file_date" in impact
        assert impact["oldest_file_date"] is not None
        assert impact["newest_file_date"] is not None

    def test_empty_bucket(self, sample_ingestion):
        """Test refresh impact when bucket is empty."""
        # Arrange
        db = Mock()
        file_state_service = Mock()
        file_state_service.get_processed_files.return_value = set()

        service = RefreshService(db=db, file_state_service=file_state_service)

        # Patch file discovery to return empty list
        with patch.object(service, '_discover_files', return_value=[]):
            # Act
            impact = service._estimate_full_refresh_impact(sample_ingestion)

        # Assert
        assert impact["files_to_process"] == 0
        assert impact["estimated_data_size_gb"] == 0.0
        assert impact["estimated_cost_usd"] == 0.0
        assert impact["oldest_file_date"] is None
        assert impact["newest_file_date"] is None

    def test_azure_blob_discovery_not_yet_supported(self):
        """Test that Azure Blob discovery is not yet supported."""
        # Arrange
        db = Mock()
        service = RefreshService(db=db)

        ingestion = Mock()
        ingestion.source_type = "azure_blob"
        ingestion.source_path = "https://account.blob.core.windows.net/container/path"
        ingestion.source_credentials = {}

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported source type"):
            service._discover_files(ingestion)

    def test_gcs_discovery_not_yet_supported(self):
        """Test that GCS discovery is not yet supported."""
        # Arrange
        db = Mock()
        service = RefreshService(db=db)

        ingestion = Mock()
        ingestion.source_type = "gcs"
        ingestion.source_path = "gs://bucket/path"
        ingestion.source_credentials = {}

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported source type"):
            service._discover_files(ingestion)
