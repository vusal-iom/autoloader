"""
File State Service - Business logic for file state management
"""

from typing import Set, List, Optional
from app.repositories.processed_file_repository import ProcessedFileRepository
from app.models.domain import ProcessedFile
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)


class FileStateService:
    """Service for managing file processing state"""

    def __init__(self, db: Session):
        self.db = db
        self.repo = ProcessedFileRepository(db)

    def get_processed_files(
        self,
        ingestion_id: str,
        statuses: Optional[List[str]] = None
    ) -> Set[str]:
        """
        Get set of processed file paths for an ingestion.

        Args:
            ingestion_id: Ingestion ID
            statuses: Filter by statuses (default: SUCCESS, SKIPPED)

        Returns:
            Set of file paths that have been processed
        """
        return self.repo.get_processed_file_paths(ingestion_id, statuses)

    def get_failed_files(self, ingestion_id: str, max_retries: int = 3) -> List[ProcessedFile]:
        """
        Get failed files eligible for retry.

        Args:
            ingestion_id: Ingestion ID
            max_retries: Maximum retry count

        Returns:
            List of ProcessedFile records
        """
        return self.repo.get_failed_files(ingestion_id, max_retries)

    def lock_file_for_processing(
        self,
        ingestion_id: str,
        file_path: str,
        run_id: str,
        file_metadata: Optional[dict] = None
    ) -> Optional[ProcessedFile]:
        """
        Atomically lock a file for processing.

        Safe for concurrent execution by multiple workers.

        Args:
            ingestion_id: Ingestion ID
            file_path: Full file path
            run_id: Current run ID
            file_metadata: Optional metadata (size, modified_at, etag)

        Returns:
            ProcessedFile if lock acquired, None if already locked
        """
        return self.repo.lock_file_for_processing(
            ingestion_id, file_path, run_id, file_metadata
        )

    def mark_file_success(
        self,
        file_record: ProcessedFile,
        records_ingested: int,
        bytes_read: int
    ):
        """Mark file as successfully processed"""
        self.repo.mark_success(file_record, records_ingested, bytes_read)

    def mark_file_failed(
        self,
        file_record: ProcessedFile,
        error: Exception,
        message: Optional[str] = None,
        error_type: Optional[str] = None,
        internal_error: Optional[str] = None
    ):
        """Mark file as failed with error details"""
        self.repo.mark_failed(
            file_record,
            error,
            message=message,
            error_type=error_type,
            internal_error=internal_error
        )

    def mark_file_skipped(self, file_record: ProcessedFile, reason: str):
        """Mark file as skipped (e.g., too large, schema mismatch)"""
        self.repo.mark_skipped(file_record, reason)

    def clear_processed_files(self, ingestion_id: str) -> int:
        """
        Clear all processed file records for an ingestion.

        This allows users to reprocess all files from scratch.
        Use case: Manual "overwrite mode" by deleting table + clearing processed files.

        Args:
            ingestion_id: Ingestion ID to clear files for

        Returns:
            Number of records deleted
        """
        count = self.repo.delete_by_ingestion(ingestion_id)
        logger.info(f"Cleared {count} processed files for ingestion {ingestion_id}")
        return count
