"""
ProcessedFile Repository - Data access for file state tracking
"""

from typing import List, Set, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models.domain import ProcessedFile, ProcessedFileStatus
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class ProcessedFileRepository:
    """Repository for ProcessedFile data access"""

    def __init__(self, db: Session):
        self.db = db

    def create(self, processed_file: ProcessedFile) -> ProcessedFile:
        """
        Create new ProcessedFile record.

        Args:
            processed_file: ProcessedFile instance to create

        Returns:
            Created ProcessedFile with ID
        """
        self.db.add(processed_file)
        self.db.commit()
        self.db.refresh(processed_file)
        logger.debug(f"Created ProcessedFile: {processed_file.id}")
        return processed_file

    def get_by_id(self, file_id: str) -> Optional[ProcessedFile]:
        """Get ProcessedFile by ID"""
        return self.db.query(ProcessedFile).filter(ProcessedFile.id == file_id).first()

    def get_by_file_path(self, ingestion_id: str, file_path: str) -> Optional[ProcessedFile]:
        """
        Get ProcessedFile by ingestion_id and file_path.

        Args:
            ingestion_id: Ingestion ID
            file_path: Full file path (e.g., s3://bucket/path/file.json)

        Returns:
            ProcessedFile if exists, None otherwise
        """
        return self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.file_path == file_path
        ).first()

    def get_processed_file_paths(
        self,
        ingestion_id: str,
        statuses: Optional[List[str]] = None
    ) -> Set[str]:
        """
        Get set of file paths that have been processed.

        Args:
            ingestion_id: Ingestion ID
            statuses: List of statuses to include (default: SUCCESS, SKIPPED)

        Returns:
            Set of file paths
        """
        if statuses is None:
            statuses = [ProcessedFileStatus.SUCCESS.value, ProcessedFileStatus.SKIPPED.value]

        files = self.db.query(ProcessedFile.file_path).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status.in_(statuses)
        ).all()

        return {f.file_path for f in files}

    def get_failed_files(self, ingestion_id: str, max_retries: int = 3) -> List[ProcessedFile]:
        """
        Get failed files eligible for retry.

        Args:
            ingestion_id: Ingestion ID
            max_retries: Maximum retry count

        Returns:
            List of ProcessedFile records with status=FAILED and retry_count < max_retries
        """
        return self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status == ProcessedFileStatus.FAILED.value,
            ProcessedFile.retry_count < max_retries
        ).all()

    def get_stale_processing_files(
        self,
        ingestion_id: str,
        stale_threshold_hours: int = 1
    ) -> List[ProcessedFile]:
        """
        Get files stuck in PROCESSING state (likely crashed worker).

        Args:
            ingestion_id: Ingestion ID
            stale_threshold_hours: Hours before considering file stale

        Returns:
            List of ProcessedFile records stuck in PROCESSING
        """
        threshold = datetime.utcnow() - timedelta(hours=stale_threshold_hours)

        return self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status == ProcessedFileStatus.PROCESSING.value,
            ProcessedFile.processing_started_at < threshold
        ).all()

    def lock_file_for_processing(
        self,
        ingestion_id: str,
        file_path: str,
        run_id: str,
        file_metadata: Optional[dict] = None
    ) -> Optional[ProcessedFile]:
        """
        Atomically lock a file for processing using SELECT FOR UPDATE SKIP LOCKED.

        This method is safe for concurrent execution by multiple workers.

        Args:
            ingestion_id: Ingestion ID
            file_path: Full file path
            run_id: Current run ID
            file_metadata: Optional metadata (size, modified_at, etag)

        Returns:
            ProcessedFile record if lock acquired, None if already locked
        """
        # Try to get existing record with row-level lock
        file_record = self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.file_path == file_path
        ).with_for_update(skip_locked=True).first()

        if file_record:
            # Record exists - check status
            if file_record.status == ProcessedFileStatus.PROCESSING.value:
                # Another worker is processing this file
                logger.debug(f"File already being processed: {file_path}")
                return None

            if file_record.status == ProcessedFileStatus.SUCCESS.value:
                # Already successfully processed - check if file changed
                if file_metadata and self._file_changed(file_record, file_metadata):
                    # File modified since last processing - re-process
                    logger.info(f"File changed, re-processing: {file_path}")
                    file_record.status = ProcessedFileStatus.PROCESSING.value
                    file_record.retry_count += 1
                    file_record.run_id = run_id
                    file_record.processing_started_at = datetime.utcnow()
                    self._update_file_metadata(file_record, file_metadata)
                else:
                    # No changes, skip
                    logger.debug(f"File unchanged, skipping: {file_path}")
                    return None
            else:
                # FAILED or DISCOVERED - process/retry
                logger.info(f"Processing file (retry={file_record.retry_count}): {file_path}")
                file_record.status = ProcessedFileStatus.PROCESSING.value
                file_record.retry_count += 1
                file_record.run_id = run_id
                file_record.processing_started_at = datetime.utcnow()
                if file_metadata:
                    self._update_file_metadata(file_record, file_metadata)
        else:
            # Create new record
            logger.info(f"Discovered new file: {file_path}")
            file_record = ProcessedFile(
                ingestion_id=ingestion_id,
                run_id=run_id,
                file_path=file_path,
                status=ProcessedFileStatus.PROCESSING.value,
                discovered_at=datetime.utcnow(),
                processing_started_at=datetime.utcnow(),
                retry_count=0
            )
            if file_metadata:
                self._update_file_metadata(file_record, file_metadata)

            self.db.add(file_record)

        self.db.commit()
        self.db.refresh(file_record)
        return file_record

    def mark_success(
        self,
        file_record: ProcessedFile,
        records_ingested: int,
        bytes_read: int
    ):
        """
        Mark file as successfully processed.

        Args:
            file_record: ProcessedFile instance
            records_ingested: Number of records ingested
            bytes_read: Bytes read from file
        """
        from datetime import timezone

        file_record.status = ProcessedFileStatus.SUCCESS.value
        now = datetime.now(timezone.utc)
        file_record.processed_at = now
        file_record.records_ingested = records_ingested
        file_record.bytes_read = bytes_read

        if file_record.processing_started_at:
            duration = (now - file_record.processing_started_at).total_seconds()
            file_record.processing_duration_ms = int(duration * 1000)

        file_record.error_message = None
        file_record.error_type = None

        self.db.commit()
        logger.info(f"Marked file SUCCESS: {file_record.file_path} ({records_ingested} records)")

    def mark_failed(self, file_record: ProcessedFile, error: Exception):
        """
        Mark file as failed with error details.

        Args:
            file_record: ProcessedFile instance
            error: Exception that caused failure
        """
        from datetime import timezone

        file_record.status = ProcessedFileStatus.FAILED.value
        file_record.error_message = str(error)
        file_record.error_type = type(error).__name__
        now = datetime.now(timezone.utc)
        file_record.processed_at = now

        if file_record.processing_started_at:
            duration = (now - file_record.processing_started_at).total_seconds()
            file_record.processing_duration_ms = int(duration * 1000)

        self.db.commit()
        logger.error(f"Marked file FAILED: {file_record.file_path} - {error}")

    def mark_skipped(self, file_record: ProcessedFile, reason: str):
        """
        Mark file as skipped (e.g., too large, schema mismatch).

        Args:
            file_record: ProcessedFile instance
            reason: Reason for skipping
        """
        file_record.status = ProcessedFileStatus.SKIPPED.value
        file_record.error_message = reason
        file_record.processed_at = datetime.utcnow()

        self.db.commit()
        logger.warning(f"Marked file SKIPPED: {file_record.file_path} - {reason}")

    def update(self, file_record: ProcessedFile) -> ProcessedFile:
        """Update ProcessedFile record"""
        self.db.commit()
        self.db.refresh(file_record)
        return file_record

    def delete_by_ingestion(self, ingestion_id: str) -> int:
        """
        Delete all ProcessedFile records for a specific ingestion.

        This allows users to clear file tracking history and reprocess files.
        Use case: Manual "overwrite mode" by deleting table + clearing processed files.

        Args:
            ingestion_id: Ingestion ID to clear files for

        Returns:
            Number of records deleted
        """
        count = self.db.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion_id
        ).delete()
        self.db.commit()
        logger.info(f"Deleted {count} ProcessedFile records for ingestion {ingestion_id}")
        return count

    def _file_changed(self, record: ProcessedFile, metadata: dict) -> bool:
        """
        Check if file has been modified since last processing.

        Args:
            record: Existing ProcessedFile record
            metadata: New metadata from file discovery

        Returns:
            True if file changed, False otherwise
        """
        # Compare ETag (most reliable)
        if metadata.get('etag') and record.file_etag:
            return metadata['etag'] != record.file_etag

        # Fallback to modification time
        if metadata.get('modified_at') and record.file_modified_at:
            return metadata['modified_at'] > record.file_modified_at

        # If no metadata to compare, assume not changed
        return False

    def _update_file_metadata(self, record: ProcessedFile, metadata: dict):
        """
        Update file record with metadata from cloud storage.

        Args:
            record: ProcessedFile instance
            metadata: Metadata dict with size, modified_at, etag
        """
        if 'size' in metadata:
            record.file_size_bytes = metadata['size']
        if 'modified_at' in metadata:
            record.file_modified_at = metadata['modified_at']
        if 'etag' in metadata:
            record.file_etag = metadata['etag']
