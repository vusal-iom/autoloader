"""
Batch File Processor - Processes files with Spark batch API

Replaces streaming Auto Loader with explicit batch processing.
"""

from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum
from pyspark.sql import DataFrame
from sqlalchemy.orm import Session
from app.spark.connect_client import SparkConnectClient
from app.services.file_state_service import FileStateService
from app.repositories.schema_version_repository import SchemaVersionRepository
from app.models.domain import Ingestion, ProcessedFile
from app.services.spark_file_reader import SparkFileReader
from app.services.iceberg_table_writer import IcebergTableWriter
from app.services.file_processing_errors import FileProcessingError, FileErrorCategory, wrap_error
import logging
import json

logger = logging.getLogger(__name__)


class BatchFileProcessor:
    """
    Processes files in batches with PostgreSQL state tracking.

    Phase 1: Sequential processing
    Phase 4: Parallel processing with ThreadPool
    """

    def __init__(
        self,
        spark_client: SparkConnectClient,
        state_service: FileStateService,
        ingestion: Ingestion,
        db: Session,
        schema_version_repo: Optional[SchemaVersionRepository] = None
    ):
        self.spark = spark_client
        self.state = state_service
        self.ingestion = ingestion
        self.db = db
        self.schema_version_repo = schema_version_repo or SchemaVersionRepository(db)
        
        # Initialize helpers
        self.reader = SparkFileReader(spark_client)
        self.writer = IcebergTableWriter(spark_client, self.schema_version_repo, db)

    def process_files(
        self,
        files: List[Dict],
        run_id: str
    ) -> Dict[str, int]:
        """
        Process list of files.

        Args:
            files: List of file metadata dicts from FileDiscoveryService
            run_id: ID of the current run for tracking

        Returns:
            Metrics dict: {success: N, failed: N, skipped: N, errors: [ {file_path, error_type, retryable, message, internal_error, stage} ]}
        """
        metrics = {'success': 0, 'failed': 0, 'skipped': 0, 'errors': []}

        logger.info(f"Processing {len(files)} files for ingestion {self.ingestion.id}")

        for file_info in files:
            file_path = file_info['path']

            # Atomically lock file for processing
            file_record = self.state.lock_file_for_processing(
                ingestion_id=self.ingestion.id,
                file_path=file_path,
                run_id=run_id,
                file_metadata=file_info
            )

            if not file_record:
                # Another worker is processing or file already done
                logger.debug(f"Skipping {file_path} (locked or completed)")
                metrics['skipped'] += 1
                continue

            try:
                # Process single file
                logger.info(f"Processing file: {file_path}")
                result = self._process_single_file(file_path)

                # Mark success
                self.state.mark_file_success(
                    file_record,
                    records_ingested=result['record_count'],
                    bytes_read=file_info.get('size', 0)
                )
                metrics['success'] += 1
                logger.info(f"Successfully processed {file_path}: {result['record_count']} records")

            except FileProcessingError as e:
                # Mark failure from predictable error
                self.state.mark_file_failed(
                    file_record,
                    e,
                    message=e.user_message,
                    error_type=e.category.value,
                    internal_error=e.raw_error
                )
                metrics['failed'] += 1
                metrics['errors'].append({
                    'file_path': file_path,
                    'error_type': e.category.value,
                    'retryable': e.retryable,
                    'message': e.user_message,
                    'internal_error': e.raw_error
                })
                logger.error(f"Failed to process {file_path}: {e}", exc_info=True)

            except Exception as e:
                # Unexpected error -> wrap and handle
                wrapped = wrap_error(file_path, e)
                self.state.mark_file_failed(
                    file_record,
                    wrapped,
                    message=wrapped.user_message,
                    error_type=wrapped.category.value,
                    internal_error=wrapped.raw_error
                )
                metrics['failed'] += 1
                metrics['errors'].append({
                    'file_path': file_path,
                    'error_type': wrapped.category.value,
                    'retryable': wrapped.retryable,
                    'message': wrapped.user_message,
                    'internal_error': wrapped.raw_error
                })
                logger.error(f"Failed to process {file_path}: {e}", exc_info=True)

        logger.info(f"Batch complete: {metrics}")
        return metrics

    def _process_single_file(self, file_path: str) -> Dict:
        """
        Process a single file with Spark batch API.

        Args:
            file_path: Full S3 path (e.g., s3://bucket/path/file.json)

        Returns:
            Dict with processing results: {record_count: N}
        """
        # Step 1: Read file with Spark DataFrame batch API
        if self.ingestion.schema_json:
            # Use predefined schema
            df = self.reader.read_file_with_schema(file_path, self.ingestion)
        else:
            # Infer schema from file
            df = self.reader.read_file_infer_schema(file_path, self.ingestion)

        # Step 2: Get record count (for metrics)
        try:
            record_count = int(df.count())  # Convert to Python int (from numpy.int64)
        except Exception as e:
            raise wrap_error(file_path, e)

        if record_count == 0:
            logger.warning(f"File {file_path} is empty, skipping write")
            return {'record_count': 0}

        # Step 3: Write to Iceberg table (with schema tracking)
        try:
            self.writer.write(df, file_path, self.ingestion)
        except Exception as e:
             raise wrap_error(file_path, e)

        return {'record_count': record_count}
