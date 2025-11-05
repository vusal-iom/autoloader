"""
Batch Orchestrator - Coordinates batch processing workflow

Entry point for all batch ingestion runs.
"""

from typing import Dict
from sqlalchemy.orm import Session
from app.models.domain import Ingestion, Run, RunStatus
from app.services.file_discovery_service import FileDiscoveryService
from app.services.file_state_service import FileStateService
from app.services.batch_file_processor import BatchFileProcessor
from app.spark.connect_client import SparkConnectClient
from app.repositories.run_repository import RunRepository
from app.config import get_spark_connect_credentials
from datetime import datetime
import json
import logging
import uuid

logger = logging.getLogger(__name__)


class BatchOrchestrator:
    """
    Orchestrates batch processing workflow.

    Flow:
    1. Create Run record
    2. Discover files from S3
    3. Check processed files in PostgreSQL
    4. Compute new files
    5. Process files
    6. Update Run record
    """

    def __init__(self, db: Session):
        self.db = db
        self.run_repo = RunRepository(db)

    def run_ingestion(self, ingestion: Ingestion) -> Run:
        """
        Execute batch ingestion for an ingestion configuration.

        Args:
            ingestion: Ingestion configuration

        Returns:
            Run record with metrics
        """
        logger.info(f"Starting batch ingestion run for: {ingestion.name} ({ingestion.id})")

        # Step 1: Create Run record
        run = self._create_run(ingestion)

        try:
            # Step 2: Initialize services
            discovery_service = self._init_discovery_service(ingestion)
            state_service = FileStateService(self.db)
            spark_client = self._init_spark_client(ingestion)
            processor = BatchFileProcessor(spark_client, state_service, ingestion)

            # Step 3: Discover files from S3
            logger.info(f"Discovering files from {ingestion.source_path}")
            discovered_files = self._discover_files(discovery_service, ingestion)
            logger.info(f"Discovered {len(discovered_files)} files")

            # Step 4: Get already-processed files
            processed_file_paths = state_service.get_processed_files(
                ingestion_id=ingestion.id,
                statuses=["SUCCESS", "SKIPPED"]
            )
            logger.info(f"Already processed: {len(processed_file_paths)} files")

            # Step 5: Compute new files
            new_files = [
                f for f in discovered_files
                if f.path not in processed_file_paths
            ]
            logger.info(f"New files to process: {len(new_files)}")

            if len(new_files) == 0:
                logger.info("No new files to process")
                self._complete_run(run, metrics={'success': 0, 'failed': 0, 'skipped': 0})
                return run

            # Step 6: Process files
            # Convert FileMetadata to dict
            file_dicts = [f.to_dict() for f in new_files]
            metrics = processor.process_files(file_dicts, run_id=run.id)

            # Step 7: Update Run record
            self._complete_run(run, metrics)

            logger.info(f"Batch ingestion run complete: {run.id} - {metrics}")
            return run

        except Exception as e:
            logger.error(f"Batch ingestion run failed: {e}", exc_info=True)
            self._fail_run(run, str(e))
            raise

    def _create_run(self, ingestion: Ingestion) -> Run:
        """Create new Run record with status=RUNNING"""
        run = Run(
            id=str(uuid.uuid4()),
            ingestion_id=ingestion.id,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
            trigger="manual",  # Phase 1: manual only
            cluster_id=ingestion.cluster_id
        )
        return self.run_repo.create(run)

    def _init_discovery_service(self, ingestion: Ingestion) -> FileDiscoveryService:
        """Initialize FileDiscoveryService from ingestion config"""
        credentials = json.loads(ingestion.source_credentials) if isinstance(ingestion.source_credentials, str) else ingestion.source_credentials

        return FileDiscoveryService(
            source_type=ingestion.source_type,
            credentials=credentials,
            region=credentials.get('aws_region', 'us-east-1')
        )

    def _init_spark_client(self, ingestion: Ingestion) -> SparkConnectClient:
        """Initialize Spark Connect client"""
        # Get Spark Connect URL and token from cluster config
        spark_url, spark_token = get_spark_connect_credentials(ingestion.cluster_id)

        return SparkConnectClient(
            connect_url=spark_url,
            token=spark_token
        )

    def _discover_files(
        self,
        discovery_service: FileDiscoveryService,
        ingestion: Ingestion
    ) -> list:
        """
        Discover files from source.

        Args:
            discovery_service: FileDiscoveryService instance
            ingestion: Ingestion configuration

        Returns:
            List of FileMetadata objects
        """
        # Parse bucket and prefix from source_path
        # Example: s3://my-bucket/data/events/ → bucket=my-bucket, prefix=data/events/
        # Also support s3a:// format used by Spark
        source_path = ingestion.source_path

        if source_path.startswith("s3://"):
            parts = source_path.replace("s3://", "").split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
        elif source_path.startswith("s3a://"):
            parts = source_path.replace("s3a://", "").split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
        else:
            raise ValueError(f"Invalid source_path format: {source_path}")

        # List files
        files = discovery_service.list_files(
            bucket=bucket,
            prefix=prefix,
            pattern=ingestion.source_file_pattern,  # e.g., "*.json"
            max_files=None  # No limit in Phase 1
        )

        return files

    def _complete_run(self, run: Run, metrics: Dict):
        """
        Complete run with success status and metrics.

        Args:
            run: Run record
            metrics: Metrics dict from BatchFileProcessor
        """
        run.status = RunStatus.SUCCESS.value
        run.ended_at = datetime.utcnow()
        run.files_processed = metrics['success'] + metrics['failed']

        # Duration
        if run.started_at:
            duration = (run.ended_at - run.started_at).total_seconds()
            run.duration_seconds = int(duration)

        # Note: records_ingested, bytes_read will be aggregated from processed_files table
        # For Phase 1, we can set them to 0 or aggregate in a separate query

        self.run_repo.update(run)
        logger.info(f"Run completed: {run.id}")

    def _fail_run(self, run: Run, error_message: str):
        """
        Mark run as failed with error message.

        Args:
            run: Run record
            error_message: Error message
        """
        run.status = RunStatus.FAILED.value
        run.ended_at = datetime.utcnow()
        run.errors = [{"message": error_message, "timestamp": datetime.utcnow().isoformat()}]

        if run.started_at:
            duration = (run.ended_at - run.started_at).total_seconds()
            run.duration_seconds = int(duration)

        self.run_repo.update(run)
        logger.error(f"Run failed: {run.id} - {error_message}")
