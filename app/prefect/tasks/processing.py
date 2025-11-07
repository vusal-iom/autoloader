"""
File processing tasks for Prefect flows.
"""
from prefect import task, get_run_logger
from typing import List, Dict

from app.database import SessionLocal
from app.repositories.ingestion_repository import IngestionRepository
from app.services.file_state_service import FileStateService
from app.services.batch_file_processor import BatchFileProcessor
from app.spark.connect_client import SparkConnectClient
from app.config import get_spark_connect_credentials


@task(
    name="process_files",
    retries=2,
    retry_delay_seconds=120,
    timeout_seconds=3600,
    tags=["processing"]
)
def process_files_task(
    ingestion_id: str,
    run_id: str,
    files_to_process: List[Dict]
) -> Dict[str, int]:
    """
    Process files via Spark Connect.

    Args:
        ingestion_id: UUID of the ingestion
        run_id: UUID of the run
        files_to_process: List of file metadata dicts

    Returns:
        Metrics dict: {success: N, failed: N, skipped: N}
    """
    logger = get_run_logger()
    logger.info(f"Processing {len(files_to_process)} files")

    db = SessionLocal()
    try:
        # Get ingestion config
        repo = IngestionRepository(db)
        ingestion = repo.get_by_id(ingestion_id)

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Initialize services
        spark_url, spark_token = get_spark_connect_credentials(ingestion.cluster_id)
        spark_client = SparkConnectClient(
            connect_url=spark_url,
            token=spark_token
        )

        state_service = FileStateService(db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion)

        # Process files
        metrics = processor.process_files(files_to_process, run_id=run_id)

        logger.info(f"Processing complete: {metrics}")
        return metrics

    finally:
        db.close()
