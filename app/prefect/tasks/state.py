"""
File state management tasks for Prefect flows.
"""
from prefect import task, get_run_logger
from typing import List
from uuid import UUID

from app.database import SessionLocal
from app.services.file_state_service import FileStateService


@task(
    name="check_file_state",
    retries=1,
    retry_delay_seconds=30,
    tags=["state"]
)
def check_file_state_task(ingestion_id: str) -> List[str]:
    """
    Get list of already-processed file paths.

    Args:
        ingestion_id: UUID of the ingestion

    Returns:
        List of processed file paths
    """
    logger = get_run_logger()

    db = SessionLocal()
    try:
        state_service = FileStateService(db)
        processed = state_service.get_processed_files(
            ingestion_id=UUID(ingestion_id),
            statuses=["SUCCESS", "SKIPPED"]
        )

        logger.info(f"Found {len(processed)} already-processed files")
        return list(processed)

    finally:
        db.close()
