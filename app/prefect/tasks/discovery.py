"""
File discovery tasks for Prefect flows.
"""
from prefect import task, get_run_logger
from typing import List, Dict
import json

from app.database import SessionLocal
from app.repositories.ingestion_repository import IngestionRepository
from app.services.file_discovery_service import FileDiscoveryService


@task(
    name="discover_files",
    retries=2,
    retry_delay_seconds=5,
    tags=["discovery"]
)
def discover_files_task(ingestion_id: str) -> List[Dict]:
    """
    Discover new files from cloud storage.

    Args:
        ingestion_id: UUID of the ingestion

    Returns:
        List of file metadata dicts
    """
    logger = get_run_logger()
    logger.info(f"Discovering files for ingestion {ingestion_id}")

    db = SessionLocal()
    try:
        # Get ingestion config
        repo = IngestionRepository(db)
        ingestion = repo.get_by_id(ingestion_id)

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Initialize discovery service
        credentials = (
            json.loads(ingestion.source_credentials)
            if isinstance(ingestion.source_credentials, str)
            else ingestion.source_credentials
        )

        discovery = FileDiscoveryService(
            source_type=ingestion.source_type,
            credentials=credentials,
            region=credentials.get('aws_region', 'us-east-1')
        )

        # Discover files
        files = discovery.discover_files_from_path(
            source_path=ingestion.source_path,
            pattern=ingestion.source_file_pattern,
            max_files=None
        )

        logger.info(f"Discovered {len(files)} files")
        return [f.to_dict() for f in files]

    finally:
        db.close()
