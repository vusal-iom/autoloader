"""
Run record management tasks for Prefect flows.
"""
from prefect import task, get_run_logger
from typing import Dict
from datetime import datetime
import uuid

from app.database import SessionLocal
from app.repositories.run_repository import RunRepository
from app.repositories.ingestion_repository import IngestionRepository
from app.models.domain import Run, RunStatus, ProcessedFile
from sqlalchemy import func


@task(
    name="create_run_record",
    retries=1,
    tags=["database"]
)
def create_run_record_task(ingestion_id: str, trigger: str = "scheduled") -> str:
    """
    Create Run record with status=RUNNING.

    Args:
        ingestion_id: UUID of the ingestion
        trigger: Trigger type (scheduled, manual, retry)

    Returns:
        Run ID (UUID string)
    """
    logger = get_run_logger()

    db = SessionLocal()
    try:
        # Get ingestion to fetch cluster_id
        ingestion_repo = IngestionRepository(db)
        ingestion = ingestion_repo.get_by_id(ingestion_id)

        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        run = Run(
            id=str(uuid.uuid4()),
            ingestion_id=ingestion_id,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow(),
            trigger=trigger,
            cluster_id=ingestion.cluster_id
        )

        run_repo = RunRepository(db)
        created_run = run_repo.create(run)
        db.commit()

        logger.info(f"Created run record: {created_run.id}")
        return created_run.id

    finally:
        db.close()


@task(
    name="complete_run_record",
    retries=2,
    tags=["database"]
)
def complete_run_record_task(run_id: str, metrics: Dict[str, int]):
    """
    Update Run record with final status and metrics.

    Args:
        run_id: UUID of the run
        metrics: Metrics dict from processing
    """
    logger = get_run_logger()

    db = SessionLocal()
    try:
        run_repo = RunRepository(db)
        run = run_repo.get_run(run_id)

        if not run:
            raise ValueError(f"Run not found: {run_id}")

        success_count = metrics.get('success', 0)
        failed_count = metrics.get('failed', 0)
        status = RunStatus.SUCCESS.value
        if failed_count > 0 and success_count > 0:
            status = RunStatus.PARTIAL.value
        elif failed_count > 0 and success_count == 0:
            status = RunStatus.FAILED.value

        # Update status and timing
        run.status = status
        run.ended_at = datetime.utcnow()
        run.files_processed = success_count + failed_count

        # Calculate duration
        if run.started_at:
            duration = (run.ended_at - run.started_at).total_seconds()
            run.duration_seconds = int(duration)

        # Aggregate metrics from ProcessedFile table
        result = db.query(
            func.sum(ProcessedFile.records_ingested).label('total_records'),
            func.sum(ProcessedFile.bytes_read).label('total_bytes')
        ).filter(
            ProcessedFile.run_id == run_id,
            ProcessedFile.status == 'SUCCESS'
        ).first()

        run.records_ingested = int(result.total_records) if result.total_records else 0
        run.bytes_read = int(result.total_bytes) if result.total_bytes else 0

        # Store error summary for UI visibility when applicable
        errors_list = metrics.get('errors') or []
        if failed_count > 0:
            run.errors = {
                "failed_files": failed_count,
                "sample": errors_list[:5],
                "counts": _count_by_category(errors_list)
            }

        db.commit()

        logger.info(f"Run completed: {run_id} - {run.records_ingested} records")

    finally:
        db.close()


def _count_by_category(errors: list) -> dict:
    """Aggregate errors by category for quick UI summaries."""
    counts = {}
    for err in errors:
        category = (err.get('error_type') if isinstance(err, dict) else None) or "unknown"
        counts[category] = counts.get(category, 0) + 1
    return counts


@task(
    name="fail_run_record",
    retries=2,
    tags=["database"]
)
def fail_run_record_task(run_id: str, error_message: str):
    """
    Mark Run record as failed with error message.

    Args:
        run_id: UUID of the run
        error_message: Error message
    """
    logger = get_run_logger()

    db = SessionLocal()
    try:
        run_repo = RunRepository(db)
        run = run_repo.get_run(run_id)

        if not run:
            raise ValueError(f"Run not found: {run_id}")

        # Update status
        run.status = RunStatus.FAILED.value
        run.ended_at = datetime.utcnow()
        run.errors = [{"message": error_message, "timestamp": datetime.utcnow().isoformat()}]

        # Calculate duration
        if run.started_at:
            duration = (run.ended_at - run.started_at).total_seconds()
            run.duration_seconds = int(duration)

        db.commit()

        logger.error(f"Run failed: {run_id} - {error_message}")

    finally:
        db.close()
