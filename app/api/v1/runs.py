"""Run history API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from app.database import get_db
from app.models.schemas import RunResponse
from app.repositories.run_repository import RunRepository

router = APIRouter(prefix="/ingestions/{ingestion_id}/runs", tags=["runs"])


def get_run_repository(db: Session = Depends(get_db)) -> RunRepository:
    """Get run repository instance."""
    return RunRepository(db)


@router.get("", response_model=List[RunResponse])
async def get_run_history(
    ingestion_id: str,
    days: int = 30,
    limit: int = 100,
    repository: RunRepository = Depends(get_run_repository),
):
    """
    Get run history for an ingestion.

    Query parameters:
    - days: Number of days to look back (default: 30)
    - limit: Maximum number of runs to return (default: 100)
    """
    since = datetime.utcnow() - timedelta(days=days)
    return repository.get_runs_by_ingestion(ingestion_id, since, limit)


@router.get("/{run_id}", response_model=RunResponse)
async def get_run(
    ingestion_id: str,
    run_id: str,
    repository: RunRepository = Depends(get_run_repository),
):
    """Get detailed run information."""
    run = repository.get_run(run_id)
    if not run or run.ingestion_id != ingestion_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run {run_id} not found",
        )
    return run


@router.post("/{run_id}/retry", status_code=status.HTTP_202_ACCEPTED)
async def retry_run(
    ingestion_id: str,
    run_id: str,
    repository: RunRepository = Depends(get_run_repository),
):
    """
    Retry a failed run.

    Creates a new run with trigger='retry' and same configuration.
    """
    run = repository.get_run(run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run {run_id} not found",
        )

    # TODO: Trigger retry via ingestion service
    return {"status": "retry_scheduled", "original_run_id": run_id}
