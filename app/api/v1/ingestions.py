"""Ingestion API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.database import get_db
from app.models.schemas import (
    IngestionCreate,
    IngestionUpdate,
    IngestionResponse,
    PreviewResult,
    CostBreakdown,
)
from app.services.ingestion_service import IngestionService
from app.services.file_state_service import FileStateService

router = APIRouter(prefix="/ingestions", tags=["ingestions"])


def get_ingestion_service(db: Session = Depends(get_db)) -> IngestionService:
    """Get ingestion service instance."""
    return IngestionService(db)


def get_file_state_service(db: Session = Depends(get_db)) -> FileStateService:
    """Get file state service instance."""
    return FileStateService(db)


@router.post("", response_model=IngestionResponse, status_code=status.HTTP_201_CREATED)
async def create_ingestion(
    ingestion: IngestionCreate,
    service: IngestionService = Depends(get_ingestion_service),
    current_user: str = "user_xyz",  # TODO: Add auth dependency
):
    """
    Create a new ingestion configuration.

    Steps:
    1. Validate configuration
    2. Generate checkpoint location
    3. Create Spark Connect session
    4. Save to database
    5. Schedule if needed
    """
    return service.create_ingestion(ingestion, current_user)


@router.get("", response_model=List[IngestionResponse])
async def list_ingestions(
    tenant_id: str = "tenant_123",  # TODO: Extract from auth
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    List all ingestion configurations for the current user/tenant.

    Query parameters:
    - status: Filter by status (active, paused, error, draft)
    - source_type: Filter by source type (s3, azure_blob, gcs)
    """
    return service.list_ingestions(tenant_id)


@router.get("/{ingestion_id}", response_model=IngestionResponse)
async def get_ingestion(
    ingestion_id: str,
    service: IngestionService = Depends(get_ingestion_service),
):
    """Get ingestion details by ID."""
    ingestion = service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ingestion {ingestion_id} not found",
        )
    return ingestion


@router.put("/{ingestion_id}", response_model=IngestionResponse)
async def update_ingestion(
    ingestion_id: str,
    updates: IngestionUpdate,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Update ingestion configuration.

    Allowed updates:
    - Schedule settings
    - Quality rules
    - Alert recipients

    Restricted (require recreation):
    - Source path
    - Target table
    - File format
    """
    ingestion = service.update_ingestion(ingestion_id, updates)
    if not ingestion:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ingestion {ingestion_id} not found",
        )
    return ingestion


@router.delete("/{ingestion_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ingestion(
    ingestion_id: str,
    delete_table: bool = False,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Delete ingestion configuration.

    Query parameters:
    - delete_table: Also delete the target table (default: false)
    """
    success = service.delete_ingestion(ingestion_id, delete_table)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ingestion {ingestion_id} not found",
        )


@router.post("/{ingestion_id}/run", status_code=status.HTTP_202_ACCEPTED)
async def trigger_run(
    ingestion_id: str,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Manually trigger an ingestion run.

    Returns:
        Run ID for tracking progress
    """
    run = service.trigger_manual_run(ingestion_id)
    return {"run_id": run.id, "status": "accepted"}


@router.post("/{ingestion_id}/pause", status_code=status.HTTP_200_OK)
async def pause_ingestion(
    ingestion_id: str,
    service: IngestionService = Depends(get_ingestion_service),
):
    """Pause active ingestion (preserves checkpoint state)."""
    success = service.pause_ingestion(ingestion_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ingestion {ingestion_id} not found",
        )
    return {"status": "paused"}


@router.post("/{ingestion_id}/resume", status_code=status.HTTP_200_OK)
async def resume_ingestion(
    ingestion_id: str,
    service: IngestionService = Depends(get_ingestion_service),
):
    """Resume paused ingestion."""
    success = service.resume_ingestion(ingestion_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ingestion {ingestion_id} not found",
        )
    return {"status": "active"}


@router.delete("/{ingestion_id}/processed-files", status_code=status.HTTP_200_OK)
async def clear_processed_files(
    ingestion_id: str,
    ingestion_service: IngestionService = Depends(get_ingestion_service),
    file_state_service: FileStateService = Depends(get_file_state_service),
):
    """
    Clear all processed file records for an ingestion.

    This allows reprocessing all files from scratch without deleting the ingestion configuration.

    Use cases:
    - Manual "overwrite mode": Delete table + call this endpoint to reprocess all files
    - Keep table but reprocess: Just call this endpoint to reprocess existing files
    - Data quality fixes: Clear history and reprocess with corrected logic

    Note: This does NOT delete the ingestion configuration or the target table.
    """
    # Verify ingestion exists
    ingestion = ingestion_service.get_ingestion(ingestion_id)
    if not ingestion:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ingestion {ingestion_id} not found",
        )

    # Clear processed files
    count = file_state_service.clear_processed_files(ingestion_id)

    return {
        "message": f"Cleared {count} processed file records",
        "files_cleared": count,
        "ingestion_id": ingestion_id,
    }


@router.post("/test", response_model=PreviewResult)
async def test_configuration(
    ingestion: IngestionCreate,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Test ingestion configuration without saving.

    Returns:
    - Inferred schema
    - Sample data (first 5 rows)
    - File count
    - Estimated record count
    """
    return service.preview_ingestion(ingestion)


@router.post("/estimate-cost", response_model=CostBreakdown)
async def estimate_cost(
    ingestion: IngestionCreate,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Estimate monthly cost for ingestion configuration.

    Returns breakdown:
    - Compute cost per run
    - Monthly compute cost
    - Storage cost
    - Discovery cost
    - Total monthly cost
    """
    return service.estimate_cost(ingestion)
