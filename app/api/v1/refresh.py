"""Refresh operations API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.refresh_service import RefreshService
from app.models.schemas import RefreshRequest, RefreshOperationResponse
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingestions", tags=["Refresh Operations"])


def get_refresh_service(db: Session = Depends(get_db)) -> RefreshService:
    """Dependency injection for RefreshService."""
    return RefreshService(db)


@router.post(
    "/{ingestion_id}/refresh/full",
    response_model=RefreshOperationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Full Refresh",
    description="""
    Performs a full table refresh by dropping the table, clearing all processed
    file history, and reprocessing ALL source files from scratch.

    **Operations:**
    1. Drops the target table
    2. Clears all processed file history
    3. Triggers a new ingestion run
       - Reprocesses ALL source files

    **Use Cases:**
    - Data quality fixes requiring reprocessing of all historical files
    - Source files were corrected/updated retroactively
    - Complete snapshot refresh from all source files
    - Testing: replay entire ingestion from beginning

    **Safety:** Requires explicit confirmation. Supports dry-run mode.
    """
)
async def refresh_full(
    ingestion_id: str,
    request: RefreshRequest,
    refresh_service: RefreshService = Depends(get_refresh_service)
) -> Dict[str, Any]:
    """Full refresh: reprocess all files."""
    try:
        logger.info(f"Full refresh requested for ingestion {ingestion_id}, dry_run={request.dry_run}")
        result = refresh_service.refresh(
            ingestion_id=ingestion_id,
            confirm=request.confirm,
            mode="full",
            auto_run=request.auto_run,
            dry_run=request.dry_run
        )
        return result
    except ValueError as e:
        logger.warning(f"Validation error in full refresh: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Full refresh operation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Full refresh operation failed: {str(e)}"
        )


@router.post(
    "/{ingestion_id}/refresh/new-only",
    response_model=RefreshOperationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="New-Only Refresh",
    description="""
    Performs a new-only table refresh by dropping the table while keeping
    processed file history intact, then processing only NEW files.

    **Operations:**
    1. Drops the target table
    2. Keeps processed file history intact
    3. Triggers a new ingestion run
       - Only processes NEW files added since last run

    **Use Cases:**
    - Recover from corrupted table without reprocessing historical data
    - Change table structure/schema (force recreation)
    - Fresh table for new files only (skip historical data)
    - Testing: clean slate without expensive reprocessing

    **Safety:** Requires explicit confirmation. Supports dry-run mode.
    """
)
async def refresh_new_only(
    ingestion_id: str,
    request: RefreshRequest,
    refresh_service: RefreshService = Depends(get_refresh_service)
) -> Dict[str, Any]:
    """New-only refresh: process only new files."""
    try:
        logger.info(f"New-only refresh requested for ingestion {ingestion_id}, dry_run={request.dry_run}")
        result = refresh_service.refresh(
            ingestion_id=ingestion_id,
            confirm=request.confirm,
            mode="new_only",
            auto_run=request.auto_run,
            dry_run=request.dry_run
        )
        return result
    except ValueError as e:
        logger.warning(f"Validation error in new-only refresh: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"New-only refresh operation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"New-only refresh operation failed: {str(e)}"
        )
