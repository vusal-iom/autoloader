"""
Schema version history API endpoints.

Provides endpoints to view schema evolution history for ingestions.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from app.database import get_db
from app.repositories.schema_version_repository import SchemaVersionRepository
from app.repositories.ingestion_repository import IngestionRepository
from app.models.schemas import SchemaVersionResponse, SchemaVersionListResponse

router = APIRouter()


@router.get("/ingestions/{ingestion_id}/schema-versions", response_model=SchemaVersionListResponse)
async def get_schema_versions(
    ingestion_id: str,
    db: Session = Depends(get_db)
):
    """
    Get schema version history for an ingestion.

    Returns all schema versions in chronological order, showing how the schema
    evolved over time due to new files being processed.

    Args:
        ingestion_id: UUID of the ingestion
        db: Database session

    Returns:
        SchemaVersionListResponse with all versions and current version number

    Raises:
        404: Ingestion not found
    """
    # Verify ingestion exists
    ingestion_repo = IngestionRepository(db)
    ingestion = ingestion_repo.get_by_id(ingestion_id)

    if not ingestion:
        raise HTTPException(status_code=404, detail=f"Ingestion {ingestion_id} not found")

    # Get schema versions
    schema_repo = SchemaVersionRepository(db)
    versions = schema_repo.get_all_versions(ingestion_id)

    return SchemaVersionListResponse(
        versions=[SchemaVersionResponse.model_validate(v) for v in versions],
        total=len(versions),
        current_version=ingestion.schema_version
    )


@router.get("/ingestions/{ingestion_id}/schema-versions/latest", response_model=SchemaVersionResponse)
async def get_latest_schema_version(
    ingestion_id: str,
    db: Session = Depends(get_db)
):
    """
    Get the latest schema version for an ingestion.

    Args:
        ingestion_id: UUID of the ingestion
        db: Database session

    Returns:
        SchemaVersionResponse with latest version details

    Raises:
        404: Ingestion not found or no schema versions exist
    """
    # Verify ingestion exists
    ingestion_repo = IngestionRepository(db)
    ingestion = ingestion_repo.get_by_id(ingestion_id)

    if not ingestion:
        raise HTTPException(status_code=404, detail=f"Ingestion {ingestion_id} not found")

    # Get latest schema version
    schema_repo = SchemaVersionRepository(db)
    schema_version = schema_repo.get_latest_version(ingestion_id)

    if not schema_version:
        raise HTTPException(
            status_code=404,
            detail=f"No schema versions found for ingestion {ingestion_id}"
        )

    return SchemaVersionResponse.model_validate(schema_version)


@router.get("/ingestions/{ingestion_id}/schema-versions/{version}", response_model=SchemaVersionResponse)
async def get_schema_version_by_number(
    ingestion_id: str,
    version: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific schema version by version number.

    Args:
        ingestion_id: UUID of the ingestion
        version: Version number to retrieve
        db: Database session

    Returns:
        SchemaVersionResponse with version details

    Raises:
        404: Ingestion or version not found
    """
    # Verify ingestion exists
    ingestion_repo = IngestionRepository(db)
    ingestion = ingestion_repo.get_by_id(ingestion_id)

    if not ingestion:
        raise HTTPException(status_code=404, detail=f"Ingestion {ingestion_id} not found")

    # Get schema version
    schema_repo = SchemaVersionRepository(db)
    schema_version = schema_repo.get_version_by_number(ingestion_id, version)

    if not schema_version:
        raise HTTPException(
            status_code=404,
            detail=f"Schema version {version} not found for ingestion {ingestion_id}"
        )

    return SchemaVersionResponse.model_validate(schema_version)
