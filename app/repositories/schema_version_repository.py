"""Schema version repository for tracking schema changes."""

from typing import List, Optional
from sqlalchemy.orm import Session
from app.models.domain import SchemaVersion
import uuid
import json
import logging

logger = logging.getLogger(__name__)


class SchemaVersionRepository:
    """Repository for schema version operations."""

    def __init__(self, db: Session):
        self.db = db

    def create_version(
        self,
        ingestion_id: str,
        version: int,
        schema_json: dict,
        changes: List[dict],
        strategy_applied: str,
        affected_files: Optional[List[str]] = None
    ) -> SchemaVersion:
        """
        Create a new schema version record.

        Args:
            ingestion_id: ID of the ingestion
            version: Schema version number
            schema_json: Full schema as JSON
            changes: List of schema changes
            strategy_applied: Which strategy was used
            affected_files: Files that triggered the change

        Returns:
            Created SchemaVersion object
        """
        schema_version = SchemaVersion(
            id=str(uuid.uuid4()),
            ingestion_id=ingestion_id,
            version=version,
            schema_json=schema_json,
            changes_json=changes,
            strategy_applied=strategy_applied,
            affected_files=affected_files or []
        )

        self.db.add(schema_version)
        self.db.commit()
        self.db.refresh(schema_version)

        logger.info(f"Created schema version {version} for ingestion {ingestion_id}")
        return schema_version

    def get_latest_version(self, ingestion_id: str) -> Optional[SchemaVersion]:
        """
        Get the latest schema version for an ingestion.

        Args:
            ingestion_id: ID of the ingestion

        Returns:
            Latest SchemaVersion or None
        """
        return self.db.query(SchemaVersion) \
            .filter(SchemaVersion.ingestion_id == ingestion_id) \
            .order_by(SchemaVersion.version.desc()) \
            .first()

    def get_all_versions(self, ingestion_id: str) -> List[SchemaVersion]:
        """
        Get all schema versions for an ingestion.

        Args:
            ingestion_id: ID of the ingestion

        Returns:
            List of SchemaVersion objects ordered by version
        """
        return self.db.query(SchemaVersion) \
            .filter(SchemaVersion.ingestion_id == ingestion_id) \
            .order_by(SchemaVersion.version.asc()) \
            .all()

    def get_version_by_number(self, ingestion_id: str, version: int) -> Optional[SchemaVersion]:
        """
        Get a specific schema version.

        Args:
            ingestion_id: ID of the ingestion
            version: Version number

        Returns:
            SchemaVersion or None
        """
        return self.db.query(SchemaVersion) \
            .filter(
                SchemaVersion.ingestion_id == ingestion_id,
                SchemaVersion.version == version
            ) \
            .first()

    def get_next_version_number(self, ingestion_id: str) -> int:
        """
        Get the next version number for an ingestion.

        Args:
            ingestion_id: ID of the ingestion

        Returns:
            Next version number (starting from 1)
        """
        latest = self.get_latest_version(ingestion_id)
        return (latest.version + 1) if latest else 1
