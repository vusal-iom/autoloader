"""Ingestion repository for database operations."""
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from app.models.domain import Ingestion, IngestionStatus


class IngestionRepository:
    """Repository for ingestion CRUD operations."""

    def __init__(self, db: Session):
        self.db = db

    def create(self, ingestion: Ingestion) -> Ingestion:
        """Create a new ingestion."""
        self.db.add(ingestion)
        self.db.commit()
        self.db.refresh(ingestion)
        return ingestion

    def get_by_id(self, ingestion_id: str) -> Optional[Ingestion]:
        """Get ingestion by ID."""
        return self.db.query(Ingestion).filter(Ingestion.id == ingestion_id).first()

    def get_by_tenant(self, tenant_id: str) -> List[Ingestion]:
        """Get all ingestions for a tenant."""
        return self.db.query(Ingestion).filter(Ingestion.tenant_id == tenant_id).all()

    def get_active_ingestions(self) -> List[Ingestion]:
        """Get all active ingestions (for scheduler)."""
        return (
            self.db.query(Ingestion)
            .filter(Ingestion.status == IngestionStatus.ACTIVE)
            .all()
        )

    def update(self, ingestion: Ingestion) -> Ingestion:
        """Update an ingestion."""
        ingestion.updated_at = datetime.utcnow()
        self.db.commit()
        self.db.refresh(ingestion)
        return ingestion

    def delete(self, ingestion_id: str) -> bool:
        """Delete an ingestion."""
        ingestion = self.get_by_id(ingestion_id)
        if ingestion:
            self.db.delete(ingestion)
            self.db.commit()
            return True
        return False

    def update_status(self, ingestion_id: str, status: IngestionStatus) -> Optional[Ingestion]:
        """Update ingestion status."""
        ingestion = self.get_by_id(ingestion_id)
        if ingestion:
            ingestion.status = status
            return self.update(ingestion)
        return None

    def update_last_run(
        self, ingestion_id: str, run_id: str, next_run_time: Optional[datetime] = None
    ) -> Optional[Ingestion]:
        """Update last run information."""
        ingestion = self.get_by_id(ingestion_id)
        if ingestion:
            ingestion.last_run_id = run_id
            ingestion.last_run_time = datetime.utcnow()
            if next_run_time:
                ingestion.next_run_time = next_run_time
            return self.update(ingestion)
        return None
