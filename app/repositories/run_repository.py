"""Run repository for database operations."""
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from app.models.domain import Run, RunStatus


class RunRepository:
    """Repository for run CRUD operations."""

    def __init__(self, db: Session):
        self.db = db

    def create(self, run: Run) -> Run:
        """Create a new run."""
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def get_run(self, run_id: str) -> Optional[Run]:
        """Get run by ID."""
        return self.db.query(Run).filter(Run.id == run_id).first()

    def get_runs_by_ingestion(
        self, ingestion_id: str, since: datetime, limit: int = 100
    ) -> List[Run]:
        """Get runs for an ingestion since a specific time."""
        return (
            self.db.query(Run)
            .filter(Run.ingestion_id == ingestion_id)
            .filter(Run.started_at >= since)
            .order_by(Run.started_at.desc())
            .limit(limit)
            .all()
        )

    def update(self, run: Run) -> Run:
        """Update a run."""
        self.db.commit()
        self.db.refresh(run)
        return run

    def update_status(
        self,
        run_id: str,
        status: RunStatus,
        ended_at: Optional[datetime] = None,
    ) -> Optional[Run]:
        """Update run status."""
        run = self.get_run(run_id)
        if run:
            run.status = status
            if ended_at:
                run.ended_at = ended_at
                if run.started_at:
                    run.duration_seconds = int((ended_at - run.started_at).total_seconds())
            return self.update(run)
        return None

    def update_metrics(
        self,
        run_id: str,
        files_processed: int,
        records_ingested: int,
        bytes_read: int,
        bytes_written: int,
    ) -> Optional[Run]:
        """Update run metrics."""
        run = self.get_run(run_id)
        if run:
            run.files_processed = files_processed
            run.records_ingested = records_ingested
            run.bytes_read = bytes_read
            run.bytes_written = bytes_written
            return self.update(run)
        return None
