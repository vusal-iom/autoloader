"""Ingestion service - business logic for ingestion operations."""
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
from datetime import datetime

from app.models.domain import Ingestion, IngestionStatus, Run, RunStatus
from app.models.schemas import (
    IngestionCreate,
    IngestionUpdate,
    IngestionResponse,
    PreviewResult,
    CostBreakdown,
    IngestionMetadata,
)
from app.repositories.ingestion_repository import IngestionRepository
from app.repositories.run_repository import RunRepository
from app.services.spark_service import SparkService
from app.services.cost_estimator import CostEstimator
from app.config import get_settings, get_spark_connect_url

settings = get_settings()


class IngestionService:
    """Service for managing ingestion configurations."""

    def __init__(self, db: Session):
        self.db = db
        self.ingestion_repo = IngestionRepository(db)
        self.run_repo = RunRepository(db)
        self.spark_service = SparkService()
        self.cost_estimator = CostEstimator()

    def create_ingestion(self, data: IngestionCreate, user_id: str) -> IngestionResponse:
        """
        Create a new ingestion configuration.

        Steps:
        1. Validate configuration
        2. Generate IDs and paths
        3. Test Spark Connect connectivity
        4. Save to database
        5. Schedule if needed
        """
        ingestion_id = f"ing_{uuid.uuid4().hex[:12]}"
        tenant_id = "tenant_123"  # TODO: Get from auth

        # Generate checkpoint location
        checkpoint_location = f"{settings.checkpoint_base_path}{tenant_id}/{ingestion_id}/"

        # Create domain model
        # Note: spark_connect_url and token are retrieved at runtime from cluster_id
        ingestion = Ingestion(
            id=ingestion_id,
            tenant_id=tenant_id,
            name=data.name,
            status=IngestionStatus.DRAFT,
            cluster_id=data.cluster_id,
            # Source
            source_type=data.source.type,
            source_path=data.source.path,
            source_file_pattern=data.source.file_pattern,
            source_credentials=data.source.credentials,
            # Format
            format_type=data.format.type,
            format_options=data.format.options.dict() if data.format.options else None,
            schema_inference=data.format.schema.inference if data.format.schema else "auto",
            schema_evolution_enabled=data.format.schema.evolution_enabled if data.format.schema else True,
            schema_json=data.format.schema.schema_json if data.format.schema else None,
            # Destination
            destination_catalog=data.destination.catalog,
            destination_database=data.destination.database,
            destination_table=data.destination.table,
            write_mode=data.destination.write_mode,
            partitioning_enabled=data.destination.partitioning.enabled if data.destination.partitioning else False,
            partitioning_columns=data.destination.partitioning.columns if data.destination.partitioning else None,
            z_ordering_enabled=data.destination.optimization.z_ordering_enabled if data.destination.optimization else False,
            z_ordering_columns=data.destination.optimization.z_ordering_columns if data.destination.optimization else None,
            # Schedule (batch mode only)
            schedule_frequency=data.schedule.frequency,
            schedule_time=data.schedule.time,
            schedule_timezone=data.schedule.timezone,
            schedule_cron=data.schedule.cron_expression,
            backfill_enabled=data.schedule.backfill.enabled if data.schedule.backfill else False,
            backfill_start_date=data.schedule.backfill.start_date if data.schedule.backfill else None,
            # Quality
            row_count_threshold=data.quality.row_count_threshold if data.quality else None,
            alerts_enabled=data.quality.alerts_enabled if data.quality else True,
            alert_recipients=data.quality.alert_recipients if data.quality else None,
            # Metadata
            checkpoint_location=checkpoint_location,
            created_by=user_id,
        )

        # Estimate cost
        cost = self.cost_estimator.estimate(data)
        ingestion.estimated_monthly_cost = cost.total_monthly

        # Save to database
        ingestion = self.ingestion_repo.create(ingestion)

        # Activate if not draft
        # TODO: Schedule job if scheduled mode

        return self._to_response(ingestion)

    def list_ingestions(self, tenant_id: str) -> List[IngestionResponse]:
        """List all ingestions for a tenant."""
        ingestions = self.ingestion_repo.get_by_tenant(tenant_id)
        return [self._to_response(ing) for ing in ingestions]

    def get_ingestion(self, ingestion_id: str) -> Optional[IngestionResponse]:
        """Get ingestion by ID."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        return self._to_response(ingestion) if ingestion else None

    def update_ingestion(
        self, ingestion_id: str, updates: IngestionUpdate
    ) -> Optional[IngestionResponse]:
        """Update ingestion configuration."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return None

        # Update allowed fields
        if updates.status:
            ingestion.status = updates.status
            # TODO: Handle status-specific logic (start/stop scheduler, etc.)

        if updates.schedule:
            ingestion.schedule_frequency = updates.schedule.frequency
            ingestion.schedule_time = updates.schedule.time
            ingestion.schedule_cron = updates.schedule.cron_expression

        if updates.quality:
            ingestion.row_count_threshold = updates.quality.row_count_threshold
            ingestion.alerts_enabled = updates.quality.alerts_enabled

        if updates.alert_recipients:
            ingestion.alert_recipients = updates.alert_recipients

        ingestion = self.ingestion_repo.update(ingestion)
        return self._to_response(ingestion)

    def delete_ingestion(self, ingestion_id: str, delete_table: bool = False) -> bool:
        """Delete an ingestion."""
        # TODO: Stop scheduled job if exists
        # TODO: Cleanup checkpoints
        # TODO: Delete table if requested

        return self.ingestion_repo.delete(ingestion_id)

    def trigger_manual_run(self, ingestion_id: str) -> str:
        """Trigger a manual ingestion run."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion {ingestion_id} not found")

        # Create run record
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        run = Run(
            id=run_id,
            ingestion_id=ingestion_id,
            started_at=datetime.utcnow(),
            status=RunStatus.RUNNING,
            trigger="manual",
            cluster_id=ingestion.cluster_id,
        )
        self.run_repo.create(run)

        # TODO: Execute ingestion asynchronously via Spark Connect
        # self.spark_service.execute_ingestion(ingestion, run_id)

        return run_id

    def pause_ingestion(self, ingestion_id: str) -> bool:
        """Pause an active ingestion (stops scheduler from triggering new runs)."""
        result = self.ingestion_repo.update_status(ingestion_id, IngestionStatus.PAUSED)
        # TODO: Unregister scheduled job from scheduler
        return result is not None

    def resume_ingestion(self, ingestion_id: str) -> bool:
        """Resume a paused ingestion (re-registers with scheduler)."""
        result = self.ingestion_repo.update_status(ingestion_id, IngestionStatus.ACTIVE)
        # TODO: Re-register scheduled job with scheduler
        return result is not None

    def preview_ingestion(self, data: IngestionCreate) -> PreviewResult:
        """
        Preview ingestion without saving.

        Returns:
        - Inferred schema
        - Sample data
        - File count
        - Estimated records
        """
        # TODO: Use Spark Connect to preview
        return self.spark_service.preview(data)

    def estimate_cost(self, data: IngestionCreate) -> CostBreakdown:
        """Estimate monthly cost for ingestion."""
        return self.cost_estimator.estimate(data)

    def _to_response(self, ingestion: Ingestion) -> IngestionResponse:
        """Convert domain model to response schema."""
        from app.models.schemas import (
            SourceConfig,
            FormatConfig,
            FormatOptions,
            SchemaConfig,
            DestinationConfig,
            ScheduleConfig,
            BackfillConfig,
            QualityConfig,
            PartitioningConfig,
            OptimizationConfig,
        )

        return IngestionResponse(
            id=ingestion.id,
            tenant_id=ingestion.tenant_id,
            name=ingestion.name,
            status=ingestion.status,
            cluster_id=ingestion.cluster_id,
            spark_connect_url=get_spark_connect_url(ingestion.cluster_id),  # Computed dynamically
            source=SourceConfig(
                type=ingestion.source_type,
                path=ingestion.source_path,
                file_pattern=ingestion.source_file_pattern,
                credentials=ingestion.source_credentials,
            ),
            format=FormatConfig(
                type=ingestion.format_type,
                options=FormatOptions(**ingestion.format_options) if ingestion.format_options else None,
                schema=SchemaConfig(
                    inference=ingestion.schema_inference,
                    evolution_enabled=ingestion.schema_evolution_enabled,
                    schema_json=ingestion.schema_json,
                ),
            ),
            destination=DestinationConfig(
                catalog=ingestion.destination_catalog,
                database=ingestion.destination_database,
                table=ingestion.destination_table,
                write_mode=ingestion.write_mode,
                partitioning=PartitioningConfig(
                    enabled=ingestion.partitioning_enabled,
                    columns=ingestion.partitioning_columns or [],
                ),
                optimization=OptimizationConfig(
                    z_ordering_enabled=ingestion.z_ordering_enabled,
                    z_ordering_columns=ingestion.z_ordering_columns or [],
                ),
            ),
            schedule=ScheduleConfig(
                frequency=ingestion.schedule_frequency,
                time=ingestion.schedule_time,
                timezone=ingestion.schedule_timezone,
                cron_expression=ingestion.schedule_cron,
                backfill=BackfillConfig(
                    enabled=ingestion.backfill_enabled,
                    start_date=ingestion.backfill_start_date,
                ),
            ),
            quality=QualityConfig(
                row_count_threshold=ingestion.row_count_threshold,
                alerts_enabled=ingestion.alerts_enabled,
                alert_recipients=ingestion.alert_recipients or [],
            ),
            metadata=IngestionMetadata(
                checkpoint_location=ingestion.checkpoint_location,
                last_run_id=ingestion.last_run_id,
                last_run_time=ingestion.last_run_time,
                next_run_time=ingestion.next_run_time,
                schema_version=ingestion.schema_version,
                estimated_monthly_cost=ingestion.estimated_monthly_cost,
            ),
            created_at=ingestion.created_at,
            updated_at=ingestion.updated_at,
            created_by=ingestion.created_by,
        )
