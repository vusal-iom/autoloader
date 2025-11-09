"""Ingestion service - business logic for ingestion operations."""
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
import logging
from datetime import datetime
from fastapi import HTTPException

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
from app.services.prefect_service import get_prefect_service
from app.config import get_settings, get_spark_connect_url

logger = logging.getLogger(__name__)
settings = get_settings()

# Constants
DEFAULT_TENANT_ID = "tenant_123"  # TODO: Get from auth
TRIGGER_TYPE_MANUAL = "manual"


class IngestionService:
    """Service for managing ingestion configurations."""

    def __init__(self, db: Session):
        self.db = db
        self.ingestion_repo = IngestionRepository(db)
        self.run_repo = RunRepository(db)
        self.spark_service = SparkService()
        self.cost_estimator = CostEstimator()

    @staticmethod
    async def _ensure_prefect_available(error_message: str):
        """
        Ensure Prefect service is available, raise HTTPException if not.

        Args:
            error_message: Custom error message to include in exception

        Raises:
            HTTPException: 503 if Prefect service is unavailable
        """
        prefect = await get_prefect_service()
        if not prefect.initialized:
            raise HTTPException(
                status_code=503,
                detail=f"{error_message}: Orchestration service unavailable"
            )
        return prefect

    def _generate_checkpoint_location(self, tenant_id: str, ingestion_id: str) -> str:
        """Generate checkpoint location path for an ingestion."""
        return f"{settings.checkpoint_base_path}{tenant_id}/{ingestion_id}/"

    def _build_ingestion_from_create(
        self, data: IngestionCreate, ingestion_id: str, user_id: str, checkpoint_location: str
    ) -> Ingestion:
        """Build Ingestion domain model from IngestionCreate schema."""
        return Ingestion(
            id=ingestion_id,
            tenant_id=DEFAULT_TENANT_ID,
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
            format_options=data.format.options.model_dump() if data.format.options else None,
            schema_inference=data.format.schema.inference if data.format.schema else "auto",
            schema_evolution_enabled=data.format.schema.evolution_enabled if data.format.schema else True,
            schema_json=data.format.schema.schema_json if data.format.schema else None,
            on_schema_change=data.on_schema_change.value,
            # Destination
            destination_catalog=data.destination.catalog,
            destination_database=data.destination.database,
            destination_table=data.destination.table,
            write_mode=data.destination.write_mode,
            partitioning_enabled=data.destination.partitioning.enabled if data.destination.partitioning else False,
            partitioning_columns=data.destination.partitioning.columns if data.destination.partitioning else None,
            z_ordering_enabled=data.destination.optimization.z_ordering_enabled if data.destination.optimization else False,
            z_ordering_columns=data.destination.optimization.z_ordering_columns if data.destination.optimization else None,
            # Schedule
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

    async def create_ingestion(self, data: IngestionCreate, user_id: str) -> IngestionResponse:
        """
        Create a new ingestion configuration.

        Steps:
        1. Generate IDs and paths
        2. Build ingestion model
        3. Estimate cost
        4. Save to database
        5. Create Prefect deployment if scheduling enabled
        """
        ingestion_id = f"ing_{uuid.uuid4().hex[:12]}"
        checkpoint_location = self._generate_checkpoint_location(DEFAULT_TENANT_ID, ingestion_id)

        # Build ingestion model
        ingestion = self._build_ingestion_from_create(data, ingestion_id, user_id, checkpoint_location)

        # Estimate cost
        cost = self.cost_estimator.estimate(data)
        ingestion.estimated_monthly_cost = cost.total_monthly

        # Save to database
        ingestion = self.ingestion_repo.create(ingestion)

        # Create Prefect deployment if scheduling enabled
        if ingestion.schedule_frequency:
            try:
                prefect = await self._ensure_prefect_available("Cannot create ingestion")
                deployment_id = await prefect.create_deployment(ingestion)

                # Store deployment ID
                ingestion.prefect_deployment_id = deployment_id
                self.db.commit()
                self.db.refresh(ingestion)

                logger.info(f"Created Prefect deployment for ingestion {ingestion_id}")
            except HTTPException:
                # Rollback ingestion creation
                self.ingestion_repo.delete(ingestion_id)
                raise

        return self._to_response(ingestion)

    def list_ingestions(self, tenant_id: str) -> List[IngestionResponse]:
        """List all ingestions for a tenant."""
        ingestions = self.ingestion_repo.get_by_tenant(tenant_id)
        return [self._to_response(ing) for ing in ingestions]

    def get_ingestion(self, ingestion_id: str) -> Optional[IngestionResponse]:
        """Get ingestion by ID."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        return self._to_response(ingestion) if ingestion else None

    async def update_ingestion(
        self, ingestion_id: str, updates: IngestionUpdate
    ) -> Optional[IngestionResponse]:
        """Update ingestion configuration."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return None

        schedule_changed = False

        # Update allowed fields
        if updates.status:
            ingestion.status = updates.status

        if updates.schedule:
            schedule_changed = True
            ingestion.schedule_frequency = updates.schedule.frequency
            ingestion.schedule_time = updates.schedule.time
            ingestion.schedule_cron = updates.schedule.cron_expression

        if updates.quality:
            ingestion.row_count_threshold = updates.quality.row_count_threshold
            ingestion.alerts_enabled = updates.quality.alerts_enabled

        if updates.alert_recipients:
            ingestion.alert_recipients = updates.alert_recipients

        ingestion = self.ingestion_repo.update(ingestion)

        # Update Prefect deployment schedule if changed
        if schedule_changed and ingestion.prefect_deployment_id:
            prefect = await self._ensure_prefect_available("Cannot update schedule")
            await prefect.update_deployment_schedule(
                deployment_id=ingestion.prefect_deployment_id,
                schedule_frequency=ingestion.schedule_frequency,
                schedule_time=ingestion.schedule_time,
                schedule_cron=ingestion.schedule_cron,
                timezone=ingestion.schedule_timezone or "UTC"
            )
            logger.info(f"Updated Prefect deployment schedule for ingestion {ingestion_id}")

        return self._to_response(ingestion)

    async def delete_ingestion(self, ingestion_id: str, delete_table: bool = False) -> bool:
        """Delete an ingestion."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return False

        # Delete Prefect deployment first
        if ingestion.prefect_deployment_id:
            try:
                prefect = await get_prefect_service()
                await prefect.delete_deployment(ingestion.prefect_deployment_id)
                logger.info(f"Deleted Prefect deployment for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"Failed to delete Prefect deployment: {e}")
                # Continue with ingestion deletion anyway

        # TODO: Cleanup checkpoints
        # TODO: Delete table if requested (delete_table parameter)

        return self.ingestion_repo.delete(ingestion_id)

    async def trigger_manual_run(self, ingestion_id: str) -> dict:
        """
        Trigger a manual ingestion run via Prefect.

        Args:
            ingestion_id: Ingestion ID

        Returns:
            Dict with flow_run_id and trigger status

        Raises:
            ValueError: If ingestion not found, not active, or has no deployment
            HTTPException: If Prefect service unavailable (503)
        """
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        if ingestion.status not in (IngestionStatus.ACTIVE, IngestionStatus.DRAFT):
            raise ValueError(f"Ingestion not active: {ingestion.status}")

        if not ingestion.prefect_deployment_id:
            raise ValueError(
                f"Ingestion {ingestion_id} does not have a deployment. "
                "Please configure a schedule to enable execution."
            )

        # Trigger via Prefect
        prefect = await self._ensure_prefect_available("Orchestration service unavailable. Please try again later")

        flow_run_id = await prefect.trigger_deployment(
            deployment_id=ingestion.prefect_deployment_id,
            parameters={"ingestion_id": str(ingestion_id), "trigger": TRIGGER_TYPE_MANUAL}
        )

        logger.info(f"Triggered Prefect flow run {flow_run_id} for ingestion {ingestion_id}")

        return {
            "status": "triggered",
            "method": "prefect",
            "flow_run_id": flow_run_id,
            "message": "Ingestion run triggered via Prefect"
        }

    async def pause_ingestion(self, ingestion_id: str) -> bool:
        """Pause an active ingestion (stops scheduler from triggering new runs)."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return False

        # Pause Prefect deployment
        if ingestion.prefect_deployment_id:
            prefect = await self._ensure_prefect_available("Cannot pause ingestion")
            await prefect.pause_deployment(ingestion.prefect_deployment_id)
            logger.info(f"Paused Prefect deployment for ingestion {ingestion_id}")

        result = self.ingestion_repo.update_status(ingestion_id, IngestionStatus.PAUSED)
        return result is not None

    async def resume_ingestion(self, ingestion_id: str) -> bool:
        """Resume a paused ingestion (re-registers with scheduler)."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return False

        # Resume Prefect deployment
        if ingestion.prefect_deployment_id:
            prefect = await self._ensure_prefect_available("Cannot resume ingestion")
            await prefect.resume_deployment(ingestion.prefect_deployment_id)
            logger.info(f"Resumed Prefect deployment for ingestion {ingestion_id}")

        result = self.ingestion_repo.update_status(ingestion_id, IngestionStatus.ACTIVE)
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

    @staticmethod
    def _to_response(ingestion: Ingestion) -> IngestionResponse:
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
            SchemaEvolutionStrategy,
        )

        return IngestionResponse(
            id=ingestion.id,
            tenant_id=ingestion.tenant_id,
            name=ingestion.name,
            status=ingestion.status,
            cluster_id=ingestion.cluster_id,
            spark_connect_url=get_spark_connect_url(ingestion.cluster_id),
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
            on_schema_change=SchemaEvolutionStrategy(getattr(ingestion, 'on_schema_change', 'ignore')),
            metadata=IngestionMetadata(
                checkpoint_location=ingestion.checkpoint_location,
                last_run_id=ingestion.last_run_id,
                last_run_time=ingestion.last_run_time,
                next_run_time=ingestion.next_run_time,
                schema_version=ingestion.schema_version,
                estimated_monthly_cost=ingestion.estimated_monthly_cost,
            ),
            prefect_deployment_id=ingestion.prefect_deployment_id,
            created_at=ingestion.created_at,
            updated_at=ingestion.updated_at,
            created_by=ingestion.created_by,
        )
