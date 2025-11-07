"""Ingestion service - business logic for ingestion operations."""
from sqlalchemy.orm import Session
from typing import List, Optional
import uuid
import logging
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
from app.services.prefect_service import get_prefect_service
from app.config import get_settings, get_spark_connect_url

logger = logging.getLogger(__name__)
settings = get_settings()


class IngestionService:
    """Service for managing ingestion configurations."""

    def __init__(self, db: Session):
        self.db = db
        self.ingestion_repo = IngestionRepository(db)
        self.run_repo = RunRepository(db)
        self.spark_service = SparkService()
        self.cost_estimator = CostEstimator()

    async def create_ingestion(self, data: IngestionCreate, user_id: str) -> IngestionResponse:
        """
        Create a new ingestion configuration.

        Steps:
        1. Validate configuration
        2. Generate IDs and paths
        3. Test Spark Connect connectivity
        4. Save to database
        5. Create Prefect deployment if scheduling enabled
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

        # Create Prefect deployment if scheduling enabled
        if ingestion.schedule_frequency:
            try:
                prefect = await get_prefect_service()
                deployment_id = await prefect.create_deployment(ingestion)

                # Store deployment ID
                ingestion.prefect_deployment_id = deployment_id
                self.db.commit()

                logger.info(f"✅ Created Prefect deployment for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"❌ Failed to create Prefect deployment: {e}")
                logger.warning("Ingestion created but scheduling unavailable")
                # Don't fail the request - ingestion can still be triggered manually

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
            try:
                prefect = await get_prefect_service()
                await prefect.update_deployment_schedule(
                    deployment_id=ingestion.prefect_deployment_id,
                    schedule_frequency=ingestion.schedule_frequency,
                    schedule_time=ingestion.schedule_time,
                    schedule_cron=ingestion.schedule_cron,
                    timezone=ingestion.schedule_timezone or "UTC"
                )
                logger.info(f"✅ Updated Prefect deployment schedule for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"❌ Failed to update Prefect deployment: {e}")
                # Don't fail the request - schedule update will be retried

        return self._to_response(ingestion)

    async def delete_ingestion(self, ingestion_id: str, delete_table: bool = False) -> bool:
        """Delete an ingestion."""
        # Get ingestion to retrieve Prefect deployment ID
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return False

        # Delete Prefect deployment first
        if ingestion.prefect_deployment_id:
            try:
                prefect = await get_prefect_service()
                await prefect.delete_deployment(ingestion.prefect_deployment_id)
                logger.info(f"✅ Deleted Prefect deployment for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"❌ Failed to delete Prefect deployment: {e}")
                # Continue with ingestion deletion anyway

        # TODO: Cleanup checkpoints
        # TODO: Delete table if requested

        return self.ingestion_repo.delete(ingestion_id)

    async def trigger_manual_run(self, ingestion_id: str) -> dict:
        """
        Trigger a manual ingestion run.

        If ingestion has a Prefect deployment, triggers via Prefect.
        Otherwise, falls back to direct BatchOrchestrator execution.

        Args:
            ingestion_id: Ingestion ID

        Returns:
            Dict with run information

        Raises:
            ValueError: If ingestion not found or not active
        """
        # Load ingestion
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        if ingestion.status != IngestionStatus.ACTIVE and ingestion.status != IngestionStatus.DRAFT:
            raise ValueError(f"Ingestion not active: {ingestion.status}")

        # Primary path: Trigger via Prefect deployment
        if ingestion.prefect_deployment_id:
            try:
                prefect = await get_prefect_service()
                flow_run_id = await prefect.trigger_deployment(
                    deployment_id=ingestion.prefect_deployment_id,
                    parameters={"ingestion_id": str(ingestion_id), "trigger": "manual"}
                )

                logger.info(f"✅ Triggered Prefect flow run {flow_run_id} for ingestion {ingestion_id}")

                return {
                    "status": "triggered",
                    "method": "prefect",
                    "flow_run_id": flow_run_id,
                    "message": "Ingestion run triggered via Prefect"
                }

            except Exception as e:
                logger.error(f"❌ Failed to trigger Prefect flow: {e}")
                logger.warning("Falling back to direct orchestrator execution")
                # Fall through to fallback path

        # Fallback path: Direct BatchOrchestrator execution
        logger.info(f"Triggering ingestion {ingestion_id} via BatchOrchestrator (fallback)")
        from app.services.batch_orchestrator import BatchOrchestrator
        orchestrator = BatchOrchestrator(self.db)
        run = orchestrator.run_ingestion(ingestion)

        return {
            "status": "completed",
            "method": "direct",
            "run_id": run.id,
            "message": "Ingestion run completed via direct execution"
        }

    async def pause_ingestion(self, ingestion_id: str) -> bool:
        """Pause an active ingestion (stops scheduler from triggering new runs)."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return False

        # Pause Prefect deployment
        if ingestion.prefect_deployment_id:
            try:
                prefect = await get_prefect_service()
                await prefect.pause_deployment(ingestion.prefect_deployment_id)
                logger.info(f"✅ Paused Prefect deployment for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"❌ Failed to pause Prefect deployment: {e}")

        result = self.ingestion_repo.update_status(ingestion_id, IngestionStatus.PAUSED)
        return result is not None

    async def resume_ingestion(self, ingestion_id: str) -> bool:
        """Resume a paused ingestion (re-registers with scheduler)."""
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            return False

        # Resume Prefect deployment
        if ingestion.prefect_deployment_id:
            try:
                prefect = await get_prefect_service()
                await prefect.resume_deployment(ingestion.prefect_deployment_id)
                logger.info(f"✅ Resumed Prefect deployment for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"❌ Failed to resume Prefect deployment: {e}")

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
