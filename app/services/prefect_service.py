"""
Prefect Service - Manages Prefect deployments and flow runs.

This service wraps the Prefect API client and provides methods for:
- Creating/updating/deleting deployments
- Triggering flow runs
- Querying run history
- Managing schedules
"""
import logging
from typing import Optional, List, Dict, Any
from uuid import UUID
from datetime import datetime

from prefect import get_client
from prefect.client.schemas.schedules import CronSchedule
from prefect.client.schemas.actions import DeploymentScheduleCreate, DeploymentScheduleUpdate, DeploymentUpdate
from prefect.client.schemas.filters import DeploymentFilter, FlowRunFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.exceptions import ObjectNotFound

from app.config import get_settings
from app.models.domain import Ingestion

logger = logging.getLogger(__name__)
settings = get_settings()


class PrefectService:
    """Service for managing Prefect deployments and flow runs."""

    def __init__(self):
        self.api_url = settings.prefect_api_url
        self.initialized = False
        self.ingestion_flow_id: Optional[str] = None

    async def initialize(self):
        """
        Initialize service by checking Prefect connectivity.
        Call this during application startup.
        """
        try:
            async with get_client() as client:
                # Test connectivity
                health = await client.api_healthcheck()
                logger.info(f"✅ Prefect server connected: {self.api_url}")

                # Note: Flows are registered when first deployed, not at startup
                self.initialized = True

        except Exception as e:
            logger.error(f"❌ Failed to connect to Prefect server: {e}")
            logger.warning("Prefect features will be unavailable")
            self.initialized = False

    async def create_deployment(
        self,
        ingestion: Ingestion,
    ) -> str:
        """
        Create a Prefect deployment for an ingestion.

        Args:
            ingestion: Ingestion domain model

        Returns:
            Deployment ID (UUID string)

        Raises:
            Exception: If Prefect is not initialized or deployment creation fails
        """
        if not self.initialized:
            raise RuntimeError("PrefectService not initialized. Call initialize() first.")

        try:
            async with get_client() as client:
                # Import flow to register it
                from app.prefect.flows.run_ingestion import run_ingestion_flow
                from prefect.filesystems import LocalFileSystem

                # Build schedule from ingestion configuration
                schedule = self._build_schedule(ingestion)

                # Determine work queue
                work_queue_name = self._get_work_queue(ingestion)

                # Get or create flow
                flow_id = await self._get_or_create_flow_id(client)

                # Build schedules list for Prefect 3.x
                schedules = []
                if schedule:
                    schedules.append(
                        DeploymentScheduleCreate(
                            schedule=schedule,
                            active=True
                        )
                    )

                # Create/get LocalFileSystem storage block for worker code
                storage_block_name = "autoloader-code"
                try:
                    # Try to load existing block
                    storage = await LocalFileSystem.load(storage_block_name)
                except Exception:
                    # Create new block if it doesn't exist
                    storage = LocalFileSystem(basepath="/opt/prefect")
                    await storage.save(storage_block_name, overwrite=True)

                # Create deployment using client API
                deployment_id = await client.create_deployment(
                    name=f"ingestion-{ingestion.id}",
                    flow_id=flow_id,
                    work_pool_name="default-work-pool",
                    work_queue_name=work_queue_name,
                    schedules=schedules,
                    parameters={"ingestion_id": str(ingestion.id), "trigger": "scheduled"},
                    tags=[
                        "autoloader",
                        f"tenant-{ingestion.tenant_id}",
                        f"source-{ingestion.source_type}",
                    ],
                    description=f"Autoloader ingestion: {ingestion.name}",
                    paused=False,
                    # Reference to storage block
                    storage_document_id=storage._block_document_id,
                    # Path within storage
                    path=".",
                    # Entrypoint relative to storage path
                    entrypoint="app/prefect/flows/run_ingestion.py:run_ingestion_flow",
                )

                deployment_id = str(deployment_id)

                logger.info(
                    f"✅ Created Prefect deployment {deployment_id} for ingestion {ingestion.id}"
                )

                return deployment_id

        except Exception as e:
            logger.error(f"❌ Failed to create deployment for ingestion {ingestion.id}: {e}")
            raise

    async def update_deployment_schedule(
        self,
        deployment_id: str,
        schedule_frequency: Optional[str],
        schedule_time: Optional[str] = None,
        schedule_cron: Optional[str] = None,
        timezone: str = "UTC"
    ):
        """
        Update deployment schedule.

        Args:
            deployment_id: Deployment UUID
            schedule_frequency: Frequency (hourly, daily, weekly, custom)
            schedule_time: Time string (HH:MM) for daily/weekly
            schedule_cron: Custom cron expression
            timezone: IANA timezone
        """
        if not self.initialized:
            raise RuntimeError("PrefectService not initialized")

        try:
            async with get_client() as client:
                # Build schedule
                schedules = []
                if schedule_frequency:
                    cron_expr = self._build_cron_expression(
                        schedule_frequency, schedule_time, schedule_cron
                    )
                    if cron_expr:
                        schedule = CronSchedule(cron=cron_expr, timezone=timezone)
                        schedules.append(
                            DeploymentScheduleUpdate(
                                schedule=schedule,
                                active=True
                            )
                        )

                await client.update_deployment(
                    deployment_id=UUID(deployment_id),
                    deployment=DeploymentUpdate(schedules=schedules)
                )

                logger.info(f"✅ Updated schedule for deployment {deployment_id}")

        except Exception as e:
            logger.error(f"❌ Failed to update deployment {deployment_id}: {e}")
            raise

    async def pause_deployment(self, deployment_id: str):
        """
        Pause deployment (disable schedule).

        Args:
            deployment_id: Deployment UUID
        """
        if not self.initialized:
            logger.warning("PrefectService not initialized, skipping pause")
            return

        try:
            async with get_client() as client:
                await client.set_deployment_paused_state(
                    deployment_id=UUID(deployment_id),
                    paused=True,
                )

                logger.info(f"⏸️  Paused deployment {deployment_id}")

        except ObjectNotFound:
            logger.warning(f"Deployment {deployment_id} not found, may have been deleted")
        except Exception as e:
            logger.error(f"❌ Failed to pause deployment {deployment_id}: {e}")
            raise

    async def resume_deployment(self, deployment_id: str):
        """
        Resume deployment (enable schedule).

        Args:
            deployment_id: Deployment UUID
        """
        if not self.initialized:
            raise RuntimeError("PrefectService not initialized")

        try:
            async with get_client() as client:
                await client.set_deployment_paused_state(
                    deployment_id=UUID(deployment_id),
                    paused=False,
                )

                logger.info(f"▶️  Resumed deployment {deployment_id}")

        except Exception as e:
            logger.error(f"❌ Failed to resume deployment {deployment_id}: {e}")
            raise

    async def delete_deployment(self, deployment_id: str):
        """
        Delete deployment.

        Args:
            deployment_id: Deployment UUID
        """
        if not self.initialized:
            logger.warning("PrefectService not initialized, skipping delete")
            return

        try:
            async with get_client() as client:
                await client.delete_deployment(UUID(deployment_id))

                logger.info(f"🗑️  Deleted deployment {deployment_id}")

        except ObjectNotFound:
            logger.warning(f"Deployment {deployment_id} not found, may have been already deleted")
        except Exception as e:
            logger.error(f"❌ Failed to delete deployment {deployment_id}: {e}")
            raise

    async def trigger_deployment(
        self,
        deployment_id: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Manually trigger a deployment (create flow run).

        Args:
            deployment_id: Deployment UUID
            parameters: Optional override parameters

        Returns:
            Flow run ID (UUID string)
        """
        if not self.initialized:
            raise RuntimeError("PrefectService not initialized")

        try:
            async with get_client() as client:
                # Override trigger parameter to indicate manual run
                params = parameters or {}
                if "trigger" not in params:
                    params["trigger"] = "manual"

                flow_run = await client.create_flow_run_from_deployment(
                    deployment_id=UUID(deployment_id),
                    parameters=params,
                )

                flow_run_id = str(flow_run.id)
                logger.info(f"🚀 Triggered deployment {deployment_id}, flow run: {flow_run_id}")

                return flow_run_id

        except Exception as e:
            logger.error(f"❌ Failed to trigger deployment {deployment_id}: {e}")
            raise

    async def get_flow_runs(
        self,
        deployment_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get flow runs for a deployment.

        Args:
            deployment_id: Deployment UUID
            limit: Maximum number of runs to return

        Returns:
            List of flow run dicts
        """
        if not self.initialized:
            logger.warning("PrefectService not initialized, returning empty list")
            return []

        try:
            async with get_client() as client:
                flow_runs = await client.read_flow_runs(
                    deployment_filter=DeploymentFilter(
                        id={"any_": [UUID(deployment_id)]}
                    ),
                    limit=limit,
                    sort=FlowRunSort.START_TIME_DESC,
                )

                return [
                    {
                        "id": str(run.id),
                        "state": run.state.type.value if run.state else None,
                        "state_name": run.state.name if run.state else None,
                        "start_time": run.start_time.isoformat() if run.start_time else None,
                        "end_time": run.end_time.isoformat() if run.end_time else None,
                        "total_run_time": run.total_run_time.total_seconds() if run.total_run_time else None,
                        "parameters": run.parameters,
                    }
                    for run in flow_runs
                ]

        except Exception as e:
            logger.error(f"❌ Failed to get flow runs for deployment {deployment_id}: {e}")
            return []

    async def get_deployment_info(self, deployment_id: str) -> Optional[Dict[str, Any]]:
        """
        Get deployment information.

        Args:
            deployment_id: Deployment UUID

        Returns:
            Deployment info dict or None if not found
        """
        if not self.initialized:
            return None

        try:
            async with get_client() as client:
                deployment = await client.read_deployment(UUID(deployment_id))

                return {
                    "id": str(deployment.id),
                    "name": deployment.name,
                    "is_schedule_active": deployment.is_schedule_active,
                    "paused": deployment.paused,
                    "schedule": str(deployment.schedule) if deployment.schedule else None,
                    "work_queue_name": deployment.work_queue_name,
                    "tags": deployment.tags,
                }

        except ObjectNotFound:
            logger.warning(f"Deployment {deployment_id} not found")
            return None
        except Exception as e:
            logger.error(f"❌ Failed to get deployment info {deployment_id}: {e}")
            return None

    def _get_work_queue(self, ingestion: Ingestion) -> str:
        """
        Determine work queue based on ingestion characteristics.

        Args:
            ingestion: Ingestion configuration

        Returns:
            Work queue name
        """
        # TODO: Implement resource-based routing
        # For now, use default queue
        return settings.prefect_default_work_queue

    def _build_schedule(self, ingestion: Ingestion) -> Optional[CronSchedule]:
        """
        Build Prefect schedule from ingestion configuration.

        Args:
            ingestion: Ingestion configuration

        Returns:
            CronSchedule or None
        """
        if not ingestion.schedule_frequency:
            return None

        cron_expr = self._build_cron_expression(
            ingestion.schedule_frequency,
            ingestion.schedule_time,
            ingestion.schedule_cron
        )

        if not cron_expr:
            return None

        timezone = ingestion.schedule_timezone or "UTC"
        return CronSchedule(cron=cron_expr, timezone=timezone)

    def _build_cron_expression(
        self,
        frequency: str,
        time_str: Optional[str] = None,
        custom_cron: Optional[str] = None
    ) -> Optional[str]:
        """
        Build cron expression from schedule configuration.

        Args:
            frequency: Frequency (hourly, daily, weekly, custom)
            time_str: Time string in HH:MM format
            custom_cron: Custom cron expression

        Returns:
            Cron expression string or None
        """
        if frequency == "custom":
            return custom_cron

        if frequency == "hourly":
            return "0 * * * *"

        if frequency == "daily":
            hour, minute = self._parse_schedule_time(time_str)
            return f"{minute} {hour} * * *"

        if frequency == "weekly":
            hour, minute = self._parse_schedule_time(time_str)
            return f"{minute} {hour} * * 0"  # Sunday

        return None

    def _parse_schedule_time(self, time_str: Optional[str]) -> tuple[int, int]:
        """
        Parse schedule_time string (HH:MM) into (hour, minute).

        Args:
            time_str: Time string in HH:MM format

        Returns:
            Tuple of (hour, minute)
        """
        if not time_str:
            return (0, 0)

        try:
            parts = time_str.split(":")
            return (int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            logger.warning(f"Invalid time format: {time_str}, defaulting to 00:00")
            return (0, 0)

    async def _get_or_create_flow_id(self, client) -> UUID:
        """
        Get or create the ingestion flow ID.

        Args:
            client: Prefect client

        Returns:
            Flow UUID
        """
        if self.ingestion_flow_id:
            return UUID(self.ingestion_flow_id)

        # Import flow
        from app.prefect.flows.run_ingestion import run_ingestion_flow

        # Try to find existing flow by name
        try:
            flows = await client.read_flows()
            flow = next((f for f in flows if f.name == run_ingestion_flow.name), None)

            if flow:
                self.ingestion_flow_id = str(flow.id)
                logger.info(f"Found existing flow: {self.ingestion_flow_id}")
                return UUID(self.ingestion_flow_id)
        except Exception as e:
            logger.warning(f"Error reading flows: {e}")

        # Create flow if it doesn't exist
        from prefect.client.schemas.actions import FlowCreate

        try:
            flow_data = FlowCreate(name=run_ingestion_flow.name)
            created_flow_id = await client.create_flow(flow_data)
            # create_flow returns UUID directly in Prefect 3.x
            self.ingestion_flow_id = str(created_flow_id)
            logger.info(f"Created new flow: {self.ingestion_flow_id}")
            return created_flow_id if isinstance(created_flow_id, UUID) else UUID(self.ingestion_flow_id)
        except Exception as e:
            logger.error(f"Failed to create flow: {e}")
            raise


# Global singleton instance
_prefect_service: Optional[PrefectService] = None


async def get_prefect_service() -> PrefectService:
    """
    Get singleton Prefect service instance.

    Returns:
        PrefectService instance
    """
    global _prefect_service

    if _prefect_service is None:
        _prefect_service = PrefectService()
        await _prefect_service.initialize()

    return _prefect_service


def get_prefect_service_sync() -> PrefectService:
    """
    Get singleton Prefect service instance (synchronous).

    WARNING: This does NOT call initialize().
    Only use this if you're sure the service was already initialized at startup.

    Returns:
        PrefectService instance (may not be initialized)
    """
    global _prefect_service

    if _prefect_service is None:
        _prefect_service = PrefectService()
        logger.warning("PrefectService created without initialization (sync context)")

    return _prefect_service
