"""Cost estimation service."""
from app.models.schemas import IngestionCreate, CostBreakdown
from app.config import get_settings

settings = get_settings()


class CostEstimator:
    """Service for estimating ingestion costs."""

    def estimate(self, config: IngestionCreate) -> CostBreakdown:
        """
        Estimate monthly cost for an ingestion configuration.

        Algorithm:
        1. Estimate file count and size from source
        2. Calculate processing time based on file size and format
        3. Calculate runs per month from schedule
        4. Compute costs:
           - Compute: runs_per_month * processing_time * cluster_dbus * dbu_rate
           - Storage: file_size * runs_per_month * storage_rate
           - Discovery: file_count * runs_per_month * list_operation_rate
        """
        # TODO: Actually sample source to get accurate estimates
        # For now, use mock values
        estimated_file_count = 100
        estimated_avg_file_size_mb = 50
        estimated_total_size_gb = (estimated_file_count * estimated_avg_file_size_mb) / 1024

        # Estimate processing time (rough heuristic)
        # JSON/CSV: 1 minute per GB, Parquet: 30 seconds per GB
        processing_time_per_gb = 1.0 if config.format.type in ["json", "csv"] else 0.5
        estimated_duration_minutes = estimated_total_size_gb * processing_time_per_gb

        # Calculate runs per month
        runs_per_month = self._get_runs_per_month(config.schedule.frequency or "daily")

        # Get cluster info (mock for now)
        # TODO: Fetch from cluster API
        cluster_dbus = 64  # Example: 8 workers * 8 DBU/worker

        # Calculate costs
        compute_per_run = (
            (estimated_duration_minutes / 60.0)  # Convert to hours
            * cluster_dbus
            * settings.cost_per_dbu_hour
        )
        compute_monthly = compute_per_run * runs_per_month

        storage_monthly = (
            estimated_total_size_gb * runs_per_month * settings.storage_cost_per_gb
        )

        discovery_monthly = (
            estimated_file_count
            * runs_per_month
            / 1000.0
            * settings.list_operation_cost_per_1000
        )

        total_monthly = compute_monthly + storage_monthly + discovery_monthly

        return CostBreakdown(
            compute_per_run=round(compute_per_run, 2),
            compute_monthly=round(compute_monthly, 2),
            storage_monthly=round(storage_monthly, 2),
            discovery_monthly=round(discovery_monthly, 4),
            total_monthly=round(total_monthly, 2),
            breakdown_details={
                "estimated_file_count": estimated_file_count,
                "estimated_total_size_gb": round(estimated_total_size_gb, 2),
                "estimated_duration_minutes": round(estimated_duration_minutes, 2),
                "runs_per_month": runs_per_month,
                "cluster_dbus": cluster_dbus,
            },
        )

    def _get_runs_per_month(self, frequency: str) -> int:
        """Calculate runs per month based on frequency."""
        frequency_map = {
            "hourly": 24 * 30,  # 720 runs/month
            "daily": 30,
            "weekly": 4,
            "monthly": 1,
        }
        return frequency_map.get(frequency, 30)  # Default to daily
