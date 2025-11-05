"""
Refresh Service - Business logic for refresh operations
"""

from typing import Dict, Any, List
from sqlalchemy.orm import Session
from app.models.domain import Ingestion, Run
from app.services.spark_service import SparkService
from app.services.file_state_service import FileStateService
from app.services.ingestion_service import IngestionService
from app.repositories.ingestion_repository import IngestionRepository
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class RefreshService:
    """Service for refresh operations."""

    def __init__(
        self,
        db: Session,
        spark_service: SparkService = None,
        file_state_service: FileStateService = None,
        ingestion_service: IngestionService = None,
        ingestion_repo: IngestionRepository = None
    ):
        self.db = db
        self.spark = spark_service or SparkService()
        self.file_state = file_state_service or FileStateService(db)
        self.ingestion_service = ingestion_service or IngestionService(db)
        self.ingestion_repo = ingestion_repo or IngestionRepository(db)

    def refresh(
        self,
        ingestion_id: str,
        confirm: bool,
        mode: str = "new_only",  # "full" or "new_only"
        auto_run: bool = True,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Perform table refresh: drop table + optional clear files + run.

        Args:
            ingestion_id: Ingestion identifier
            confirm: Safety confirmation (must be True)
            mode: Refresh mode - "full" (clear history, reprocess all files)
                  or "new_only" (keep history, process new files only)
            auto_run: Trigger run after refresh
            dry_run: Preview without executing

        Returns:
            Operation result with details

        Raises:
            ValueError: If confirm is False or ingestion not found
            Exception: If operation fails
        """
        if not confirm:
            raise ValueError("Confirmation required. Set confirm=true to proceed.")

        # Get ingestion
        ingestion = self.ingestion_repo.get_by_id(ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Determine if we're doing a full refresh
        is_full_refresh = mode == "full"

        # Get impact estimate (varies based on mode)
        if is_full_refresh:
            impact = self._estimate_full_refresh_impact(ingestion)
        else:
            impact = self._estimate_incremental_refresh_impact(ingestion)

        # Build operations list based on mode
        operations_list = [
            {"operation": "table_dropped", "details": {
                "table_name": self._get_table_fqn(ingestion)
            }}
        ]
        if is_full_refresh:
            operations_list.append({
                "operation": "processed_files_cleared",
                "details": {"ingestion_id": ingestion_id}
            })
        if auto_run:
            operations_list.append({
                "operation": "run_triggered",
                "details": {"ingestion_id": ingestion_id}
            })

        # Dry run - return preview
        if dry_run:
            notes = []
            if is_full_refresh:
                notes.append("⚠️ This will reprocess ALL files from scratch")
                notes.append(f"⚠️ Estimated cost: ${impact['estimated_cost_usd']:.2f}")
            else:
                notes.append("ℹ️ Only NEW files will be processed")
                notes.append("ℹ️ Processed file history will be preserved")
                notes.append(f"💰 Cost-effective: Only processes incremental data (${impact['estimated_cost_usd']:.2f})")

            return self._build_dry_run_response(
                ingestion_id=ingestion_id,
                operations=operations_list,
                impact=impact,
                mode=mode,
                notes=notes
            )

        # Execute operations
        operations = []
        run_id = None

        # Step 1: Drop table
        try:
            table_fqn = self._get_table_fqn(ingestion)
            table_result = self.spark.drop_table(ingestion.cluster_id, table_fqn)
            operations.append({
                "operation": "table_dropped",
                "status": "success",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "details": {
                    "table_name": table_fqn,
                    "existed": table_result.get("table_info", {}).get("existed", False)
                }
            })
            logger.info(f"Dropped table {table_fqn} for ingestion {ingestion_id}")
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            operations.append({
                "operation": "table_dropped",
                "status": "failed",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "error": str(e),
                "error_code": "TABLE_DROP_FAILED"
            })
            return self._build_error_response(ingestion_id, operations, e, mode)

        # Step 2: Clear processed files (only if mode="full")
        if is_full_refresh:
            try:
                files_cleared = self.file_state.clear_processed_files(ingestion_id)
                operations.append({
                    "operation": "processed_files_cleared",
                    "status": "success",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "details": {"files_cleared": files_cleared}
                })
                logger.info(f"Cleared {files_cleared} processed files for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"Failed to clear processed files: {e}")
                operations.append({
                    "operation": "processed_files_cleared",
                    "status": "failed",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "error": str(e),
                    "error_code": "FILE_CLEAR_FAILED"
                })
                return self._build_error_response(ingestion_id, operations, e, mode)

        # Step 3: Trigger run (if auto_run)
        if auto_run:
            try:
                run = self.ingestion_service.trigger_manual_run(ingestion_id)
                run_id = run.id
                operations.append({
                    "operation": "run_triggered",
                    "status": "success",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "details": {
                        "run_id": run_id,
                        "run_url": f"/api/v1/runs/{run_id}"
                    }
                })
                logger.info(f"Triggered run {run_id} for ingestion {ingestion_id}")
            except Exception as e:
                logger.error(f"Failed to trigger run: {e}")
                operations.append({
                    "operation": "run_triggered",
                    "status": "failed",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "error": str(e),
                    "error_code": "RUN_TRIGGER_FAILED"
                })
                return self._build_error_response(ingestion_id, operations, e, mode)

        # Build response notes
        notes = []
        if is_full_refresh:
            notes.append("Table dropped and will be recreated")
            notes.append("ALL files will be reprocessed")
        else:
            notes.append("Table dropped and will be recreated")
            notes.append("Only NEW files will be processed")
            notes.append(f"Processed file history preserved ({impact.get('files_skipped', 0)} files remain marked as processed)")

        return self._build_success_response(
            ingestion_id=ingestion_id,
            run_id=run_id,
            operations=operations,
            impact=impact,
            mode=mode,
            notes=notes
        )

    # Helper methods

    def _get_table_fqn(self, ingestion: Ingestion) -> str:
        """Get fully qualified table name."""
        return f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}"

    def _estimate_full_refresh_impact(self, ingestion: Ingestion) -> Dict[str, Any]:
        """
        Estimate impact of full refresh (ALL files).

        Note: This is a simplified estimation. In production, you would:
        - Query actual file list from source
        - Get real file sizes
        - Use cost estimator service for accurate pricing
        """
        # Get processed file count as proxy for total files
        processed_files = self.file_state.get_processed_files(ingestion.id)
        total_files = len(processed_files) if processed_files else 100  # Default estimate

        # Rough estimates (would use actual file discovery in production)
        estimated_size_gb = total_files * 0.5  # 500MB per file average
        estimated_cost = estimated_size_gb * 0.25  # $0.25/GB
        estimated_duration = max(5, total_files * 2)  # 2 min per file, min 5 min

        return {
            "files_to_process": total_files,
            "files_skipped": 0,
            "estimated_data_size_gb": round(estimated_size_gb, 2),
            "estimated_cost_usd": round(estimated_cost, 2),
            "estimated_duration_minutes": estimated_duration
        }

    def _estimate_incremental_refresh_impact(self, ingestion: Ingestion) -> Dict[str, Any]:
        """
        Estimate impact of incremental refresh (NEW files only).

        Note: This is a simplified estimation. In production, you would:
        - Actually discover new files from source
        - Get real file sizes
        - Use cost estimator service
        """
        # Get count of already processed files
        processed_files = self.file_state.get_processed_files(ingestion.id)
        files_processed = len(processed_files) if processed_files else 0

        # Estimate new files (in production, would do actual discovery)
        # For now, assume 5% new files or minimum 10
        new_files = max(10, int(files_processed * 0.05))

        estimated_size_gb = new_files * 0.5  # 500MB per file average
        estimated_cost = estimated_size_gb * 0.25  # $0.25/GB
        estimated_duration = max(5, new_files * 2)  # 2 min per file

        return {
            "files_to_process": new_files,
            "files_skipped": files_processed,
            "estimated_data_size_gb": round(estimated_size_gb, 2),
            "estimated_cost_usd": round(estimated_cost, 2),
            "estimated_duration_minutes": estimated_duration
        }

    def _build_success_response(
        self,
        ingestion_id: str,
        run_id: str,
        operations: List[Dict],
        impact: Dict,
        mode: str,
        notes: List[str] = None
    ) -> Dict[str, Any]:
        """Build success response."""
        message = "Full refresh completed successfully" if mode == "full" else "New-only refresh completed successfully"

        return {
            "status": "accepted",
            "message": message,
            "ingestion_id": ingestion_id,
            "run_id": run_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "mode": mode,
            "operations": operations,
            "impact": impact,
            "warnings": self._build_warnings(impact, mode),
            "notes": notes or []
        }

    def _build_dry_run_response(
        self,
        ingestion_id: str,
        operations: List[Dict],
        impact: Dict,
        mode: str,
        notes: List[str] = None
    ) -> Dict[str, Any]:
        """Build dry run response."""
        endpoint = "full" if mode == "full" else "new-only"

        return {
            "status": "dry_run",
            "message": "Preview of operations (not executed)",
            "ingestion_id": ingestion_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "mode": mode,
            "would_perform": operations,
            "impact": impact,
            "warnings": self._build_warnings(impact, mode),
            "notes": notes or [],
            "next_steps": {
                "to_proceed": f"POST /api/v1/ingestions/{ingestion_id}/refresh/{endpoint} with confirm=true",
                "to_cancel": "No action needed"
            }
        }

    def _build_error_response(
        self,
        ingestion_id: str,
        operations: List[Dict],
        error: Exception,
        mode: str
    ) -> Dict[str, Any]:
        """Build partial failure response."""
        # Build a minimal impact estimate even on failure
        impact = {
            "files_to_process": 0,
            "files_skipped": 0,
            "estimated_data_size_gb": 0.0,
            "estimated_cost_usd": 0.0,
            "estimated_duration_minutes": 0
        }

        return {
            "status": "partial_failure",
            "message": "Some operations failed",
            "ingestion_id": ingestion_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "mode": mode,
            "operations": operations,
            "impact": impact,
            "warnings": [],
            "notes": [f"Error: {str(error)}"],
            "error": str(error),
            "recovery": self._build_recovery_instructions(operations, ingestion_id)
        }

    def _build_warnings(self, impact: Dict, mode: str) -> List[str]:
        """Build warning messages based on impact."""
        warnings = []

        if mode == "full":
            warnings.append(
                f"This operation will reprocess {impact['files_to_process']} files "
                f"({impact['estimated_data_size_gb']} GB)"
            )

        if impact['estimated_cost_usd'] > 10:
            warnings.append(f"Estimated cost: ${impact['estimated_cost_usd']:.2f}")

        return warnings

    def _build_recovery_instructions(
        self,
        operations: List[Dict],
        ingestion_id: str
    ) -> Dict[str, str]:
        """Build recovery instructions for partial failures."""
        completed_ops = [op['operation'] for op in operations if op.get('status') == 'success']
        failed_ops = [op['operation'] for op in operations if op.get('status') == 'failed']

        if 'run_triggered' in failed_ops and 'table_dropped' in completed_ops:
            return {
                "message": "Table dropped but run failed to trigger",
                "action": f"Manually trigger run: POST /api/v1/ingestions/{ingestion_id}/run",
                "state": "Table dropped, file history may be cleared, no run triggered"
            }

        return {
            "message": "Operation partially completed",
            "action": "Review operations list and retry if needed",
            "state": f"Completed: {', '.join(completed_ops)}. Failed: {', '.join(failed_ops)}"
        }
