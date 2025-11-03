"""Ingestion executor - executes ingestion using Spark Connect."""
from typing import Dict, Any
from datetime import datetime
from app.models.domain import Ingestion
from app.spark.session_manager import get_session_pool
from app.config import get_spark_connect_credentials


class IngestionExecutor:
    """Executes ingestion operations via Spark Connect."""

    def __init__(self):
        self.session_pool = get_session_pool()

    def execute(self, ingestion: Ingestion, run_id: str) -> Dict[str, Any]:
        """
        Execute an ingestion.

        Args:
            ingestion: Ingestion configuration
            run_id: Run ID for tracking

        Returns:
            Execution result with metrics
        """
        client = None
        try:
            # Get Spark Connect credentials dynamically from cluster_id
            spark_url, spark_token = get_spark_connect_credentials(ingestion.cluster_id)

            # Get Spark Connect client from pool
            client = self.session_pool.get_client(
                cluster_id=ingestion.cluster_id,
                connect_url=spark_url,
                token=spark_token,
            )

            # Build format options
            format_options = ingestion.format_options or {}

            # Create streaming DataFrame
            df = client.read_stream(
                source_type=ingestion.source_type,
                source_path=ingestion.source_path,
                format_type=ingestion.format_type,
                checkpoint_location=ingestion.checkpoint_location,
                format_options=format_options,
                source_credentials=ingestion.source_credentials,
            )

            # Apply any transformations (future enhancement)
            # df = self._apply_transformations(df, ingestion)

            # Build target table name
            target_table = f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}"

            # Write stream with availableNow trigger (batch mode - processes all available data then stops)
            query = client.write_stream(
                df=df,
                target_table=target_table,
                checkpoint_location=ingestion.checkpoint_location,
                write_mode=ingestion.write_mode,
                partition_columns=ingestion.partitioning_columns if ingestion.partitioning_enabled else None,
                trigger_mode="availableNow",
            )

            # Wait for completion (availableNow automatically terminates when all data is processed)
            query.awaitTermination()

            # Collect metrics after completion
            metrics = self._monitor_query(query)

            return {
                "success": True,
                "run_id": run_id,
                "metrics": metrics,
            }

        except Exception as e:
            return {
                "success": False,
                "run_id": run_id,
                "error": str(e),
                "error_type": type(e).__name__,
            }

        finally:
            # Return client to pool
            if client:
                self.session_pool.return_client(ingestion.cluster_id, client)

    def preview(self, ingestion_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preview ingestion without executing.

        Args:
            ingestion_config: Ingestion configuration

        Returns:
            Preview result with schema and sample data
        """
        client = None
        try:
            # Get client from pool
            cluster_id = ingestion_config["cluster_id"]
            connect_url = ingestion_config["spark_connect_url"]
            token = ingestion_config["spark_connect_token"]

            client = self.session_pool.get_client(
                cluster_id=cluster_id,
                connect_url=connect_url,
                token=token,
            )

            # Preview files
            result = client.preview_files(
                source_path=ingestion_config["source"]["path"],
                format_type=ingestion_config["format"]["type"],
                format_options=ingestion_config["format"].get("options", {}),
                limit=100,
            )

            return {
                "success": True,
                "preview": result,
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

        finally:
            if client:
                self.session_pool.return_client(cluster_id, client)

    def _monitor_query(self, query) -> Dict[str, Any]:
        """
        Monitor streaming query and collect metrics.

        Args:
            query: StreamingQuery instance

        Returns:
            Metrics dictionary
        """
        metrics = {
            "files_processed": 0,
            "records_ingested": 0,
            "bytes_read": 0,
            "bytes_written": 0,
        }

        try:
            # Get recent progress
            recent_progress = query.recentProgress

            if recent_progress:
                for progress in recent_progress:
                    # Aggregate metrics
                    if progress.numInputRows:
                        metrics["records_ingested"] += progress.numInputRows

                    # Get source metrics
                    if progress.sources:
                        for source in progress.sources:
                            if hasattr(source, "numInputFiles"):
                                metrics["files_processed"] += source.numInputFiles
                            if hasattr(source, "inputBytes"):
                                metrics["bytes_read"] += source.inputBytes

                    # Get sink metrics
                    if progress.sink and hasattr(progress.sink, "numOutputRows"):
                        # Estimate bytes written (rough estimate)
                        metrics["bytes_written"] += progress.sink.numOutputRows * 100  # Assume 100 bytes/row

        except Exception as e:
            # If monitoring fails, just log and continue
            print(f"Failed to collect metrics: {e}")

        return metrics

    def _apply_transformations(self, df, ingestion: Ingestion):
        """
        Apply transformations to DataFrame (future enhancement).

        Args:
            df: DataFrame
            ingestion: Ingestion configuration

        Returns:
            Transformed DataFrame
        """
        # Future: Apply light SQL transformations
        # - Column selection
        # - Filtering
        # - Column renaming
        # - Type casting
        return df
