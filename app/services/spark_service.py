"""Spark service - high-level interface for Spark operations."""
from typing import Dict, Any
from app.models.schemas import IngestionCreate, PreviewResult, ColumnSchema
from app.config import get_spark_connect_credentials
from app.spark.connect_client import SparkConnectClient


class SparkService:
    """Service for Spark-related operations."""

    def preview(self, config: IngestionCreate) -> PreviewResult:
        """
        Preview ingestion configuration.

        Args:
            config: Ingestion configuration

        Returns:
            PreviewResult with schema and sample data
        """
        # Convert Pydantic model to dict for executor
        # Get Spark connection credentials dynamically
        spark_url, spark_token = get_spark_connect_credentials(config.cluster_id)

        ingestion_dict = {
            "cluster_id": config.cluster_id,
            "spark_connect_url": spark_url,
            "spark_connect_token": spark_token,
            "source": {
                "path": config.source.path,
                "type": config.source.type,
                "credentials": config.source.credentials,
            },
            "format": {
                "type": config.format.type,
                "options": config.format.options.dict() if config.format.options else {},
            },
        }


        # todo: we need to fix this
        # result = self.executor.preview(ingestion_dict)
        result = None

        if not result["success"]:
            raise Exception(f"Preview failed: {result.get('error')}")

        preview_data = result["preview"]

        # Convert to PreviewResult
        schema = [
            ColumnSchema(
                name=col["name"],
                data_type=col["data_type"],
                nullable=col["nullable"],
            )
            for col in preview_data["schema"]
        ]

        # Convert sample data to strings for display
        sample_data = [str(row) for row in preview_data["sample_data"]]

        # Estimate records based on file count and avg rows per file
        estimated_records = preview_data["file_count"] * 1000  # Rough estimate

        return PreviewResult(
            schema=schema,
            sample_data=sample_data,
            file_count=preview_data["file_count"],
            estimated_records=estimated_records,
        )

    def test_connection(self, cluster_id: str, connect_url: str, token: str) -> Dict[str, Any]:
        """
        Test Spark Connect connection.

        Args:
            cluster_id: Cluster ID
            connect_url: Spark Connect URL
            token: Authentication token

        Returns:
            Connection test result
        """
        client = SparkConnectClient(connect_url, token)
        try:
            return client.test_connection()
        finally:
            client.stop()

    def drop_table(self, cluster_id: str, table_fqn: str) -> Dict[str, Any]:
        """
        Drop a table using Spark Connect.

        Args:
            cluster_id: Cluster ID
            table_fqn: Fully qualified table name (e.g., catalog.database.table)

        Returns:
            Dict with operation result and table metadata
        """
        spark_url, spark_token = get_spark_connect_credentials(cluster_id)
        client = SparkConnectClient(spark_url, spark_token)

        try:
            spark = client.connect()

            # Check if table exists before dropping (for response metadata)
            try:
                spark.sql(f"DESCRIBE TABLE {table_fqn}")
                table_existed = True
            except Exception:
                # Table doesn't exist - that's fine
                table_existed = False

            # Drop table (IF EXISTS for idempotency)
            drop_query = f"DROP TABLE IF EXISTS {table_fqn}"
            spark.sql(drop_query)

            return {
                "success": True,
                "table_name": table_fqn,
                "table_info": {
                    "table_name": table_fqn,
                    "existed": table_existed
                }
            }
        except Exception as e:
            raise Exception(f"Failed to drop table {table_fqn}: {str(e)}")
        finally:
            client.stop()
