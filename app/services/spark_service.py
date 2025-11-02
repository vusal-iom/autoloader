"""Spark service - high-level interface for Spark operations."""
from typing import Dict, Any
from app.models.schemas import IngestionCreate, PreviewResult, ColumnSchema
from app.spark.executor import IngestionExecutor


class SparkService:
    """Service for Spark-related operations."""

    def __init__(self):
        self.executor = IngestionExecutor()

    def preview(self, config: IngestionCreate) -> PreviewResult:
        """
        Preview ingestion configuration.

        Args:
            config: Ingestion configuration

        Returns:
            PreviewResult with schema and sample data
        """
        # Convert Pydantic model to dict for executor
        ingestion_dict = {
            "cluster_id": config.cluster_id,
            "spark_connect_url": f"sc://{config.cluster_id}.iomete.com:15002",
            "spark_connect_token": "mock_token",  # TODO: Get actual token
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

        result = self.executor.preview(ingestion_dict)

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
        from app.spark.session_manager import get_session_pool

        pool = get_session_pool()
        client = None

        try:
            client = pool.get_client(cluster_id, connect_url, token)
            result = client.test_connection()
            return result
        finally:
            if client:
                pool.return_client(cluster_id, client)
