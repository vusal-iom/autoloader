"""Spark Connect client for executing ingestion operations."""
from pyspark.sql import SparkSession
from typing import Dict, Any, List, Optional


class SparkConnectClient:
    """Client for Spark Connect operations."""

    def __init__(self, connect_url: str, token: str):
        """
        Initialize Spark Connect client.

        Args:
            connect_url: Spark Connect URL (e.g., sc://host:15002)
            token: Authentication token
        """
        self.connect_url = connect_url
        self.token = token
        self.session: Optional[SparkSession] = None

    def connect(self) -> SparkSession:
        """
        Establish Spark Connect session.

        Returns:
            SparkSession instance
        """
        # Check if session needs to be created or recreated
        needs_new_session = False

        if self.session is None:
            needs_new_session = True
        else:
            # Check if session is still active by trying a simple operation
            try:
                # Try to get version to test if session is active
                _ = self.session.version
            except Exception:
                # Session is closed/invalid, need to recreate
                needs_new_session = True
                self.session = None

        if needs_new_session:
            self.session = (
                SparkSession.builder.remote(self.connect_url)
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate()
            )

        return self.session

    def test_connection(self) -> Dict[str, Any]:
        """
        Test Spark Connect connectivity.

        Returns:
            Connection test result with Spark version and status
        """
        try:
            spark = self.connect()
            version = spark.version
            return {
                "status": "success",
                "spark_version": version,
                "connect_url": self.connect_url,
            }
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "connect_url": self.connect_url,
            }

    def preview_files(
        self,
        source_path: str,
        format_type: str,
        format_options: Dict[str, Any],
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Preview files without creating a streaming query.

        Args:
            source_path: Source path
            format_type: File format
            format_options: Format options
            limit: Number of rows to preview

        Returns:
            Preview result with schema and sample data
        """
        spark = self.connect()

        # Read sample data (batch mode)
        df = spark.read.format(format_type).options(**format_options).load(source_path).limit(limit)

        # Infer schema
        schema = [
            {
                "name": field.name,
                "data_type": field.dataType.simpleString,
                "nullable": field.nullable,
            }
            for field in df.schema.fields
        ]

        # Get sample data
        sample_data = [row.asDict() for row in df.take(5)]

        # Count files
        file_count = spark.read.format("binaryFile").load(source_path).count()

        return {
            "schema": schema,
            "sample_data": sample_data,
            "file_count": file_count,
        }

    def stop(self):
        """Stop Spark session."""
        if self.session:
            self.session.stop()
            self.session = None
