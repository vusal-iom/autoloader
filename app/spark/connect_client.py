"""Spark Connect client for executing ingestion operations."""
from pyspark.sql import SparkSession
from typing import Dict, Any, List, Optional


class SparkConnectClient:
    """Client for Spark Connect operations."""

    def __init__(self, connect_url: str, token: str, s3_credentials: Optional[Dict[str, str]] = None):
        """
        Initialize Spark Connect client.

        Args:
            connect_url: Spark Connect URL (e.g., sc://host:15002)
            token: Authentication token
            s3_credentials: Optional S3 credentials dict with 'aws_access_key_id' and 'aws_secret_access_key'
        """
        self.connect_url = connect_url
        self.token = token
        self.s3_credentials = s3_credentials
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
            builder = (
                SparkSession.builder.remote(self.connect_url)
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.adaptive.enabled", "true")
            )

            # Configure S3 credentials at session creation time
            if self.s3_credentials and self.s3_credentials.get('aws_access_key_id'):
                builder = builder.config(
                    "spark.hadoop.fs.s3a.access.key",
                    self.s3_credentials['aws_access_key_id']
                )
                builder = builder.config(
                    "spark.hadoop.fs.s3a.secret.key",
                    self.s3_credentials['aws_secret_access_key']
                )

            self.session = builder.getOrCreate()

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

    def stop(self):
        """Stop Spark session."""
        if self.session:
            self.session.stop()
            self.session = None
