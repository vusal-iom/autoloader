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

    def read_stream(
        self,
        source_type: str,
        source_path: str,
        format_type: str,
        checkpoint_location: str,
        format_options: Dict[str, Any],
        source_credentials: Optional[Dict[str, Any]] = None,
    ):
        """
        Create a streaming DataFrame using Auto Loader.

        Args:
            source_type: s3, azure_blob, gcs
            source_path: Source path
            format_type: File format (json, csv, parquet, etc.)
            checkpoint_location: Checkpoint path
            format_options: Format-specific options
            source_credentials: Source credentials

        Returns:
            Streaming DataFrame
        """
        spark = self.connect()

        # Build cloudFiles options
        options = {
            "cloudFiles.format": format_type,
            "cloudFiles.schemaLocation": f"{checkpoint_location}/schema",
            "cloudFiles.inferColumnTypes": "true",
        }

        # Add source-specific options
        if source_type == "s3":
            if source_credentials:
                options["cloudFiles.region"] = source_credentials.get("region", "us-east-1")
                # Set AWS credentials if using access keys
                if "access_key_id" in source_credentials:
                    spark.conf.set("spark.hadoop.fs.s3a.access.key", source_credentials["access_key_id"])
                    spark.conf.set("spark.hadoop.fs.s3a.secret.key", source_credentials["secret_access_key"])

        # Add format-specific options
        for key, value in format_options.items():
            options[key] = str(value)

        # Create streaming DataFrame
        df = spark.readStream.format("cloudFiles").options(**options).load(source_path)

        return df

    def write_stream(
        self,
        df,
        target_table: str,
        checkpoint_location: str,
        write_mode: str = "append",
        partition_columns: Optional[List[str]] = None,
        trigger_mode: str = "availableNow",
    ):
        """
        Write streaming DataFrame to Iceberg table.

        Args:
            df: Streaming DataFrame
            target_table: Target table (catalog.database.table)
            checkpoint_location: Checkpoint location
            write_mode: Write mode (append, complete, update)
            partition_columns: Partition columns
            trigger_mode: Trigger mode (availableNow, processingTime)

        Returns:
            StreamingQuery
        """
        writer = (
            df.writeStream.format("iceberg")
            .outputMode(write_mode)
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
        )

        # Add partitioning if specified
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        # Set trigger
        if trigger_mode == "availableNow":
            from pyspark.sql.streaming import Trigger
            writer = writer.trigger(Trigger.AvailableNow())
        elif trigger_mode.startswith("processingTime"):
            from pyspark.sql.streaming import Trigger
            interval = trigger_mode.split(":")[1] if ":" in trigger_mode else "10 seconds"
            writer = writer.trigger(Trigger.ProcessingTime(interval))

        # Write to table
        query = writer.toTable(target_table)

        return query

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
