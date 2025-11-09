"""
Batch File Processor - Processes files with Spark batch API

Replaces streaming Auto Loader with explicit batch processing.
"""

from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from sqlalchemy.orm import Session
from app.spark.connect_client import SparkConnectClient
from app.services.file_state_service import FileStateService
from app.services.schema_evolution_service import SchemaEvolutionService, SchemaComparison
from app.repositories.schema_version_repository import SchemaVersionRepository
from app.models.domain import Ingestion, ProcessedFile
import logging
import json

logger = logging.getLogger(__name__)


class BatchFileProcessor:
    """
    Processes files in batches with PostgreSQL state tracking.

    Phase 1: Sequential processing
    Phase 4: Parallel processing with ThreadPool
    """

    def __init__(
        self,
        spark_client: SparkConnectClient,
        state_service: FileStateService,
        ingestion: Ingestion,
        db: Session,
        schema_version_repo: Optional[SchemaVersionRepository] = None
    ):
        self.spark = spark_client
        self.state = state_service
        self.ingestion = ingestion
        self.db = db
        self.schema_version_repo = schema_version_repo or SchemaVersionRepository(db)

    def process_files(
        self,
        files: List[Dict],
        run_id: str
    ) -> Dict[str, int]:
        """
        Process list of files.

        Args:
            files: List of file metadata dicts from FileDiscoveryService
            run_id: ID of the current run for tracking

        Returns:
            Metrics dict: {success: N, failed: N, skipped: N}
        """
        metrics = {'success': 0, 'failed': 0, 'skipped': 0}

        logger.info(f"Processing {len(files)} files for ingestion {self.ingestion.id}")

        for file_info in files:
            file_path = file_info['path']

            # Atomically lock file for processing
            file_record = self.state.lock_file_for_processing(
                ingestion_id=self.ingestion.id,
                file_path=file_path,
                run_id=run_id,
                file_metadata=file_info
            )

            if not file_record:
                # Another worker is processing or file already done
                logger.debug(f"Skipping {file_path} (locked or completed)")
                metrics['skipped'] += 1
                continue

            try:
                # Process single file
                logger.info(f"Processing file: {file_path}")
                result = self._process_single_file(file_path, file_info)

                # Mark success
                self.state.mark_file_success(
                    file_record,
                    records_ingested=result['record_count'],
                    bytes_read=file_info.get('size', 0)
                )
                metrics['success'] += 1
                logger.info(f"Successfully processed {file_path}: {result['record_count']} records")

            except Exception as e:
                # Mark failure
                self.state.mark_file_failed(file_record, e)
                metrics['failed'] += 1
                logger.error(f"Failed to process {file_path}: {e}", exc_info=True)

        logger.info(f"Batch complete: {metrics}")
        return metrics

    def _process_single_file(self, file_path: str, file_info: Dict) -> Dict:
        """
        Process a single file with Spark batch API.

        Args:
            file_path: Full S3 path (e.g., s3://bucket/path/file.json)
            file_info: File metadata dict

        Returns:
            Dict with processing results: {record_count: N}
        """
        # Step 1: Read file with Spark DataFrame batch API
        if self.ingestion.schema_json:
            # Use predefined schema
            df = self._read_file_with_schema(file_path)
        else:
            # Infer schema from file
            df = self._read_file_infer_schema(file_path)

        # Step 2: Get record count (for metrics)
        record_count = int(df.count())  # Convert to Python int (from numpy.int64)

        if record_count == 0:
            logger.warning(f"File {file_path} is empty, skipping write")
            return {'record_count': 0}

        # Step 3: Write to Iceberg table (with schema tracking)
        self._write_to_iceberg(df, file_path)

        return {'record_count': record_count}

    def _read_file_with_schema(self, file_path: str) -> DataFrame:
        """
        Read file with predefined schema.

        Args:
            file_path: Full S3 path

        Returns:
            Spark DataFrame
        """
        spark = self.spark.connect()

        # Configure AWS credentials for S3 access
        if self.ingestion.source_type == "s3" and self.ingestion.source_credentials:
            credentials = json.loads(self.ingestion.source_credentials) if isinstance(self.ingestion.source_credentials, str) else self.ingestion.source_credentials
            if credentials.get('aws_access_key_id'):
                spark.conf.set("spark.hadoop.fs.s3a.access.key", credentials['aws_access_key_id'])
                spark.conf.set("spark.hadoop.fs.s3a.secret.key", credentials['aws_secret_access_key'])

        reader = spark.read.format(self.ingestion.format_type)

        # Apply schema if available
        if self.ingestion.schema_json:
            from pyspark.sql.types import StructType
            schema_dict = json.loads(self.ingestion.schema_json) if isinstance(self.ingestion.schema_json, str) else self.ingestion.schema_json
            schema = StructType.fromJson(schema_dict)
            reader = reader.schema(schema)

        # Apply format options
        if self.ingestion.format_options:
            format_options = json.loads(self.ingestion.format_options) if isinstance(self.ingestion.format_options, str) else self.ingestion.format_options
            for key, value in format_options.items():
                reader = reader.option(key, value)

        return reader.load(file_path)

    def _read_file_infer_schema(self, file_path: str) -> DataFrame:
        """
        Read file with schema inference.

        Args:
            file_path: Full S3 path

        Returns:
            Spark DataFrame
        """
        spark = self.spark.connect()

        # Configure AWS credentials for S3 access
        if self.ingestion.source_type == "s3" and self.ingestion.source_credentials:
            credentials = json.loads(self.ingestion.source_credentials) if isinstance(self.ingestion.source_credentials, str) else self.ingestion.source_credentials
            if credentials.get('aws_access_key_id'):
                spark.conf.set("spark.hadoop.fs.s3a.access.key", credentials['aws_access_key_id'])
                spark.conf.set("spark.hadoop.fs.s3a.secret.key", credentials['aws_secret_access_key'])

        reader = spark.read \
            .format(self.ingestion.format_type) \
            .option("inferSchema", "true")

        # Apply format options
        if self.ingestion.format_options:
            format_options = json.loads(self.ingestion.format_options) if isinstance(self.ingestion.format_options, str) else self.ingestion.format_options
            for key, value in format_options.items():
                reader = reader.option(key, value)

        return reader.load(file_path)

    def _write_to_iceberg(self, df: DataFrame, file_path: str):
        """
        Write DataFrame to Iceberg table with schema evolution support.

        Args:
            df: Spark DataFrame to write
            file_path: Path of the file being processed (for schema version tracking)
        """
        table_identifier = f"{self.ingestion.destination_catalog}.{self.ingestion.destination_database}.{self.ingestion.destination_table}"
        spark = self.spark.connect()

        # Check if table exists and handle schema evolution
        table_exists = self._table_exists(spark, table_identifier)

        if table_exists:
            # Get schema evolution strategy
            strategy = getattr(self.ingestion, 'on_schema_change', 'ignore')

            # Apply schema evolution based on strategy (with tracking)
            self._apply_schema_evolution(spark, table_identifier, df, strategy, file_path)

        writer = df.write.format("iceberg")

        # Apply write mode
        if self.ingestion.write_mode:
            writer = writer.mode(self.ingestion.write_mode)
        else:
            writer = writer.mode("append")  # Default

        # Apply partitioning if configured (only on table creation)
        if not table_exists and self.ingestion.partitioning_enabled and self.ingestion.partitioning_columns:
            writer = writer.partitionBy(*self.ingestion.partitioning_columns)

        # For Iceberg, use saveAsTable to create table if it doesn't exist
        # This will create the table on first write and append on subsequent writes
        writer.saveAsTable(table_identifier)
        logger.debug(f"Wrote DataFrame to {table_identifier}")

    def _table_exists(self, spark, table_identifier: str) -> bool:
        """
        Check if Iceberg table exists.

        Args:
            spark: SparkSession
            table_identifier: Full table identifier (catalog.database.table)

        Returns:
            True if table exists, False otherwise
        """
        try:
            # Use DESCRIBE TABLE to check existence
            # This triggers actual table lookup unlike spark.table() which is lazy
            spark.sql(f"DESCRIBE TABLE {table_identifier}")
            return True
        except Exception as e:
            # Table doesn't exist or other error
            logger.debug(f"Table {table_identifier} does not exist: {e}")
            return False

    def _apply_schema_evolution(
        self,
        spark,
        table_identifier: str,
        df: DataFrame,
        strategy: str,
        file_path: str
    ):
        """
        Apply schema evolution based on strategy and record version history.

        Args:
            spark: SparkSession
            table_identifier: Full table identifier
            df: DataFrame with new data schema
            strategy: Schema evolution strategy
            file_path: Path of file that triggered the schema change
        """
        try:
            # Get current table schema
            existing_table = spark.table(table_identifier)
            target_schema = existing_table.schema
            source_schema = df.schema

            # Compare schemas
            comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

            if not comparison.has_changes:
                logger.debug("No schema changes detected")
                return

            logger.info(f"Schema changes detected: {len(comparison.added_columns)} added, "
                       f"{len(comparison.removed_columns)} removed, "
                       f"{len(comparison.modified_columns)} modified")

            # Apply evolution strategy
            SchemaEvolutionService.apply_schema_evolution(
                spark, table_identifier, comparison, strategy
            )

            # Record schema version after successful evolution
            # (only if strategy actually modifies the schema)
            if strategy in ["append_new_columns", "sync_all_columns"]:
                self._record_schema_version(
                    spark,
                    table_identifier,
                    comparison,
                    strategy,
                    file_path
                )

        except Exception as e:
            logger.error(f"Failed to apply schema evolution for {table_identifier}: {e}")
            raise

    def _record_schema_version(
        self,
        spark,
        table_identifier: str,
        comparison: SchemaComparison,
        strategy: str,
        file_path: str
    ):
        """
        Record schema version change in database.

        Args:
            spark: SparkSession
            table_identifier: Full table identifier
            comparison: SchemaComparison with detected changes
            strategy: Schema evolution strategy that was applied
            file_path: Path of file that triggered the change
        """
        try:
            # Get next version number
            next_version = self.schema_version_repo.get_next_version_number(self.ingestion.id)

            # Get current schema after evolution
            current_table = spark.table(table_identifier)
            current_schema = current_table.schema

            # Convert schema to JSON
            schema_json = current_schema.jsonValue()

            # Convert schema changes to dict list
            changes = [change.to_dict() for change in comparison.get_changes()]

            # Create schema version record
            schema_version = self.schema_version_repo.create_version(
                ingestion_id=self.ingestion.id,
                version=next_version,
                schema_json=schema_json,
                changes=changes,
                strategy_applied=strategy,
                affected_files=[file_path]
            )

            # Update ingestion's current schema version
            self.ingestion.schema_version = next_version
            self.db.commit()

            logger.info(
                f"Recorded schema version {next_version} for ingestion {self.ingestion.id}. "
                f"Changes: {len(changes)} modifications triggered by {file_path}"
            )

        except Exception as e:
            logger.error(f"Failed to record schema version: {e}", exc_info=True)
            # Don't fail the entire ingestion if version recording fails
            # Just log the error and continue
