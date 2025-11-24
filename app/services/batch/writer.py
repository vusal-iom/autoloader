import logging
from typing import Optional
from pyspark.sql import DataFrame
from app.spark.connect_client import SparkConnectClient
from app.models.domain import Ingestion
from app.services.schema_evolution_service import SchemaEvolutionService, SchemaComparison
from app.repositories.schema_version_repository import SchemaVersionRepository
from app.services.batch.errors import FileProcessingError, FileErrorCategory

logger = logging.getLogger(__name__)

class IcebergTableWriter:
    """
    Service for writing DataFrames to Iceberg tables.
    Handles table existence checks, schema evolution, and version recording.
    """

    def __init__(
        self,
        spark_client: SparkConnectClient,
        schema_version_repo: SchemaVersionRepository,
        db_session=None # Optional, depending on how repo is used
    ):
        self.spark_client = spark_client
        self.schema_version_repo = schema_version_repo
        self.db = db_session

    def _classify_error(self, error: Exception) -> dict:
        """Classify writer-specific errors."""
        message = str(error) if error else "Unknown error"
        lower = message.lower()

        category = FileErrorCategory.UNKNOWN
        retryable = True
        user_message = message

        if "schema" in lower or "cannot resolve" in lower or "analysisexception" in lower:
            category = FileErrorCategory.SCHEMA_MISMATCH
            retryable = False
            user_message = "Schema mismatch detected. Align schema or enable evolution before retrying."
        elif "write" in lower or "iceberg" in lower:
            category = FileErrorCategory.WRITE_FAILURE
            retryable = True
            user_message = "Failed to write to the destination table. Retry or check destination health."
        elif "connection" in lower or "timeout" in lower or "timed out" in lower:
            category = FileErrorCategory.CONNECTIVITY
            retryable = True
            user_message = "Temporary connectivity issue. Safe to retry."

        return {
            "category": category,
            "retryable": retryable,
            "user_message": user_message
        }

    def _wrap_error(self, file_path: str, error: Exception) -> FileProcessingError:
        """Wrap writer exceptions into FileProcessingError."""
        classification = self._classify_error(error)
        return FileProcessingError(
            category=classification["category"],
            retryable=classification["retryable"],
            user_message=classification["user_message"],
            raw_error=str(error),
            file_path=file_path
        )

    def write(self, df: DataFrame, file_path: str, ingestion: Ingestion):
        """
        Write DataFrame to Iceberg table with schema evolution support.

        Args:
            df: Spark DataFrame to write
            file_path: Path of the file being processed
            ingestion: Ingestion configuration
        """
        table_identifier = f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}"
        spark = self.spark_client.connect()

        try:
            # Check if table exists and handle schema evolution
            table_exists = self._table_exists(spark, table_identifier)

            if table_exists:
                # Get schema evolution strategy
                strategy = getattr(ingestion, 'on_schema_change', 'ignore')

                # Apply schema evolution based on strategy (with tracking)
                df = self._apply_schema_evolution(spark, table_identifier, df, strategy, file_path, ingestion)

            writer = df.write.format("iceberg")

            # Apply write mode
            if ingestion.write_mode:
                writer = writer.mode(ingestion.write_mode)
            else:
                writer = writer.mode("append")  # Default

            # Apply partitioning if configured (only on table creation)
            if not table_exists and ingestion.partitioning_enabled and ingestion.partitioning_columns:
                writer = writer.partitionBy(*ingestion.partitioning_columns)

            # For Iceberg, use saveAsTable to create table if it doesn't exist
            writer.saveAsTable(table_identifier)
            logger.debug(f"Wrote DataFrame to {table_identifier}")

        except Exception as e:
            raise self._wrap_error(file_path, e)

    def _table_exists(self, spark, table_identifier: str) -> bool:
        """Check if Iceberg table exists."""
        try:
            spark.sql(f"DESCRIBE TABLE {table_identifier}")
            return True
        except Exception as e:
            logger.debug(f"Table {table_identifier} does not exist: {e}")
            return False

    def _apply_schema_evolution(
        self,
        spark,
        table_identifier: str,
        df: DataFrame,
        strategy: str,
        file_path: str,
        ingestion: Ingestion
    ) -> DataFrame:
        """Apply schema evolution based on strategy and record version history."""
        try:
            # Get current table schema
            existing_table = spark.table(table_identifier)
            target_schema = existing_table.schema
            source_schema = df.schema

            # Compare schemas
            comparison = SchemaEvolutionService.compare_schemas(source_schema, target_schema)

            if not comparison.has_changes:
                logger.debug("No schema changes detected")
                return df

            logger.info(f"Schema changes detected: {len(comparison.added_columns)} added, "
                       f"{len(comparison.removed_columns)} removed, "
                       f"{len(comparison.modified_columns)} modified")

            # For 'ignore' strategy, select only columns that exist in target table
            if strategy == "ignore":
                df = SchemaEvolutionService.align_dataframe_to_target_schema(df, target_schema)
                logger.info(
                    "Strategy 'ignore': Dropped extra columns and aligned missing columns to target schema"
                )
                return df

            # Apply evolution strategy for other strategies
            SchemaEvolutionService.apply_schema_evolution(
                spark, table_identifier, comparison, strategy
            )

            # Record schema version after successful evolution
            if strategy in ["append_new_columns", "sync_all_columns"]:
                self._record_schema_version(
                    spark,
                    table_identifier,
                    comparison,
                    strategy,
                    file_path,
                    ingestion
                )

            return df

        except Exception as e:
            logger.error(f"Failed to apply schema evolution for {table_identifier}: {e}")
            raise

    def _record_schema_version(
        self,
        spark,
        table_identifier: str,
        comparison: SchemaComparison,
        strategy: str,
        file_path: str,
        ingestion: Ingestion
    ):
        """Record schema version change in database."""
        try:
            # Get next version number based on ingestion's current schema version
            next_version = ingestion.schema_version + 1

            # Get current schema after evolution
            current_table = spark.table(table_identifier)
            current_schema = current_table.schema

            # Convert schema to JSON
            schema_json = current_schema.jsonValue()

            # Convert schema changes to dict list
            changes = [change.to_dict() for change in comparison.get_changes()]

            # Create schema version record
            self.schema_version_repo.create_version(
                ingestion_id=ingestion.id,
                version=next_version,
                schema_json=schema_json,
                changes=changes,
                strategy_applied=strategy,
                affected_files=[file_path]
            )

            # Update ingestion's current schema version
            ingestion.schema_version = next_version
            if self.db:
                self.db.commit()

            logger.info(
                f"Recorded schema version {next_version} for ingestion {ingestion.id}. "
                f"Changes: {len(changes)} modifications triggered by {file_path}"
            )

        except Exception as e:
            logger.error(f"Failed to record schema version: {e}", exc_info=True)
            # Don't fail the entire ingestion if version recording fails
