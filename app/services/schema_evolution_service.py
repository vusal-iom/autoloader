"""
Schema Evolution Service - Handles schema comparison and evolution strategies.
"""

from typing import List, Dict, Tuple, Optional
from pyspark.sql.types import StructType, StructField, DataType
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SchemaChangeType(str, Enum):
    """Types of schema changes."""
    NEW_COLUMN = "NEW_COLUMN"
    REMOVED_COLUMN = "REMOVED_COLUMN"
    TYPE_CHANGE = "TYPE_CHANGE"


@dataclass
class SchemaChange:
    """Represents a single schema change."""
    change_type: SchemaChangeType
    column_name: str
    old_type: Optional[str] = None
    new_type: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "change_type": self.change_type.value,
            "column_name": self.column_name,
            "old_type": self.old_type,
            "new_type": self.new_type
        }


@dataclass
class SchemaComparison:
    """Result of schema comparison."""
    added_columns: List[StructField]
    removed_columns: List[StructField]
    modified_columns: List[Tuple[str, str, str]]  # (name, old_type, new_type)
    has_changes: bool

    def get_changes(self) -> List[SchemaChange]:
        """Get list of SchemaChange objects."""
        changes = []

        for field in self.added_columns:
            changes.append(SchemaChange(
                change_type=SchemaChangeType.NEW_COLUMN,
                column_name=field.name,
                new_type=field.dataType.simpleString()
            ))

        for field in self.removed_columns:
            changes.append(SchemaChange(
                change_type=SchemaChangeType.REMOVED_COLUMN,
                column_name=field.name,
                old_type=field.dataType.simpleString()
            ))

        for col_name, old_type, new_type in self.modified_columns:
            changes.append(SchemaChange(
                change_type=SchemaChangeType.TYPE_CHANGE,
                column_name=col_name,
                old_type=old_type,
                new_type=new_type
            ))

        return changes


class IncompatibleTypeChangeError(Exception):
    """Raised when an incompatible type change is detected."""
    def __init__(self, column: str, old_type: str, new_type: str):
        self.column = column
        self.old_type = old_type
        self.new_type = new_type
        self.message = f"Cannot change column '{column}' from {old_type} to {new_type}"
        super().__init__(self.message)


class SchemaMismatchError(Exception):
    """Raised when schema mismatch detected and strategy is 'fail'."""
    def __init__(self, comparison: SchemaComparison):
        self.comparison = comparison
        changes = comparison.get_changes()

        msg_parts = ["Schema Mismatch Detected!\n\nChanges:"]
        for change in changes:
            if change.change_type == SchemaChangeType.NEW_COLUMN:
                msg_parts.append(f"  + {change.column_name} ({change.new_type}) [NEW]")
            elif change.change_type == SchemaChangeType.REMOVED_COLUMN:
                msg_parts.append(f"  - {change.column_name} ({change.old_type}) [REMOVED]")
            elif change.change_type == SchemaChangeType.TYPE_CHANGE:
                msg_parts.append(f"  ~ {change.column_name}: {change.old_type} → {change.new_type} [TYPE CHANGED]")

        msg_parts.append("\nAction Required:")
        msg_parts.append("1. Update on_schema_change setting to 'append_new_columns' or 'sync_all_columns'")
        msg_parts.append("2. Perform manual schema migration")
        msg_parts.append("3. Review and approve schema changes")

        self.message = "\n".join(msg_parts)
        super().__init__(self.message)


class SchemaEvolutionService:
    """Service for handling schema evolution."""

    @staticmethod
    def compare_schemas(source_schema: StructType, target_schema: StructType) -> SchemaComparison:
        """
        Compare two Spark schemas and detect differences.

        Args:
            source_schema: Schema from source files
            target_schema: Schema from target table

        Returns:
            SchemaComparison object with detected changes
        """
        # Convert to dictionaries for easier comparison (case-insensitive)
        source_fields = {field.name.lower(): field for field in source_schema.fields}
        target_fields = {field.name.lower(): field for field in target_schema.fields}

        # Detect added columns (in source but not in target)
        added_columns = [
            field for name, field in source_fields.items()
            if name not in target_fields
        ]

        # Detect removed columns (in target but not in source)
        removed_columns = [
            field for name, field in target_fields.items()
            if name not in source_fields
        ]

        # Detect type changes
        modified_columns = []
        for name in source_fields.keys():
            if name in target_fields:
                source_type = source_fields[name].dataType.simpleString()
                target_type = target_fields[name].dataType.simpleString()
                if source_type != target_type:
                    modified_columns.append((
                        source_fields[name].name,  # Use original case
                        target_type,
                        source_type
                    ))

        has_changes = bool(added_columns or removed_columns or modified_columns)

        return SchemaComparison(
            added_columns=added_columns,
            removed_columns=removed_columns,
            modified_columns=modified_columns,
            has_changes=has_changes
        )

    @staticmethod
    def is_type_change_compatible(old_type: str, new_type: str) -> bool:
        """
        Check if a type change is compatible (can be done without data loss).

        Compatible changes:
        - int → long
        - float → double
        - string → varchar (and vice versa)
        - Widening numeric conversions

        Args:
            old_type: Old data type
            new_type: New data type

        Returns:
            True if compatible, False otherwise
        """

        old_type_lower = old_type.lower()
        new_type_lower = new_type.lower()

        # Same type is always compatible
        if old_type == new_type:
            return True

        # Define compatible type transitions
        compatible_transitions = {
            'int': ['long', 'bigint'],
            'integer': ['long', 'bigint'],
            'float': ['double'],
            'string': ['varchar'],
            'varchar': ['string'],
            'date': ['timestamp'],
        }

        # Check equality after case normalization
        if old_type_lower == new_type_lower:
            return True

        return new_type_lower in compatible_transitions.get(old_type_lower, [])

    @staticmethod
    def apply_schema_evolution(
        spark,
        table_identifier: str,
        comparison: SchemaComparison,
        strategy: str
    ) -> None:
        """
        Apply schema evolution based on strategy.

        Args:
            spark: SparkSession
            table_identifier: Full table identifier (catalog.database.table)
            comparison: SchemaComparison with detected changes
            strategy: Schema evolution strategy (ignore, append_new_columns, sync_all_columns, fail)

        Raises:
            SchemaMismatchError: If strategy is 'fail' and changes detected
            IncompatibleTypeChangeError: If incompatible type change detected
        """
        if not comparison.has_changes:
            logger.debug("No schema changes detected")
            return

        logger.info(f"Applying schema evolution strategy: {strategy}")

        if strategy == "ignore":
            logger.info("Strategy 'ignore': Schema changes will be ignored")
            return

        if strategy == "fail":
            logger.error("Schema mismatch detected with 'fail' strategy")
            raise SchemaMismatchError(comparison)

        if strategy == "append_new_columns":
            SchemaEvolutionService._apply_append_new_columns(
                spark, table_identifier, comparison
            )

        elif strategy == "sync_all_columns":
            SchemaEvolutionService._apply_sync_all_columns(
                spark, table_identifier, comparison
            )

    @staticmethod
    def _apply_append_new_columns(
        spark,
        table_identifier: str,
        comparison: SchemaComparison
    ) -> None:
        """
        Apply append_new_columns strategy.

        Adds new columns from source, but keeps removed columns.
        """
        if not comparison.added_columns:
            logger.debug("No new columns to add")
            return

        logger.info(f"Adding {len(comparison.added_columns)} new columns to {table_identifier}")

        for field in comparison.added_columns:
            col_name = field.name
            col_type = field.dataType.simpleString()

            try:
                alter_sql = f"ALTER TABLE {table_identifier} ADD COLUMN {col_name} {col_type}"
                logger.debug(f"Executing: {alter_sql}")
                spark.sql(alter_sql)
                logger.info(f"Added column: {col_name} ({col_type})")
            except Exception as e:
                logger.error(f"Failed to add column {col_name}: {e}")
                raise

    @staticmethod
    def _apply_sync_all_columns(
        spark,
        table_identifier: str,
        comparison: SchemaComparison
    ) -> None:
        """
        Apply sync_all_columns strategy.

        Adds new columns, removes old columns, and updates types if compatible.
        """
        # Add new columns
        if comparison.added_columns:
            logger.info(f"Adding {len(comparison.added_columns)} new columns")
            for field in comparison.added_columns:
                col_name = field.name
                col_type = field.dataType.simpleString()

                try:
                    alter_sql = f"ALTER TABLE {table_identifier} ADD COLUMN {col_name} {col_type}"
                    logger.debug(f"Executing: {alter_sql}")
                    spark.sql(alter_sql)
                    logger.info(f"Added column: {col_name} ({col_type})")
                except Exception as e:
                    logger.error(f"Failed to add column {col_name}: {e}")
                    raise

        # Remove old columns
        if comparison.removed_columns:
            logger.info(f"Removing {len(comparison.removed_columns)} old columns")
            for field in comparison.removed_columns:
                col_name = field.name

                try:
                    alter_sql = f"ALTER TABLE {table_identifier} DROP COLUMN {col_name}"
                    logger.debug(f"Executing: {alter_sql}")
                    spark.sql(alter_sql)
                    logger.info(f"Removed column: {col_name}")
                except Exception as e:
                    logger.error(f"Failed to remove column {col_name}: {e}")
                    raise

        # Update column types (if compatible)
        if comparison.modified_columns:
            logger.info(f"Checking {len(comparison.modified_columns)} type changes")
            for col_name, old_type, new_type in comparison.modified_columns:
                if SchemaEvolutionService.is_type_change_compatible(old_type, new_type):
                    try:
                        alter_sql = f"ALTER TABLE {table_identifier} ALTER COLUMN {col_name} TYPE {new_type}"
                        logger.debug(f"Executing: {alter_sql}")
                        spark.sql(alter_sql)
                        logger.info(f"Changed column type: {col_name} from {old_type} to {new_type}")
                    except Exception as e:
                        logger.error(f"Failed to alter column type {col_name}: {e}")
                        raise
                else:
                    raise IncompatibleTypeChangeError(col_name, old_type, new_type)

    @staticmethod
    def validate_strategy_compatibility(format_type: str, strategy: str) -> None:
        """
        Validate that strategy is compatible with table format.

        Args:
            format_type: Table format (iceberg, parquet, etc.)
            strategy: Schema evolution strategy

        Raises:
            ValueError: If strategy is not compatible with format
        """
        # Evolution strategies require Iceberg
        evolution_strategies = ["append_new_columns", "sync_all_columns"]

        if strategy in evolution_strategies and format_type.lower() != "iceberg":
            raise ValueError(
                f"Strategy '{strategy}' requires Iceberg table format. "
                f"Current format: {format_type}. "
                f"Please use 'ignore' or 'fail' strategy, or change destination format to Iceberg."
            )
