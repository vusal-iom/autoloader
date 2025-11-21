"""
Schema Evolution Service - Handles schema comparison and evolution strategies.
"""

from typing import List, Dict, Tuple, Optional
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, DataType, ArrayType, MapType
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SchemaChangeType(str, Enum):
    """Types of schema changes."""
    NEW_COLUMN = "NEW_COLUMN"
    REMOVED_COLUMN = "REMOVED_COLUMN"
    TYPE_CHANGE = "TYPE_CHANGE"
    NEW_NESTED_COLUMN = "NEW_NESTED_COLUMN"


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
    nested_field_additions: List[Tuple[str, DataType]]
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

        for path, data_type in self.nested_field_additions:
            changes.append(SchemaChange(
                change_type=SchemaChangeType.NEW_NESTED_COLUMN,
                column_name=path,
                new_type=data_type.simpleString()
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
            elif change.change_type == SchemaChangeType.NEW_NESTED_COLUMN:
                msg_parts.append(f"  + {change.column_name} ({change.new_type}) [NEW NESTED]")

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

        # Detect type changes and nested additions
        modified_columns: List[Tuple[str, str, str]] = []
        nested_field_additions: List[Tuple[str, DataType]] = []

        def compare_types(source_type: DataType, target_type: DataType, parent_path: str) -> bool:
            """Return True if there is an incompatible type change."""
            if isinstance(source_type, StructType) and isinstance(target_type, StructType):
                return compare_struct_types(source_type, target_type, parent_path)
            if isinstance(source_type, ArrayType) and isinstance(target_type, ArrayType):
                return compare_array_types(source_type, target_type, parent_path)
            if isinstance(source_type, MapType) and isinstance(target_type, MapType):
                return compare_map_types(source_type, target_type, parent_path)

            return source_type.simpleString() != target_type.simpleString()

        def compare_struct_types(source_struct: StructType, target_struct: StructType, parent_path: str) -> bool:
            type_changed = False
            source_fields_map = {field.name.lower(): field for field in source_struct.fields}
            target_fields_map = {field.name.lower(): field for field in target_struct.fields}

            for name, source_field in source_fields_map.items():
                child_path = f"{parent_path}.{source_field.name}" if parent_path else source_field.name
                if name not in target_fields_map:
                    nested_field_additions.append((child_path, source_field.dataType))
                    continue
                target_field = target_fields_map[name]
                if compare_types(source_field.dataType, target_field.dataType, child_path):
                    type_changed = True

            for name in target_fields_map.keys():
                if name not in source_fields_map:
                    return True

            return type_changed

        def compare_array_types(source_array: ArrayType, target_array: ArrayType, parent_path: str) -> bool:
            element_path = f"{parent_path}.element" if parent_path else "element"
            element_change = compare_types(source_array.elementType, target_array.elementType, element_path)
            contains_null_diff = source_array.containsNull != target_array.containsNull
            return element_change or contains_null_diff

        def compare_map_types(source_map: MapType, target_map: MapType, parent_path: str) -> bool:
            if source_map.keyType.simpleString() != target_map.keyType.simpleString():
                return True
            value_path = f"{parent_path}.value" if parent_path else "value"
            value_change = compare_types(source_map.valueType, target_map.valueType, value_path)
            return value_change or source_map.valueContainsNull != target_map.valueContainsNull

        for name in source_fields.keys():
            if name in target_fields:
                source_field = source_fields[name]
                target_field = target_fields[name]
                if compare_types(source_field.dataType, target_field.dataType, source_field.name):
                    modified_columns.append((
                        source_field.name,
                        target_field.dataType.simpleString(),
                        source_field.dataType.simpleString()
                    ))

        has_changes = bool(added_columns or removed_columns or modified_columns or nested_field_additions)

        return SchemaComparison(
            added_columns=added_columns,
            removed_columns=removed_columns,
            modified_columns=modified_columns,
            nested_field_additions=nested_field_additions,
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
        if not comparison.added_columns and not comparison.nested_field_additions:
            logger.debug("No new columns to add")
            return

        if comparison.added_columns:
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

        SchemaEvolutionService._apply_nested_field_additions(
            spark,
            table_identifier,
            comparison.nested_field_additions
        )

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

        SchemaEvolutionService._apply_nested_field_additions(
            spark,
            table_identifier,
            comparison.nested_field_additions
        )

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
    def _apply_nested_field_additions(
        spark,
        table_identifier: str,
        nested_field_additions: List[Tuple[str, DataType]]
    ) -> None:
        """Apply nested field additions using Iceberg's dotted path syntax."""
        if not nested_field_additions:
            logger.debug("No nested fields to add")
            return

        logger.info(f"Adding {len(nested_field_additions)} nested fields to {table_identifier}")

        for path, data_type in nested_field_additions:
            col_type = SchemaEvolutionService._format_data_type_for_sql(data_type)

            try:
                alter_sql = f"ALTER TABLE {table_identifier} ADD COLUMN {path} {col_type}"
                logger.debug(f"Executing: {alter_sql}")
                spark.sql(alter_sql)
                logger.info(f"Added nested field: {path} ({col_type})")
            except Exception as e:
                logger.error(f"Failed to add nested field {path}: {e}")
                raise

    @staticmethod
    def _format_data_type_for_sql(data_type: DataType) -> str:
        """Convert a Spark DataType into an Iceberg SQL compatible string."""
        if isinstance(data_type, StructType):
            inner = ", ".join(
                f"{field.name}: {SchemaEvolutionService._format_data_type_for_sql(field.dataType)}"
                for field in data_type.fields
            )
            return f"STRUCT<{inner}>"

        if isinstance(data_type, ArrayType):
            element_type = SchemaEvolutionService._format_data_type_for_sql(data_type.elementType)
            return f"ARRAY<{element_type}>"

        if isinstance(data_type, MapType):
            key_type = SchemaEvolutionService._format_data_type_for_sql(data_type.keyType)
            value_type = SchemaEvolutionService._format_data_type_for_sql(data_type.valueType)
            return f"MAP<{key_type}, {value_type}>"

        return data_type.simpleString().upper()

    @staticmethod
    def align_dataframe_to_target_schema(df: DataFrame, target_schema: StructType) -> DataFrame:
        """
        Align a DataFrame to the target schema by:
        - Dropping extra columns not present in the target table
        - Adding missing target columns with nulls
        - Ordering/aliasing columns to match the target schema
        """
        source_columns_map = {col.lower(): col for col in df.columns}

        aligned_columns = []
        for field in target_schema.fields:
            source_col_name = source_columns_map.get(field.name.lower())
            if source_col_name:
                aligned_columns.append(
                    F.col(source_col_name).cast(field.dataType).alias(field.name)
                )
            else:
                aligned_columns.append(
                    F.lit(None).cast(field.dataType).alias(field.name)
                )

        return df.select(*aligned_columns)
