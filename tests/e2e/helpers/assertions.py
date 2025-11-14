"""
E2E Test Assertions.

Provides verification functions for test assertions.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession

from tests.helpers.logger import TestLogger

from .constants import SUCCESS_STATUSES


# =============================================================================
# Run Metric Assertions
# =============================================================================

def assert_run_metrics(
    run: Dict[str, Any],
    expected_files: Optional[int] = None,
    expected_records: Optional[int] = None,
    expected_errors: int = 0,
    logger: Optional[TestLogger] = None
):
    """
    Assert run metrics match expectations.

    Args:
        run: Run data dict
        expected_files: Expected number of files processed (None to skip)
        expected_records: Expected number of records ingested (None to skip)
        expected_errors: Expected number of errors (default: 0)
        logger: Optional logger for detailed output
    """
    assert run["status"] in SUCCESS_STATUSES, f"Run status: {run['status']}"
    assert run["started_at"] is not None, "Run should have started_at timestamp"
    assert run["ended_at"] is not None, "Run should have ended_at timestamp"

    metrics = run.get("metrics", {})

    if logger:
        logger.metric("Status", run["status"])
        logger.metric("Files Processed", metrics.get("files_processed", 0))
        logger.metric("Records Ingested", metrics.get("records_ingested", 0))
        logger.metric("Bytes Read", metrics.get("bytes_read", 0))
        logger.metric("Duration", f"{metrics.get('duration_seconds', 0)}s")

    if expected_files is not None:
        actual_files = metrics.get("files_processed", 0)
        assert actual_files == expected_files, \
            f"Expected {expected_files} files processed, got {actual_files}"

    if expected_records is not None:
        actual_records = metrics.get("records_ingested", 0)
        assert actual_records == expected_records, \
            f"Expected {expected_records} records ingested, got {actual_records}"

    errors = run.get('errors') or []
    assert len(errors) == expected_errors, \
        f"Expected {expected_errors} errors, got {len(errors)}: {errors}"


# =============================================================================
# Table Data Verification
# =============================================================================

def verify_table_data(
    spark_session: SparkSession,
    table_identifier: str,
    expected_count: Optional[int] = None,
    expected_fields: Optional[List[str]] = None,
    unexpected_fields: Optional[List[str]] = None,
    check_duplicates: bool = True,
    logger: Optional[TestLogger] = None
):
    """
    Verify Iceberg table data.

    Args:
        spark_session: Spark session
        table_identifier: Full table identifier (catalog.database.table)
        expected_count: Expected record count (None to skip)
        expected_fields: List of fields that must be present (None to skip)
        unexpected_fields: List of fields that must NOT be present (None to skip)
        check_duplicates: Check for duplicate IDs (default: True)
        logger: Optional logger for detailed output

    Returns:
        DataFrame for further verification
    """
    df = spark_session.table(table_identifier)

    # Count
    if expected_count is not None:
        actual_count = df.count()
        if logger:
            logger.metric("Record Count", actual_count)
        assert actual_count == expected_count, \
            f"Expected {expected_count} records, got {actual_count}"

    # Schema
    schema = df.schema
    field_names = [field.name for field in schema.fields]

    if logger:
        logger.metric("Schema Fields", field_names)

    if expected_fields:
        for field in expected_fields:
            assert field in field_names, f"Missing expected field: {field}"

    if unexpected_fields:
        for field in unexpected_fields:
            assert field not in field_names, f"Unexpected field present: {field}"

    # Duplicates
    if check_duplicates:
        record_count = df.count()
        distinct_count = df.select("id").distinct().count()
        if logger:
            logger.metric("Distinct IDs", distinct_count)

        assert distinct_count == record_count, \
            f"Found {record_count - distinct_count} duplicate IDs"

    return df


def verify_schema_evolution(
    spark_session: SparkSession,
    table_identifier: str,
    old_record_filter: str,
    new_record_filter: str,
    old_count: int,
    new_count: int,
    new_fields: List[str],
    logger: Optional[TestLogger] = None
):
    """
    Verify schema evolution backward and forward compatibility.

    Args:
        spark_session: Spark session
        table_identifier: Full table identifier
        old_record_filter: SQL filter for old records (e.g., "id < 3000")
        new_record_filter: SQL filter for new records (e.g., "id >= 3000")
        old_count: Expected count of old records
        new_count: Expected count of new records
        new_fields: List of fields added in evolution
        logger: Optional logger
    """
    df = spark_session.table(table_identifier)

    # Verify old records have NULL for new fields (backward compatibility)
    _verify_backward_compatibility(
        df=df,
        old_record_filter=old_record_filter,
        old_count=old_count,
        new_fields=new_fields,
        logger=logger
    )

    # Verify new records have values for new fields (forward compatibility)
    _verify_forward_compatibility(
        df=df,
        new_record_filter=new_record_filter,
        new_count=new_count,
        new_fields=new_fields,
        logger=logger
    )


def _verify_backward_compatibility(df, old_record_filter, old_count, new_fields, logger):
    """Verify old records have NULL for new fields."""
    old_records_df = df.filter(old_record_filter)
    actual_old_count = old_records_df.count()

    assert actual_old_count == old_count, \
        f"Expected {old_count} old records, got {actual_old_count}"

    for field in new_fields:
        null_count = old_records_df.filter(f"{field} IS NULL").count()
        assert null_count == old_count, \
            f"Expected all {old_count} old records to have NULL {field}, got {null_count}"

    if logger:
        logger.success(f"Backward compatibility: {old_count} old records have NULL for new fields")


def _verify_forward_compatibility(df, new_record_filter, new_count, new_fields, logger):
    """Verify new records have values for new fields."""
    new_records_df = df.filter(new_record_filter)
    actual_new_count = new_records_df.count()

    assert actual_new_count == new_count, \
        f"Expected {new_count} new records, got {actual_new_count}"

    for field in new_fields:
        non_null_count = new_records_df.filter(f"{field} IS NOT NULL").count()
        assert non_null_count == new_count, \
            f"Expected all {new_count} new records to have {field}, got {non_null_count}"

    if logger:
        logger.success(f"Forward compatibility: {new_count} new records have values for new fields")


# =============================================================================
# Content Verification
# =============================================================================

def verify_table_content(
    df_or_table: Any,
    expected_data: List[Dict[str, Any]],
    spark_session: Optional[SparkSession] = None,
    columns: Optional[List[str]] = None,
    ignore_column_order: bool = True,
    ignore_row_order: bool = True,
    logger: Optional[TestLogger] = None
):
    """
    Verify DataFrame or table contains exactly the expected data.

    Compares actual data against expected rows with comprehensive diff output
    on mismatch. Verifies all values including nulls.

    Args:
        df_or_table: Either a DataFrame or table identifier string
        expected_data: List of dicts representing expected rows
        spark_session: Spark session (required if df_or_table is a string)
        columns: Subset of columns to compare (None = all columns)
        ignore_column_order: Whether to ignore column order (default: True)
        ignore_row_order: Whether to sort rows before comparison (default: True)
        logger: Optional logger for detailed output

    Returns:
        DataFrame for chaining

    Raises:
        AssertionError: If data does not match, with detailed diff output

    Example:
        verify_table_content(
            df,
            expected_data=[
                {"id": 1, "name": "Alice", "email": None},
                {"id": 2, "name": "Bob", "email": "bob@example.com"}
            ],
            logger=logger
        )
    """
    from pyspark.sql.types import StructType

    # Get DataFrame from input
    if isinstance(df_or_table, str):
        if spark_session is None:
            raise ValueError("spark_session is required when df_or_table is a table identifier")
        df = spark_session.table(df_or_table)
        table_name = df_or_table
    elif hasattr(df_or_table, 'schema') and hasattr(df_or_table, 'collect'):
        # Check for DataFrame duck-typing (works for both regular and Spark Connect DataFrames)
        df = df_or_table
        table_name = "DataFrame"
        # Get spark session from DataFrame if not provided
        if spark_session is None:
            spark_session = df.sparkSession
    else:
        raise ValueError(f"df_or_table must be DataFrame or string, got {type(df_or_table)}")

    if logger:
        logger.step(f"Verifying content of {table_name}")

    # Get columns to compare
    if columns is None:
        columns = df.columns
    else:
        # Validate requested columns exist
        missing_cols = set(columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

    # Select and potentially reorder columns
    if ignore_column_order:
        columns = sorted(columns)

    df_to_compare = df.select(*columns)

    # Convert expected data to DataFrame
    if not expected_data:
        # For empty expected data, create empty DataFrame with same schema as actual
        expected_df = spark_session.createDataFrame([], df_to_compare.schema)
    else:
        # Create DataFrame with schema from actual data to handle NULL values correctly
        # This ensures type compatibility when expected data has all-NULL columns
        expected_df = spark_session.createDataFrame(expected_data, schema=df_to_compare.schema)
        if ignore_column_order:
            expected_df = expected_df.select(*sorted(expected_df.columns))

    # Sort rows if needed (skip if no rows or no columns)
    if ignore_row_order and columns:
        # Sort by all columns for deterministic ordering
        df_to_compare = df_to_compare.sort(*columns)
        expected_df = expected_df.sort(*columns)

    # Collect data
    actual_rows = df_to_compare.collect()
    expected_rows = expected_df.collect()

    actual_count = len(actual_rows)
    expected_count = len(expected_rows)

    if logger:
        logger.metric("Expected Rows", expected_count)
        logger.metric("Actual Rows", actual_count)

    # Compare
    if actual_count != expected_count or actual_rows != expected_rows:
        _generate_detailed_diff(
            actual_rows=actual_rows,
            expected_rows=expected_rows,
            columns=columns,
            table_name=table_name
        )

    if logger:
        logger.success(f"Content verification passed: {actual_count} rows match exactly")

    return df


def _generate_detailed_diff(actual_rows, expected_rows, columns, table_name):
    """
    Generate detailed diff output showing mismatches.

    Shows:
    - Row count mismatch
    - Missing rows (in expected but not actual)
    - Extra rows (in actual but not expected)
    - Value mismatches (rows present in both but with different values)
    """
    actual_count = len(actual_rows)
    expected_count = len(expected_rows)

    # Convert rows to comparable format (dict with None for NULL)
    def row_to_dict(row, cols):
        return {col: row[col] for col in cols}

    actual_dicts = [row_to_dict(row, columns) for row in actual_rows]
    expected_dicts = [row_to_dict(row, columns) for row in expected_rows]

    # Build error message
    error_lines = [
        f"\nContent verification failed for {table_name}",
        f"Expected {expected_count} rows, got {actual_count} rows",
        ""
    ]

    # Find missing rows (in expected but not actual)
    missing_rows = []
    for exp_dict in expected_dicts:
        if exp_dict not in actual_dicts:
            missing_rows.append(exp_dict)

    # Find extra rows (in actual but not expected)
    extra_rows = []
    for act_dict in actual_dicts:
        if act_dict not in expected_dicts:
            extra_rows.append(act_dict)

    # Show missing rows
    if missing_rows:
        error_lines.append(f"MISSING ROWS ({len(missing_rows)} rows in expected but not in actual):")
        for i, row in enumerate(missing_rows[:5], 1):  # Show first 5
            error_lines.append(f"  {i}. {_format_row_dict(row)}")
        if len(missing_rows) > 5:
            error_lines.append(f"  ... and {len(missing_rows) - 5} more")
        error_lines.append("")

    # Show extra rows
    if extra_rows:
        error_lines.append(f"EXTRA ROWS ({len(extra_rows)} rows in actual but not in expected):")
        for i, row in enumerate(extra_rows[:5], 1):  # Show first 5
            error_lines.append(f"  {i}. {_format_row_dict(row)}")
        if len(extra_rows) > 5:
            error_lines.append(f"  ... and {len(extra_rows) - 5} more")
        error_lines.append("")

    # Show side-by-side comparison for first few rows
    if actual_count > 0 and expected_count > 0:
        error_lines.append("FIRST 3 ROWS COMPARISON:")
        max_rows = min(3, actual_count, expected_count)
        for i in range(max_rows):
            exp_dict = expected_dicts[i] if i < len(expected_dicts) else {}
            act_dict = actual_dicts[i] if i < len(actual_dicts) else {}

            error_lines.append(f"  Row {i + 1}:")
            error_lines.append(f"    Expected: {_format_row_dict(exp_dict)}")
            error_lines.append(f"    Actual:   {_format_row_dict(act_dict)}")

            # Highlight differences
            if exp_dict != act_dict:
                diffs = []
                all_keys = set(exp_dict.keys()) | set(act_dict.keys())
                for key in sorted(all_keys):
                    exp_val = exp_dict.get(key, "<missing>")
                    act_val = act_dict.get(key, "<missing>")
                    if exp_val != act_val:
                        diffs.append(f"{key}: {exp_val} != {act_val}")
                if diffs:
                    error_lines.append(f"    Diff:     {', '.join(diffs)}")
            error_lines.append("")

    # Show all expected data if small
    if expected_count <= 10:
        error_lines.append("EXPECTED DATA (all rows):")
        for i, row in enumerate(expected_dicts, 1):
            error_lines.append(f"  {i}. {_format_row_dict(row)}")
        error_lines.append("")

    # Show all actual data if small
    if actual_count <= 10:
        error_lines.append("ACTUAL DATA (all rows):")
        for i, row in enumerate(actual_dicts, 1):
            error_lines.append(f"  {i}. {_format_row_dict(row)}")
        error_lines.append("")

    raise AssertionError("\n".join(error_lines))


def _format_row_dict(row_dict: Dict[str, Any]) -> str:
    """Format a row dict for display, showing None as None."""
    items = []
    for key, value in sorted(row_dict.items()):
        if value is None:
            items.append(f"{key}=None")
        elif isinstance(value, str):
            items.append(f'{key}="{value}"')
        else:
            items.append(f"{key}={value}")
    return "{" + ", ".join(items) + "}"


# =============================================================================
# Test Summary Formatting
# =============================================================================

def print_test_summary(details: List[tuple], footer_message: Optional[str] = None):
    """
    Print a formatted test summary with consistent styling.

    Args:
        details: List of (label, value) tuples to display
        footer_message: Optional message to display at the bottom

    Example:
        print_test_summary([
            ("Ingestion ID", ingestion_id),
            ("Run ID", run_id),
            ("Files Processed", 3),
            ("Records Ingested", 3000),
            ("Table", table_identifier),
            ("Status", "SUCCESS")
        ])
    """
    print(f"\nSummary:")
    for label, value in details:
        print(f"  - {label}: {value}")

    if footer_message:
        print(f"\n{footer_message}")

    print("="*80 + "\n")
