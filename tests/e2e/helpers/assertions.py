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
