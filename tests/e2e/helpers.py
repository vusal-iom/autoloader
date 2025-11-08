"""
E2E Test Helper Functions.

Provides reusable utilities for end-to-end tests:
- Ingestion creation
- Run execution and polling
- Metric verification
- Table data verification
- Test data generation
- Logging utilities
- Test summary formatting
"""

import time
import json
import os
import pytest
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession
from prefect import get_client
from prefect.exceptions import ObjectNotFound


# =============================================================================
# Logging Utilities
# =============================================================================

class E2ELogger:
    """Simple logger for e2e tests with optional verbosity."""

    def __init__(self, verbose: bool = None):
        """
        Initialize logger.

        Args:
            verbose: If True, logs detailed output. If None, reads from E2E_VERBOSE env var.
        """
        if verbose is None:
            verbose = os.getenv("E2E_VERBOSE", "false").lower() == "true"
        self.verbose = verbose

    def section(self, title: str):
        """Log section header (always shown)."""
        print(f"\n{'='*80}")
        print(title)
        print('='*80)

    def phase(self, title: str):
        """Log phase header (always shown)."""
        print(f"\n{title}")

    def step(self, msg: str, always: bool = False):
        """Log step message (only if verbose or always=True)."""
        if self.verbose or always:
            print(f"  {msg}")

    def metric(self, key: str, value: Any):
        """Log metric (only if verbose)."""
        if self.verbose:
            print(f"  {key}: {value}")

    def success(self, msg: str, always: bool = True):
        """Log success message."""
        if self.verbose or always:
            print(f"  ✅ {msg}")

    def error(self, msg: str):
        """Log error message (always shown)."""
        print(f"  ❌ {msg}")


# =============================================================================
# Ingestion Helpers
# =============================================================================

def create_standard_ingestion(
    api_client: TestClient,
    cluster_id: str,
    test_bucket: str,
    minio_config: Dict[str, str],
    table_name: str,
    name: str = "E2E Test Ingestion",
    evolution_enabled: bool = True,
    **overrides
) -> Dict[str, Any]:
    """
    Create a standard S3 JSON ingestion configuration.

    Args:
        api_client: FastAPI test client
        cluster_id: Cluster ID for ingestion
        test_bucket: S3 bucket name
        minio_config: MinIO configuration dict
        table_name: Destination table name
        name: Ingestion name
        evolution_enabled: Enable schema evolution
        **overrides: Override specific payload fields

    Returns:
        Created ingestion response dict
    """
    payload = {
        "name": name,
        "cluster_id": cluster_id,
        "source": {
            "type": "s3",
            "path": f"s3://{test_bucket}/data/",
            "file_pattern": "*.json",
            "credentials": {
                "aws_access_key_id": minio_config["aws_access_key_id"],
                "aws_secret_access_key": minio_config["aws_secret_access_key"],
                "endpoint_url": minio_config["endpoint_url"]
            }
        },
        "format": {
            "type": "json",
            "options": {
                "multiline": False
            },
            "schema": {
                "inference": "auto",
                "evolution_enabled": evolution_enabled
            }
        },
        "destination": {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": table_name,
            "write_mode": "append",
            "partitioning": {
                "enabled": False,
                "columns": []
            },
            "optimization": {
                "z_ordering_enabled": False,
                "z_ordering_columns": []
            }
        },
        "schedule": {
            "frequency": None,  # Manual trigger only
            "time": None,
            "timezone": "UTC",
            "cron_expression": None,
            "backfill": {
                "enabled": False
            }
        },
        "quality": {
            "row_count_threshold": None,
            "alerts_enabled": False,
            "alert_recipients": []
        }
    }

    # Apply any overrides (deep merge for nested dicts)
    if overrides:
        for key, value in overrides.items():
            if isinstance(value, dict) and key in payload:
                payload[key].update(value)
            else:
                payload[key] = value

    response = api_client.post("/api/v1/ingestions", json=payload)
    assert response.status_code == 201, f"Failed to create ingestion: {response.text}"

    return response.json()


# =============================================================================
# Run Execution Helpers
# =============================================================================

def trigger_run(api_client: TestClient, ingestion_id: str) -> str:
    """
    Trigger a manual ingestion run.

    Args:
        api_client: FastAPI test client
        ingestion_id: Ingestion ID

    Returns:
        Run ID
    """
    response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
    assert response.status_code == 202, f"Failed to trigger run: {response.text}"

    run_response = response.json()
    run_id = run_response["run_id"]

    assert run_id is not None
    assert run_response["status"] in ["accepted", "running", "completed"]

    return run_id


def wait_for_run_completion(
    api_client: TestClient,
    ingestion_id: str,
    run_id: str,
    timeout: int = 180,
    poll_interval: int = 2,
    logger: Optional[E2ELogger] = None
) -> Dict[str, Any]:
    """
    Poll for run completion.

    Args:
        api_client: FastAPI test client
        ingestion_id: Ingestion ID
        run_id: Run ID
        timeout: Max wait time in seconds (default: 180)
        poll_interval: Poll interval in seconds (default: 2)
        logger: Optional logger for progress output

    Returns:
        Final run data dict

    Raises:
        pytest.fail if run fails or times out
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run_id}")
        assert response.status_code == 200, f"Failed to get run status: {response.text}"

        run = response.json()
        run_status = run["status"]

        if logger and logger.verbose:
            elapsed = int(time.time() - start_time)
            print(f"  ⏱️  {elapsed}s - Status: {run_status}", end="")

        if run_status in ["success", "completed"]:
            if logger and logger.verbose:
                print(" ✅")
            elapsed = int(time.time() - start_time)
            if logger:
                logger.success(f"Run completed in {elapsed}s", always=True)
            return run
        elif run_status == "failed":
            if logger and logger.verbose:
                print(" ❌")
            pytest.fail(f"Run failed: {run.get('errors', [])}")
        else:
            if logger and logger.verbose:
                print()
            time.sleep(poll_interval)

    pytest.fail(f"Run did not complete within {timeout}s. Last status: {run.get('status', 'unknown')}")


def get_run(api_client: TestClient, ingestion_id: str, run_id: str) -> Dict[str, Any]:
    """
    Get run details.

    Args:
        api_client: FastAPI test client
        ingestion_id: Ingestion ID
        run_id: Run ID

    Returns:
        Run data dict
    """
    response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run_id}")
    assert response.status_code == 200
    return response.json()


# =============================================================================
# Verification Helpers
# =============================================================================

def assert_run_metrics(
    run: Dict[str, Any],
    expected_files: Optional[int] = None,
    expected_records: Optional[int] = None,
    expected_errors: int = 0,
    logger: Optional[E2ELogger] = None
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
    assert run["status"] in ["success", "completed"], f"Run status: {run['status']}"
    assert run["started_at"] is not None
    assert run["ended_at"] is not None

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
        f"Expected {expected_errors} errors, got {len(errors)}"


def verify_table_data(
    spark_session: SparkSession,
    table_identifier: str,
    expected_count: Optional[int] = None,
    expected_fields: Optional[List[str]] = None,
    unexpected_fields: Optional[List[str]] = None,
    check_duplicates: bool = True,
    logger: Optional[E2ELogger] = None
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
    logger: Optional[E2ELogger] = None
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

    # Verify old records have NULL for new fields
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

    # Verify new records have values for new fields
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
# Test Data Helpers
# =============================================================================

def generate_unique_table_name(prefix: str = "e2e_test") -> str:
    """Generate unique table name with timestamp."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{timestamp}"


def upload_json_files(
    minio_client,
    test_bucket: str,
    start_file_idx: int,
    num_files: int,
    records_per_file: int = 1000,
    schema_type: str = "base",
    logger: Optional[E2ELogger] = None
) -> List[Dict[str, Any]]:
    """
    Upload JSON files to MinIO with specified schema.

    Args:
        minio_client: Boto3 S3 client
        test_bucket: Target bucket name
        start_file_idx: Starting index for file numbering
        num_files: Number of files to create
        records_per_file: Number of records per file
        schema_type: "base" (5 fields) or "evolved" (7 fields)
        logger: Optional logger

    Returns:
        List of file metadata dicts
    """
    files_created = []

    for file_idx in range(start_file_idx, start_file_idx + num_files):
        records = []

        for record_idx in range(records_per_file):
            record_id = file_idx * records_per_file + record_idx

            # Base schema (5 fields)
            record = {
                "id": record_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user_id": f"user-{record_id % 100}",
                "event_type": ["login", "pageview", "click", "purchase"][record_id % 4],
                "value": record_id * 10.5
            }

            # Add evolved fields if requested
            if schema_type == "evolved":
                regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
                record["region"] = regions[record_id % len(regions)]
                record["metadata"] = {
                    "browser": ["chrome", "firefox", "safari"][record_id % 3],
                    "version": f"{100 + (record_id % 50)}.0",
                    "device": ["desktop", "mobile", "tablet"][record_id % 3]
                }

            records.append(record)

        # Convert to newline-delimited JSON
        file_content = "\n".join([json.dumps(record) for record in records])

        # Upload to MinIO
        key = f"data/batch_{file_idx}.json"
        minio_client.put_object(
            Bucket=test_bucket,
            Key=key,
            Body=file_content.encode('utf-8')
        )

        file_path = f"s3://{test_bucket}/{key}"
        file_size = len(file_content.encode('utf-8'))

        files_created.append({
            "path": file_path,
            "key": key,
            "size": file_size,
            "records": len(records)
        })

        if logger:
            schema_desc = "5 fields" if schema_type == "base" else "7 fields"
            logger.step(f"Uploaded: {key} ({len(records)} records, {schema_desc})")

    return files_created


def get_table_identifier(ingestion: Dict[str, Any]) -> str:
    """
    Get full table identifier from ingestion config.

    Args:
        ingestion: Ingestion data dict

    Returns:
        Full table identifier (catalog.database.table)
    """
    dest = ingestion["destination"]
    return f"{dest['catalog']}.{dest['database']}.{dest['table']}"


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
            ("Status", "SUCCESS ✅")
        ])
    """
    print(f"\nSummary:")
    for label, value in details:
        print(f"  - {label}: {value}")

    if footer_message:
        print(f"\n{footer_message}")

    print("="*80 + "\n")


# =============================================================================
# Prefect Deployment Verification Helpers
# =============================================================================

async def verify_prefect_deployment_exists(
    deployment_id: str,
    expected_name: Optional[str] = None,
    expected_tags: Optional[List[str]] = None,
    logger: Optional[E2ELogger] = None
) -> Any:
    """
    Verify that a Prefect deployment exists and optionally check its properties.

    Args:
        deployment_id: Prefect deployment ID
        expected_name: Expected deployment name (None to skip)
        expected_tags: List of tags that must be present (None to skip)
        logger: Optional logger for detailed output

    Returns:
        Deployment object from Prefect

    Raises:
        pytest.fail if deployment not found or properties don't match
    """
    async with get_client() as prefect_client:
        try:
            deployment = await prefect_client.read_deployment(deployment_id)

            if logger:
                logger.success(f"Deployment found: {deployment.name}")
                logger.step(f"Paused: {deployment.paused}")
                logger.step(f"Tags: {deployment.tags}")
                has_active_schedule = any(s.active for s in deployment.schedules) if deployment.schedules else False
                logger.step(f"Has active schedules: {has_active_schedule}")

            # Verify name if provided
            if expected_name is not None:
                assert deployment.name == expected_name, \
                    f"Expected deployment name '{expected_name}', got '{deployment.name}'"

            # Verify tags if provided
            if expected_tags:
                for tag in expected_tags:
                    assert tag in deployment.tags, \
                        f"Expected tag '{tag}' not found in deployment tags: {deployment.tags}"

            return deployment

        except ObjectNotFound:
            pytest.fail(f"Deployment {deployment_id} not found in Prefect server")


async def verify_prefect_deployment_active(
    deployment_id: str,
    logger: Optional[E2ELogger] = None
):
    """
    Verify that a Prefect deployment is active (not paused, has active schedules).

    Args:
        deployment_id: Prefect deployment ID
        logger: Optional logger for detailed output

    Raises:
        AssertionError if deployment is paused or has no active schedules
    """
    async with get_client() as prefect_client:
        deployment = await prefect_client.read_deployment(deployment_id)

        has_active_schedule = any(s.active for s in deployment.schedules) if deployment.schedules else False

        if logger:
            logger.step(f"Deployment paused: {deployment.paused}")
            logger.step(f"Has active schedules: {has_active_schedule}")

        assert deployment.paused is False, \
            f"Expected deployment to be active, but it is paused"
        assert has_active_schedule is True, \
            "Expected at least one active schedule"

        if logger:
            logger.success("Deployment is active ✅")


async def verify_prefect_deployment_paused(
    deployment_id: str,
    logger: Optional[E2ELogger] = None
):
    """
    Verify that a Prefect deployment is paused.

    Args:
        deployment_id: Prefect deployment ID
        logger: Optional logger for detailed output

    Raises:
        AssertionError if deployment is not paused
    """
    async with get_client() as prefect_client:
        deployment = await prefect_client.read_deployment(deployment_id)

        if logger:
            logger.step(f"Deployment paused: {deployment.paused}")

        assert deployment.paused is True, \
            f"Expected deployment to be paused, but it is active"

        if logger:
            logger.success("Deployment is paused ✅")


async def verify_prefect_deployment_deleted(
    deployment_id: str,
    logger: Optional[E2ELogger] = None
):
    """
    Verify that a Prefect deployment has been deleted.

    Args:
        deployment_id: Prefect deployment ID
        logger: Optional logger for detailed output

    Raises:
        pytest.fail if deployment still exists
    """
    async with get_client() as prefect_client:
        try:
            await prefect_client.read_deployment(deployment_id)
            pytest.fail(f"Deployment {deployment_id} should have been deleted but still exists")
        except ObjectNotFound:
            if logger:
                logger.success("Deployment deleted from Prefect server ✅")
