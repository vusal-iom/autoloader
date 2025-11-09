"""
E2E Test Factories.

Provides functions for creating test data and ingestion configurations.
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from fastapi.testclient import TestClient

from .logger import E2ELogger
from .constants import (
    SCHEMA_TYPE_BASE,
    SCHEMA_TYPE_EVOLVED,
    DEFAULT_RECORDS_PER_FILE,
    TEST_TABLE_PREFIX,
)


# =============================================================================
# Ingestion Creation
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
    payload = _build_ingestion_payload(
        name=name,
        cluster_id=cluster_id,
        test_bucket=test_bucket,
        minio_config=minio_config,
        table_name=table_name,
        evolution_enabled=evolution_enabled,
    )

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


def _build_ingestion_payload(
    name: str,
    cluster_id: str,
    test_bucket: str,
    minio_config: Dict[str, str],
    table_name: str,
    evolution_enabled: bool,
) -> Dict[str, Any]:
    """Build standard ingestion payload with sensible defaults."""
    return {
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


# =============================================================================
# Test Data Generation
# =============================================================================

def generate_unique_table_name(prefix: str = TEST_TABLE_PREFIX) -> str:
    """Generate unique table name with timestamp."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{timestamp}"


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


def upload_json_files(
    minio_client,
    test_bucket: str,
    start_file_idx: int,
    num_files: int,
    records_per_file: int = DEFAULT_RECORDS_PER_FILE,
    schema_type: str = SCHEMA_TYPE_BASE,
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
        records = _generate_records(
            file_idx=file_idx,
            records_per_file=records_per_file,
            schema_type=schema_type
        )

        file_metadata = _upload_records_to_minio(
            minio_client=minio_client,
            test_bucket=test_bucket,
            file_idx=file_idx,
            records=records,
            schema_type=schema_type,
            logger=logger
        )

        files_created.append(file_metadata)

    return files_created


def _generate_records(
    file_idx: int,
    records_per_file: int,
    schema_type: str
) -> List[Dict[str, Any]]:
    """Generate records with specified schema type."""
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
        if schema_type == SCHEMA_TYPE_EVOLVED:
            record.update(_get_evolved_fields(record_id))

        records.append(record)

    return records


def _get_evolved_fields(record_id: int) -> Dict[str, Any]:
    """Get additional fields for evolved schema."""
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
    return {
        "region": regions[record_id % len(regions)],
        "metadata": {
            "browser": ["chrome", "firefox", "safari"][record_id % 3],
            "version": f"{100 + (record_id % 50)}.0",
            "device": ["desktop", "mobile", "tablet"][record_id % 3]
        }
    }


def _upload_records_to_minio(
    minio_client,
    test_bucket: str,
    file_idx: int,
    records: List[Dict[str, Any]],
    schema_type: str,
    logger: Optional[E2ELogger]
) -> Dict[str, Any]:
    """Upload records to MinIO and return file metadata."""
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

    if logger:
        schema_desc = "5 fields" if schema_type == SCHEMA_TYPE_BASE else "7 fields"
        logger.step(f"Uploaded: {key} ({len(records)} records, {schema_desc})")

    return {
        "path": file_path,
        "key": key,
        "size": file_size,
        "records": len(records)
    }
