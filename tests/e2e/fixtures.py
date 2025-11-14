"""
E2E Test Fixtures.

Provides fixtures specific to end-to-end integration tests:
- MinIO client for S3-compatible storage
- Test bucket creation and cleanup
- Sample JSON file generation and upload
- Spark Connect session for data verification
"""

import pytest
import boto3
import json
import os
from typing import Generator, Dict, List
from datetime import datetime, timezone
from pyspark.sql import SparkSession




@pytest.fixture(scope="function")
def sample_json_files(minio_client, test_bucket) -> List[Dict[str, any]]:
    """
    Generate and upload sample JSON files to MinIO.

    Creates 3 JSON files with 1000 records each.
    Total: 3000 records.

    Returns:
        List of file metadata dicts with keys: path, size, key
    """
    num_files = int(os.getenv("TEST_DATA_NUM_FILES", "3"))
    records_per_file = int(os.getenv("TEST_DATA_RECORDS_PER_FILE", "1000"))

    files_created = []

    for file_idx in range(num_files):
        # Generate data for this file
        records = []
        for record_idx in range(records_per_file):
            record_id = file_idx * records_per_file + record_idx
            record = {
                "id": record_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user_id": f"user-{record_id % 100}",
                "event_type": ["login", "pageview", "click", "purchase"][record_id % 4],
                "value": record_id * 10.5
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

        print(f"  Uploaded: {key} ({len(records)} records, {file_size} bytes)")

    print(f"Created {len(files_created)} test files with {num_files * records_per_file} total records\n")

    return files_created


@pytest.fixture(scope="function")
def e2e_api_client_no_override(ensure_services_ready, test_engine) -> Generator:
    """
    Create FastAPI test client WITHOUT database dependency override.

    This is specifically for Prefect e2e tests where Prefect tasks create their own
    database sessions via SessionLocal(), so we need real database commits.

    Unlike the standard api_client fixture, this does NOT override get_db,
    allowing both the API and Prefect tasks to use the same real database.

    Depends on test_engine to ensure database tables are created before tests run.
    """
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    yield client

    # No cleanup of overrides since we didn't set any
