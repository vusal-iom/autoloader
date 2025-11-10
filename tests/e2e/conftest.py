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


@pytest.fixture(scope="session")
def minio_config() -> Dict[str, str]:
    """
    MinIO configuration for test client (running on host).

    Uses localhost:9000 to connect from the test host machine.
    """
    return {
        "endpoint_url": os.getenv("TEST_MINIO_ENDPOINT", "http://localhost:9000"),
        "aws_access_key_id": os.getenv("TEST_MINIO_ACCESS_KEY", "minioadmin"),
        "aws_secret_access_key": os.getenv("TEST_MINIO_SECRET_KEY", "minioadmin"),
        "region_name": "us-east-1"
    }


@pytest.fixture(scope="session")
def minio_config_for_ingestion() -> Dict[str, str]:
    """
    MinIO configuration for ingestion (stored in DB, used by worker).

    Uses minio:9000 (Docker service name) for container-to-container communication.
    This is what gets stored in the ingestion config and used by the Prefect worker.
    """
    return {
        "endpoint_url": os.getenv("TEST_MINIO_ENDPOINT_FOR_WORKER", "http://minio:9000"),
        "aws_access_key_id": os.getenv("TEST_MINIO_ACCESS_KEY", "minioadmin"),
        "aws_secret_access_key": os.getenv("TEST_MINIO_SECRET_KEY", "minioadmin"),
        "region_name": "us-east-1"
    }


@pytest.fixture(scope="session")
def minio_client(minio_config: Dict[str, str], ensure_services_ready):
    """
    Create boto3 S3 client for MinIO.

    Uses MinIO from docker-compose.test.yml on localhost:9000.
    """
    client = boto3.client(
        's3',
        endpoint_url=minio_config["endpoint_url"],
        aws_access_key_id=minio_config["aws_access_key_id"],
        aws_secret_access_key=minio_config["aws_secret_access_key"],
        region_name=minio_config["region_name"]
    )

    # Verify connection
    try:
        client.list_buckets()
    except Exception as e:
        raise RuntimeError(f"Failed to connect to MinIO: {e}")

    return client


@pytest.fixture(scope="session")
def lakehouse_bucket(minio_client) -> Generator[str, None, None]:
    """
    Create lakehouse bucket for Iceberg warehouse (once per session) and clean up after.

    This bucket is used by Spark to store Iceberg table data.

    Yields:
        Bucket name ("test-lakehouse")
    """
    bucket_name = "test-lakehouse"

    # Create bucket
    try:
        minio_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Created lakehouse bucket: {bucket_name}")
    except Exception as e:
        # If bucket already exists, that's fine
        if "BucketAlreadyOwnedByYou" not in str(e) and "BucketAlreadyExists" not in str(e):
            raise RuntimeError(f"Failed to create lakehouse bucket: {e}")

    yield bucket_name

    # Cleanup: Delete all objects in bucket
    try:
        # List and delete all objects (may be paginated)
        paginator = minio_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)

        objects_to_delete = []
        for page in pages:
            if 'Contents' in page:
                objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])

        # Delete objects in batches (S3 limit is 1000 per request)
        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                minio_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': batch}
                )

        # Delete bucket
        minio_client.delete_bucket(Bucket=bucket_name)
        print(f"🧹 Cleaned up lakehouse bucket: {bucket_name} ({len(objects_to_delete)} objects deleted)")

    except Exception as e:
        print(f"⚠️  Warning: Failed to cleanup lakehouse bucket {bucket_name}: {e}")


@pytest.fixture(scope="function")
def test_bucket(minio_client) -> Generator[str, None, None]:
    """
    Create a test bucket for each test and clean up after.

    Yields:
        Bucket name (e.g., "test-bucket-12345678")
    """
    # Generate unique bucket name
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    bucket_name = f"test-bucket-{timestamp}"

    # Create bucket
    try:
        minio_client.create_bucket(Bucket=bucket_name)
        print(f"\n✅ Created test bucket: {bucket_name}")
    except Exception as e:
        raise RuntimeError(f"Failed to create bucket {bucket_name}: {e}")

    yield bucket_name

    # Cleanup: Delete all objects in bucket
    try:
        # List and delete all objects
        response = minio_client.list_objects_v2(Bucket=bucket_name)

        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            minio_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )

        # Delete bucket
        minio_client.delete_bucket(Bucket=bucket_name)
        print(f"🧹 Cleaned up test bucket: {bucket_name}")

    except Exception as e:
        print(f"⚠️  Warning: Failed to cleanup bucket {bucket_name}: {e}")


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

        print(f"  ✅ Uploaded: {key} ({len(records)} records, {file_size} bytes)")

    print(f"📁 Created {len(files_created)} test files with {num_files * records_per_file} total records\n")

    return files_created


@pytest.fixture(scope="session")
def spark_connect_url() -> str:
    """Get Spark Connect URL from environment."""
    return os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002")


@pytest.fixture(scope="function")
def spark_session(spark_connect_url: str, ensure_services_ready) -> Generator[SparkSession, None, None]:
    """
    Create Spark Connect session for E2E tests with full service checks.

    Overrides the base spark_session fixture to ensure all services are ready
    and uses E2E-specific app name.

    Uses Spark from docker-compose.test.yml on localhost:15002.
    """
    try:
        spark = SparkSession.builder \
            .remote(spark_connect_url) \
            .appName("E2E-Test-Verification") \
            .getOrCreate()

        print(f"✅ Connected to Spark: {spark_connect_url}\n")

        yield spark

        # Cleanup
        spark.stop()
        print("🧹 Stopped Spark session")

    except Exception as e:
        raise RuntimeError(
            f"Failed to connect to Spark Connect at {spark_connect_url}: {e}\n"
            "Ensure Spark is running: docker-compose -f docker-compose.test.yml up -d"
        )


@pytest.fixture(scope="function")
def test_tenant_id() -> str:
    """Get test tenant ID from environment."""
    return os.getenv("TEST_TENANT_ID", "test-tenant-001")


@pytest.fixture(scope="function")
def test_cluster_id() -> str:
    """Get test cluster ID from environment."""
    return os.getenv("TEST_CLUSTER_ID", "test-cluster-001")


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
