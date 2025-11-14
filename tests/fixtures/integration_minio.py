from datetime import datetime, timezone
from typing import Generator, Dict

import boto3
import pytest


@pytest.fixture(scope="session")
def minio_config() -> Dict[str, str]:
    return _minio_config(is_docker_network=False)

@pytest.fixture(scope="session")
def minio_config_for_ingestion() -> Dict[str, str]:
    return _minio_config(is_docker_network=True)

def _minio_config(is_docker_network: bool=False) -> Dict[str, str]:
    # if called inside prefect worker as part of docker, then the dns should be `minio`
    endpoint_url = "http://minio:9000" if is_docker_network else "http://localhost:9000"
    return {
        "endpoint_url": endpoint_url,
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
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
        print(f"Created lakehouse bucket: {bucket_name}")
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
        print(f"Cleaned up lakehouse bucket: {bucket_name} ({len(objects_to_delete)} objects deleted)")

    except Exception as e:
        print(f"Warning: Failed to cleanup lakehouse bucket {bucket_name}: {e}")


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
        print(f"Created test bucket: {bucket_name}")
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
        print(f"Cleaned up test bucket: {bucket_name}")

    except Exception as e:
        print(f"Warning: Failed to cleanup bucket {bucket_name}: {e}")