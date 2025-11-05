"""Pytest configuration and shared fixtures"""
import os
import tempfile
from typing import Generator, Dict, Any
from unittest.mock import MagicMock

import boto3
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.database import Base, get_db
from app.main import app
from tests.fixtures.data_generator import upload_test_files_to_s3


# Test configuration
TEST_TENANT_ID = "test-tenant-001"
TEST_CLUSTER_ID = "test-cluster-001"
MINIO_BUCKET = "test-ingestion-bucket"
MINIO_PREFIX = "data/"


@pytest.fixture(scope="session")
def test_config() -> Dict[str, Any]:
    """Global test configuration"""
    return {
        "tenant_id": TEST_TENANT_ID,
        "cluster_id": TEST_CLUSTER_ID,
        "minio_bucket": MINIO_BUCKET,
        "minio_prefix": MINIO_PREFIX,
        "spark_connect_url": os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002"),
        "spark_connect_token": os.getenv("TEST_SPARK_CONNECT_TOKEN", "test-token"),
    }


@pytest.fixture(scope="function")
def test_db() -> Generator[Session, None, None]:
    """
    Create a fresh test database for each test function
    Uses SQLite in-memory database for speed
    """
    # Create temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    database_url = f"sqlite:///{temp_db.name}"

    # Create engine and session
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
    )

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Create all tables
    Base.metadata.create_all(bind=engine)

    # Create session
    db = TestingSessionLocal()

    try:
        yield db
    finally:
        db.close()
        engine.dispose()
        # Clean up temp file
        try:
            os.unlink(temp_db.name)
        except Exception:
            pass


@pytest.fixture(scope="function")
def test_db_postgres() -> Generator[Session, None, None]:
    """
    Create a fresh test database session using real PostgreSQL
    Uses PostgreSQL from docker-compose for integration tests

    Prerequisites:
    - PostgreSQL running on localhost:5432 (docker-compose.test.yml)
    - Database: autoloader_test
    - User: test_user
    - Password: test_password
    """
    # Get PostgreSQL connection from environment or use defaults
    database_url = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql://test_user:test_password@localhost:5432/autoloader_test"
    )

    # Create engine and session
    engine = create_engine(database_url)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Create all tables
    Base.metadata.create_all(bind=engine)

    # Create session
    db = TestingSessionLocal()

    try:
        yield db
    finally:
        # Clean up test data
        db.rollback()

        # Delete test data from all tables
        try:
            # Delete in reverse order to respect foreign keys
            from app.models.domain import ProcessedFile, Run, Ingestion

            db.query(ProcessedFile).delete()
            db.query(Run).delete()
            db.query(Ingestion).delete()
            db.commit()
        except Exception as e:
            print(f"Warning: Failed to clean up test data: {e}")
            db.rollback()

        db.close()
        engine.dispose()


@pytest.fixture(scope="function")
def api_client(test_db: Session) -> Generator[TestClient, None, None]:
    """
    Create FastAPI test client with test database
    """
    # Override database dependency
    def override_get_db():
        try:
            yield test_db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    client = TestClient(app)

    yield client

    # Clean up
    app.dependency_overrides.clear()


@pytest.fixture(scope="session")
def minio_container():
    """
    Start MinIO container for S3-compatible testing

    For now, this expects MinIO to be running externally.
    To use testcontainers, install: testcontainers[minio]
    """
    # Check if MinIO is available
    minio_endpoint = os.getenv("TEST_MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("TEST_MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("TEST_MINIO_SECRET_KEY", "minioadmin")

    # For now, we expect MinIO to be running
    # TODO: Add testcontainers integration for automatic MinIO startup
    # from testcontainers.minio import MinioContainer
    # container = MinioContainer()
    # container.start()

    container_info = {
        "endpoint": minio_endpoint,
        "access_key": minio_access_key,
        "secret_key": minio_secret_key,
    }

    yield container_info

    # Cleanup if using testcontainers
    # container.stop()


@pytest.fixture(scope="function")
def s3_client(minio_container: Dict[str, str]):
    """Create S3 client for testing (using MinIO)"""
    client = boto3.client(
        's3',
        endpoint_url=minio_container["endpoint"],
        aws_access_key_id=minio_container["access_key"],
        aws_secret_access_key=minio_container["secret_key"],
        region_name='us-east-1'
    )

    yield client


@pytest.fixture(scope="function")
def test_bucket(s3_client, test_config: Dict[str, Any]) -> str:
    """
    Create test bucket and clean up after test
    """
    bucket_name = test_config["minio_bucket"]

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception as e:
        # If bucket already exists, try to empty it
        try:
            objects = s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        except Exception:
            pass

    yield bucket_name

    # Cleanup - delete all objects and bucket
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        s3_client.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass


@pytest.fixture(scope="function")
def test_data_s3(s3_client, test_bucket: str, test_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upload test JSON data to S3/MinIO

    Returns metadata about uploaded files
    """
    prefix = test_config["minio_prefix"]

    uploaded_keys = upload_test_files_to_s3(
        s3_client=s3_client,
        bucket=test_bucket,
        prefix=prefix,
        num_files=3,
        records_per_file=1000,
        file_format="json"
    )

    return {
        "bucket": test_bucket,
        "prefix": prefix,
        "keys": uploaded_keys,
        "num_files": 3,
        "records_per_file": 1000,
        "total_records": 3000,
        "endpoint": os.getenv("TEST_MINIO_ENDPOINT", "http://localhost:9000"),
        "access_key": os.getenv("TEST_MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("TEST_MINIO_SECRET_KEY", "minioadmin"),
    }


@pytest.fixture(scope="session")
def spark_connect_available() -> bool:
    """
    Check if Spark Connect is available for testing

    Returns True if available, False otherwise
    """
    spark_url = os.getenv("TEST_SPARK_CONNECT_URL")

    if not spark_url:
        return False

    from app.spark.connect_client import SparkConnectClient
    try:
        client = SparkConnectClient(spark_url, "")
        client.test_connection()
        return True
    except Exception:
        return False


@pytest.fixture(scope="function")
def sample_ingestion_config(test_config: Dict[str, Any], test_data_s3: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sample ingestion configuration for testing
    Matches the IngestionCreate schema structure
    """
    return {
        "name": "E2E Test S3 JSON Ingestion",
        "cluster_id": test_config["cluster_id"],
        "source": {
            "type": "s3",
            "path": f"s3a://{test_data_s3['bucket']}/{test_data_s3['prefix']}",
            "file_pattern": "*.json",
            "credentials": {
                "aws_access_key_id": test_data_s3["access_key"],
                "aws_secret_access_key": test_data_s3["secret_key"],
                "endpoint_url": test_data_s3["endpoint"]
            }
        },
        "format": {
            "type": "json",
            "options": {
                "multiline": False
            },
            "schema": {
                "inference": "auto",
                "evolution_enabled": True
            }
        },
        "destination": {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "e2e_test_table",
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
            "mode": "scheduled",
            "frequency": "daily",
            "time": None,
            "timezone": "UTC",
            "cron_expression": None,
            "backfill": {
                "enabled": False,
                "start_date": None
            }
        },
        "quality": {
            "row_count_threshold": None,
            "alerts_enabled": True,
            "alert_recipients": []
        }
    }


# Pytest markers for selective test execution
def pytest_configure(config):
    """Register custom pytest markers"""
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "requires_spark: mark test as requiring Spark Connect"
    )
    config.addinivalue_line(
        "markers", "requires_minio: mark test as requiring MinIO"
    )
