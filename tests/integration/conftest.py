"""
Integration test fixtures.

Provides Spark Connect session and SQLite database for integration tests.

IMPORTANT: Integration tests require Spark Connect to be running.
Start services with: docker-compose -f docker-compose.test.yml up -d
"""

import pytest
import tempfile
import time
import httpx
from pathlib import Path
from typing import Generator
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base


@pytest.fixture(scope="session")
def test_database_url() -> str:
    """
    Use SQLite for integration tests (no docker-compose required).

    Overrides the root conftest fixture.
    """
    return "sqlite:///./test_integration.db"


@pytest.fixture(scope="session")
def test_engine(test_database_url: str):
    """Create test database engine with SQLite."""
    engine = create_engine(
        test_database_url,
        connect_args={"check_same_thread": False}  # Needed for SQLite
    )

    # Create all tables
    Base.metadata.create_all(bind=engine)

    yield engine

    # Cleanup: Drop all tables and delete file
    Base.metadata.drop_all(bind=engine)
    engine.dispose()

    # Remove SQLite database file
    db_file = Path("./test_integration.db")
    if db_file.exists():
        db_file.unlink()


@pytest.fixture(scope="function")
def test_db(test_engine) -> Generator[Session, None, None]:
    """
    Create a fresh database session for each test.

    Automatically rolls back after each test to ensure isolation.
    """
    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=test_engine
    )

    session = TestingSessionLocal()

    yield session

    # Rollback and close
    session.rollback()
    session.close()


@pytest.fixture(scope="session")
def ensure_spark_connect_ready():
    """
    Ensure Spark Connect is running and test-lakehouse bucket exists.

    Integration tests require:
    - Spark Connect at sc://localhost:15002
    - MinIO at localhost:9000
    - test-lakehouse bucket for Iceberg warehouse

    Raises:
        RuntimeError: If Spark Connect or MinIO is not available
    """
    spark_ui_url = "http://localhost:4040"
    minio_url = "http://localhost:9000/minio/health/live"
    timeout = 5  # seconds - fail fast for integration tests

    print("\nChecking integration test prerequisites...")

    # Check Spark Connect
    print(f"  → Spark Connect: ", end="")
    start_time = time.time()
    spark_ready = False
    while time.time() - start_time < timeout:
        try:
            response = httpx.get(spark_ui_url, timeout=2, follow_redirects=False)
            if response.status_code in [200, 302, 403]:
                print("Ready")
                spark_ready = True
                break
        except Exception:
            pass
        time.sleep(0.5)

    if not spark_ready:
        raise RuntimeError(
            f"\nSpark Connect not available at {spark_ui_url}\n"
            f"Integration tests require Spark Connect to be running.\n"
            f"Start services with:\n"
            f"  docker-compose -f docker-compose.test.yml up -d\n"
            f"  (Wait 1-2 minutes for Spark to fully start)"
        )

    # Check MinIO
    print(f"  → MinIO: ", end="")
    start_time = time.time()
    minio_ready = False
    while time.time() - start_time < timeout:
        try:
            response = httpx.get(minio_url, timeout=2)
            if response.status_code == 200:
                print("Ready")
                minio_ready = True
                break
        except Exception:
            pass
        time.sleep(0.5)

    if not minio_ready:
        raise RuntimeError(
            f"\nMinIO not available at {minio_url}\n"
            f"Integration tests require MinIO for Iceberg warehouse.\n"
            f"Start services with:\n"
            f"  docker-compose -f docker-compose.test.yml up -d"
        )

    # Ensure test-lakehouse bucket exists (required for test_catalog)
    print(f"  → test-lakehouse bucket: ", end="")
    try:
        import boto3
        minio_client = boto3.client(
            's3',
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1"
        )
        try:
            minio_client.create_bucket(Bucket='test-lakehouse')
            print("Created")
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                print("Exists")
            else:
                raise
    except Exception as e:
        raise RuntimeError(f"\nFailed to create test-lakehouse bucket: {e}")

    return True


# Override spark_session fixture to ensure Spark Connect is ready
@pytest.fixture(scope="session")
def spark_session(ensure_spark_connect_ready) -> Generator[SparkSession, None, None]:
    """
    Create Spark Connect session for integration tests.

    Overrides root conftest spark_session to add Spark Connect availability check.
    Uses Spark Connect at sc://localhost:15002 as per CLAUDE.md instructions.
    """
    import os
    spark_connect_url = os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002")

    try:
        spark = SparkSession.builder \
            .remote(spark_connect_url) \
            .appName("Integration-Test") \
            .getOrCreate()

        print(f"Connected to Spark Connect: {spark_connect_url}")

        yield spark

        # Cleanup
        spark.stop()
        print("Stopped Spark session")

    except Exception as e:
        raise RuntimeError(
            f"Failed to connect to Spark Connect at {spark_connect_url}: {e}\n"
            "Ensure Spark is running: docker-compose -f docker-compose.test.yml up -d"
        )


