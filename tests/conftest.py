"""
Shared pytest fixtures for all tests.

Provides common fixtures for database, API client, and service health checks.
"""

import pytest
import os
import time
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from fastapi.testclient import TestClient
import httpx
from pyspark.sql import SparkSession

from app.main import app
from app.database import Base, get_db
from app.config import get_settings


# Get settings
settings = get_settings()


@pytest.fixture(scope="session")
def test_database_url() -> str:
    """
    Get test database URL from environment or use default PostgreSQL.

    For E2E tests, we use PostgreSQL from docker-compose.test.yml.
    For unit tests, SQLite can be used.
    """
    return os.getenv(
        "DATABASE_URL",
        "postgresql://test_user:test_password@localhost:5432/autoloader_test"
    )


@pytest.fixture(scope="session")
def test_engine(test_database_url: str):
    """Create test database engine."""
    engine = create_engine(test_database_url)

    # Create all tables
    Base.metadata.create_all(bind=engine)

    yield engine

    # Cleanup: Drop all tables after tests
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def test_db(test_engine) -> Generator[Session, None, None]:
    """
    Create a fresh database session for each test.

    Automatically rolls back after each test to ensure isolation.
    """
    # Create session
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


@pytest.fixture(scope="function")
def api_client(test_db: Session) -> Generator[TestClient, None, None]:
    """
    Create FastAPI test client with database dependency override.

    Uses TestClient for synchronous testing of async endpoints.
    """
    # Override database dependency
    def override_get_db():
        try:
            yield test_db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    # Create test client
    client = TestClient(app)

    yield client

    # Clean up
    app.dependency_overrides.clear()


@pytest.fixture(scope="session")
def ensure_services_ready():
    """
    Ensure all required services (MinIO, Spark, PostgreSQL, Prefect) are ready.

    This fixture runs once per test session and checks service health.
    Raises exception if services are not available.
    """
    services = {
        "MinIO": {
            "url": "http://localhost:9000/minio/health/live",
            "timeout": 30
        },
        "PostgreSQL": {
            "url": "postgresql://test_user:test_password@localhost:5432/autoloader_test",
            "timeout": 30
        },
        "Spark UI": {
            "url": "http://localhost:4040",
            "timeout": 60  # Spark takes longer to start
        },
        "Prefect": {
            "url": "http://localhost:4200/api/health",
            "timeout": 30
        }
    }

    print("\n🔍 Checking service health...")

    # Check MinIO
    print("  → MinIO: ", end="")
    if check_http_service(services["MinIO"]["url"], services["MinIO"]["timeout"]):
        print("✅ Ready")
    else:
        raise RuntimeError(
            "MinIO not ready. Please start services:\n"
            "  docker-compose -f docker-compose.test.yml up -d"
        )

    # Check PostgreSQL
    print("  → PostgreSQL: ", end="")
    if check_postgres_service(services["PostgreSQL"]["url"], services["PostgreSQL"]["timeout"]):
        print("✅ Ready")
    else:
        raise RuntimeError(
            "PostgreSQL not ready. Please start services:\n"
            "  docker-compose -f docker-compose.test.yml up -d"
        )

    # Check Spark UI (indicates Spark Connect is running)
    print("  → Spark Connect: ", end="")
    if check_http_service(services["Spark UI"]["url"], services["Spark UI"]["timeout"]):
        print("✅ Ready")
    else:
        raise RuntimeError(
            "Spark Connect not ready. Please start services:\n"
            "  docker-compose -f docker-compose.test.yml up -d\n"
            "  Note: Spark may take 1-2 minutes to start"
        )

    # Check Prefect API (server ready)
    print("  → Prefect Server: ", end="")
    if check_http_service(services["Prefect"]["url"], services["Prefect"]["timeout"]):
        print("✅ Ready")
    else:
        raise RuntimeError(
            "Prefect server not ready. Please start services:\n"
            "  docker-compose -f docker-compose.test.yml up -d"
        )

    # Check Prefect work pool exists (worker has initialized)
    print("  → Prefect Work Pool: ", end="")
    if check_prefect_work_pool("default-work-pool", timeout=60):
        print("✅ Ready")
    else:
        raise RuntimeError(
            "Prefect work pool not ready. Worker may still be initializing.\n"
            "  Wait a moment and try again, or check: docker logs autoloader-test-prefect-worker"
        )

    print("✅ All services ready!\n")

    yield True


def check_http_service(url: str, timeout: int) -> bool:
    """
    Check if HTTP service is accessible.

    Args:
        url: Service URL
        timeout: Maximum wait time in seconds

    Returns:
        True if service is ready, False otherwise
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = httpx.get(url, timeout=2, follow_redirects=False)
            # Accept 200, 302 (redirect), 403 (auth required but service is up)
            if response.status_code in [200, 302, 403]:
                return True
        except Exception:
            pass

        time.sleep(1)

    return False


def check_postgres_service(url: str, timeout: int) -> bool:
    """
    Check if PostgreSQL service is accessible.

    Args:
        url: PostgreSQL connection URL
        timeout: Maximum wait time in seconds

    Returns:
        True if service is ready, False otherwise
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            from sqlalchemy import create_engine
            engine = create_engine(url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return True
        except Exception:
            pass

        time.sleep(1)

    return False


def check_prefect_work_pool(pool_name: str, timeout: int) -> bool:
    """
    Check if Prefect work pool exists.

    Args:
        pool_name: Name of the work pool
        timeout: Maximum wait time in seconds

    Returns:
        True if work pool exists, False otherwise
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = httpx.post(
                "http://localhost:4200/api/work_pools/filter",
                json={},
                timeout=5
            )
            if response.status_code == 200:
                pools = response.json()
                if any(pool.get("name") == pool_name for pool in pools):
                    return True
        except Exception:
            pass

        time.sleep(2)

    return False


@pytest.fixture(scope="session")
def spark_connect_url() -> str:
    """Get Spark Connect URL from environment."""
    return os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002")


@pytest.fixture(scope="function")
def spark_session(spark_connect_url: str) -> Generator[SparkSession, None, None]:
    """
    Create Spark Connect session for data verification.

    Uses Spark from docker-compose.test.yml on localhost:15002.
    """
    try:
        spark = SparkSession.builder \
            .remote(spark_connect_url) \
            .appName("Integration-Test") \
            .getOrCreate()

        print(f"Connected to Spark: {spark_connect_url}")

        yield spark

        # Cleanup
        spark.stop()
        print("Stopped Spark session")

    except Exception as e:
        raise RuntimeError(
            f"Failed to connect to Spark Connect at {spark_connect_url}: {e}\n"
            "Ensure Spark is running: docker-compose -f docker-compose.test.yml up -d"
        )
