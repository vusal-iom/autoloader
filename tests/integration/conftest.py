"""
Integration Test Fixtures Configuration.

Imports fixtures for integration tests that use real PostgreSQL database
but may not require all external services (MinIO, Spark).

All fixtures here are automatically discovered by pytest for integration tests.
"""

# Import database fixtures for integration tests
from tests.fixtures.integration_db import (
    test_database_url,
    test_engine,
    test_db,
    api_client
)

# Import service readiness checks (if integration tests need Docker services)
from tests.fixtures.ensure_services_ready import ensure_services_ready

# Optionally import MinIO/Spark if needed for specific integration tests
# from tests.fixtures.integration_minio import minio_client, test_bucket
# from tests.fixtures.integration_spark import spark_session

__all__ = [
    # Database
    "test_database_url",
    "test_engine",
    "test_db",
    "api_client",
    # Service health (if needed)
    "ensure_services_ready",
]
