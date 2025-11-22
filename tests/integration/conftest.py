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

from tests.fixtures.integration_spark import (
    spark_connect_url,
    spark_client,
    spark_session
)

from tests.fixtures.integration_minio import (
    minio_config,
    minio_client,
    lakehouse_bucket
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

    # Spark
    "spark_connect_url",
    "spark_client",
    "spark_session",

    # Mino
    "minio_config",
    "minio_client",
    "lakehouse_bucket",
    "upload_file"
]

import json
import io
import pytest
from typing import Any, List

@pytest.fixture
def upload_file(minio_client, lakehouse_bucket):
    """
    Fixture that provides a helper to upload files to MinIO and cleans them up after the test.
    
    Returns:
        Callable[[str, Any], str]: A function that takes (key, content), uploads it, 
                                   and returns the s3a:// path.
    """
    uploaded_keys: List[str] = []

    def _upload(key: str, content: Any) -> str:
        if isinstance(content, bytes):
            data = content
        elif isinstance(content, str):
            data = content.encode('utf-8')
        else:
            data = json.dumps(content).encode('utf-8')

        minio_client.put_object(
            Bucket=lakehouse_bucket,
            Key=key,
            Body=io.BytesIO(data)
        )
        uploaded_keys.append(key)
        return f"s3a://{lakehouse_bucket}/{key}"

    yield _upload

    # Cleanup
    for key in uploaded_keys:
        try:
            minio_client.delete_object(Bucket=lakehouse_bucket, Key=key)
        except Exception:
            pass  # Ignore cleanup errors
