"""
Root conftest.py for pytest fixture discovery.

This file enables automatic fixture discovery for all tests.
Specific fixtures are organized in subdirectories:
- tests/e2e/conftest.py: E2E test fixtures (PostgreSQL, MinIO, Spark)
- tests/integration/conftest.py: Integration test fixtures (PostgreSQL, API)
- tests/unit/conftest.py: Unit test fixtures (SQLite)

Modular fixture definitions are kept in tests/fixtures/ for organization.
"""
import os
import uuid
from typing import List

import pytest

from tests.helpers.logger import TestLogger


@pytest.fixture(scope="function")
def test_tenant_id() -> str:
    """Get test tenant ID from environment."""
    return os.getenv("TEST_TENANT_ID", "test-tenant-001")


@pytest.fixture(scope="function")
def test_cluster_id() -> str:
    """Get test cluster ID from environment."""
    return os.getenv("TEST_CLUSTER_ID", "test-cluster-001")


@pytest.fixture
def temporary_table(spark_session):
    """Factory fixture that creates unique Iceberg tables and cleans them up."""
    created: List[tuple[str, TestLogger]] = []

    def _create(prefix: str, logger: TestLogger) -> str:
        table_name = f"{prefix}_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"
        created.append((table_id, logger))
        return table_id

    yield _create

    for table_id, logger in created:
        spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
        logger.step(f"Cleaned up table: {table_id}", always=True)