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

import pytest


@pytest.fixture(scope="function")
def test_tenant_id() -> str:
    """Get test tenant ID from environment."""
    return os.getenv("TEST_TENANT_ID", "test-tenant-001")


@pytest.fixture(scope="function")
def test_cluster_id() -> str:
    """Get test cluster ID from environment."""
    return os.getenv("TEST_CLUSTER_ID", "test-cluster-001")