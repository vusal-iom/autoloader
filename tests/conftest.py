"""
Root conftest.py for pytest fixture discovery.

This file enables automatic fixture discovery for all tests.
Specific fixtures are organized in subdirectories:
- tests/e2e/conftest.py: E2E test fixtures (PostgreSQL, MinIO, Spark)
- tests/integration/conftest.py: Integration test fixtures (PostgreSQL, API)
- tests/unit/conftest.py: Unit test fixtures (SQLite)

Modular fixture definitions are kept in tests/fixtures/ for organization.
"""
