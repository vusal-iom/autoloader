"""
Unit Test Fixtures Configuration.

Imports fixtures for unit tests that use local SQLite database.
No external services (Docker) required for unit tests.

All fixtures here are automatically discovered by pytest for unit tests.
"""
from typing import Generator
import pytest
from sqlalchemy.orm import Session, sessionmaker
from fastapi.testclient import TestClient
from app.main import app
from app.database import Base, get_db

# Import local SQLite fixtures for unit tests
from tests.fixtures.local_db import (
    test_local_database_url,
    test_local_db_engine,
    test_local_db
)

__all__ = [
    "test_local_database_url",
    "test_local_db_engine",
    "test_local_db",
]


@pytest.fixture(scope="function")
def api_client(test_local_db: Session) -> Generator[TestClient, None, None]:
    """
    Create FastAPI test client with database dependency override.

    Uses TestClient for synchronous testing of async endpoints.
    """
    # Override database dependency
    def override_get_db():
        try:
            yield test_local_db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    # Create test client
    client = TestClient(app)

    yield client

    # Clean up
    app.dependency_overrides.clear()