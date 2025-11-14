from typing import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.database import Base, get_db
from app.main import app

# Get settings
settings = get_settings()

@pytest.fixture(scope="session")
def test_database_url() -> str:
    return "postgresql://test_user:test_password@localhost:5432/autoloader_test"


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