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
from app.spark.connect_client import SparkConnectClient

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


