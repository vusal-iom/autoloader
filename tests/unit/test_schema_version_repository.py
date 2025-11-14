"""
Unit tests for SchemaVersionRepository.

Tests version tracking, retrieval, and version number generation.
"""
import pytest
import uuid
from app.repositories.schema_version_repository import SchemaVersionRepository
from app.models.domain import SchemaVersion

from tests.fixtures.local_db import test_local_db, test_local_db_engine, test_local_database_url

class TestSchemaVersionRepository:
    """Test SchemaVersionRepository CRUD operations."""

    def test_create_version(self, test_local_db):
        """Test creating a schema version record."""
        repo = SchemaVersionRepository(test_local_db)

        schema_json = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"}
            ]
        }

        changes = [
            {
                "change_type": "NEW_COLUMN",
                "column_name": "email",
                "new_type": "string"
            }
        ]

        version = repo.create_version(
            ingestion_id="test-ingestion-1",
            version=1,
            schema_json=schema_json,
            changes=changes,
            strategy_applied="append_new_columns",
            affected_files=["s3://bucket/file1.json"]
        )

        assert version.id is not None
        assert version.ingestion_id == "test-ingestion-1"
        assert version.version == 1
        assert version.schema_json == schema_json
        assert version.changes_json == changes
        assert version.strategy_applied == "append_new_columns"
        assert version.affected_files == ["s3://bucket/file1.json"]
        assert version.detected_at is not None

    def test_get_latest_version(self, test_db):
        """Test retrieving the latest schema version."""
        repo = SchemaVersionRepository(test_db)

        # Create multiple versions
        for i in range(1, 4):
            repo.create_version(
                ingestion_id="test-ingestion-1",
                version=i,
                schema_json={"version": i},
                changes=[],
                strategy_applied="append_new_columns"
            )

        latest = repo.get_latest_version("test-ingestion-1")

        assert latest is not None
        assert latest.version == 3
        assert latest.schema_json == {"version": 3}

    def test_get_latest_version_no_versions(self, test_db):
        """Test get_latest_version returns None when no versions exist."""
        repo = SchemaVersionRepository(test_db)

        latest = repo.get_latest_version("nonexistent-ingestion")

        assert latest is None

    def test_get_all_versions(self, test_db):
        """Test retrieving all versions for an ingestion."""
        repo = SchemaVersionRepository(test_db)

        # Create versions for two different ingestions
        for i in range(1, 4):
            repo.create_version(
                ingestion_id="ingestion-1",
                version=i,
                schema_json={"version": i},
                changes=[],
                strategy_applied="append_new_columns"
            )

        repo.create_version(
            ingestion_id="ingestion-2",
            version=1,
            schema_json={"version": 1},
            changes=[],
            strategy_applied="append_new_columns"
        )

        versions = repo.get_all_versions("ingestion-1")

        assert len(versions) == 3
        assert versions[0].version == 1
        assert versions[1].version == 2
        assert versions[2].version == 3

    def test_get_version_by_number(self, test_db):
        """Test retrieving a specific version by number."""
        repo = SchemaVersionRepository(test_db)

        # Create multiple versions
        for i in range(1, 4):
            repo.create_version(
                ingestion_id="test-ingestion-1",
                version=i,
                schema_json={"version": i},
                changes=[],
                strategy_applied="append_new_columns"
            )

        version = repo.get_version_by_number("test-ingestion-1", 2)

        assert version is not None
        assert version.version == 2
        assert version.schema_json == {"version": 2}

    def test_get_version_by_number_not_found(self, test_db):
        """Test get_version_by_number returns None for nonexistent version."""
        repo = SchemaVersionRepository(test_db)

        repo.create_version(
            ingestion_id="test-ingestion-1",
            version=1,
            schema_json={"version": 1},
            changes=[],
            strategy_applied="append_new_columns"
        )

        version = repo.get_version_by_number("test-ingestion-1", 99)

        assert version is None

    def test_get_next_version_number_first_version(self, test_db):
        """Test get_next_version_number returns 1 for new ingestion."""
        repo = SchemaVersionRepository(test_db)

        next_version = repo.get_next_version_number("new-ingestion")

        assert next_version == 1

    def test_get_next_version_number_increments(self, test_db):
        """Test get_next_version_number increments correctly."""
        repo = SchemaVersionRepository(test_db)
        ingestion_id = f"test-ingestion-{uuid.uuid4()}"

        # Create version 1
        repo.create_version(
            ingestion_id=ingestion_id,
            version=1,
            schema_json={"version": 1},
            changes=[],
            strategy_applied="append_new_columns"
        )

        next_version = repo.get_next_version_number(ingestion_id)
        assert next_version == 2

        # Create version 2
        repo.create_version(
            ingestion_id=ingestion_id,
            version=2,
            schema_json={"version": 2},
            changes=[],
            strategy_applied="append_new_columns"
        )

        next_version = repo.get_next_version_number(ingestion_id)
        assert next_version == 3

    def test_multiple_ingestions_isolated(self, test_db):
        """Test that versions are isolated per ingestion."""
        repo = SchemaVersionRepository(test_db)
        ingestion_id_1 = f"ingestion-{uuid.uuid4()}"
        ingestion_id_2 = f"ingestion-{uuid.uuid4()}"

        # Create versions for two different ingestions
        repo.create_version(
            ingestion_id=ingestion_id_1,
            version=1,
            schema_json={"ingestion": 1},
            changes=[],
            strategy_applied="append_new_columns"
        )

        repo.create_version(
            ingestion_id=ingestion_id_2,
            version=1,
            schema_json={"ingestion": 2},
            changes=[],
            strategy_applied="append_new_columns"
        )

        # Each ingestion should have only its own versions
        versions_1 = repo.get_all_versions(ingestion_id_1)
        versions_2 = repo.get_all_versions(ingestion_id_2)

        assert len(versions_1) == 1
        assert len(versions_2) == 1
        assert versions_1[0].schema_json == {"ingestion": 1}
        assert versions_2[0].schema_json == {"ingestion": 2}

    def test_affected_files_optional(self, test_db):
        """Test that affected_files is optional."""
        repo = SchemaVersionRepository(test_db)

        version = repo.create_version(
            ingestion_id="test-ingestion-1",
            version=1,
            schema_json={"test": "schema"},
            changes=[],
            strategy_applied="append_new_columns"
            # affected_files not provided
        )

        assert version.affected_files == []

    def test_changes_stored_correctly(self, test_db):
        """Test that changes are stored and retrieved correctly."""
        repo = SchemaVersionRepository(test_db)

        changes = [
            {
                "change_type": "NEW_COLUMN",
                "column_name": "email",
                "new_type": "string"
            },
            {
                "change_type": "TYPE_CHANGE",
                "column_name": "id",
                "old_type": "integer",
                "new_type": "bigint"
            },
            {
                "change_type": "REMOVED_COLUMN",
                "column_name": "deprecated_field",
                "old_type": "string"
            }
        ]

        version = repo.create_version(
            ingestion_id="test-ingestion-1",
            version=1,
            schema_json={"test": "schema"},
            changes=changes,
            strategy_applied="sync_all_columns"
        )

        assert len(version.changes_json) == 3
        assert version.changes_json[0]["change_type"] == "NEW_COLUMN"
        assert version.changes_json[1]["change_type"] == "TYPE_CHANGE"
        assert version.changes_json[2]["change_type"] == "REMOVED_COLUMN"
