"""
Unit tests for schema version API endpoints.

Tests the REST API for viewing schema version history.
"""
import uuid

import pytest

from app.models.domain import Ingestion, IngestionStatus
from app.repositories.schema_version_repository import SchemaVersionRepository


@pytest.fixture
def ingestion(test_local_db):
    """Create test ingestion with unique ID."""
    ingestion_id = f"test-ingestion-{uuid.uuid4()}"
    ingestion = Ingestion(
        id=ingestion_id,
        tenant_id="test-tenant",
        name="Test API Ingestion",
        source_type="s3",
        source_path="s3://test-bucket/data/",
        format_type="json",
        destination_catalog="test_catalog",
        destination_database="default",
        destination_table="api_test_table",
        status=IngestionStatus.ACTIVE,
        schema_version=3,
        cluster_id="test-cluster-id",
        created_by="test-user",
        checkpoint_location=f"/tmp/test-checkpoints/{ingestion_id}"
    )
    test_local_db.add(ingestion)
    test_local_db.commit()
    test_local_db.refresh(ingestion)
    return ingestion


@pytest.fixture
def schema_versions(test_local_db, ingestion):
    """Create test schema versions."""
    repo = SchemaVersionRepository(test_local_db)

    versions = []
    for i in range(1, 4):
        version = repo.create_version(
            ingestion_id=ingestion.id,
            version=i,
            schema_json={
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "name", "type": "string"},
                    {"name": f"field_{i}", "type": "string"}
                ]
            },
            changes=[
                {
                    "change_type": "NEW_COLUMN",
                    "column_name": f"field_{i}",
                    "new_type": "string"
                }
            ],
            strategy_applied="append_new_columns",
            affected_files=[f"s3://bucket/file{i}.json"]
        )
        versions.append(version)

    return versions


class TestSchemaVersionsAPI:
    """Test schema versions API endpoints."""

    def test_get_all_schema_versions(self, api_client, ingestion, schema_versions):
        """Test GET /api/v1/ingestions/{id}/schema-versions."""
        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions")

        assert response.status_code == 200

        data = response.json()
        assert "versions" in data
        assert "total" in data
        assert "current_version" in data

        assert data["total"] == 3
        assert data["current_version"] == 3
        assert len(data["versions"]) == 3

        # Verify versions are ordered
        assert data["versions"][0]["version"] == 1
        assert data["versions"][1]["version"] == 2
        assert data["versions"][2]["version"] == 3

        # Verify version structure
        version = data["versions"][0]
        assert "id" in version
        assert "ingestion_id" in version
        assert version["ingestion_id"] == ingestion.id
        assert "version" in version
        assert "detected_at" in version
        assert "schema_json" in version
        assert "changes_json" in version
        assert "strategy_applied" in version
        assert version["strategy_applied"] == "append_new_columns"
        assert "affected_files" in version
        assert len(version["affected_files"]) == 1

    def test_get_all_schema_versions_empty(self, api_client, test_local_db):
        """Test getting versions for ingestion with no versions."""
        # Create ingestion without versions
        ingestion_id = f"empty-ingestion-{uuid.uuid4()}"
        ingestion = Ingestion(
            id=ingestion_id,
            tenant_id="test-tenant",
            name="Empty Ingestion",
            source_type="s3",
            source_path="s3://test/",
            format_type="json",
            destination_catalog="test_catalog",
            destination_database="default",
            destination_table="empty_table",
            status=IngestionStatus.ACTIVE,
            schema_version=1,
            cluster_id="test-cluster-id",
            created_by="test-user",
            checkpoint_location=f"/tmp/test-checkpoints/{ingestion_id}"
        )
        test_local_db.add(ingestion)
        test_local_db.commit()

        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions")

        assert response.status_code == 200

        data = response.json()
        assert data["total"] == 0
        assert data["current_version"] == 1
        assert len(data["versions"]) == 0

    def test_get_all_schema_versions_ingestion_not_found(self, api_client):
        """Test 404 when ingestion doesn't exist."""
        response = api_client.get("/api/v1/ingestions/nonexistent-id/schema-versions")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_schema_version_by_number(self, api_client, ingestion, schema_versions):
        """Test GET /api/v1/ingestions/{id}/schema-versions/{version}."""
        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions/2")

        assert response.status_code == 200

        data = response.json()
        assert data["version"] == 2
        assert data["ingestion_id"] == ingestion.id
        assert "schema_json" in data
        assert data["schema_json"]["fields"][2]["name"] == "field_2"
        assert "changes_json" in data
        assert len(data["changes_json"]) == 1
        assert data["changes_json"][0]["column_name"] == "field_2"
        assert data["affected_files"] == ["s3://bucket/file2.json"]

    def test_get_schema_version_by_number_not_found(self, api_client, ingestion):
        """Test 404 when version doesn't exist."""
        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions/99")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_schema_version_ingestion_not_found(self, api_client):
        """Test 404 when ingestion doesn't exist."""
        response = api_client.get("/api/v1/ingestions/nonexistent/schema-versions/1")

        assert response.status_code == 404

    def test_get_latest_schema_version(self, api_client, ingestion, schema_versions):
        """Test GET /api/v1/ingestions/{id}/schema-versions/latest."""
        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions/latest")

        assert response.status_code == 200

        data = response.json()
        assert data["version"] == 3  # Latest version
        assert data["ingestion_id"] == ingestion.id
        assert data["schema_json"]["fields"][2]["name"] == "field_3"
        assert data["affected_files"] == ["s3://bucket/file3.json"]

    def test_get_latest_schema_version_no_versions(self, api_client, test_local_db):
        """Test 404 when no versions exist."""
        # Create ingestion without versions
        ingestion_id = f"no-versions-ingestion-{uuid.uuid4()}"
        ingestion = Ingestion(
            id=ingestion_id,
            tenant_id="test-tenant",
            name="No Versions Ingestion",
            source_type="s3",
            source_path="s3://test/",
            format_type="json",
            destination_catalog="test_catalog",
            destination_database="default",
            destination_table="no_versions_table",
            status=IngestionStatus.ACTIVE,
            schema_version=1,
            cluster_id="test-cluster-id",
            created_by="test-user",
            checkpoint_location=f"/tmp/test-checkpoints/{ingestion_id}"
        )
        test_local_db.add(ingestion)
        test_local_db.commit()

        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions/latest")

        assert response.status_code == 404
        assert "no schema versions" in response.json()["detail"].lower()

    def test_get_latest_schema_version_ingestion_not_found(self, api_client):
        """Test 404 when ingestion doesn't exist."""
        response = api_client.get("/api/v1/ingestions/nonexistent/schema-versions/latest")

        assert response.status_code == 404

    def test_schema_version_response_structure(self, api_client, ingestion, schema_versions):
        """Test that response matches SchemaVersionResponse schema."""
        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions/1")

        assert response.status_code == 200

        data = response.json()

        # Required fields
        assert isinstance(data["id"], str)
        assert isinstance(data["ingestion_id"], str)
        assert isinstance(data["version"], int)
        assert isinstance(data["detected_at"], str)
        assert isinstance(data["schema_json"], dict)

        # Optional fields
        if data["changes_json"] is not None:
            assert isinstance(data["changes_json"], list)
        if data["strategy_applied"] is not None:
            assert isinstance(data["strategy_applied"], str)
        if data["affected_files"] is not None:
            assert isinstance(data["affected_files"], list)

    def test_schema_version_list_response_structure(self, api_client, ingestion, schema_versions):
        """Test that list response matches SchemaVersionListResponse schema."""
        response = api_client.get(f"/api/v1/ingestions/{ingestion.id}/schema-versions")

        assert response.status_code == 200

        data = response.json()

        # Required fields
        assert isinstance(data["versions"], list)
        assert isinstance(data["total"], int)
        assert isinstance(data["current_version"], int)

        # Verify consistency
        assert data["total"] == len(data["versions"])
        assert data["current_version"] == ingestion.schema_version

    def test_multiple_ingestions_isolated(self, api_client, test_local_db):
        """Test that versions are properly isolated between ingestions."""
        repo_schema = SchemaVersionRepository(test_local_db)

        # Create two ingestions with unique IDs
        ingestion_id_1 = f"ingestion-{uuid.uuid4()}"
        ingestion_id_2 = f"ingestion-{uuid.uuid4()}"

        ingestion1 = Ingestion(
            id=ingestion_id_1,
            tenant_id="test-tenant",
            name="Ingestion 1",
            source_type="s3",
            source_path="s3://test/",
            format_type="json",
            destination_catalog="test_catalog",
            destination_database="default",
            destination_table="table1",
            status=IngestionStatus.ACTIVE,
            schema_version=2,
            cluster_id="test-cluster-id",
            created_by="test-user",
            checkpoint_location=f"/tmp/test-checkpoints/{ingestion_id_1}"
        )
        ingestion2 = Ingestion(
            id=ingestion_id_2,
            tenant_id="test-tenant",
            name="Ingestion 2",
            source_type="s3",
            source_path="s3://test/",
            format_type="json",
            destination_catalog="test_catalog",
            destination_database="default",
            destination_table="table2",
            status=IngestionStatus.ACTIVE,
            schema_version=1,
            cluster_id="test-cluster-id",
            created_by="test-user",
            checkpoint_location=f"/tmp/test-checkpoints/{ingestion_id_2}"
        )
        test_local_db.add(ingestion1)
        test_local_db.add(ingestion2)
        test_local_db.commit()

        # Create versions for ingestion1 only
        repo_schema.create_version(
            ingestion_id=ingestion_id_1,
            version=1,
            schema_json={"test": 1},
            changes=[],
            strategy_applied="append_new_columns"
        )
        repo_schema.create_version(
            ingestion_id=ingestion_id_1,
            version=2,
            schema_json={"test": 2},
            changes=[],
            strategy_applied="append_new_columns"
        )

        # Get versions for ingestion1
        response1 = api_client.get(f"/api/v1/ingestions/{ingestion_id_1}/schema-versions")
        assert response1.status_code == 200
        assert response1.json()["total"] == 2

        # Get versions for ingestion2 (should be empty)
        response2 = api_client.get(f"/api/v1/ingestions/{ingestion_id_2}/schema-versions")
        assert response2.status_code == 200
        assert response2.json()["total"] == 0
