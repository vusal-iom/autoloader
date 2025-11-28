"""
Factory for creating test Ingestion objects with sensible defaults.
"""
import json
import uuid
from typing import Optional

from sqlalchemy.orm import Session

from app.models.domain import Ingestion, IngestionStatus


def create_test_ingestion(
    test_db: Session,
    *,
    format_type: str = "json",
    format_options: Optional[dict] = None,
    schema_json: Optional[str] = None,
    name_prefix: str = "Test Ingestion"
) -> Ingestion:
    """
    Create and persist a test Ingestion with sensible defaults.

    Args:
        test_db: SQLAlchemy session
        format_type: File format (json, csv, parquet)
        format_options: Optional format options dict (will be JSON-encoded)
        schema_json: Optional schema JSON string
        name_prefix: Prefix for ingestion name

    Returns:
        Persisted Ingestion object
    """
    unique_id = str(uuid.uuid4())
    ingestion = Ingestion(
        id=f"test-ingestion-{unique_id}",
        tenant_id="test-tenant",
        name=f"{name_prefix} {unique_id[:8]}",
        cluster_id="test-cluster-1",
        source_type="s3",
        source_path="s3://test-bucket/data/",
        source_credentials={"aws_access_key_id": "test", "aws_secret_access_key": "test"},
        format_type=format_type,
        format_options=json.dumps(format_options) if format_options else None,
        schema_json=schema_json,
        destination_catalog="test_catalog",
        destination_database="test_db",
        destination_table=f"batch_test_{unique_id[:8]}",
        checkpoint_location=f"/tmp/test-checkpoint-{unique_id}",
        status=IngestionStatus.ACTIVE,
        on_schema_change="append_new_columns",
        schema_version=1,
        created_by="test-user"
    )
    test_db.add(ingestion)
    test_db.commit()
    test_db.refresh(ingestion)
    return ingestion
