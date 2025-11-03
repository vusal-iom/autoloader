"""SQLAlchemy domain models."""
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, JSON, Text, Enum as SQLEnum
from sqlalchemy.sql import func
import enum
from datetime import datetime

from app.database import Base


class IngestionStatus(str, enum.Enum):
    """Ingestion status enum."""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"


class RunStatus(str, enum.Enum):
    """Run status enum."""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class Ingestion(Base):
    """Ingestion configuration model."""

    __tablename__ = "ingestions"

    id = Column(String, primary_key=True, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    status = Column(SQLEnum(IngestionStatus), default=IngestionStatus.DRAFT)

    # Cluster information
    cluster_id = Column(String, nullable=False)
    # Note: spark_connect_url and spark_connect_token are retrieved dynamically
    # from cluster_id at runtime via get_spark_connect_url() and cluster API

    # Source configuration
    source_type = Column(String, nullable=False)  # s3, azure_blob, gcs
    source_path = Column(String, nullable=False)
    source_file_pattern = Column(String, nullable=True)
    source_credentials = Column(JSON, nullable=True)  # Encrypted

    # Format configuration
    format_type = Column(String, nullable=False)  # json, csv, parquet, etc.
    format_options = Column(JSON, nullable=True)
    schema_inference = Column(String, default="auto")  # auto, manual
    schema_evolution_enabled = Column(Boolean, default=True)
    schema_json = Column(JSON, nullable=True)

    # Destination configuration
    destination_catalog = Column(String, nullable=False)
    destination_database = Column(String, nullable=False)
    destination_table = Column(String, nullable=False)
    write_mode = Column(String, default="append")  # append, overwrite, merge
    partitioning_enabled = Column(Boolean, default=False)
    partitioning_columns = Column(JSON, nullable=True)
    z_ordering_enabled = Column(Boolean, default=False)
    z_ordering_columns = Column(JSON, nullable=True)

    # Schedule configuration
    schedule_mode = Column(String, default="scheduled")  # scheduled, continuous
    schedule_frequency = Column(String, nullable=True)  # daily, hourly, weekly, custom
    schedule_time = Column(String, nullable=True)
    schedule_timezone = Column(String, default="UTC")
    schedule_cron = Column(String, nullable=True)
    backfill_enabled = Column(Boolean, default=False)
    backfill_start_date = Column(DateTime, nullable=True)

    # Quality rules
    row_count_threshold = Column(Integer, nullable=True)
    alerts_enabled = Column(Boolean, default=True)
    alert_recipients = Column(JSON, nullable=True)

    # Metadata
    checkpoint_location = Column(String, nullable=False)
    last_run_id = Column(String, nullable=True)
    last_run_time = Column(DateTime, nullable=True)
    next_run_time = Column(DateTime, nullable=True)
    schema_version = Column(Integer, default=1)
    estimated_monthly_cost = Column(Float, nullable=True)

    # Audit fields
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_by = Column(String, nullable=False)


class Run(Base):
    """Ingestion run history model."""

    __tablename__ = "runs"

    id = Column(String, primary_key=True, index=True)
    ingestion_id = Column(String, nullable=False, index=True)

    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    status = Column(SQLEnum(RunStatus), default=RunStatus.RUNNING)
    trigger = Column(String, nullable=False)  # scheduled, manual, retry

    # Metrics
    files_processed = Column(Integer, default=0)
    records_ingested = Column(Integer, default=0)
    bytes_read = Column(Integer, default=0)
    bytes_written = Column(Integer, default=0)
    duration_seconds = Column(Integer, nullable=True)
    error_count = Column(Integer, default=0)

    # Error details
    errors = Column(JSON, nullable=True)

    # Spark job information
    spark_job_id = Column(String, nullable=True)
    cluster_id = Column(String, nullable=False)


class SchemaVersion(Base):
    """Schema version history model."""

    __tablename__ = "schema_versions"

    id = Column(String, primary_key=True, index=True)
    ingestion_id = Column(String, nullable=False, index=True)
    version = Column(Integer, nullable=False)

    detected_at = Column(DateTime, nullable=False, server_default=func.now())
    schema_json = Column(JSON, nullable=False)
    affected_files = Column(JSON, nullable=True)

    # Resolution
    resolution_type = Column(String, nullable=True)  # auto_merge, backfill, ignore, manual
    resolved_at = Column(DateTime, nullable=True)
    resolved_by = Column(String, nullable=True)
