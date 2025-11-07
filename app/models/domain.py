"""SQLAlchemy domain models."""
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, JSON, Text, Enum as SQLEnum, BigInteger, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from datetime import datetime
import uuid

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


class ProcessedFileStatus(str, enum.Enum):
    """Status of a processed file."""
    DISCOVERED = "DISCOVERED"      # File found, not yet processed
    PROCESSING = "PROCESSING"      # Currently being processed
    SUCCESS = "SUCCESS"            # Successfully processed
    FAILED = "FAILED"              # Processing failed (retryable)
    SKIPPED = "SKIPPED"            # Intentionally skipped


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
    # write_mode: Only "append" is supported. "overwrite" and "merge" are not implemented.
    # For full refresh, see docs/overwrite-mode-alternative.md
    write_mode = Column(String, default="append")
    partitioning_enabled = Column(Boolean, default=False)
    partitioning_columns = Column(JSON, nullable=True)
    z_ordering_enabled = Column(Boolean, default=False)
    z_ordering_columns = Column(JSON, nullable=True)

    # Schedule configuration (batch mode only - processes available data then stops)
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

    # Prefect integration
    prefect_deployment_id = Column(String(255), nullable=True)
    prefect_flow_id = Column(String(255), nullable=True)

    # Audit fields
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_by = Column(String, nullable=False)

    # Relationships
    processed_files = relationship("ProcessedFile", back_populates="ingestion", cascade="all, delete-orphan")


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

    # Relationships
    processed_files = relationship("ProcessedFile", back_populates="run")

    @property
    def metrics(self):
        """Return metrics as a structured object for API serialization."""
        # Import here to avoid circular dependency
        from app.models.schemas import RunMetrics
        return RunMetrics(
            files_processed=self.files_processed or 0,
            records_ingested=self.records_ingested or 0,
            bytes_read=self.bytes_read or 0,
            bytes_written=self.bytes_written or 0,
            duration_seconds=self.duration_seconds,
            error_count=self.error_count or 0
        )


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


class ProcessedFile(Base):
    """
    Tracks processing state of individual files.

    Each file discovered from cloud storage gets a record here.
    This is the source of truth for "has this file been processed?"
    """
    __tablename__ = "processed_files"

    # Primary key
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # Foreign keys
    ingestion_id = Column(String(36), ForeignKey("ingestions.id", ondelete="CASCADE"), nullable=False)
    run_id = Column(String(36), ForeignKey("runs.id", ondelete="SET NULL"), nullable=True)

    # File identification
    file_path = Column(Text, nullable=False)  # Full path: s3://bucket/path/to/file.json
    file_size_bytes = Column(BigInteger, nullable=True)
    file_modified_at = Column(DateTime(timezone=True), nullable=True)
    file_etag = Column(String(255), nullable=True)  # S3 ETag for change detection

    # Processing state
    status = Column(
        String(20),
        nullable=False,
        default="DISCOVERED"
    )  # DISCOVERED, PROCESSING, SUCCESS, FAILED, SKIPPED

    discovered_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    processing_started_at = Column(DateTime(timezone=True), nullable=True)
    processed_at = Column(DateTime(timezone=True), nullable=True)

    # Metrics
    records_ingested = Column(Integer, nullable=True)
    bytes_read = Column(BigInteger, nullable=True)
    processing_duration_ms = Column(Integer, nullable=True)

    # Error tracking
    error_message = Column(Text, nullable=True)
    error_type = Column(String(100), nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    ingestion = relationship("Ingestion", back_populates="processed_files")
    run = relationship("Run", back_populates="processed_files")

    # Constraints
    __table_args__ = (
        Index("idx_processed_files_ingestion_status", "ingestion_id", "status"),
        Index("idx_processed_files_run", "run_id"),
        Index("idx_processed_files_path", "file_path"),
        Index("idx_processed_files_status_date", "status", "processed_at"),
    )

    def __repr__(self):
        return f"<ProcessedFile(id={self.id}, file_path={self.file_path}, status={self.status})>"
