"""Pydantic schemas for API requests and responses."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class IngestionStatus(str, Enum):
    """Ingestion status."""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"


class RunStatus(str, Enum):
    """Run status."""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


# Source Configuration
class SourceConfig(BaseModel):
    """Source configuration schema."""
    type: str = Field(..., description="Source type: s3, azure_blob, gcs")
    path: str = Field(..., description="Source path (bucket/path)")
    file_pattern: Optional[str] = Field(None, description="File pattern (e.g., *.json)")
    credentials: Optional[Dict[str, Any]] = Field(None, description="Source credentials")


# Format Configuration
class FormatOptions(BaseModel):
    """Format-specific options."""
    multiline: Optional[bool] = None
    compression: Optional[str] = None
    header: Optional[bool] = None
    delimiter: Optional[str] = None
    quote: Optional[str] = None


class SchemaConfig(BaseModel):
    """Schema configuration."""
    inference: str = Field(default="auto", description="auto or manual")
    evolution_enabled: bool = Field(default=True, description="Enable schema evolution")
    schema_json: Optional[Dict[str, Any]] = None


class FormatConfig(BaseModel):
    """Format configuration schema."""
    type: str = Field(..., description="Format: json, csv, parquet, avro, orc")
    options: Optional[FormatOptions] = None
    schema: Optional[SchemaConfig] = SchemaConfig()


# Destination Configuration
class PartitioningConfig(BaseModel):
    """Partitioning configuration."""
    enabled: bool = False
    columns: List[str] = []


class OptimizationConfig(BaseModel):
    """Optimization configuration."""
    z_ordering_enabled: bool = False
    z_ordering_columns: List[str] = []


class DestinationConfig(BaseModel):
    """Destination configuration schema."""
    catalog: str
    database: str
    table: str
    write_mode: str = Field(default="append", description="append, overwrite, merge")
    partitioning: Optional[PartitioningConfig] = PartitioningConfig()
    optimization: Optional[OptimizationConfig] = OptimizationConfig()


# Schedule Configuration
class BackfillConfig(BaseModel):
    """Backfill configuration."""
    enabled: bool = False
    start_date: Optional[datetime] = None


class ScheduleConfig(BaseModel):
    """Schedule configuration schema (batch mode - processes available data then stops)."""
    frequency: Optional[str] = Field(None, description="daily, hourly, weekly, custom")
    time: Optional[str] = None
    timezone: str = "UTC"
    cron_expression: Optional[str] = None
    backfill: Optional[BackfillConfig] = BackfillConfig()


# Quality Configuration
class QualityConfig(BaseModel):
    """Data quality configuration."""
    row_count_threshold: Optional[int] = None
    alerts_enabled: bool = True
    alert_recipients: List[str] = []


# Ingestion Requests/Responses
class IngestionCreate(BaseModel):
    """Create ingestion request."""
    name: str
    cluster_id: str
    source: SourceConfig
    format: FormatConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    quality: Optional[QualityConfig] = QualityConfig()


class IngestionUpdate(BaseModel):
    """Update ingestion request."""
    status: Optional[IngestionStatus] = None
    schedule: Optional[ScheduleConfig] = None
    quality: Optional[QualityConfig] = None
    alert_recipients: Optional[List[str]] = None


class IngestionMetadata(BaseModel):
    """Ingestion metadata."""
    checkpoint_location: str
    last_run_id: Optional[str] = None
    last_run_time: Optional[datetime] = None
    next_run_time: Optional[datetime] = None
    schema_version: int = 1
    estimated_monthly_cost: Optional[float] = None


class IngestionResponse(BaseModel):
    """Ingestion response."""
    id: str
    tenant_id: str
    name: str
    status: IngestionStatus
    cluster_id: str
    spark_connect_url: str

    source: SourceConfig
    format: FormatConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    quality: QualityConfig

    metadata: IngestionMetadata

    created_at: datetime
    updated_at: datetime
    created_by: str

    class Config:
        from_attributes = True


# Preview/Test
class ColumnSchema(BaseModel):
    """Column schema."""
    name: str
    data_type: str
    nullable: bool
    is_new: bool = False


class PreviewResult(BaseModel):
    """Preview result."""
    schema: List[ColumnSchema]
    sample_data: List[str]
    file_count: int
    estimated_records: int


# Cost Estimation
class CostBreakdown(BaseModel):
    """Cost breakdown."""
    compute_per_run: float
    compute_monthly: float
    storage_monthly: float
    discovery_monthly: float
    total_monthly: float
    breakdown_details: Dict[str, Any]


# Run History
class RunMetrics(BaseModel):
    """Run metrics."""
    files_processed: int = 0
    records_ingested: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    duration_seconds: Optional[int] = None
    error_count: int = 0


class RunError(BaseModel):
    """Run error."""
    file: str
    error_type: str
    message: str
    timestamp: datetime


class RunResponse(BaseModel):
    """Run response."""
    id: str
    ingestion_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    status: RunStatus
    trigger: str
    metrics: RunMetrics
    errors: Optional[List[RunError]] = None
    spark_job_id: Optional[str] = None

    class Config:
        from_attributes = True


# Schema Evolution
class SchemaChange(BaseModel):
    """Schema change."""
    change_type: str  # NEW_COLUMN, REMOVED_COLUMN, TYPE_CHANGE
    column_name: str
    old_type: Optional[str] = None
    new_type: Optional[str] = None


class SchemaEvolutionResponse(BaseModel):
    """Schema evolution response."""
    id: str
    ingestion_id: str
    version: int
    detected_at: datetime
    schema: List[ColumnSchema]
    changes: List[SchemaChange]
    affected_files: List[str]
    resolution_type: Optional[str] = None

    class Config:
        from_attributes = True


# Cluster Information
class ClusterInfo(BaseModel):
    """Cluster information."""
    id: str
    name: str
    status: str  # running, stopped
    workers: int
    dbu_per_worker: int
    spark_connect_url: str
    pricing: Dict[str, float]
