"""Pydantic schemas for API requests and responses."""
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class IngestionStatus(str, Enum):
    """Ingestion status."""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"


class SchemaEvolutionStrategy(str, Enum):
    """Schema evolution strategy for handling schema changes."""
    IGNORE = "ignore"
    APPEND_NEW_COLUMNS = "append_new_columns"
    SYNC_ALL_COLUMNS = "sync_all_columns"
    FAIL = "fail"


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
    write_mode: str = Field(
        default="append",
        description="Write mode: 'append' only. Note: 'overwrite' and 'merge' are not supported. "
                    "For full refresh, see docs/overwrite-mode-alternative.md"
    )
    partitioning: Optional[PartitioningConfig] = PartitioningConfig()
    optimization: Optional[OptimizationConfig] = OptimizationConfig()

    @field_validator('write_mode')
    @classmethod
    def validate_write_mode(cls, v: str) -> str:
        """
        Validate write_mode is supported.

        Only 'append' mode is supported. Overwrite and merge are not implemented.
        For overwrite-like behavior, see docs/overwrite-mode-alternative.md
        """
        allowed = ['append']
        if v not in allowed:
            raise ValueError(
                f"write_mode '{v}' is not supported. Only 'append' is supported. "
                f"For full refresh behavior, delete the table and optionally clear processed files. "
                f"See docs/overwrite-mode-alternative.md for details."
            )
        return v


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
    on_schema_change: SchemaEvolutionStrategy = SchemaEvolutionStrategy.IGNORE


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
    on_schema_change: SchemaEvolutionStrategy

    metadata: IngestionMetadata

    prefect_deployment_id: Optional[str] = None  # Prefect deployment UUID

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


class SchemaVersionResponse(BaseModel):
    """Schema version history response."""
    id: str
    ingestion_id: str
    version: int
    detected_at: datetime
    schema_json: Dict[str, Any]
    changes_json: Optional[List[Dict[str, Any]]] = None
    strategy_applied: Optional[str] = None
    affected_files: Optional[List[str]] = None

    class Config:
        from_attributes = True


class SchemaVersionListResponse(BaseModel):
    """List of schema versions for an ingestion."""
    versions: List[SchemaVersionResponse]
    total: int
    current_version: int


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


# Refresh Operations
class RefreshRequest(BaseModel):
    """Request schema for table refresh operation."""

    confirm: bool = Field(
        ...,
        description="Must be true to proceed. Safety confirmation required.",
        examples=[True]
    )

    auto_run: bool = Field(
        default=True,
        description="Automatically trigger ingestion run after refresh",
        examples=[True]
    )

    dry_run: bool = Field(
        default=False,
        description="Preview operations without executing them",
        examples=[False]
    )

    @field_validator('confirm')
    @classmethod
    def confirm_must_be_true(cls, v: bool) -> bool:
        if not v:
            raise ValueError(
                "Confirmation required. This is a destructive operation. "
                "Set confirm=true to proceed."
            )
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "confirm": True,
                "auto_run": True,
                "dry_run": False
            }
        }


class OperationStatus(str, Enum):
    """Operation status."""
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class Operation(BaseModel):
    """Details of a single operation within a refresh."""

    operation: str = Field(
        ...,
        description="Operation type",
        examples=["table_dropped"]
    )
    status: OperationStatus = Field(
        ...,
        description="Operation status",
        examples=["success"]
    )
    timestamp: datetime = Field(
        ...,
        description="When operation was performed",
        examples=["2025-01-05T10:30:01Z"]
    )
    details: Dict[str, Any] = Field(
        default_factory=dict,
        description="Operation-specific details"
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if operation failed"
    )
    error_code: Optional[str] = Field(
        default=None,
        description="Machine-readable error code"
    )


class ImpactEstimate(BaseModel):
    """Estimated impact of the refresh operation."""

    files_to_process: int = Field(
        ...,
        description="Number of files that will be processed",
        examples=[1247]
    )
    files_skipped: Optional[int] = Field(
        default=None,
        description="Number of files that will be skipped",
        examples=[0]
    )
    estimated_data_size_gb: float = Field(
        ...,
        description="Estimated data size to process (GB)",
        examples=[450.2]
    )
    estimated_cost_usd: float = Field(
        ...,
        description="Estimated processing cost (USD)",
        examples=[125.50]
    )
    estimated_duration_minutes: int = Field(
        ...,
        description="Estimated processing time (minutes)",
        examples=[45]
    )


class RefreshOperationResponse(BaseModel):
    """Standard response for refresh operations."""

    status: str = Field(
        ...,
        description="Overall operation status",
        examples=["accepted"]
    )
    message: str = Field(
        ...,
        description="Human-readable summary",
        examples=["Full refresh completed successfully"]
    )
    ingestion_id: str = Field(
        ...,
        description="Ingestion identifier",
        examples=["ing-abc123"]
    )
    run_id: Optional[str] = Field(
        default=None,
        description="Run identifier if run was triggered",
        examples=["run-xyz789"]
    )
    timestamp: datetime = Field(
        ...,
        description="Response timestamp",
        examples=["2025-01-05T10:30:00Z"]
    )
    mode: str = Field(
        ...,
        description="Refresh mode: full or new_only",
        examples=["full"]
    )
    operations: List[Operation] = Field(
        default_factory=list,
        description="List of operations performed"
    )
    impact: ImpactEstimate = Field(
        ...,
        description="Impact estimate"
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="Warnings about the operation"
    )
    notes: List[str] = Field(
        default_factory=list,
        description="Additional notes and context"
    )
    would_perform: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Operations that would be performed (dry run only)"
    )
    next_steps: Optional[Dict[str, str]] = Field(
        default=None,
        description="Next steps (dry run only)"
    )
