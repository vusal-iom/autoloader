# models.py
"""
Data models for IOMETE Data Ingestion
Includes both Pydantic models (API) and SQLAlchemy models (Database)
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import Column, String, DateTime, JSON, Enum as SQLEnum, Float, Integer, Boolean, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

# SQLAlchemy Base
Base = declarative_base()


# ==================== ENUMS ====================

class SourceType(str, Enum):
    S3 = "s3"
    AZURE_BLOB = "azure_blob"
    GCS = "gcs"
    SFTP = "sftp"
    HDFS = "hdfs"


class FileFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    XML = "xml"


class IngestionStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"
    DELETED = "deleted"


class ProcessingMode(str, Enum):
    SCHEDULED = "scheduled"
    CONTINUOUS = "continuous"


class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class RunTrigger(str, Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    RETRY = "retry"


# ==================== PYDANTIC MODELS (API) ====================

class SourceCredentials(BaseModel):
    """Cloud storage credentials"""
    credential_type: str = Field(..., description="iam_role, access_key, service_account")
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None  # Will be encrypted
    region: Optional[str] = None
    connection_string: Optional[str] = None
    service_account_key: Optional[str] = None

    model_config = ConfigDict(extra='allow')


class SourceConfig(BaseModel):
    """Source configuration"""
    type: SourceType
    path: str = Field(..., description="Cloud storage path")
    file_pattern: Optional[str] = Field(None, description="Glob pattern for files")
    credentials: SourceCredentials


class FormatOptions(BaseModel):
    """Format-specific options"""
    multiline: Optional[bool] = False
    compression: Optional[str] = "snappy"
    header: Optional[bool] = True
    delimiter: Optional[str] = ","
    quote: Optional[str] = '"'

    model_config = ConfigDict(extra='allow')


class SchemaConfig(BaseModel):
    """Schema configuration"""
    inference: str = Field("auto", description="auto or manual")
    evolution_enabled: bool = True
    infer_types: bool = True
    schema_json: Optional[Dict[str, Any]] = None


class FormatConfig(BaseModel):
    """File format configuration"""
    type: FileFormat
    options: FormatOptions
    schema: SchemaConfig


class PartitioningConfig(BaseModel):
    """Table partitioning configuration"""
    enabled: bool = False
    columns: List[str] = Field(default_factory=list)


class OptimizationConfig(BaseModel):
    """Table optimization configuration"""
    z_ordering_enabled: bool = False
    z_ordering_columns: List[str] = Field(default_factory=list)


class DestinationConfig(BaseModel):
    """Destination table configuration"""
    catalog: str
    database: str
    table: str
    write_mode: WriteMode = WriteMode.APPEND
    partitioning: PartitioningConfig = Field(default_factory=PartitioningConfig)
    optimization: OptimizationConfig = Field(default_factory=OptimizationConfig)


class BackfillConfig(BaseModel):
    """Backfill configuration"""
    enabled: bool = False
    start_date: Optional[datetime] = None


class ScheduleConfig(BaseModel):
    """Processing schedule configuration"""
    mode: ProcessingMode
    frequency: Optional[str] = Field(None, description="daily, hourly, weekly, custom")
    time: Optional[str] = Field(None, description="HH:MM format")
    timezone: str = "UTC"
    cron_expression: Optional[str] = None
    backfill: BackfillConfig = Field(default_factory=BackfillConfig)
    processing_interval: Optional[str] = "10 seconds"  # For continuous mode


class QualityConfig(BaseModel):
    """Data quality configuration"""
    row_count_threshold: Optional[int] = None
    alerts_enabled: bool = True
    alert_recipients: List[str] = Field(default_factory=list)


class IngestionMetadata(BaseModel):
    """Internal metadata"""
    checkpoint_location: Optional[str] = None
    last_run_id: Optional[str] = None
    last_run_time: Optional[datetime] = None
    next_run_time: Optional[datetime] = None
    schema_version: int = 1
    estimated_monthly_cost: Optional[float] = None


class IngestionCreate(BaseModel):
    """Request to create new ingestion"""
    name: str = Field(..., description="Ingestion name (target table)")
    cluster_id: str = Field(..., description="IOMETE cluster ID")
    source: SourceConfig
    format: FormatConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    quality: QualityConfig = Field(default_factory=QualityConfig)


class IngestionUpdate(BaseModel):
    """Request to update ingestion (limited fields)"""
    schedule: Optional[ScheduleConfig] = None
    quality: Optional[QualityConfig] = None
    status: Optional[IngestionStatus] = None


class IngestionResponse(BaseModel):
    """Ingestion response"""
    id: str
    tenant_id: str
    name: str
    status: IngestionStatus
    cluster_id: str
    spark_connect_url: str
    created_at: datetime
    updated_at: datetime
    created_by: str
    source: SourceConfig
    format: FormatConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    quality: QualityConfig
    metadata: IngestionMetadata

    model_config = ConfigDict(from_attributes=True)


class ColumnSchema(BaseModel):
    """Column schema definition"""
    name: str
    data_type: str
    nullable: bool
    is_new: bool = False


class PreviewResult(BaseModel):
    """Preview/test result"""
    schema: List[ColumnSchema]
    sample_data: List[str]
    file_count: int
    estimated_records: int
    total_size_bytes: int
    issues: List[str] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)


class IngestionMetrics(BaseModel):
    """Ingestion execution metrics"""
    files_processed: int
    records_ingested: int
    bytes_read: int
    bytes_written: int
    duration_seconds: float
    error_count: int = 0


class RunHistoryResponse(BaseModel):
    """Run history record"""
    id: str
    ingestion_id: str
    started_at: datetime
    ended_at: Optional[datetime]
    status: RunStatus
    trigger: RunTrigger
    metrics: Optional[IngestionMetrics]
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    spark_job_id: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class SchemaChange(BaseModel):
    """Schema evolution change"""
    column_name: str
    change_type: str  # NEW_COLUMN, REMOVED_COLUMN, TYPE_CHANGE
    old_type: Optional[str] = None
    new_type: Optional[str] = None


class SchemaEvolutionResponse(BaseModel):
    """Schema evolution detection result"""
    ingestion_id: str
    version: int
    detected_at: datetime
    changes: List[SchemaChange]
    affected_files: List[str]
    old_schema: List[ColumnSchema]
    new_schema: List[ColumnSchema]


class CostEstimate(BaseModel):
    """Cost estimation result"""
    compute_per_run: float
    compute_monthly: float
    storage_monthly: float
    discovery_monthly: float
    total_monthly: float
    breakdown: Dict[str, Any]


class ClusterInfo(BaseModel):
    """Compute cluster information"""
    id: str
    name: str
    status: str  # running, stopped
    workers: int
    dbu_per_worker: int
    total_dbu: int
    spark_connect_url: str
    pricing: Dict[str, float]


# ==================== SQLAlchemy MODELS (Database) ====================

class IngestionConfig(Base):
    """Database model for ingestion configuration"""
    __tablename__ = "ingestion_configs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    status = Column(SQLEnum(IngestionStatus), nullable=False, default=IngestionStatus.DRAFT)
    cluster_id = Column(String(255), nullable=False)
    spark_connect_url = Column(String(500), nullable=False)
    spark_connect_token = Column(Text, nullable=False)  # Encrypted

    # Configurations stored as JSON
    source_config = Column(JSON, nullable=False)
    format_config = Column(JSON, nullable=False)
    destination_config = Column(JSON, nullable=False)
    schedule_config = Column(JSON, nullable=False)
    quality_config = Column(JSON, nullable=False)
    metadata_config = Column(JSON, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(255), nullable=False)

    def to_response(self) -> IngestionResponse:
        """Convert to API response model"""
        return IngestionResponse(
            id=str(self.id),
            tenant_id=str(self.tenant_id),
            name=self.name,
            status=self.status,
            cluster_id=self.cluster_id,
            spark_connect_url=self.spark_connect_url,
            created_at=self.created_at,
            updated_at=self.updated_at,
            created_by=self.created_by,
            source=SourceConfig(**self.source_config),
            format=FormatConfig(**self.format_config),
            destination=DestinationConfig(**self.destination_config),
            schedule=ScheduleConfig(**self.schedule_config),
            quality=QualityConfig(**self.quality_config),
            metadata=IngestionMetadata(**(self.metadata_config or {}))
        )


class RunHistory(Base):
    """Database model for run history"""
    __tablename__ = "run_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    ingestion_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    status = Column(SQLEnum(RunStatus), nullable=False)
    trigger = Column(SQLEnum(RunTrigger), nullable=False)

    # Metrics stored as JSON
    metrics = Column(JSON, nullable=True)
    errors = Column(JSON, nullable=True)

    spark_job_id = Column(String(255), nullable=True)
    cluster_id = Column(String(255), nullable=False)

    def to_response(self) -> RunHistoryResponse:
        """Convert to API response model"""
        return RunHistoryResponse(
            id=str(self.id),
            ingestion_id=str(self.ingestion_id),
            started_at=self.started_at,
            ended_at=self.ended_at,
            status=self.status,
            trigger=self.trigger,
            metrics=IngestionMetrics(**self.metrics) if self.metrics else None,
            errors=self.errors or [],
            spark_job_id=self.spark_job_id
        )


class SchemaVersion(Base):
    """Database model for schema versions"""
    __tablename__ = "schema_versions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    ingestion_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    detected_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    schema_json = Column(JSON, nullable=False)
    changes = Column(JSON, nullable=False)
    affected_files = Column(JSON, nullable=False)

    resolution_type = Column(String(50), nullable=True)
    resolution_applied_at = Column(DateTime, nullable=True)
    resolution_applied_by = Column(String(255), nullable=True)