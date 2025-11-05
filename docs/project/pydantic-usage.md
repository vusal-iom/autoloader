# Pydantic Usage in IOMETE Autoloader

This document explains how Pydantic is used throughout the IOMETE Autoloader project for data validation, serialization, and API schema management.

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [API Schemas](#api-schemas)
4. [Validation Patterns](#validation-patterns)
5. [Configuration with Pydantic Settings](#configuration-with-pydantic-settings)
6. [Best Practices](#best-practices)
7. [Common Patterns](#common-patterns)

## Overview

Pydantic is used in two primary ways in this project:

1. **API Request/Response Validation** (`app/models/schemas.py`) - Defines schemas for FastAPI endpoints
2. **Application Configuration** (`app/config.py`) - Manages settings with `pydantic-settings`

### Why Pydantic?

- **Type Safety**: Runtime validation of data types
- **Auto Documentation**: FastAPI uses Pydantic models to generate OpenAPI schemas
- **Data Conversion**: Automatic type coercion and serialization
- **Validation**: Built-in and custom validators for business rules
- **Environment Management**: Easy configuration from environment variables

## Core Concepts

### BaseModel

All Pydantic schemas inherit from `BaseModel`:

```python
from pydantic import BaseModel

class SourceConfig(BaseModel):
    """Source configuration schema."""
    type: str
    path: str
    file_pattern: Optional[str] = None
```

**Key Features:**
- Automatic validation on instantiation
- Type coercion (e.g., "123" → 123 for int fields)
- JSON serialization via `.model_dump()`
- Dictionary unpacking with `**data`

### Field Definitions

Pydantic uses `Field()` for advanced field configuration:

```python
from pydantic import Field

class IngestionCreate(BaseModel):
    name: str = Field(..., description="Ingestion name")
    cluster_id: str = Field(..., description="Target cluster ID")
```

**Field Parameters:**
- `...` - Required field (no default)
- `default=value` - Default value
- `description` - Documentation string (appears in OpenAPI)
- `min_length`, `max_length` - String validation
- `ge`, `le` - Numeric validation (greater/less than or equal)

### Optional Fields

```python
from typing import Optional

class ScheduleConfig(BaseModel):
    frequency: Optional[str] = Field(None, description="daily, hourly, weekly, custom")
    time: Optional[str] = None
```

**Two ways to define optionals:**
1. `Optional[str] = None` - Can be None, defaults to None
2. `Optional[str] = Field(None, ...)` - Same, but with additional metadata

## API Schemas

Located in `app/models/schemas.py`, these models define the shape of API requests and responses.

### Schema Categories

#### 1. Request Schemas

Used for incoming API requests:

```python
class IngestionCreate(BaseModel):
    """Create ingestion request."""
    name: str
    cluster_id: str
    source: SourceConfig
    format: FormatConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    quality: Optional[QualityConfig] = QualityConfig()
```

**Usage in FastAPI:**

```python
@router.post("", response_model=IngestionResponse)
async def create_ingestion(
    ingestion: IngestionCreate,  # Pydantic validates this automatically
    service: IngestionService = Depends(get_ingestion_service),
):
    return service.create_ingestion(ingestion, current_user)
```

FastAPI automatically:
- Parses JSON request body
- Validates against `IngestionCreate` schema
- Returns 422 error if validation fails
- Provides detailed error messages

#### 2. Response Schemas

Used for API responses:

```python
class IngestionResponse(BaseModel):
    """Ingestion response."""
    id: str
    tenant_id: str
    name: str
    status: IngestionStatus
    # ... more fields

    class Config:
        from_attributes = True  # Allow creation from ORM models
```

**The `from_attributes` Setting:**

This allows Pydantic to read data from SQLAlchemy ORM models:

```python
# Without from_attributes - would fail
db_ingestion = session.query(Ingestion).first()
response = IngestionResponse(**db_ingestion.__dict__)

# With from_attributes - works automatically
response = IngestionResponse.from_orm(db_ingestion)
```

#### 3. Nested Schemas

Schemas can be composed of other schemas:

```python
class IngestionCreate(BaseModel):
    name: str
    source: SourceConfig         # Nested schema
    format: FormatConfig         # Nested schema
    destination: DestinationConfig  # Nested schema
    schedule: ScheduleConfig     # Nested schema
```

**Validation is recursive:**
- Validates top-level fields
- Recursively validates all nested schemas
- Provides detailed error paths (e.g., `source.path` is invalid)

### Enum Integration

Pydantic works seamlessly with Python enums:

```python
from enum import Enum

class IngestionStatus(str, Enum):
    """Ingestion status."""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"

class IngestionResponse(BaseModel):
    status: IngestionStatus  # Only accepts valid enum values
```

**Benefits:**
- Type-safe status values
- Auto-completion in IDEs
- Validation prevents invalid statuses
- Serializes to string values in JSON

### Complex Type Structures

#### Dictionaries and Lists

```python
from typing import Dict, List, Any

class FormatOptions(BaseModel):
    multiline: Optional[bool] = None
    compression: Optional[str] = None

class QualityConfig(BaseModel):
    alert_recipients: List[str] = []  # List of strings

class SourceConfig(BaseModel):
    credentials: Optional[Dict[str, Any]] = None  # Arbitrary JSON
```

**Type Validation:**
- `List[str]` - Ensures all items are strings
- `Dict[str, Any]` - Keys must be strings, values can be anything
- Pydantic validates structure recursively

## Validation Patterns

### Default Values

```python
class SchemaConfig(BaseModel):
    inference: str = Field(default="auto", description="auto or manual")
    evolution_enabled: bool = Field(default=True)
    schema_json: Optional[Dict[str, Any]] = None
```

**How defaults work:**
- If field not provided in request, uses default
- `None` is a valid default for optional fields
- Defaults can be mutable objects (but be careful!)

### Nested Defaults

```python
class FormatConfig(BaseModel):
    type: str
    options: Optional[FormatOptions] = None
    schema: Optional[SchemaConfig] = SchemaConfig()  # Creates default instance
```

**Important:** `SchemaConfig()` creates a new instance as the default. This is safe because Pydantic creates a new instance for each model instantiation.

### Complex Nested Structures

```python
class DestinationConfig(BaseModel):
    catalog: str
    database: str
    table: str
    write_mode: str = Field(default="append")
    partitioning: Optional[PartitioningConfig] = PartitioningConfig()
    optimization: Optional[OptimizationConfig] = OptimizationConfig()
```

Example JSON:

```json
{
  "catalog": "lakehouse",
  "database": "raw",
  "table": "events",
  "write_mode": "append",
  "partitioning": {
    "enabled": true,
    "columns": ["date", "region"]
  },
  "optimization": {
    "z_ordering_enabled": true,
    "z_ordering_columns": ["user_id"]
  }
}
```

## Configuration with Pydantic Settings

See [Configuration Management](./configuration-management.md) for detailed documentation.

## Best Practices

### 1. Use Descriptive Docstrings

```python
class IngestionCreate(BaseModel):
    """
    Create ingestion request.

    This schema defines all required fields for creating a new
    data ingestion configuration.
    """
    name: str = Field(..., description="Unique ingestion name")
```

**Why:**
- Appears in OpenAPI documentation
- Helps API consumers understand fields
- Provides context for validation errors

### 2. Separate Request and Response Models

```python
# Request - Only fields user can set
class IngestionCreate(BaseModel):
    name: str
    cluster_id: str
    source: SourceConfig

# Response - Includes generated fields
class IngestionResponse(BaseModel):
    id: str              # Generated by system
    name: str
    cluster_id: str
    source: SourceConfig
    created_at: datetime  # Generated by system
```

**Benefits:**
- Clear separation of concerns
- Prevents clients from setting read-only fields
- Different validation rules for create vs update

### 3. Use Type Hints Consistently

```python
# Good
class RunMetrics(BaseModel):
    files_processed: int = 0
    records_ingested: int = 0
    bytes_read: int = 0
    duration_seconds: Optional[int] = None

# Avoid
class RunMetrics(BaseModel):
    files_processed = 0  # No type hint
    records_ingested: int  # No default, but not marked required
```

### 4. Group Related Fields

```python
# Good - Grouped by concern
class IngestionCreate(BaseModel):
    # Identity
    name: str
    cluster_id: str

    # Source
    source: SourceConfig
    format: FormatConfig

    # Destination
    destination: DestinationConfig

    # Execution
    schedule: ScheduleConfig
    quality: Optional[QualityConfig] = QualityConfig()
```

### 5. Use Nested Models for Complex Structures

```python
# Good - Nested models
class SourceConfig(BaseModel):
    type: str
    path: str
    credentials: Optional[Dict[str, Any]] = None

class IngestionCreate(BaseModel):
    source: SourceConfig

# Avoid - Flat structure
class IngestionCreate(BaseModel):
    source_type: str
    source_path: str
    source_credentials: Optional[Dict[str, Any]] = None
```

## Common Patterns

### Pattern 1: Configuration Objects

```python
class PartitioningConfig(BaseModel):
    """Partitioning configuration."""
    enabled: bool = False
    columns: List[str] = []

# Usage - can omit if using defaults
ingestion = IngestionCreate(
    name="my-ingestion",
    # partitioning not specified, uses defaults
)
```

### Pattern 2: Metadata Objects

```python
class IngestionMetadata(BaseModel):
    """System-generated metadata."""
    checkpoint_location: str
    last_run_id: Optional[str] = None
    last_run_time: Optional[datetime] = None
    next_run_time: Optional[datetime] = None
    schema_version: int = 1
    estimated_monthly_cost: Optional[float] = None
```

**Used for:**
- System-generated data
- Tracking state
- Computed values

### Pattern 3: Update Schemas

```python
class IngestionUpdate(BaseModel):
    """Update ingestion request - all fields optional."""
    status: Optional[IngestionStatus] = None
    schedule: Optional[ScheduleConfig] = None
    quality: Optional[QualityConfig] = None
    alert_recipients: Optional[List[str]] = None
```

**PATCH semantics:**
- All fields optional
- Only provided fields are updated
- Useful for partial updates

### Pattern 4: Error Responses

```python
class RunError(BaseModel):
    """Run error detail."""
    file: str
    error_type: str
    message: str
    timestamp: datetime

class RunResponse(BaseModel):
    id: str
    status: RunStatus
    errors: Optional[List[RunError]] = None  # List of structured errors
```

## Serialization and Deserialization

### To Dictionary

```python
config = SourceConfig(
    type="s3",
    path="s3://bucket/path",
    file_pattern="*.json"
)

# Pydantic v2
config_dict = config.model_dump()

# Include/exclude fields
config_dict = config.model_dump(exclude={"credentials"})
config_dict = config.model_dump(include={"type", "path"})
```

### To JSON

```python
# Pydantic v2
config_json = config.model_dump_json()

# Pretty print
config_json = config.model_dump_json(indent=2)
```

### From Dictionary

```python
data = {
    "type": "s3",
    "path": "s3://bucket/path"
}

config = SourceConfig(**data)
# or
config = SourceConfig.model_validate(data)
```

### From ORM Models

```python
# Requires: class Config: from_attributes = True

class IngestionResponse(BaseModel):
    id: str
    name: str

    class Config:
        from_attributes = True

# Convert SQLAlchemy model to Pydantic
db_ingestion = session.query(Ingestion).first()
response = IngestionResponse.from_orm(db_ingestion)
```

## Datetime Handling

```python
from datetime import datetime

class RunResponse(BaseModel):
    started_at: datetime
    ended_at: Optional[datetime] = None
```

**Pydantic automatically:**
- Parses ISO 8601 strings → datetime objects
- Serializes datetime → ISO 8601 strings
- Handles timezone-aware and naive datetimes

Example:

```python
# Input JSON
{"started_at": "2024-11-05T10:30:00Z"}

# Pydantic parses to
run.started_at  # datetime(2024, 11, 5, 10, 30, 0)

# Serializes back to
run.model_dump_json()  # {"started_at": "2024-11-05T10:30:00"}
```

## FastAPI Integration

### Request Validation

```python
@router.post("/test", response_model=PreviewResult)
async def test_configuration(
    ingestion: IngestionCreate,  # Auto-validated from request body
    service: IngestionService = Depends(get_ingestion_service),
):
    return service.preview_ingestion(ingestion)
```

**FastAPI automatically:**
1. Reads request body JSON
2. Validates against `IngestionCreate`
3. Creates Pydantic model instance
4. Passes to function
5. Returns 422 if validation fails

### Response Validation

```python
@router.get("/{ingestion_id}", response_model=IngestionResponse)
async def get_ingestion(ingestion_id: str, ...):
    ingestion = service.get_ingestion(ingestion_id)
    return ingestion  # FastAPI validates against IngestionResponse
```

**FastAPI automatically:**
1. Takes return value
2. Validates against `response_model`
3. Serializes to JSON
4. Adds to OpenAPI schema

### Validation Errors

When validation fails, FastAPI returns structured errors:

```json
{
  "detail": [
    {
      "loc": ["body", "source", "path"],
      "msg": "field required",
      "type": "value_error.missing"
    },
    {
      "loc": ["body", "schedule", "frequency"],
      "msg": "value is not a valid enumeration member",
      "type": "type_error.enum",
      "ctx": {"enum_values": ["daily", "hourly", "weekly", "custom"]}
    }
  ]
}
```

## Key Files Reference

- **`app/models/schemas.py`** - All API request/response schemas
- **`app/config.py`** - Application settings using pydantic-settings
- **`app/api/v1/ingestions.py`** - Example endpoint using Pydantic models
- **`app/models/domain.py`** - SQLAlchemy models (separate from Pydantic)

## Related Documentation

- [Configuration Management](./configuration-management.md) - Pydantic Settings in detail
- [FastAPI Documentation](https://fastapi.tiangolo.com/tutorial/body/) - Request body validation
- [Pydantic Documentation](https://docs.pydantic.dev/) - Official Pydantic docs
