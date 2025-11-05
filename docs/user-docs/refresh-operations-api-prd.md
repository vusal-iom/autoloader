# PRD: Refresh Operations API

**Document Version:** 1.0
**Date:** 2025-01-05
**Status:** Proposed
**Author:** IOMETE Engineering
**Related:** [Overwrite Mode Alternative Guide](./overwrite-mode-alternative.md)

---

## Executive Summary

This PRD defines a new set of convenience APIs that simplify common table refresh workflows in IOMETE Autoloader. Instead of requiring users to orchestrate multiple manual steps (DROP TABLE + API calls), we provide high-level action endpoints that encapsulate these workflows safely and atomically.

**Problem:** Users currently need 3+ separate operations to achieve common workflows like full refresh or table reset.

**Solution:** Introduce convenience endpoints that encapsulate these workflows while maintaining safety, flexibility, and transparency.

**Impact:**
- ✅ Reduces complexity for 90% of use cases
- ✅ Improves safety with built-in confirmations and dry-run mode
- ✅ Maintains flexibility for advanced users
- ✅ Better developer experience and API discoverability

---

## Problem Statement

### Current State (Manual Orchestration)

Users must manually orchestrate multiple operations:

```bash
# Full Refresh (reprocess all files)
psql -c "DROP TABLE raw.sales_data"
curl -X DELETE /api/v1/ingestions/123/processed-files
curl -X POST /api/v1/ingestions/123/run

# Fresh Start (new files only)
psql -c "DROP TABLE raw.sales_data"
curl -X POST /api/v1/ingestions/123/run
```

**Problems:**
1. **Complexity**: Users need to understand system internals
2. **Error-Prone**: Easy to forget steps or execute in wrong order
3. **No Atomicity**: Failures leave system in inconsistent state
4. **Poor UX**: Requires mixing SQL and API calls
5. **No Safety Nets**: Easy to accidentally trigger expensive operations
6. **No Visibility**: Hard to know what happened if something fails

### User Stories

**US-1: Data Engineer - Full Refresh**
> "As a data engineer, I need to reprocess all source files after fixing a data quality issue, so I want a single API call that safely drops the table, clears processed files, and reruns ingestion."

**US-2: Analytics Engineer - Fresh Start**
> "As an analytics engineer, I want to recreate a table from scratch but only process new files going forward, so I can recover from a corrupted table without reprocessing terabytes of historical data."

**US-3: DevOps Engineer - Scheduled Snapshots**
> "As a DevOps engineer, I need to schedule daily full snapshot refreshes, so I want a simple API endpoint I can call from cron/Airflow without managing state."

**US-4: Developer - Testing**
> "As a developer testing an ingestion, I want to know what a refresh operation would do before running it, so I can avoid accidentally triggering expensive operations."

---

## Solution Overview

### Design Philosophy

**Principle 1: Convenience with Safety**
- Provide simple, high-level APIs for common workflows
- Require explicit confirmation for destructive operations
- Support dry-run mode to preview actions

**Principle 2: Transparency**
- Detailed responses showing what operations were performed
- Clear error messages with recovery instructions
- Cost and impact estimates before execution

**Principle 3: Flexibility**
- Convenience APIs for 90% of use cases
- Granular APIs still available for advanced workflows
- Composable for custom orchestration

**Principle 4: Consistency**
- Follow REST conventions
- Consistent request/response schemas
- Predictable error handling

---

## API Design

### Endpoint Summary

| Endpoint | Method | Purpose | Use Case |
|----------|--------|---------|----------|
| `/ingestions/{id}/full-refresh` | POST | Drop table + clear files + run | Data fixes, complete reload |
| `/ingestions/{id}/fresh-start` | POST | Drop table + run (keep file history) | Fresh table, new files only |
| `/ingestions/{id}/table` | DELETE | Drop table only | Manual cleanup, advanced workflows |
| `/ingestions/{id}/processed-files` | DELETE | Clear file history only | (Existing) Backfill, reprocess |
| `/ingestions/{id}/run` | POST | Trigger run only | (Existing) Normal execution |

---

## API Specification

### 1. Full Refresh

**Endpoint:** `POST /api/v1/ingestions/{ingestion_id}/full-refresh`

**Description:** Performs a complete table refresh by dropping the table, clearing all processed file history, and triggering a new ingestion run. This reprocesses ALL source files from scratch.

**Use Cases:**
- Data quality fixes requiring reprocessing of all historical files
- Source files were corrected/updated retroactively
- Complete snapshot refresh from all source files
- Testing: replay entire ingestion from beginning

**Request:**

```json
POST /api/v1/ingestions/ing-abc123/full-refresh

{
  "confirm": true,        // REQUIRED: Must be true (safety mechanism)
  "auto_run": true,       // Optional: Trigger run immediately (default: true)
  "dry_run": false        // Optional: Preview without executing (default: false)
}
```

**Request Schema:**

```python
class FullRefreshRequest(BaseModel):
    confirm: bool = Field(
        ...,
        description="Must be true to proceed. Safety confirmation required."
    )
    auto_run: bool = Field(
        default=True,
        description="Automatically trigger ingestion run after refresh"
    )
    dry_run: bool = Field(
        default=False,
        description="Preview operations without executing them"
    )

    @validator('confirm')
    def confirm_must_be_true(cls, v):
        if not v:
            raise ValueError("Must explicitly confirm with confirm=true")
        return v
```

**Success Response (202 Accepted):**

```json
{
  "status": "accepted",
  "message": "Full refresh completed successfully",
  "ingestion_id": "ing-abc123",
  "run_id": "run-xyz789",
  "timestamp": "2025-01-05T10:30:00Z",

  "operations": [
    {
      "operation": "table_dropped",
      "status": "success",
      "timestamp": "2025-01-05T10:30:01Z",
      "details": {
        "table_name": "raw.sales_data",
        "table_size_gb": 450.2,
        "row_count": 12450000
      }
    },
    {
      "operation": "processed_files_cleared",
      "status": "success",
      "timestamp": "2025-01-05T10:30:02Z",
      "details": {
        "files_cleared": 1247,
        "oldest_file": "2024-01-01/data.parquet",
        "newest_file": "2025-01-04/data.parquet"
      }
    },
    {
      "operation": "run_triggered",
      "status": "success",
      "timestamp": "2025-01-05T10:30:03Z",
      "details": {
        "run_id": "run-xyz789",
        "run_url": "/api/v1/runs/run-xyz789"
      }
    }
  ],

  "impact": {
    "files_to_process": 1247,
    "estimated_data_size_gb": 450.2,
    "estimated_cost_usd": 125.50,
    "estimated_duration_minutes": 45
  },

  "warnings": [
    "This operation will reprocess 1,247 files (450.2 GB)",
    "Estimated cost: $125.50"
  ]
}
```

**Dry Run Response (200 OK):**

```json
{
  "status": "dry_run",
  "message": "Preview of operations (not executed)",
  "ingestion_id": "ing-abc123",

  "would_perform": [
    {
      "operation": "table_dropped",
      "details": {
        "table_name": "raw.sales_data",
        "current_size_gb": 450.2,
        "current_row_count": 12450000
      }
    },
    {
      "operation": "processed_files_cleared",
      "details": {
        "files_to_clear": 1247,
        "date_range": "2024-01-01 to 2025-01-04"
      }
    },
    {
      "operation": "run_triggered",
      "details": {
        "files_to_process": 1247,
        "data_size_gb": 450.2
      }
    }
  ],

  "impact": {
    "files_to_process": 1247,
    "estimated_data_size_gb": 450.2,
    "estimated_cost_usd": 125.50,
    "estimated_duration_minutes": 45
  },

  "warnings": [
    "⚠️ This will reprocess ALL 1,247 files from scratch",
    "⚠️ Estimated cost: $125.50",
    "⚠️ Current table (450.2 GB) will be deleted"
  ],

  "next_steps": {
    "to_proceed": "POST /api/v1/ingestions/ing-abc123/full-refresh with confirm=true",
    "to_cancel": "No action needed"
  }
}
```

**Error Responses:**

```json
// 400 Bad Request - Missing confirmation
{
  "detail": "Confirmation required. Set confirm=true to proceed.",
  "error_code": "CONFIRMATION_REQUIRED",
  "help": "This operation is destructive. Explicitly set confirm=true to proceed."
}

// 404 Not Found - Ingestion doesn't exist
{
  "detail": "Ingestion not found",
  "error_code": "INGESTION_NOT_FOUND"
}

// 409 Conflict - Run already in progress
{
  "detail": "Cannot refresh while ingestion is running",
  "error_code": "RUN_IN_PROGRESS",
  "current_run_id": "run-xyz789",
  "help": "Wait for current run to complete or cancel it first"
}

// 500 Internal Server Error - Partial failure
{
  "status": "partial_failure",
  "message": "Some operations failed",
  "ingestion_id": "ing-abc123",

  "operations": [
    {
      "operation": "table_dropped",
      "status": "success"
    },
    {
      "operation": "processed_files_cleared",
      "status": "success"
    },
    {
      "operation": "run_triggered",
      "status": "failed",
      "error": "Cluster not available",
      "error_code": "CLUSTER_UNAVAILABLE"
    }
  ],

  "recovery": {
    "message": "Table and files cleared, but run failed to trigger",
    "action": "Manually trigger run: POST /api/v1/ingestions/ing-abc123/run",
    "state": "Table dropped, file history cleared, no run triggered"
  }
}
```

---

### 2. Fresh Start

**Endpoint:** `POST /api/v1/ingestions/{ingestion_id}/fresh-start`

**Description:** Drops the target table and triggers a new ingestion run, but keeps the processed file history intact. Only processes files that were added since the last successful run (incremental behavior with a fresh table).

**Use Cases:**
- Recover from corrupted table without reprocessing historical data
- Change table structure/schema (force recreation)
- Fresh table for new files only (skip historical data)
- Testing: clean slate without expensive reprocessing

**Request:**

```json
POST /api/v1/ingestions/ing-abc123/fresh-start

{
  "confirm": true,        // REQUIRED: Must be true (safety mechanism)
  "auto_run": true,       // Optional: Trigger run immediately (default: true)
  "dry_run": false        // Optional: Preview without executing (default: false)
}
```

**Request Schema:**

```python
class FreshStartRequest(BaseModel):
    confirm: bool = Field(
        ...,
        description="Must be true to proceed. Safety confirmation required."
    )
    auto_run: bool = Field(
        default=True,
        description="Automatically trigger ingestion run after dropping table"
    )
    dry_run: bool = Field(
        default=False,
        description="Preview operations without executing them"
    )

    @validator('confirm')
    def confirm_must_be_true(cls, v):
        if not v:
            raise ValueError("Must explicitly confirm with confirm=true")
        return v
```

**Success Response (202 Accepted):**

```json
{
  "status": "accepted",
  "message": "Fresh start completed successfully",
  "ingestion_id": "ing-abc123",
  "run_id": "run-xyz789",
  "timestamp": "2025-01-05T10:30:00Z",

  "operations": [
    {
      "operation": "table_dropped",
      "status": "success",
      "timestamp": "2025-01-05T10:30:01Z",
      "details": {
        "table_name": "raw.sales_data",
        "table_size_gb": 450.2,
        "row_count": 12450000
      }
    },
    {
      "operation": "run_triggered",
      "status": "success",
      "timestamp": "2025-01-05T10:30:02Z",
      "details": {
        "run_id": "run-xyz789",
        "run_url": "/api/v1/runs/run-xyz789"
      }
    }
  ],

  "impact": {
    "files_to_process": 23,  // Only NEW files
    "estimated_data_size_gb": 8.5,
    "estimated_cost_usd": 2.30,
    "estimated_duration_minutes": 5
  },

  "notes": [
    "Table dropped and recreated",
    "Only NEW files (added since last run) will be processed",
    "Processed file history preserved (1,247 files remain marked as processed)"
  ]
}
```

**Dry Run Response (200 OK):**

```json
{
  "status": "dry_run",
  "message": "Preview of operations (not executed)",
  "ingestion_id": "ing-abc123",

  "would_perform": [
    {
      "operation": "table_dropped",
      "details": {
        "table_name": "raw.sales_data",
        "current_size_gb": 450.2,
        "current_row_count": 12450000
      }
    },
    {
      "operation": "run_triggered",
      "details": {
        "files_to_process": 23,
        "new_files_only": true,
        "data_size_gb": 8.5
      }
    }
  ],

  "impact": {
    "files_to_process": 23,
    "files_skipped": 1247,
    "estimated_data_size_gb": 8.5,
    "estimated_cost_usd": 2.30,
    "estimated_duration_minutes": 5
  },

  "notes": [
    "ℹ️ Table will be dropped and recreated",
    "ℹ️ Only 23 NEW files will be processed",
    "ℹ️ 1,247 previously processed files will be skipped",
    "💰 Cost-effective: Only processes incremental data"
  ],

  "next_steps": {
    "to_proceed": "POST /api/v1/ingestions/ing-abc123/fresh-start with confirm=true",
    "to_cancel": "No action needed"
  }
}
```

**Error Responses:** Same as Full Refresh (see above)

---

### 3. Drop Table

**Endpoint:** `DELETE /api/v1/ingestions/{ingestion_id}/table`

**Description:** Drops the target Iceberg table if it exists. Does NOT affect processed file history or trigger any runs. Granular operation for advanced workflows.

**Use Cases:**
- Manual cleanup before custom orchestration
- Advanced workflows requiring fine-grained control
- Delete table without triggering automatic reprocessing
- Compose with other operations for custom logic

**Request:**

```json
DELETE /api/v1/ingestions/ing-abc123/table

{
  "confirm": true  // REQUIRED: Must be true (safety mechanism)
}
```

**Request Schema:**

```python
class DropTableRequest(BaseModel):
    confirm: bool = Field(
        ...,
        description="Must be true to proceed. Safety confirmation required."
    )

    @validator('confirm')
    def confirm_must_be_true(cls, v):
        if not v:
            raise ValueError("Must explicitly confirm with confirm=true")
        return v
```

**Success Response (200 OK):**

```json
{
  "status": "success",
  "message": "Table dropped successfully",
  "ingestion_id": "ing-abc123",
  "timestamp": "2025-01-05T10:30:00Z",

  "details": {
    "table_name": "raw.sales_data",
    "table_existed": true,
    "table_size_gb": 450.2,
    "row_count": 12450000
  },

  "notes": [
    "Processed file history NOT affected (1,247 files still marked as processed)",
    "No ingestion run triggered",
    "Table will be recreated on next run"
  ]
}
```

**Response (Table Doesn't Exist):**

```json
{
  "status": "success",
  "message": "Table did not exist",
  "ingestion_id": "ing-abc123",
  "timestamp": "2025-01-05T10:30:00Z",

  "details": {
    "table_name": "raw.sales_data",
    "table_existed": false
  },

  "notes": [
    "Table does not exist (nothing to drop)",
    "This is not an error - idempotent operation"
  ]
}
```

**Error Responses:**

```json
// 400 Bad Request - Missing confirmation
{
  "detail": "Confirmation required. Set confirm=true to proceed.",
  "error_code": "CONFIRMATION_REQUIRED"
}

// 404 Not Found - Ingestion doesn't exist
{
  "detail": "Ingestion not found",
  "error_code": "INGESTION_NOT_FOUND"
}

// 500 Internal Server Error - Spark error
{
  "detail": "Failed to drop table",
  "error_code": "SPARK_ERROR",
  "error_message": "Cluster connection failed",
  "help": "Check cluster status and try again"
}
```

---

## Request/Response Schemas

### Common Request Schema

All refresh operations share a common base schema:

```python
from pydantic import BaseModel, Field, validator
from typing import Optional

class RefreshOperationRequest(BaseModel):
    """Base schema for refresh operations."""

    confirm: bool = Field(
        ...,
        description="Must be true to proceed. Safety confirmation required.",
        example=True
    )

    auto_run: Optional[bool] = Field(
        default=True,
        description="Automatically trigger ingestion run after operation",
        example=True
    )

    dry_run: Optional[bool] = Field(
        default=False,
        description="Preview operations without executing them",
        example=False
    )

    @validator('confirm')
    def confirm_must_be_true(cls, v):
        if not v:
            raise ValueError(
                "Confirmation required. This is a destructive operation. "
                "Set confirm=true to proceed."
            )
        return v

    class Config:
        schema_extra = {
            "example": {
                "confirm": True,
                "auto_run": True,
                "dry_run": False
            }
        }
```

### Common Response Schema

```python
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class OperationStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

class Operation(BaseModel):
    """Details of a single operation within a refresh."""

    operation: str = Field(
        ...,
        description="Operation type",
        example="table_dropped"
    )
    status: OperationStatus = Field(
        ...,
        description="Operation status",
        example="success"
    )
    timestamp: datetime = Field(
        ...,
        description="When operation was performed",
        example="2025-01-05T10:30:01Z"
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
        example=1247
    )
    files_skipped: Optional[int] = Field(
        default=None,
        description="Number of files that will be skipped",
        example=0
    )
    estimated_data_size_gb: float = Field(
        ...,
        description="Estimated data size to process (GB)",
        example=450.2
    )
    estimated_cost_usd: float = Field(
        ...,
        description="Estimated processing cost (USD)",
        example=125.50
    )
    estimated_duration_minutes: int = Field(
        ...,
        description="Estimated processing time (minutes)",
        example=45
    )

class RefreshOperationResponse(BaseModel):
    """Standard response for refresh operations."""

    status: str = Field(
        ...,
        description="Overall operation status",
        example="accepted"
    )
    message: str = Field(
        ...,
        description="Human-readable summary",
        example="Full refresh completed successfully"
    )
    ingestion_id: str = Field(
        ...,
        description="Ingestion identifier",
        example="ing-abc123"
    )
    run_id: Optional[str] = Field(
        default=None,
        description="Run identifier if run was triggered",
        example="run-xyz789"
    )
    timestamp: datetime = Field(
        ...,
        description="Response timestamp",
        example="2025-01-05T10:30:00Z"
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
```

---

## Implementation Guidelines

### Service Layer

```python
# app/services/refresh_service.py

from typing import Dict, Any, List
from sqlalchemy.orm import Session
from app.models.domain import Ingestion, Run
from app.services.spark_service import SparkService
from app.services.file_state_service import FileStateService
from app.services.ingestion_service import IngestionService
from app.repositories.ingestion_repository import IngestionRepository
import logging

logger = logging.getLogger(__name__)

class RefreshService:
    """Service for refresh operations."""

    def __init__(
        self,
        spark_service: SparkService,
        file_state_service: FileStateService,
        ingestion_service: IngestionService,
        ingestion_repo: IngestionRepository
    ):
        self.spark = spark_service
        self.file_state = file_state_service
        self.ingestion_service = ingestion_service
        self.ingestion_repo = ingestion_repo

    async def full_refresh(
        self,
        ingestion_id: str,
        confirm: bool,
        auto_run: bool = True,
        dry_run: bool = False,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Perform full refresh: drop table + clear files + run.

        Args:
            ingestion_id: Ingestion identifier
            confirm: Safety confirmation (must be True)
            auto_run: Trigger run after refresh
            dry_run: Preview without executing
            db: Database session

        Returns:
            Operation result with details

        Raises:
            ValueError: If confirm is False
            HTTPException: If operation fails
        """
        if not confirm:
            raise ValueError("Confirmation required. Set confirm=true to proceed.")

        # Get ingestion
        ingestion = self.ingestion_repo.get_by_id(db, ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Get impact estimate
        impact = await self._estimate_full_refresh_impact(ingestion, db)

        # Dry run - return preview
        if dry_run:
            return self._build_dry_run_response(
                ingestion_id=ingestion_id,
                operations=["table_dropped", "processed_files_cleared", "run_triggered"],
                impact=impact
            )

        # Execute operations
        operations = []

        # Step 1: Drop table
        try:
            table_info = await self.spark.drop_table(ingestion.destination.table)
            operations.append({
                "operation": "table_dropped",
                "status": "success",
                "details": table_info
            })
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            operations.append({
                "operation": "table_dropped",
                "status": "failed",
                "error": str(e)
            })
            return self._build_error_response(ingestion_id, operations, e)

        # Step 2: Clear processed files
        try:
            files_cleared = await self.file_state.clear_all_processed(
                ingestion_id, db
            )
            operations.append({
                "operation": "processed_files_cleared",
                "status": "success",
                "details": {"files_cleared": files_cleared}
            })
        except Exception as e:
            logger.error(f"Failed to clear processed files: {e}")
            operations.append({
                "operation": "processed_files_cleared",
                "status": "failed",
                "error": str(e)
            })
            return self._build_error_response(ingestion_id, operations, e)

        # Step 3: Trigger run (if auto_run)
        run_id = None
        if auto_run:
            try:
                run = await self.ingestion_service.trigger_run(
                    ingestion_id, db
                )
                run_id = run.id
                operations.append({
                    "operation": "run_triggered",
                    "status": "success",
                    "details": {
                        "run_id": run_id,
                        "run_url": f"/api/v1/runs/{run_id}"
                    }
                })
            except Exception as e:
                logger.error(f"Failed to trigger run: {e}")
                operations.append({
                    "operation": "run_triggered",
                    "status": "failed",
                    "error": str(e)
                })
                return self._build_error_response(ingestion_id, operations, e)

        return self._build_success_response(
            ingestion_id=ingestion_id,
            run_id=run_id,
            operations=operations,
            impact=impact
        )

    async def fresh_start(
        self,
        ingestion_id: str,
        confirm: bool,
        auto_run: bool = True,
        dry_run: bool = False,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Perform fresh start: drop table + run (keep file history).

        Args:
            ingestion_id: Ingestion identifier
            confirm: Safety confirmation (must be True)
            auto_run: Trigger run after dropping table
            dry_run: Preview without executing
            db: Database session

        Returns:
            Operation result with details
        """
        if not confirm:
            raise ValueError("Confirmation required. Set confirm=true to proceed.")

        # Get ingestion
        ingestion = self.ingestion_repo.get_by_id(db, ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Get impact estimate (only NEW files)
        impact = await self._estimate_fresh_start_impact(ingestion, db)

        # Dry run - return preview
        if dry_run:
            return self._build_dry_run_response(
                ingestion_id=ingestion_id,
                operations=["table_dropped", "run_triggered"],
                impact=impact,
                notes=[
                    "Only NEW files (added since last run) will be processed",
                    "Processed file history will be preserved"
                ]
            )

        # Execute operations
        operations = []

        # Step 1: Drop table
        try:
            table_info = await self.spark.drop_table(ingestion.destination.table)
            operations.append({
                "operation": "table_dropped",
                "status": "success",
                "details": table_info
            })
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            operations.append({
                "operation": "table_dropped",
                "status": "failed",
                "error": str(e)
            })
            return self._build_error_response(ingestion_id, operations, e)

        # Step 2: Trigger run (if auto_run)
        run_id = None
        if auto_run:
            try:
                run = await self.ingestion_service.trigger_run(
                    ingestion_id, db
                )
                run_id = run.id
                operations.append({
                    "operation": "run_triggered",
                    "status": "success",
                    "details": {
                        "run_id": run_id,
                        "run_url": f"/api/v1/runs/{run_id}"
                    }
                })
            except Exception as e:
                logger.error(f"Failed to trigger run: {e}")
                operations.append({
                    "operation": "run_triggered",
                    "status": "failed",
                    "error": str(e)
                })
                return self._build_error_response(ingestion_id, operations, e)

        return self._build_success_response(
            ingestion_id=ingestion_id,
            run_id=run_id,
            operations=operations,
            impact=impact,
            notes=[
                "Table dropped and will be recreated",
                "Only NEW files will be processed",
                f"Processed file history preserved"
            ]
        )

    async def drop_table(
        self,
        ingestion_id: str,
        confirm: bool,
        db: Session = None
    ) -> Dict[str, Any]:
        """
        Drop table only (no file clearing, no run trigger).

        Args:
            ingestion_id: Ingestion identifier
            confirm: Safety confirmation (must be True)
            db: Database session

        Returns:
            Operation result with details
        """
        if not confirm:
            raise ValueError("Confirmation required. Set confirm=true to proceed.")

        # Get ingestion
        ingestion = self.ingestion_repo.get_by_id(db, ingestion_id)
        if not ingestion:
            raise ValueError(f"Ingestion not found: {ingestion_id}")

        # Drop table
        try:
            table_info = await self.spark.drop_table(ingestion.destination.table)

            return {
                "status": "success",
                "message": "Table dropped successfully",
                "ingestion_id": ingestion_id,
                "details": table_info,
                "notes": [
                    "Processed file history NOT affected",
                    "No ingestion run triggered",
                    "Table will be recreated on next run"
                ]
            }
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            raise

    # Helper methods

    async def _estimate_full_refresh_impact(
        self,
        ingestion: Ingestion,
        db: Session
    ) -> Dict[str, Any]:
        """Estimate impact of full refresh (ALL files)."""
        # Get all source files
        files = await self.file_state.list_all_source_files(ingestion, db)
        total_size_gb = sum(f.size_bytes for f in files) / (1024**3)

        # Estimate cost (placeholder - use cost_estimator service)
        estimated_cost = total_size_gb * 0.25  # $0.25/GB
        estimated_duration = len(files) * 2  # 2 min per file

        return {
            "files_to_process": len(files),
            "files_skipped": 0,
            "estimated_data_size_gb": round(total_size_gb, 2),
            "estimated_cost_usd": round(estimated_cost, 2),
            "estimated_duration_minutes": estimated_duration
        }

    async def _estimate_fresh_start_impact(
        self,
        ingestion: Ingestion,
        db: Session
    ) -> Dict[str, Any]:
        """Estimate impact of fresh start (NEW files only)."""
        # Get only NEW files (not in processed history)
        new_files = await self.file_state.list_new_files(ingestion, db)
        total_size_gb = sum(f.size_bytes for f in new_files) / (1024**3)

        # Estimate cost
        estimated_cost = total_size_gb * 0.25
        estimated_duration = len(new_files) * 2

        return {
            "files_to_process": len(new_files),
            "files_skipped": await self.file_state.count_processed(ingestion.id, db),
            "estimated_data_size_gb": round(total_size_gb, 2),
            "estimated_cost_usd": round(estimated_cost, 2),
            "estimated_duration_minutes": estimated_duration
        }

    def _build_success_response(
        self,
        ingestion_id: str,
        run_id: str,
        operations: List[Dict],
        impact: Dict,
        notes: List[str] = None
    ) -> Dict[str, Any]:
        """Build success response."""
        return {
            "status": "accepted",
            "message": "Operation completed successfully",
            "ingestion_id": ingestion_id,
            "run_id": run_id,
            "operations": operations,
            "impact": impact,
            "notes": notes or []
        }

    def _build_dry_run_response(
        self,
        ingestion_id: str,
        operations: List[str],
        impact: Dict,
        notes: List[str] = None
    ) -> Dict[str, Any]:
        """Build dry run response."""
        return {
            "status": "dry_run",
            "message": "Preview of operations (not executed)",
            "ingestion_id": ingestion_id,
            "would_perform": [{"operation": op} for op in operations],
            "impact": impact,
            "notes": notes or []
        }

    def _build_error_response(
        self,
        ingestion_id: str,
        operations: List[Dict],
        error: Exception
    ) -> Dict[str, Any]:
        """Build partial failure response."""
        return {
            "status": "partial_failure",
            "message": "Some operations failed",
            "ingestion_id": ingestion_id,
            "operations": operations,
            "error": str(error)
        }
```

### API Route

```python
# app/api/v1/refresh.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.refresh_service import RefreshService
from app.models.schemas import (
    FullRefreshRequest,
    FreshStartRequest,
    DropTableRequest,
    RefreshOperationResponse
)
from typing import Dict, Any

router = APIRouter(prefix="/api/v1/ingestions", tags=["Refresh Operations"])

@router.post(
    "/{ingestion_id}/full-refresh",
    response_model=RefreshOperationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Full Refresh",
    description="""
    Performs a complete table refresh by:
    1. Dropping the target table
    2. Clearing all processed file history
    3. Triggering a new ingestion run

    This reprocesses ALL source files from scratch.

    **Use Cases:**
    - Data quality fixes requiring reprocessing of all historical files
    - Source files were corrected/updated retroactively
    - Complete snapshot refresh from all source files

    **Safety:** Requires explicit confirmation. Supports dry-run mode.
    """
)
async def full_refresh(
    ingestion_id: str,
    request: FullRefreshRequest,
    db: Session = Depends(get_db),
    refresh_service: RefreshService = Depends()
) -> Dict[str, Any]:
    """Full refresh: drop table + clear files + run."""
    try:
        return await refresh_service.full_refresh(
            ingestion_id=ingestion_id,
            confirm=request.confirm,
            auto_run=request.auto_run,
            dry_run=request.dry_run,
            db=db
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Refresh operation failed: {str(e)}"
        )

@router.post(
    "/{ingestion_id}/fresh-start",
    response_model=RefreshOperationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Fresh Start",
    description="""
    Drops the target table and triggers a new ingestion run,
    but keeps the processed file history intact.

    Only processes files that were added since the last successful run.

    **Use Cases:**
    - Recover from corrupted table without reprocessing historical data
    - Change table structure/schema (force recreation)
    - Fresh table for new files only (skip historical data)

    **Safety:** Requires explicit confirmation. Supports dry-run mode.
    """
)
async def fresh_start(
    ingestion_id: str,
    request: FreshStartRequest,
    db: Session = Depends(get_db),
    refresh_service: RefreshService = Depends()
) -> Dict[str, Any]:
    """Fresh start: drop table + run (keep file history)."""
    try:
        return await refresh_service.fresh_start(
            ingestion_id=ingestion_id,
            confirm=request.confirm,
            auto_run=request.auto_run,
            dry_run=request.dry_run,
            db=db
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fresh start operation failed: {str(e)}"
        )

@router.delete(
    "/{ingestion_id}/table",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
    summary="Drop Table",
    description="""
    Drops the target Iceberg table if it exists.

    Does NOT affect processed file history or trigger any runs.
    Granular operation for advanced workflows.

    **Use Cases:**
    - Manual cleanup before custom orchestration
    - Advanced workflows requiring fine-grained control
    - Delete table without triggering automatic reprocessing

    **Safety:** Requires explicit confirmation.
    """
)
async def drop_table(
    ingestion_id: str,
    request: DropTableRequest,
    db: Session = Depends(get_db),
    refresh_service: RefreshService = Depends()
) -> Dict[str, Any]:
    """Drop table only (no file clearing, no run trigger)."""
    try:
        return await refresh_service.drop_table(
            ingestion_id=ingestion_id,
            confirm=request.confirm,
            db=db
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Drop table operation failed: {str(e)}"
        )
```

---

## Security and Safety

### 1. Confirmation Requirement

All destructive operations MUST require explicit `confirm=true`:

```python
@validator('confirm')
def confirm_must_be_true(cls, v):
    if not v:
        raise ValueError(
            "Confirmation required. This is a destructive operation. "
            "Set confirm=true to proceed."
        )
    return v
```

**Rejected requests:**
```bash
# Missing confirm
curl -X POST /api/v1/ingestions/123/full-refresh -d '{}'
# 400 Bad Request: "Confirmation required"

# confirm=false
curl -X POST /api/v1/ingestions/123/full-refresh -d '{"confirm": false}'
# 400 Bad Request: "Confirmation required"
```

### 2. Dry Run Mode

All convenience endpoints support dry-run mode to preview operations:

```bash
curl -X POST /api/v1/ingestions/123/full-refresh \
  -d '{"confirm": true, "dry_run": true}'
```

Response shows what WOULD happen without executing.

### 3. Run Conflict Detection

Prevent refreshes during active runs:

```python
if ingestion.current_run and ingestion.current_run.is_running:
    raise HTTPException(
        status_code=409,
        detail="Cannot refresh while ingestion is running",
        headers={"X-Current-Run-ID": ingestion.current_run.id}
    )
```

### 4. Authorization

Refresh operations require appropriate permissions:

```python
@router.post("/{ingestion_id}/full-refresh")
async def full_refresh(
    ingestion_id: str,
    request: FullRefreshRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # Check permissions
    if not current_user.has_permission("ingestion:refresh"):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    # Check tenant isolation
    ingestion = ingestion_repo.get_by_id(db, ingestion_id)
    if ingestion.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    # Proceed with refresh
    ...
```

### 5. Audit Logging

All refresh operations must be audited:

```python
await audit_logger.log(
    user_id=current_user.id,
    tenant_id=current_user.tenant_id,
    action="full_refresh",
    resource_type="ingestion",
    resource_id=ingestion_id,
    details={
        "operations": ["table_dropped", "files_cleared", "run_triggered"],
        "files_affected": 1247,
        "estimated_cost": 125.50
    }
)
```

### 6. Rate Limiting

Prevent accidental repeated calls:

```python
@router.post("/{ingestion_id}/full-refresh")
@limiter.limit("5/hour")  # Max 5 full refreshes per hour per user
async def full_refresh(...):
    ...
```

---

## Cost Transparency

### Impact Estimates

All responses include cost estimates:

```json
{
  "impact": {
    "files_to_process": 1247,
    "estimated_data_size_gb": 450.2,
    "estimated_cost_usd": 125.50,
    "estimated_duration_minutes": 45
  }
}
```

### Cost Warnings

Large operations trigger warnings:

```python
if impact["estimated_cost_usd"] > 100:
    warnings.append(
        f"⚠️ HIGH COST OPERATION: Estimated ${impact['estimated_cost_usd']}"
    )

if impact["files_to_process"] > 1000:
    warnings.append(
        f"⚠️ LARGE OPERATION: Processing {impact['files_to_process']} files"
    )
```

### Dry Run Required for Large Operations

For very expensive operations, enforce dry-run first:

```python
LARGE_OPERATION_THRESHOLD = 500  # $500

if impact["estimated_cost_usd"] > LARGE_OPERATION_THRESHOLD and not dry_run:
    raise HTTPException(
        status_code=400,
        detail=(
            f"Operation estimated at ${impact['estimated_cost_usd']}. "
            f"Run with dry_run=true first to review."
        )
    )
```

---

## Error Handling

### Partial Failure Handling

If operations fail midway, provide clear recovery instructions:

```json
{
  "status": "partial_failure",
  "message": "Table dropped but run failed to trigger",

  "operations": [
    {"operation": "table_dropped", "status": "success"},
    {"operation": "processed_files_cleared", "status": "success"},
    {"operation": "run_triggered", "status": "failed", "error": "Cluster unavailable"}
  ],

  "recovery": {
    "message": "Table and files cleared, but run did not start",
    "action": "Manually trigger run: POST /api/v1/ingestions/123/run",
    "state": "Table dropped, file history cleared, no run triggered"
  }
}
```

### Idempotency

Operations are idempotent where possible:

- **Drop table**: Succeeds even if table doesn't exist
- **Clear files**: Succeeds even if no files to clear
- **Trigger run**: Prevents duplicate runs if one is already active

### Error Codes

Standardized error codes for client handling:

| Error Code | HTTP Status | Meaning | Client Action |
|------------|-------------|---------|---------------|
| `CONFIRMATION_REQUIRED` | 400 | Missing/false confirm | Set confirm=true |
| `INGESTION_NOT_FOUND` | 404 | Invalid ingestion ID | Check ingestion ID |
| `RUN_IN_PROGRESS` | 409 | Run already active | Wait or cancel existing run |
| `CLUSTER_UNAVAILABLE` | 500 | Spark cluster down | Retry later or check cluster |
| `INSUFFICIENT_PERMISSIONS` | 403 | No permission | Contact admin |
| `PARTIAL_FAILURE` | 500 | Some ops failed | Follow recovery instructions |

---

## Migration from Manual Workflows

### Before (Manual Orchestration)

```bash
#!/bin/bash
# Daily snapshot refresh - manual orchestration

INGESTION_ID="users-snapshot"

# Step 1: Drop table (via psql)
psql -c "DROP TABLE IF EXISTS analytics.dim_users;"

# Step 2: Clear processed files (via API)
curl -X DELETE "https://api.iomete.com/api/v1/ingestions/$INGESTION_ID/processed-files" \
  -H "Authorization: Bearer $TOKEN"

# Step 3: Trigger run (via API)
curl -X POST "https://api.iomete.com/api/v1/ingestions/$INGESTION_ID/run" \
  -H "Authorization: Bearer $TOKEN"
```

**Problems:**
- 3 separate operations
- Mixing SQL and API calls
- No error handling
- No visibility into what happened
- No safety checks

### After (Convenience API)

```bash
#!/bin/bash
# Daily snapshot refresh - single API call

INGESTION_ID="users-snapshot"

# Single API call with safety and visibility
curl -X POST "https://api.iomete.com/api/v1/ingestions/$INGESTION_ID/full-refresh" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "confirm": true,
    "auto_run": true
  }'
```

**Benefits:**
- Single API call
- Atomic operation
- Built-in error handling
- Detailed response with cost estimates
- Safety confirmations

### Backward Compatibility

Existing granular endpoints remain functional:

```bash
# Advanced users can still compose granular operations
curl -X DELETE /api/v1/ingestions/123/table -d '{"confirm": true}'
curl -X DELETE /api/v1/ingestions/123/processed-files
curl -X POST /api/v1/ingestions/123/run
```

---

## Use Case Examples

### Example 1: Daily Snapshot Refresh (Scheduled)

**Scenario:** Daily full snapshot of users dimension table.

**Solution:**

```bash
#!/bin/bash
# Scheduled via cron: 0 2 * * * (daily at 2 AM)

INGESTION_ID="users-daily-snapshot"
API_BASE="https://api.iomete.com/api/v1"
TOKEN="your_auth_token"

# Full refresh (single call)
RESPONSE=$(curl -s -X POST "$API_BASE/ingestions/$INGESTION_ID/full-refresh" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "confirm": true,
    "auto_run": true
  }')

RUN_ID=$(echo $RESPONSE | jq -r '.run_id')
echo "Snapshot refresh triggered: Run $RUN_ID"

# Optional: Wait for completion
while true; do
  STATUS=$(curl -s "$API_BASE/runs/$RUN_ID" \
    -H "Authorization: Bearer $TOKEN" | jq -r '.status')

  if [ "$STATUS" = "SUCCESS" ]; then
    echo "Snapshot refresh completed successfully"
    break
  elif [ "$STATUS" = "FAILED" ]; then
    echo "Snapshot refresh failed"
    exit 1
  fi

  sleep 30
done
```

### Example 2: Recover from Corrupted Table

**Scenario:** Table corrupted, but source files are intact. Want fresh table with new data only.

**Solution:**

```bash
#!/bin/bash
# One-time recovery operation

INGESTION_ID="transactions"
API_BASE="https://api.iomete.com/api/v1"
TOKEN="your_auth_token"

# Preview first (dry run)
curl -X POST "$API_BASE/ingestions/$INGESTION_ID/fresh-start" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "confirm": true,
    "dry_run": true
  }' | jq '.'

# Review output, then execute
read -p "Proceed with fresh start? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  curl -X POST "$API_BASE/ingestions/$INGESTION_ID/fresh-start" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "confirm": true,
      "auto_run": true
    }'
fi
```

### Example 3: Data Quality Fix (Reprocess All Files)

**Scenario:** Bug in transformation logic. Fixed. Need to reprocess all historical files.

**Solution:**

```bash
#!/bin/bash
# One-time reprocessing after bug fix

INGESTION_ID="sales-data"
API_BASE="https://api.iomete.com/api/v1"
TOKEN="your_auth_token"

echo "========================================="
echo "WARNING: Full Reprocess Operation"
echo "========================================="
echo ""

# Get cost estimate (dry run)
ESTIMATE=$(curl -s -X POST "$API_BASE/ingestions/$INGESTION_ID/full-refresh" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "confirm": true,
    "dry_run": true
  }')

FILES=$(echo $ESTIMATE | jq -r '.impact.files_to_process')
COST=$(echo $ESTIMATE | jq -r '.impact.estimated_cost_usd')
DURATION=$(echo $ESTIMATE | jq -r '.impact.estimated_duration_minutes')

echo "This will reprocess:"
echo "  - Files: $FILES"
echo "  - Estimated Cost: \$$COST"
echo "  - Estimated Duration: $DURATION minutes"
echo ""

read -p "Are you sure you want to proceed? (yes/no) " -r
echo

if [ "$REPLY" = "yes" ]; then
  echo "Starting full refresh..."
  curl -X POST "$API_BASE/ingestions/$INGESTION_ID/full-refresh" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "confirm": true,
      "auto_run": true
    }' | jq '.'
else
  echo "Aborted."
fi
```

### Example 4: Airflow Integration

**Scenario:** Orchestrate refresh operations from Airflow DAG.

**Solution:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email': ['alerts@company.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_user_snapshot_refresh',
    default_args=default_args,
    description='Daily full refresh of user dimension table',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False
)

# Full refresh task
full_refresh_task = SimpleHttpOperator(
    task_id='full_refresh_users',
    http_conn_id='iomete_api',
    endpoint='/api/v1/ingestions/users-snapshot/full-refresh',
    method='POST',
    headers={
        'Authorization': 'Bearer {{ var.value.iomete_api_token }}',
        'Content-Type': 'application/json'
    },
    data=json.dumps({
        'confirm': True,
        'auto_run': True
    }),
    response_check=lambda response: response.json()['status'] == 'accepted',
    dag=dag
)

# Monitor run completion
def wait_for_completion(**context):
    import requests
    import time

    ti = context['task_instance']
    response = ti.xcom_pull(task_ids='full_refresh_users')
    run_id = json.loads(response)['run_id']

    api_base = "https://api.iomete.com/api/v1"
    token = context['var']['value']['iomete_api_token']

    while True:
        resp = requests.get(
            f"{api_base}/runs/{run_id}",
            headers={'Authorization': f'Bearer {token}'}
        )
        status = resp.json()['status']

        if status == 'SUCCESS':
            print(f"Run {run_id} completed successfully")
            return
        elif status in ['FAILED', 'PARTIAL_SUCCESS']:
            raise Exception(f"Run {run_id} failed with status: {status}")

        time.sleep(30)

wait_task = PythonOperator(
    task_id='wait_for_completion',
    python_callable=wait_for_completion,
    provide_context=True,
    dag=dag
)

full_refresh_task >> wait_task
```

---

## Testing Strategy

### Unit Tests

```python
# tests/services/test_refresh_service.py

import pytest
from unittest.mock import Mock, AsyncMock
from app.services.refresh_service import RefreshService

@pytest.mark.asyncio
async def test_full_refresh_success(db_session, sample_ingestion):
    """Test successful full refresh."""
    # Setup mocks
    spark_service = Mock()
    spark_service.drop_table = AsyncMock(return_value={"table_dropped": True})

    file_state_service = Mock()
    file_state_service.clear_all_processed = AsyncMock(return_value=1247)

    ingestion_service = Mock()
    run = Mock(id="run-123")
    ingestion_service.trigger_run = AsyncMock(return_value=run)

    ingestion_repo = Mock()
    ingestion_repo.get_by_id = Mock(return_value=sample_ingestion)

    # Create service
    service = RefreshService(
        spark_service, file_state_service, ingestion_service, ingestion_repo
    )

    # Execute
    result = await service.full_refresh(
        ingestion_id="ing-123",
        confirm=True,
        auto_run=True,
        dry_run=False,
        db=db_session
    )

    # Assertions
    assert result["status"] == "accepted"
    assert result["run_id"] == "run-123"
    assert len(result["operations"]) == 3
    assert all(op["status"] == "success" for op in result["operations"])

    # Verify calls
    spark_service.drop_table.assert_called_once()
    file_state_service.clear_all_processed.assert_called_once()
    ingestion_service.trigger_run.assert_called_once()

@pytest.mark.asyncio
async def test_full_refresh_requires_confirmation(db_session, sample_ingestion):
    """Test that confirmation is required."""
    service = RefreshService(Mock(), Mock(), Mock(), Mock())

    with pytest.raises(ValueError, match="Confirmation required"):
        await service.full_refresh(
            ingestion_id="ing-123",
            confirm=False,
            db=db_session
        )

@pytest.mark.asyncio
async def test_full_refresh_dry_run(db_session, sample_ingestion):
    """Test dry run mode."""
    # Setup mocks
    ingestion_repo = Mock()
    ingestion_repo.get_by_id = Mock(return_value=sample_ingestion)

    file_state_service = Mock()
    file_state_service.list_all_source_files = AsyncMock(return_value=[
        Mock(size_bytes=1024*1024*1024)  # 1 GB
    ])

    service = RefreshService(Mock(), file_state_service, Mock(), ingestion_repo)

    # Execute dry run
    result = await service.full_refresh(
        ingestion_id="ing-123",
        confirm=True,
        dry_run=True,
        db=db_session
    )

    # Assertions
    assert result["status"] == "dry_run"
    assert "would_perform" in result
    assert "impact" in result

    # Verify no actual operations performed
    file_state_service.clear_all_processed.assert_not_called()

@pytest.mark.asyncio
async def test_fresh_start_success(db_session, sample_ingestion):
    """Test successful fresh start."""
    # Similar to full_refresh but without file clearing
    ...

@pytest.mark.asyncio
async def test_partial_failure_recovery(db_session, sample_ingestion):
    """Test partial failure handling and recovery info."""
    # Setup mocks - table drop succeeds, run trigger fails
    spark_service = Mock()
    spark_service.drop_table = AsyncMock(return_value={"table_dropped": True})

    ingestion_service = Mock()
    ingestion_service.trigger_run = AsyncMock(side_effect=Exception("Cluster unavailable"))

    # ... rest of test
```

### Integration Tests

```python
# tests/api/test_refresh_endpoints.py

import pytest
from fastapi.testclient import TestClient

def test_full_refresh_endpoint(client: TestClient, auth_headers, db_session):
    """Test full refresh endpoint."""
    # Create test ingestion
    ingestion = create_test_ingestion(db_session)

    # Call endpoint
    response = client.post(
        f"/api/v1/ingestions/{ingestion.id}/full-refresh",
        headers=auth_headers,
        json={
            "confirm": True,
            "auto_run": True
        }
    )

    # Assertions
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert "run_id" in data
    assert len(data["operations"]) == 3

def test_full_refresh_without_confirmation(client: TestClient, auth_headers):
    """Test that confirmation is required."""
    response = client.post(
        f"/api/v1/ingestions/ing-123/full-refresh",
        headers=auth_headers,
        json={
            "confirm": False
        }
    )

    assert response.status_code == 400
    assert "Confirmation required" in response.json()["detail"]

def test_dry_run_mode(client: TestClient, auth_headers, db_session):
    """Test dry run mode."""
    ingestion = create_test_ingestion(db_session)

    response = client.post(
        f"/api/v1/ingestions/{ingestion.id}/full-refresh",
        headers=auth_headers,
        json={
            "confirm": True,
            "dry_run": True
        }
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "dry_run"
    assert "would_perform" in data
    assert "impact" in data

def test_run_conflict_detection(client: TestClient, auth_headers, db_session):
    """Test that refresh is blocked during active run."""
    ingestion = create_test_ingestion(db_session)

    # Start a run
    start_run(ingestion.id)

    # Try to refresh
    response = client.post(
        f"/api/v1/ingestions/{ingestion.id}/full-refresh",
        headers=auth_headers,
        json={"confirm": True}
    )

    assert response.status_code == 409
    assert "Cannot refresh while ingestion is running" in response.json()["detail"]
```

### E2E Tests

```python
# tests/e2e/test_refresh_workflows.py

import pytest
from time import sleep

@pytest.mark.e2e
def test_full_refresh_workflow(client, auth_headers, spark_client, db_session):
    """End-to-end test of full refresh workflow."""
    # Setup: Create ingestion with sample data
    ingestion = create_ingestion_with_data(db_session, spark_client)

    # Execute full refresh
    response = client.post(
        f"/api/v1/ingestions/{ingestion.id}/full-refresh",
        headers=auth_headers,
        json={"confirm": True, "auto_run": True}
    )

    assert response.status_code == 202
    run_id = response.json()["run_id"]

    # Wait for completion
    wait_for_run_completion(client, auth_headers, run_id, timeout=300)

    # Verify results
    run = get_run(client, auth_headers, run_id)
    assert run["status"] == "SUCCESS"

    # Verify table was recreated
    table_exists = check_table_exists(spark_client, ingestion.destination.table)
    assert table_exists

    # Verify all files were reprocessed
    processed_files = get_processed_files(db_session, ingestion.id)
    assert len(processed_files) > 0
```

---

## Documentation Requirements

### 1. User Documentation

Create comprehensive user guide:
- **docs/user-docs/refresh-operations-guide.md**
  - Overview of refresh operations
  - When to use each operation
  - Step-by-step examples
  - Best practices
  - Cost considerations
  - FAQ

### 2. API Documentation

Update OpenAPI spec:
- Add new endpoints with detailed descriptions
- Request/response examples
- Error code reference
- Authentication requirements

### 3. Migration Guide

Update existing overwrite mode alternative doc:
- **docs/user-docs/overwrite-mode-alternative.md**
  - Add section on new convenience APIs
  - Comparison: manual vs. API approach
  - Migration examples

### 4. Changelog

Document the new feature:
- **CHANGELOG.md**
  - New convenience endpoints for refresh operations
  - Breaking changes (if any)
  - Deprecation notices (if any)

---

## Success Metrics

### KPIs

1. **Adoption Rate**
   - % of users using convenience APIs vs. manual orchestration
   - Target: 70% adoption within 3 months

2. **Error Reduction**
   - % reduction in failed refresh operations
   - Target: 50% reduction in user-caused errors

3. **Time Savings**
   - Average time to complete refresh workflow
   - Target: 80% reduction (from ~5 min to ~1 min)

4. **Support Tickets**
   - % reduction in support tickets related to refresh operations
   - Target: 60% reduction

5. **API Usage**
   - Number of refresh API calls per week
   - Track trends over time

### Monitoring

Track the following metrics:

```python
# Prometheus metrics
refresh_operations_total = Counter(
    'autoloader_refresh_operations_total',
    'Total number of refresh operations',
    ['operation_type', 'status']
)

refresh_duration_seconds = Histogram(
    'autoloader_refresh_duration_seconds',
    'Duration of refresh operations',
    ['operation_type']
)

refresh_cost_usd = Histogram(
    'autoloader_refresh_cost_usd',
    'Estimated cost of refresh operations',
    ['operation_type']
)
```

---

## Rollout Plan

### Phase 1: Internal Alpha (Week 1-2)
- Deploy to internal development environment
- Internal team testing
- Bug fixes and refinements
- Documentation review

### Phase 2: Beta (Week 3-4)
- Deploy to staging environment
- Invite select beta users
- Gather feedback
- Performance testing
- Security review

### Phase 3: General Availability (Week 5+)
- Deploy to production
- Announce new feature
- Update documentation
- Monitor adoption and metrics
- Provide support

### Feature Flags

Use feature flags for controlled rollout:

```python
ENABLE_REFRESH_OPERATIONS_API = os.getenv(
    'ENABLE_REFRESH_OPERATIONS_API',
    'false'
).lower() == 'true'

if not ENABLE_REFRESH_OPERATIONS_API:
    raise HTTPException(
        status_code=503,
        detail="Refresh operations API is not available"
    )
```

---

## Risks and Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Accidental expensive operations | HIGH | MEDIUM | Require confirmation, dry-run mode, cost warnings |
| Partial failures leave inconsistent state | MEDIUM | LOW | Detailed error responses with recovery instructions |
| API abuse (repeated calls) | MEDIUM | LOW | Rate limiting, audit logging |
| Security vulnerabilities | HIGH | LOW | Authorization checks, tenant isolation, code review |
| Performance issues with large tables | MEDIUM | MEDIUM | Async operations, proper indexing, monitoring |
| User confusion about operations | MEDIUM | MEDIUM | Clear naming, documentation, examples |

---

## Open Questions

1. **Should we support batch refresh operations?**
   - Refresh multiple ingestions in one call?
   - Use case: coordinated refresh of dependent tables

2. **Should we add scheduled refresh support?**
   - Configure refresh schedule directly in ingestion config?
   - Or keep it as external orchestration concern?

3. **Should we add notification webhooks?**
   - Notify on refresh completion/failure?
   - Slack/email integration?

4. **Should we expose table metadata in responses?**
   - Row counts, data size, partitions?
   - Requires Spark queries - performance impact?

5. **Should we support selective file refresh?**
   - Reprocess only specific date ranges or partitions?
   - Complex filtering logic?

---

## Future Enhancements

### Phase 2 Features (Post-MVP)

1. **Batch Refresh**
   ```json
   POST /api/v1/batch-refresh
   {
     "ingestion_ids": ["ing-1", "ing-2", "ing-3"],
     "operation": "full_refresh",
     "confirm": true
   }
   ```

2. **Scheduled Refresh**
   ```json
   PUT /api/v1/ingestions/{id}
   {
     "refresh_schedule": {
       "operation": "full_refresh",
       "cron": "0 2 * * *"
     }
   }
   ```

3. **Conditional Refresh**
   ```json
   POST /api/v1/ingestions/{id}/refresh-if
   {
     "condition": "file_count > 100",
     "operation": "full_refresh"
   }
   ```

4. **Selective Refresh**
   ```json
   POST /api/v1/ingestions/{id}/refresh-range
   {
     "start_date": "2025-01-01",
     "end_date": "2025-01-31",
     "operation": "reprocess"
   }
   ```

---

## Conclusion

This PRD defines a comprehensive set of convenience APIs for refresh operations in IOMETE Autoloader. The design prioritizes:

1. **Simplicity** - One API call for common workflows
2. **Safety** - Built-in confirmations and dry-run mode
3. **Transparency** - Detailed responses with cost estimates
4. **Flexibility** - Granular APIs still available for advanced users

**Next Steps:**
1. Review and approval
2. Implementation (service layer, API routes, tests)
3. Documentation (user guide, API spec, examples)
4. Testing (unit, integration, e2e)
5. Rollout (alpha, beta, GA)

---

**Approval:**

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Product Owner | | | |
| Engineering Lead | | | |
| Security Review | | | |
| Documentation | | | |

---

**Document Version:** 1.0
**Last Updated:** 2025-01-05
**Author:** IOMETE Engineering
