# Product Requirements Document
# IOMETE Data Ingestion - Automated File Ingestion Feature

**Version:** 1.0  
**Last Updated:** November 2, 2025  
**Author:** Product Team  
**Status:** Ready for Development

---

## Executive Summary

IOMETE will implement a **zero-code, UI-driven data ingestion system** that enables users to automatically load files from cloud storage (S3, Azure Blob, GCS) into IOMETE tables on a scheduled or continuous basis. This feature eliminates the need for users to write custom Spark code for common ingestion patterns while providing enterprise-grade reliability, monitoring, and cost transparency.

**Key Differentiators:**
- Visual configuration (no code required for 90% of use cases)
- Upfront cost estimation before committing
- Intelligent schema evolution handling with guided resolution
- Built-in preview/testing before going live
- Business-friendly monitoring (not just technical metrics)

---

## 1. Problem Statement

### Current State
Users currently write custom Spark jobs to ingest files from cloud storage, which requires:
- Writing and testing Spark code (time-consuming, error-prone)
- Managing checkpoints manually (complex, easy to mess up)
- Monitoring through Spark logs (technical, not user-friendly)
- No cost visibility until after jobs run
- Schema changes break pipelines with cryptic errors

### Target State
Users configure ingestion through a visual wizard, test their configuration, and activate it with one click. The system handles all Spark complexity, checkpoint management, error recovery, and provides clear monitoring and cost estimation.

### Market Context
- **Databricks Auto Loader**: Powerful but complex, requires code, technical monitoring
- **Snowflake Snowpipe**: Simple but proprietary, always-on (expensive for batch), limited transformation
- **IOMETE Opportunity**: Combine simplicity of Snowpipe with power of Auto Loader, optimized for scheduled batch ingestion

---

## 2. Goals & Success Metrics

### Business Goals
1. Increase platform usage (more data ingested = more compute hours)
2. Reduce time-to-value for new customers (setup in minutes vs. days)
3. Improve customer retention (easier to use = stickier)
4. Differentiate from Databricks/Snowflake in mid-market

### Success Metrics

**Primary Metrics:**
- Number of active ingestion pipelines per customer
- Average setup time: < 10 minutes (vs. 2-4 hours with manual Spark code)
- Adoption rate: 80% of customers use ingestion feature within first month
- Pipeline success rate: > 99% (fewer failures than manual jobs)

**Secondary Metrics:**
- Customer satisfaction (NPS) for ingestion feature: > 8/10
- Support tickets related to ingestion: < 5% of total tickets
- Average monthly compute hours per pipeline: 10-20 hours (usage growth)

**Leading Indicators:**
- Number of preview/test runs per week
- Draft configurations saved (intent to use)
- Schema evolution incidents handled without support tickets

---

## 3. User Personas & User Stories

### Primary Persona: Data Engineer (Maya)
**Background:**
- 3-5 years experience
- Works at mid-size company (200-2000 employees)
- Comfortable with SQL, some Spark experience
- Time-constrained (manages multiple projects)
- Values reliability and visibility

**Pain Points:**
- "I spend 2 days writing and testing Spark jobs for simple file ingestion"
- "Schema changes break my pipeline and I don't find out until the next day"
- "I can't estimate costs before running jobs"
- "Checkpoint corruption means reprocessing everything"

**User Stories:**
```
As Maya, I want to...
- Set up file ingestion without writing Spark code
- See upfront cost estimates before committing
- Get alerted when schema changes (with suggested fixes)
- Monitor ingestion health at a glance
- Test configuration before going live
```

### Secondary Persona: Data Analyst (Alex)
**Background:**
- Strong SQL skills, no Spark experience
- Needs to load data for analysis
- Non-technical (business background)
- Wants self-service tools

**Pain Points:**
- "I have to wait for data engineers to write ingestion jobs"
- "I don't understand Spark errors"
- "I can't tell if my data is loading correctly"

**User Stories:**
```
As Alex, I want to...
- Set up ingestion myself without engineering help
- Understand error messages in plain English
- See exactly what data was loaded (sample preview)
- Know if something goes wrong (clear alerts)
```

---

## 4. Functional Requirements

### 4.1 Core Workflow: Ingestion Wizard

**FR-1: Multi-Step Configuration Wizard**

**Step 1: Source Configuration**
- **FR-1.1** Support source types: AWS S3, Azure Blob, Google Cloud Storage
- **FR-1.2** Accept bucket/path input with validation
- **FR-1.3** Support file pattern filtering (glob patterns: `*.json`, `data-2024-*.parquet`)
- **FR-1.4** Credential management:
    - Store credentials securely (encrypted)
    - Support IAM roles/service accounts (preferred)
    - Support access keys as fallback
- **FR-1.5** Connection testing:
    - Validate credentials
    - List files in path
    - Show file count and total size
    - Display "Connection successful - X files discovered"
- **FR-1.6** Region selection for S3

**Step 2: Format & Schema Configuration**
- **FR-2.1** Support file formats: JSON, CSV, Parquet, Avro, ORC, XML
- **FR-2.2** Schema inference:
    - Option 1: Auto-detect schema from sample files
    - Option 2: User provides schema manually
    - Sample first 10 files for inference
- **FR-2.3** Schema evolution options:
    - Enable/disable automatic schema evolution
    - Configure merge behavior for new columns
- **FR-2.4** Format-specific options:
    - CSV: delimiter, header, quote character
    - JSON: single-line vs. multiline
    - Compression: auto-detect or specify (gzip, snappy, lz4)
- **FR-2.5** Column type inference (string, int, double, timestamp, etc.)
- **FR-2.6** Show schema preview with sample values

**Step 3: Destination Configuration**
- **FR-3.1** Target table selection:
    - Catalog dropdown
    - Database/schema dropdown
    - Table name input
    - Show full qualified name (catalog.database.table)
- **FR-3.2** Table creation options:
    - Create new table
    - Append to existing table
    - Overwrite existing table
    - Merge/upsert (requires primary key configuration)
- **FR-3.3** Optimization options:
    - Partitioning: enable/disable, select partition column(s)
    - Z-Ordering: enable/disable, select clustering columns
    - Table properties (optional)
- **FR-3.4** Validate table doesn't exist (for new) or exists (for append)

**Step 4: Schedule & Quality**
- **FR-4.1** Processing mode:
    - Scheduled (batch): daily, hourly, weekly, custom cron
    - Continuous (streaming): always-on processing
    - Recommend scheduled for batch data
- **FR-4.2** Schedule configuration:
    - Frequency selector (daily, hourly, weekly, custom)
    - Time picker (UTC with timezone conversion display)
    - Cron expression input for advanced users
- **FR-4.3** Backfill options:
    - Process only new files
    - Include existing files (show count)
    - Start from specific date
- **FR-4.4** Data quality rules (optional):
    - Row count threshold alerts
    - Schema validation
    - Null value checks
- **FR-4.5** Cost estimation:
    - Calculate based on:
        - File count × file size
        - Processing frequency
        - Cluster size (auto-recommended)
    - Display breakdown: compute + storage + file discovery
    - Show monthly estimate
- **FR-4.6** Configuration summary display

**FR-2: Preview & Test Mode**
- **FR-2.1** Test connection to source
- **FR-2.2** Discover files in source path
- **FR-2.3** Analyze sample files (first 10):
    - Infer schema
    - Detect file format
    - Show record count estimate
    - Preview sample data (first 5 rows)
- **FR-2.4** Data quality checks:
    - Detect null values (percentage)
    - Detect malformed records
    - Detect schema inconsistencies across files
- **FR-2.5** Show optimization recommendations:
    - Partitioning suggestions
    - File notification vs. directory listing
    - Cluster size recommendations
- **FR-2.6** Allow return to edit before saving

**FR-3: Save & Activate**
- **FR-3.1** Save configuration as draft (not active)
- **FR-3.2** Activate ingestion:
    - Generate Spark job under the hood
    - Create checkpoint location automatically
    - Schedule job (if scheduled mode)
    - Start streaming (if continuous mode)
- **FR-3.3** Validate configuration before saving:
    - All required fields present
    - Credentials valid
    - Target table accessible
    - No naming conflicts

---

### 4.2 Monitoring Dashboard

**FR-10: Ingestion List View**
- **FR-10.1** Show all ingestion pipelines for user
- **FR-10.2** Display key stats at top:
    - Total active ingestions
    - Files processed (24h)
    - Data ingested (24h)
    - Success rate
- **FR-10.3** Per-ingestion card shows:
    - Name (target table name)
    - Status badge (healthy, warning, error)
    - Source type and schedule tags
    - Last run time
    - Next scheduled run time
    - Source path
    - Quick actions: Run Now, Edit, Pause, Delete
- **FR-10.4** Filter/sort options:
    - By status (all, healthy, warnings, errors)
    - By schedule type (all, scheduled, continuous)
    - By source type (S3, Azure, GCS)
    - Sort by: name, last run, next run, created date

**FR-11: Ingestion Detail View**
- **FR-11.1** Expandable metrics section:
    - Last 7 days performance
    - Files processed count
    - Records ingested count
    - Average processing time
    - Failed runs count
- **FR-11.2** Visual trend chart:
    - Daily volume processed (sparkline)
    - Hover to see details
- **FR-11.3** Configuration display:
    - Source details
    - Format and schema settings
    - Destination table
    - Schedule configuration
    - Monthly cost
- **FR-11.4** Alert section (if issues exist):
    - Schema evolution detected
    - Processing delays
    - Quality check failures
    - With actionable links to resolve

**FR-12: Run History**
- **FR-12.1** Show last 30 days of runs
- **FR-12.2** Per-run details:
    - Start/end time
    - Duration
    - Files processed
    - Records loaded
    - Status (success, failed, partial)
    - Error messages (if failed)
- **FR-12.3** Link to Spark logs for debugging
- **FR-12.4** Retry failed runs (one-click)

---

### 4.3 Schema Evolution Handling

**FR-20: Schema Change Detection**
- **FR-20.1** Detect schema changes during ingestion:
    - New columns added
    - Columns removed
    - Type changes (string → int, etc.)
- **FR-20.2** Pause ingestion when change detected (default behavior)
- **FR-20.3** Create alert/notification:
    - Email notification (if configured)
    - In-app notification
    - Show warning badge on dashboard

**FR-21: Schema Resolution UI**
- **FR-21.1** Display "Schema Evolution Detected" screen:
    - Clear explanation of what happened
    - List affected files (not yet processed)
    - Record count impact
- **FR-21.2** Visual before/after schema comparison:
    - Side-by-side view
    - Highlight new/removed/changed columns
    - Show data types
- **FR-21.3** Present resolution options:
    - **Option 1: Auto-merge** (recommended)
        - Add new columns automatically
        - Existing records have NULL for new columns
        - Show SQL preview of changes
        - Impact: no data loss, backward compatible
    - **Option 2: Backfill with default**
        - Add column and populate existing records
        - User provides default value or calculation
        - Impact: consistent data model, requires compute
    - **Option 3: Create new version table**
        - Keep current table, create new table for new schema
        - Impact: preserves history, more complex queries
    - **Option 4: Ignore new column**
        - Continue processing, drop new column
        - Impact: data loss for new column
- **FR-21.4** Apply solution:
    - Execute schema change (if auto-merge or backfill)
    - Resume ingestion automatically
    - Record decision in audit log
- **FR-21.5** "Review Later" option:
    - Keep ingestion paused
    - Save for manual review

---

### 4.4 Management Operations

**FR-30: Run Operations**
- **FR-30.1** Manual trigger: "Run Now"
    - Trigger ingestion immediately (ad-hoc)
    - Process all available files since last run
    - Show progress indicator
- **FR-30.2** Pause/Resume:
    - Pause active ingestion
    - Resume paused ingestion
    - Preserve checkpoint state
- **FR-30.3** Delete ingestion:
    - Confirmation dialog
    - Option to delete target table or keep it
    - Clean up checkpoints and state

**FR-31: Edit Configuration**
- **FR-31.1** Allow editing:
    - Schedule settings
    - Quality rules
    - Alert recipients
- **FR-31.2** Restricted edits (require recreation):
    - Source path (would need new checkpoint)
    - Target table (different destination)
    - File format (different schema)
    - Show warning that this requires re-creating pipeline
- **FR-31.3** Save changes and apply:
    - Update job configuration
    - Restart if currently running

**FR-32: Alerts & Notifications**
- **FR-32.1** Configure notification channels:
    - Email addresses (comma-separated)
    - Slack webhook (future)
    - PagerDuty (future)
- **FR-32.2** Alert triggers:
    - Run failure
    - Schema evolution detected
    - Row count below threshold
    - Processing time exceeds threshold (SLA breach)
- **FR-32.3** Alert content:
    - What happened
    - Which ingestion
    - Timestamp
    - Quick link to dashboard
    - Suggested action

---

## 5. Technical Requirements

### 5.1 Architecture

**Backend Components:**

```
┌─────────────────────────────────────────────────────────────┐
│                     IOMETE Platform                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐         ┌────────────────────┐       │
│  │  Ingestion API  │◄────────┤  UI (React/Next)   │       │
│  │  (REST/GraphQL) │         │                    │       │
│  └────────┬────────┘         └────────────────────┘       │
│           │                                                │
│           ▼                                                │
│  ┌─────────────────────────────────────────┐             │
│  │   Ingestion Service (Core Logic)        │             │
│  │  - Configuration management             │             │
│  │  - Job generation                       │             │
│  │  - Scheduling                           │             │
│  │  - Monitoring                           │             │
│  └──────────┬──────────────────────────────┘             │
│             │                                             │
│             ├──────► Job Scheduler (Cron/Quartz)         │
│             │                                             │
│             ├──────► Spark Job Orchestrator              │
│             │        - Generate Spark code               │
│             │        - Manage checkpoints                │
│             │        - Execute jobs                      │
│             │                                             │
│             └──────► Metadata Store (PostgreSQL)         │
│                      - Ingestion configs                 │
│                      - Run history                       │
│                      - Schema versions                   │
│                                                           │
│  ┌──────────────────────────────────────────┐            │
│  │    Apache Spark Cluster                  │            │
│  │  - Auto Loader jobs (generated)          │            │
│  │  - Checkpoint management                 │            │
│  │  - Apache Iceberg writes                 │            │
│  └──────────────────────────────────────────┘            │
│                                                           │
└───────────────────────────────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │   Cloud Storage (S3/Azure/GCS)     │
        │   - Source files                    │
        │   - Checkpoints                    │
        │   - Apache Iceberg tables          │
        └────────────────────────────────────┘
```

**TR-1: API Layer**
- RESTful API for all ingestion operations
- Authentication via existing IOMETE auth system
- Rate limiting: 100 requests/minute per user
- Endpoints:
    - `POST /api/v1/ingestions` - Create ingestion
    - `GET /api/v1/ingestions` - List ingestions
    - `GET /api/v1/ingestions/{id}` - Get ingestion details
    - `PUT /api/v1/ingestions/{id}` - Update ingestion
    - `DELETE /api/v1/ingestions/{id}` - Delete ingestion
    - `POST /api/v1/ingestions/{id}/run` - Trigger manual run
    - `POST /api/v1/ingestions/{id}/pause` - Pause ingestion
    - `POST /api/v1/ingestions/{id}/resume` - Resume ingestion
    - `POST /api/v1/ingestions/test` - Test configuration (preview mode)
    - `GET /api/v1/ingestions/{id}/runs` - Get run history
    - `GET /api/v1/ingestions/{id}/metrics` - Get metrics

**TR-2: Ingestion Service (Core Logic)**
- Written in Scala/Java (to integrate with Spark ecosystem)
- Responsibilities:
    1. Configuration validation and storage
    2. Spark job generation from configuration
    3. Checkpoint management (create, monitor, clean up)
    4. Schema inference and evolution detection
    5. Cost estimation calculations
    6. Job scheduling integration
- Design patterns:
    - Strategy pattern for different source types (S3, Azure, GCS)
    - Builder pattern for Spark job generation
    - Repository pattern for data access

**TR-3: Job Generation Engine**
- Generate Spark code from configuration
- Use Spark's structured streaming with cloudFiles source
- Template-based generation:
    - Base template for all ingestions
    - Source-specific templates (S3, Azure, GCS)
    - Format-specific options (CSV, JSON, Parquet)
- Output: Executable Spark application (JAR or Python script)
- Example generated code structure:
```scala
spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpointPath)
  .option("cloudFiles.useNotifications", "true")  // if enabled
  .load(sourcePath)
  .writeStream
  .format("iceberg")
  .option("checkpointLocation", checkpointPath)
  .trigger(Trigger.AvailableNow)  // for scheduled batch
  .toTable(targetTable)
```

**TR-4: Checkpoint Management**
- Auto-generate checkpoint location: `s3://iomete-checkpoints/{tenant}/{ingestion-id}/`
- Store in metadata DB for reference
- Monitor checkpoint health:
    - Size growth over time
    - Last modified timestamp
    - Corruption detection
- Cleanup strategy:
    - Retain for 30 days after ingestion deleted
    - Archive old checkpoints (optional)

**TR-5: Scheduler Integration**
- Use existing IOMETE job scheduler (if available) or implement lightweight scheduler
- Cron-based scheduling for batch jobs
- Support:
    - Daily: specific time
    - Hourly: specific minute
    - Weekly: day + time
    - Custom: cron expression
- Job execution:
    - Submit Spark job to cluster
    - Track job ID and status
    - Capture metrics (start/end time, records processed)
    - Handle failures (retry with backoff)

**TR-6: Monitoring & Metrics**
- Collect metrics from Spark job execution:
    - Files processed count
    - Records ingested count
    - Processing duration
    - Input bytes read
    - Output bytes written
    - Errors encountered
- Store in time-series format (InfluxDB or PostgreSQL)
- Aggregations:
    - Hourly, daily, weekly rollups
    - 30-day retention for detailed metrics
    - 1-year retention for daily aggregates
- Expose via metrics API for dashboard

**TR-7: Schema Versioning**
- Store schema history in metadata DB:
    - Ingestion ID
    - Version number (auto-increment)
    - Schema JSON (column name, type, nullable)
    - Detection timestamp
    - Resolution (if applied)
    - Sample files that triggered change
- Enable time-travel queries on schema changes
- Support schema comparison (diff between versions)

---

### 5.2 Data Models

**Ingestion Configuration Model**
```json
{
  "id": "uuid",
  "tenant_id": "uuid",
  "name": "aws_logs_raw",
  "status": "active|paused|error|draft",
  "created_at": "timestamp",
  "updated_at": "timestamp",
  "created_by": "user_id",
  
  "source": {
    "type": "s3|azure_blob|gcs",
    "path": "s3://bucket/path/",
    "file_pattern": "*.json",
    "credentials": {
      "type": "iam_role|access_key|service_account",
      "access_key_id": "encrypted",
      "secret_access_key": "encrypted",
      "region": "us-east-1"
    }
  },
  
  "format": {
    "type": "json|csv|parquet|avro|orc",
    "options": {
      "multiline": false,
      "compression": "snappy",
      "header": true,
      "delimiter": ","
    },
    "schema": {
      "inference": "auto|manual",
      "evolution_enabled": true,
      "schema_json": {...}  // optional for manual
    }
  },
  
  "destination": {
    "catalog": "analytics",
    "database": "logs",
    "table": "aws_logs_raw",
    "write_mode": "append|overwrite|merge",
    "partitioning": {
      "enabled": true,
      "columns": ["timestamp"]
    },
    "optimization": {
      "z_ordering_enabled": false,
      "z_ordering_columns": []
    }
  },
  
  "schedule": {
    "mode": "scheduled|continuous",
    "frequency": "daily|hourly|weekly|custom",
    "time": "02:00",
    "timezone": "UTC",
    "cron_expression": "0 2 * * *",
    "backfill": {
      "enabled": false,
      "start_date": null
    }
  },
  
  "quality": {
    "row_count_threshold": 1000,
    "alerts_enabled": true,
    "alert_recipients": ["email@example.com"]
  },
  
  "metadata": {
    "checkpoint_location": "s3://checkpoints/uuid/",
    "last_run_id": "uuid",
    "last_run_time": "timestamp",
    "next_run_time": "timestamp",
    "schema_version": 2,
    "estimated_monthly_cost": 60.0
  }
}
```

**Run History Model**
```json
{
  "id": "uuid",
  "ingestion_id": "uuid",
  "started_at": "timestamp",
  "ended_at": "timestamp",
  "status": "success|failed|partial|running",
  "trigger": "scheduled|manual|retry",
  
  "metrics": {
    "files_processed": 127,
    "records_ingested": 45230,
    "bytes_read": 1234567890,
    "bytes_written": 987654321,
    "duration_seconds": 480,
    "error_count": 0
  },
  
  "errors": [
    {
      "file": "s3://bucket/bad-file.json",
      "error_type": "parse_error",
      "message": "Invalid JSON on line 42",
      "timestamp": "timestamp"
    }
  ],
  
  "spark_job_id": "application_1234567890_0001",
  "cluster_id": "cluster-uuid"
}
```

**Schema Version Model**
```json
{
  "id": "uuid",
  "ingestion_id": "uuid",
  "version": 2,
  "detected_at": "timestamp",
  "schema": {
    "columns": [
      {
        "name": "event_id",
        "type": "string",
        "nullable": false,
        "is_new": false
      },
      {
        "name": "severity_score",
        "type": "double",
        "nullable": true,
        "is_new": true
      }
    ]
  },
  "affected_files": [
    "s3://bucket/file1.json",
    "s3://bucket/file2.json"
  ],
  "resolution": {
    "type": "auto_merge|backfill|ignore|manual",
    "applied_at": "timestamp",
    "applied_by": "user_id"
  }
}
```

---

### 5.3 Implementation Details

**TR-10: File Discovery Optimization**
- Default: Directory listing mode (simpler, works everywhere)
- Advanced: File notification mode (S3 events, Azure Event Grid)
    - Only enable when file count > 1000 or frequency > hourly
    - Auto-setup notification infrastructure (SNS/SQS for S3, Event Grid for Azure)
    - Cache file events for 7 days
- Incremental listing:
    - Track last processed timestamp
    - Only list files modified after last run
    - Reduces S3 LIST API calls (cost savings)

**TR-11: Cost Estimation Algorithm**
```python
def estimate_monthly_cost(config):
    # Constants (adjust based on actual pricing)
    COST_PER_DBU_HOUR = 0.40  # Example rate
    CLUSTER_SIZE_DBU = 8      # Example: medium cluster
    STORAGE_COST_PER_GB = 0.023
    LIST_OPERATION_COST = 0.005 / 1000  # Per 1000 LIST calls
    
    # Estimates
    files_per_run = estimate_file_count(config.source.path)
    avg_file_size_mb = estimate_avg_file_size(config.source.path)
    processing_time_minutes = estimate_processing_time(
        files_per_run, 
        avg_file_size_mb, 
        config.format.type
    )
    
    # Calculate
    runs_per_month = get_runs_per_month(config.schedule)
    compute_cost = (
        runs_per_month * 
        (processing_time_minutes / 60) * 
        CLUSTER_SIZE_DBU * 
        COST_PER_DBU_HOUR
    )
    
    storage_cost = (
        files_per_run * 
        avg_file_size_mb / 1024 * 
        runs_per_month * 
        STORAGE_COST_PER_GB
    )
    
    discovery_cost = (
        files_per_run * 
        runs_per_month * 
        LIST_OPERATION_COST
    )
    
    return {
        "compute": compute_cost,
        "storage": storage_cost,
        "discovery": discovery_cost,
        "total": compute_cost + storage_cost + discovery_cost
    }
```

**TR-12: Schema Evolution Detection**
```python
def detect_schema_changes(ingestion_id, new_files):
    current_schema = get_current_schema(ingestion_id)
    sample_files = new_files[:10]  # Sample first 10
    
    inferred_schema = infer_schema_from_files(sample_files)
    
    changes = compare_schemas(current_schema, inferred_schema)
    
    if changes:
        # Pause ingestion
        pause_ingestion(ingestion_id)
        
        # Create schema version record
        create_schema_version(
            ingestion_id=ingestion_id,
            new_schema=inferred_schema,
            changes=changes,
            affected_files=new_files
        )
        
        # Send notification
        notify_schema_change(ingestion_id, changes)
        
    return changes
```

**TR-13: Exactly-Once Semantics**
- Use Spark's checkpoint mechanism for state management
- Store processed file list in checkpoint
- On restart, resume from checkpoint
- For merge mode:
    - Use Apache Iceberg merge (upsert)
    - Require primary key configuration
    - Deduplicate before writing

**TR-14: Error Handling & Retries**
- Transient errors (network, throttling):
    - Retry up to 3 times with exponential backoff
    - 1 min, 5 min, 15 min intervals
- Schema errors:
    - Pause ingestion immediately
    - Create alert for user review
    - No automatic retry
- File format errors:
    - Log error with file path
    - Continue processing other files
    - Capture bad data in `_rescued_data` column (Spark feature)
- Cluster failures:
    - Automatic retry by scheduler
    - Resume from checkpoint

**TR-15: Security**
- Encrypt credentials at rest (AES-256)
- Use IAM roles/service accounts where possible (preferred over access keys)
- Access control:
    - Users can only see/edit their own ingestions
    - Admins can see all ingestions
- Audit logging:
    - Log all configuration changes
    - Log all manual runs
    - Log schema resolution decisions
- Network security:
    - Support VPC endpoints for private connectivity
    - Support private link for Azure/GCS

---

## 6. UI/UX Requirements

### 6.1 Design System
- Use existing IOMETE design system (colors, typography, components)
- Follow Material Design or similar modern UI patterns
- Responsive design (desktop primary, tablet secondary)
- Accessibility: WCAG 2.1 Level AA compliance

### 6.2 Key UI Screens

**Screen 1: Ingestion Wizard**
- Reference: `ingestion-wizard-complete.html`
- Multi-step form (4 steps)
- Progress indicator at top
- Validation on each step before proceeding
- "Save as Draft" available at any step
- Navigation: Back/Next buttons + clickable progress circles

**Screen 2: Dashboard / List View**
- Reference: `ingestion-dashboard.html`
- Card-based layout for each ingestion
- Summary stats at top
- Quick actions on each card (Run, Edit, Pause)
- Visual status indicators (green=healthy, yellow=warning, red=error)
- Expandable cards to see detailed metrics

**Screen 3: Schema Evolution Handler**
- Reference: `schema-evolution.html`
- Alert-style layout (orange warning banner)
- Side-by-side schema comparison
- Option cards for resolution (radio button selection)
- Preview of changes before applying
- "Apply Solution" primary action button

**Screen 4: Preview/Test Mode**
- Reference: `preview-test.html`
- Tabbed interface (Overview, Schema, Sample Data, Recommendations)
- Status indicators for validation steps
- Sample data preview (formatted, syntax-highlighted)
- Estimated cost box
- "Looks Good - Create Ingestion" primary action

### 6.3 UI Components to Build

**Reusable Components:**
1. `SourceSelector` - Card grid for selecting source type
2. `ConnectionTester` - Connection test with status indicator
3. `SchemaPreview` - Table showing inferred schema
4. `CostEstimator` - Cost breakdown card
5. `MetricsChart` - Sparkline chart for trends
6. `StatusBadge` - Color-coded status indicator
7. `AlertBox` - Info/warning/error alert boxes
8. `IngestionCard` - Card component for list view
9. `SchemaComparison` - Side-by-side schema diff view
10. `ConfigSummary` - Read-only config display

### 6.4 User Flows

**Flow 1: First-Time User Creating Ingestion**
```
1. User clicks "New Ingestion" button on dashboard
2. Wizard opens at Step 1 (Source)
3. User selects AWS S3
4. User enters bucket path and credentials
5. User clicks "Test Connection" → success message shown
6. User clicks "Next"
7. Step 2 (Format): User selects JSON format
8. System auto-detects schema → shows preview
9. User clicks "Next"
10. Step 3 (Destination): User enters table name
11. User clicks "Next"
12. Step 4 (Schedule): User selects Daily at 02:00
13. System shows cost estimate: $60/month
14. User clicks "Test & Preview"
15. Preview screen shows sample data and schema
16. User clicks "Looks Good - Create Ingestion"
17. Success message: "Ingestion created! First run scheduled for tomorrow at 02:00"
18. Redirects to dashboard showing new ingestion card
```

**Flow 2: Handling Schema Evolution**
```
1. Ingestion runs on schedule
2. System detects new column in source files
3. System pauses ingestion automatically
4. User receives email notification
5. User logs in and sees warning badge on dashboard
6. User clicks on ingestion card
7. Alert box shows "Schema Evolution Detected - Review Now"
8. User clicks alert → opens schema evolution screen
9. User sees before/after comparison (new column highlighted)
10. User reviews 4 resolution options
11. User selects "Auto-merge" (recommended option)
12. User clicks "Apply Solution"
13. System applies schema change (adds column to table)
14. System resumes ingestion automatically
15. User returns to dashboard → warning badge gone
```

**Flow 3: Monitoring Active Ingestion**
```
1. User opens dashboard
2. User sees ingestion card with metrics:
   - Status: Healthy (green)
   - Last run: 2 hours ago
   - Files processed: 127 (last 7 days)
   - Avg processing time: 8 min
3. User clicks card to expand details
4. User sees sparkline chart of daily volumes
5. User sees "Next run: In 22 hours"
6. User clicks "Run Now" to trigger manual run
7. Status changes to "Running..." with spinner
8. After 8 minutes, status updates to "Completed"
9. Metrics refresh showing new run
```

---

## 7. Non-Functional Requirements

### 7.1 Performance
- **NFR-1:** Configuration save operation < 2 seconds
- **NFR-2:** Dashboard load time < 3 seconds (100 ingestions)
- **NFR-3:** Preview/test mode < 10 seconds for 10 sample files
- **NFR-4:** Schema inference < 5 seconds for sample files
- **NFR-5:** Job scheduling latency < 1 minute (job starts within 1 min of scheduled time)

### 7.2 Scalability
- **NFR-10:** Support 1,000 active ingestion pipelines per tenant
- **NFR-11:** Support ingesting 10,000 files per run
- **NFR-12:** Support file sizes up to 5 GB per file
- **NFR-13:** Support 100 concurrent ingestion jobs across platform
- **NFR-14:** Metadata database handles 1M+ run history records

### 7.3 Reliability
- **NFR-20:** Ingestion success rate > 99%
- **NFR-21:** Exactly-once processing guarantees (no duplicates)
- **NFR-22:** Automatic recovery from transient failures (3 retries)
- **NFR-23:** Checkpoint corruption detection and alerting
- **NFR-24:** Service uptime > 99.5% (downtime < 3.6 hours/month)

### 7.4 Availability
- **NFR-30:** API availability > 99.9%
- **NFR-31:** Dashboard accessible 24/7
- **NFR-32:** Scheduled jobs execute on time 99.5% of the time
- **NFR-33:** Alert delivery within 5 minutes of event

### 7.5 Security
- **NFR-40:** All credentials encrypted at rest (AES-256)
- **NFR-41:** All API calls authenticated and authorized
- **NFR-42:** All configuration changes audit logged
- **NFR-43:** Support for VPC private connectivity
- **NFR-44:** Compliance with SOC 2 requirements

### 7.6 Usability
- **NFR-50:** Average time to configure ingestion < 10 minutes
- **NFR-51:** User can complete wizard without documentation
- **NFR-52:** Error messages actionable (what to do, not just what failed)
- **NFR-53:** Cost estimation accuracy within ±20% of actual
- **NFR-54:** Zero-code setup for 90% of use cases

---

## 8. Implementation Phases

### Phase 1: MVP (Months 1-3)
**Goal:** Basic scheduled batch ingestion for AWS S3

**Deliverables:**
- ✅ Ingestion wizard (4 steps) for S3 only
- ✅ Support JSON, CSV, Parquet formats
- ✅ Auto schema inference
- ✅ Scheduled mode (daily, hourly)
- ✅ Basic monitoring dashboard
- ✅ Manual "Run Now" trigger
- ✅ Cost estimation (basic algorithm)
- ✅ Preview/test mode

**Success Criteria:**
- 10 beta customers using feature
- 50+ active ingestion pipelines created
- Average setup time < 15 minutes
- Success rate > 95%

### Phase 2: Production Ready (Months 4-6)
**Goal:** Add reliability, monitoring, schema evolution

**Deliverables:**
- ✅ Azure Blob and GCS support
- ✅ Schema evolution detection and handling UI
- ✅ Enhanced monitoring (7-day metrics, charts)
- ✅ Run history view
- ✅ Email alerts/notifications
- ✅ Edit configuration (schedule, quality rules)
- ✅ Pause/resume operations
- ✅ Improved cost estimation (file-based analysis)
- ✅ File notification mode for high-volume use cases

**Success Criteria:**
- 50+ customers using feature
- 500+ active pipelines
- Schema evolution incidents handled without support tickets
- Average setup time < 10 minutes
- Success rate > 98%

### Phase 3: Advanced Features (Months 7-12)
**Goal:** Power user features, optimizations, integrations

**Deliverables:**
- ✅ Continuous streaming mode (always-on processing)
- ✅ Advanced format options (Avro, ORC, XML)
- ✅ Merge/upsert mode for updates
- ✅ Transformation during ingestion (light SQL transforms)
- ✅ Data quality rules (row count, null checks, etc.)
- ✅ Template library (common patterns)
- ✅ API for programmatic configuration
- ✅ Advanced monitoring (lineage, data quality metrics)
- ✅ Integration with dbt, Airflow (external orchestration)
- ✅ Cost optimization recommendations
- ✅ Multi-region support

**Success Criteria:**
- 100+ customers using feature
- 2,000+ active pipelines
- 30% of customers use advanced features
- Customer NPS for feature > 50
- Feature drives 20% increase in compute usage

---

## 9. Testing Requirements

### 9.1 Unit Testing
- All API endpoints tested (request/response validation)
- All service methods tested (configuration validation, job generation)
- Schema inference tested with various file formats
- Cost estimation algorithm tested with edge cases
- Error handling tested (missing files, bad credentials, etc.)
- Target: 80% code coverage

### 9.2 Integration Testing
- End-to-end wizard flow (source → format → destination → schedule)
- Job generation and execution (generated Spark code runs successfully)
- Checkpoint management (create, resume, recover)
- Schema evolution detection and resolution
- Scheduler integration (jobs trigger on time)
- Alert/notification delivery
- Target: All critical paths covered

### 9.3 Performance Testing
- Load test: 1,000 concurrent API requests
- Ingestion test: 10,000 files, 100 GB total
- Dashboard load test: 100 active ingestions displayed
- Database query performance (run history, metrics aggregation)
- Target: Meet NFR performance requirements

### 9.4 User Acceptance Testing (UAT)
- Beta program with 10-20 customers
- Test scenarios:
    1. New user creates first ingestion (S3 JSON daily)
    2. User handles schema evolution (new column detected)
    3. User monitors ingestion health over 1 week
    4. User edits schedule (daily → hourly)
    5. User troubleshoots failed run
- Collect feedback on:
    - Ease of use (wizard clarity)
    - Time to value (setup speed)
    - Monitoring usefulness
    - Cost estimation accuracy
    - Error message clarity

### 9.5 Security Testing
- Penetration testing of API endpoints
- Credential encryption validation
- Access control verification (users can't access others' configs)
- SQL injection prevention
- XSS prevention in UI

---

## 10. Documentation Requirements

### 10.1 User Documentation
- **Getting Started Guide**
    - "Create Your First Ingestion" (5-minute quickstart)
    - Step-by-step wizard walkthrough with screenshots
- **User Manual**
    - Configuration options explained
    - Schedule settings guide
    - Schema evolution handling guide
    - Monitoring dashboard guide
    - Troubleshooting common issues
- **Video Tutorials**
    - "Ingest Data from S3 in 5 Minutes"
    - "Handle Schema Changes"
    - "Monitor Your Ingestion Pipelines"

### 10.2 Developer Documentation
- **API Reference**
    - Full OpenAPI/Swagger spec
    - Authentication guide
    - Example requests/responses
    - Error codes reference
- **Architecture Guide**
    - System architecture diagram
    - Component descriptions
    - Data flow diagrams
    - Database schema
- **Operator Guide**
    - Deployment instructions
    - Configuration options
    - Monitoring and alerting setup
    - Backup and recovery procedures
    - Scaling guidelines

### 10.3 FAQ
- How is this different from writing Spark code manually?
- What happens if my schema changes?
- How accurate is the cost estimation?
- Can I transform data during ingestion?
- What file formats are supported?
- How do I troubleshoot a failed ingestion?
- Can I use IAM roles instead of access keys?
- How do I migrate from manual jobs to ingestion?

---

## 11. Open Questions & Decisions Needed

### 11.1 Open Questions
1. **Q:** Should we support database sources (MySQL, PostgreSQL) in Phase 1?
    - **Recommendation:** No, focus on file-based ingestion first. Add database sources in Phase 3.

2. **Q:** Should we allow custom Spark transformations in the wizard?
    - **Recommendation:** Phase 1: No. Phase 3: Add light SQL transforms (filter, select, rename).

3. **Q:** How should we handle very large files (>10 GB)?
    - **Recommendation:** Phase 1: Document 5 GB limit. Phase 2: Add file splitting/parallel processing.

4. **Q:** Should we support Kafka/streaming sources in Phase 1?
    - **Recommendation:** No. Phase 3: Add Kafka connector as separate source type.

5. **Q:** What's the default cluster size for ingestion jobs?
    - **Recommendation:** Medium (8 DBU) as default. Auto-recommend based on file size analysis.

### 11.2 Decisions Needed from Leadership
1. **Priority:** Is this feature high priority for Q1 2026? (Assuming yes based on context)
2. **Team Size:** How many engineers can be allocated? (Recommend: 2-3 FTE)
3. **Beta Program:** Can we recruit 10-20 beta customers? (Recommend: Start recruiting in Month 2)
4. **Pricing:** Should ingestion feature be included in all tiers or premium only?
    - **Recommendation:** Free tier: 3 ingestions. Pro tier: 25 ingestions. Enterprise: Unlimited.

### 11.3 Risks & Mitigations

**Risk 1: Checkpoint Corruption**
- **Impact:** High - users lose state, must reprocess all files
- **Mitigation:**
    - Implement checkpoint health monitoring
    - Automatic backup of checkpoints (daily)
    - Recovery tools for corrupted checkpoints
    - Clear docs on checkpoint management

**Risk 2: Cost Estimation Inaccuracy**
- **Impact:** Medium - users surprised by actual costs
- **Mitigation:**
    - Start with conservative estimates (overestimate slightly)
    - Track actual vs. estimated costs
    - Improve algorithm based on real data
    - Show estimation accuracy in UI ("Estimates typically within ±20%")

**Risk 3: Schema Evolution Edge Cases**
- **Impact:** Medium - complex schema changes not handled automatically
- **Mitigation:**
    - Start with simple cases (new columns, removed columns)
    - Document unsupported cases (type changes, nested schema changes)
    - Provide manual override options
    - Collect edge cases from beta for Phase 2

**Risk 4: Scale Issues (1000+ Files per Run)**
- **Impact:** High - performance degrades, jobs fail
- **Mitigation:**
    - Load test with realistic file counts
    - Implement batch processing (process in chunks of 1000)
    - File notification mode for high-volume cases
    - Document limits and recommendations

**Risk 5: Third-Party Dependencies (Spark Versions)**
- **Impact:** Medium - breaking changes in Spark upgrades
- **Mitigation:**
    - Pin to specific Spark version (e.g., 3.5.x)
    - Test with each Spark minor version
    - Abstract Spark-specific code behind interfaces
    - Maintain compatibility layer

---

## 12. Success Metrics & KPIs

### 12.1 Adoption Metrics
- Number of active ingestion pipelines
- Number of customers using ingestion feature
- Percentage of new customers using ingestion in first 30 days
- Growth rate (MoM increase in pipelines)

**Targets:**
- Month 3: 50 pipelines, 10 customers
- Month 6: 500 pipelines, 50 customers
- Month 12: 2,000 pipelines, 100 customers

### 12.2 Usage Metrics
- Total files processed per month
- Total data ingested per month (GB)
- Average ingestion frequency (daily, hourly, etc.)
- Percentage using scheduled vs. continuous mode
- Average configuration time (minutes)

**Targets:**
- Average config time: < 10 minutes
- 80% use scheduled mode (validates our focus)
- 1M+ files processed per month by Month 12

### 12.3 Quality Metrics
- Ingestion success rate (percentage of runs that succeed)
- Schema evolution incidents (number per month)
- Schema evolution resolution rate (percentage resolved without support)
- Mean time to resolution (MTTR) for failures
- Customer-reported bugs (count per month)

**Targets:**
- Success rate: > 98%
- Schema evolution self-resolution: > 90%
- MTTR: < 30 minutes
- Customer bugs: < 10 per month

### 12.4 Business Impact Metrics
- Compute hours driven by ingestion feature
- Revenue attributed to ingestion (if priced separately)
- Customer retention rate (do customers with ingestion churn less?)
- Support ticket reduction (fewer ingestion-related tickets)
- Time-to-value improvement (days to first production pipeline)

**Targets:**
- 20% increase in compute usage from ingestion
- 50% reduction in ingestion-related support tickets
- Time-to-value: < 1 week (from signup to production pipeline)

### 12.5 Customer Satisfaction Metrics
- Net Promoter Score (NPS) for ingestion feature
- Customer satisfaction score (CSAT) post-setup
- Feature usage survey responses
- Customer testimonials/case studies

**Targets:**
- NPS: > 50
- CSAT: > 4.5/5
- 5+ customer testimonials by Month 12

---

## 13. Appendix

### A. Glossary
- **Ingestion:** The process of loading data from external sources into IOMETE tables
- **Auto Loader:** Databricks' feature for incremental file ingestion (competitor reference)
- **Checkpoint:** Spark's state management mechanism for tracking processed files
- **Schema Evolution:** When the structure of source data changes (new columns, type changes)
- **Backfill:** Processing historical/existing files in addition to new files
- **DBU:** Databricks Unit, a unit of compute measurement (used for cost estimation)
- **Apache Iceberg:** Open-source table format that provides ACID transactions and schema evolution

### B. References
- Databricks Auto Loader Documentation: https://docs.databricks.com/ingestion/auto-loader/
- Snowflake Snowpipe Documentation: https://docs.snowflake.com/en/user-guide/data-load-snowpipe
- Apache Spark Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Apache Iceberg Documentation: https://iceberg.apache.org/

### C. Mockups Reference
UI mockups available at:
- Ingestion Wizard (Complete): `ingestion-wizard-complete.html`
- Monitoring Dashboard: `ingestion-dashboard.html`
- Schema Evolution Handler: `schema-evolution.html`
- Preview/Test Mode: `preview-test.html`

### D. Competitive Analysis Summary

| Feature | Databricks Auto Loader | Snowflake Snowpipe | IOMETE Ingestion |
|---------|----------------------|-------------------|------------------|
| Setup | Code required | Minimal config | Visual wizard |
| Mode | Streaming or batch | Always-on (continuous) | Scheduled or continuous |
| Cost visibility | Post-facto | Post-facto | Upfront estimation |
| Schema evolution | Automatic (with config) | Limited | Guided resolution UI |
| Monitoring | Technical (Spark metrics) | Basic | Business-friendly |
| Pricing | Compute (DBU) | Per-GB + compute | Compute + storage |
| Cloud support | S3, Azure, GCS | S3, Azure, GCS | S3, Azure, GCS |
| Transformation | Full Spark power | Basic SQL | Light SQL (Phase 3) |
| Best for | Complex pipelines, tech teams | Simple ingestion, always-on | Scheduled batch, mixed teams |

### E. Change Log

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | Nov 2, 2025 | Initial PRD | Product Team |

---

## 14. Approval & Sign-off

**Prepared by:** Product Team  
**Review required from:**
- [ ] Engineering Lead (technical feasibility)
- [ ] Design Lead (UI/UX review)
- [ ] Security Lead (security requirements)
- [ ] Product Manager (business requirements)
- [ ] CTO (strategic alignment)

**Approved by:** _________________________  Date: ___________

---

**END OF PRD**