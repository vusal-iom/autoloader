# E2E Test Case 01: Basic S3 JSON Ingestion

## Overview

**Test ID:** E2E-01
**Priority:** P0 (Critical)
**Status:** Planned
**Category:** Happy Path / Core Functionality

## Description

End-to-end test for the most fundamental use case: creating and manually running a basic S3 JSON ingestion into an Iceberg table. This test validates the complete data pipeline from configuration to data landing in the destination table.

## Objectives

1. Verify the core ingestion flow works end-to-end
2. Validate API endpoints for ingestion CRUD and manual triggering
3. Confirm Spark Connect integration functions correctly
4. Ensure data is correctly written to Iceberg tables
5. Validate schema inference and metrics collection

## Prerequisites

### Infrastructure
- Test Spark cluster with Spark Connect enabled
- MinIO or LocalStack for S3 emulation (or test S3 bucket)
- PostgreSQL test database (or SQLite for simplicity)
- Test Iceberg catalog configured

### Test Data
- Sample JSON files with consistent schema
- File size: 1-10 MB per file
- Record count: 1,000-10,000 records
- Sample schema:
  ```json
  {
    "id": "string",
    "timestamp": "timestamp",
    "user_id": "string",
    "event_type": "string",
    "properties": {
      "key1": "value1",
      "key2": "value2"
    }
  }
  ```

## Test Setup

### 1. Environment Setup

```python
@pytest.fixture(scope="module")
async def test_environment():
    """Setup test environment with MinIO, Spark, and database"""
    # Start MinIO container
    minio_container = MinIOContainer()
    minio_container.start()

    # Create test bucket and upload sample files
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_container.get_connection_url(),
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    s3_client.create_bucket(Bucket='test-ingestion-bucket')

    # Upload test JSON files
    upload_test_files(s3_client, 'test-ingestion-bucket', 'data/')

    # Setup test database
    test_db = setup_test_database()

    # Verify Spark Connect is available
    spark_url = os.getenv('TEST_SPARK_CONNECT_URL')
    assert spark_url, "TEST_SPARK_CONNECT_URL must be set"

    yield {
        'minio': minio_container,
        's3_client': s3_client,
        'bucket': 'test-ingestion-bucket',
        'spark_url': spark_url,
        'db': test_db
    }

    # Teardown
    minio_container.stop()
    cleanup_test_database(test_db)
```

### 2. Test Data Generation

```python
def upload_test_files(s3_client, bucket: str, prefix: str):
    """Upload sample JSON files to test bucket"""
    for i in range(3):
        data = generate_sample_data(num_records=1000, batch_id=i)
        file_content = '\n'.join([json.dumps(record) for record in data])
        s3_client.put_object(
            Bucket=bucket,
            Key=f'{prefix}batch_{i}.json',
            Body=file_content.encode('utf-8')
        )
```

## Test Steps

### Step 1: Create Ingestion Configuration

**API Request:**
```http
POST /api/v1/ingestions
Content-Type: application/json

{
  "tenant_id": "test-tenant-001",
  "name": "E2E Test S3 JSON Ingestion",
  "cluster_id": "test-cluster-001",
  "spark_connect_url": "sc://localhost:15002",
  "spark_connect_token": "test-token",
  "source_type": "S3",
  "source_path": "s3a://test-ingestion-bucket/data/",
  "source_file_pattern": "*.json",
  "source_credentials": {
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "endpoint_url": "http://localhost:9000"
  },
  "format_type": "JSON",
  "format_options": {
    "multiline": false
  },
  "schema_handling": "INFER",
  "destination_catalog": "test_catalog",
  "destination_database": "test_db",
  "destination_table": "e2e_test_table",
  "write_mode": "APPEND",
  "schedule_mode": "MANUAL"
}
```

**Expected Response:**
```json
{
  "id": "<generated-uuid>",
  "status": "DRAFT",
  "created_at": "2025-11-02T10:00:00Z",
  ...
}
```

**Assertions:**
- Response status: `201 CREATED`
- Response contains valid `id`
- `status` is `DRAFT`
- All provided fields are correctly stored

### Step 2: Activate Ingestion

**API Request:**
```http
PUT /api/v1/ingestions/{id}
Content-Type: application/json

{
  "status": "ACTIVE"
}
```

**Expected Response:**
- Response status: `200 OK`
- `status` is now `ACTIVE`

### Step 3: Trigger Manual Run

**API Request:**
```http
POST /api/v1/ingestions/{id}/run
```

**Expected Response:**
```json
{
  "run_id": "<generated-uuid>",
  "status": "RUNNING",
  "message": "Ingestion run started"
}
```

**Assertions:**
- Response status: `202 ACCEPTED`
- Response contains valid `run_id`
- Initial status is `RUNNING`

### Step 4: Poll Run Status

**API Request (repeated until completion):**
```http
GET /api/v1/ingestions/{id}/runs/{run_id}
```

**Expected Response (final state):**
```json
{
  "id": "<run-id>",
  "ingestion_id": "<ingestion-id>",
  "status": "COMPLETED",
  "started_at": "2025-11-02T10:00:00Z",
  "ended_at": "2025-11-02T10:02:30Z",
  "trigger": "MANUAL",
  "files_processed": 3,
  "records_ingested": 3000,
  "bytes_read": 1048576,
  "bytes_written": 950000,
  "duration_seconds": 150,
  "errors": [],
  "spark_job_id": "job-xyz"
}
```

**Assertions:**
- Final status is `COMPLETED`
- `files_processed` = 3
- `records_ingested` = 3000
- `errors` is empty
- `duration_seconds` > 0
- `ended_at` > `started_at`

### Step 5: Verify Data in Iceberg Table

**Direct Spark Query:**
```python
def verify_iceberg_table(spark_url: str, catalog: str, database: str, table: str):
    """Query the Iceberg table to verify data was written"""
    spark = SparkSession.builder \
        .remote(spark_url) \
        .getOrCreate()

    df = spark.table(f"{catalog}.{database}.{table}")

    # Verify record count
    count = df.count()
    assert count == 3000, f"Expected 3000 records, got {count}"

    # Verify schema
    schema = df.schema
    assert 'id' in schema.fieldNames()
    assert 'timestamp' in schema.fieldNames()
    assert 'user_id' in schema.fieldNames()
    assert 'event_type' in schema.fieldNames()

    # Verify sample data
    sample = df.limit(10).collect()
    assert len(sample) == 10

    # Verify no duplicates
    distinct_count = df.select('id').distinct().count()
    assert distinct_count == count, "Found duplicate records"
```

**Assertions:**
- Table exists and is readable
- Record count matches expected (3000)
- Schema matches inferred schema
- No duplicate records
- Data quality checks pass

### Step 6: Verify Metrics and History

**API Request:**
```http
GET /api/v1/ingestions/{id}/runs
```

**Expected Response:**
```json
{
  "runs": [
    {
      "id": "<run-id>",
      "status": "COMPLETED",
      "started_at": "2025-11-02T10:00:00Z",
      "ended_at": "2025-11-02T10:02:30Z",
      "files_processed": 3,
      "records_ingested": 3000
    }
  ],
  "total": 1
}
```

**Assertions:**
- Run history contains the completed run
- Metrics are correctly recorded

## Expected Results

### Success Criteria

✓ Ingestion configuration created successfully
✓ Manual run triggered and completed without errors
✓ All 3 JSON files processed
✓ 3000 records ingested into Iceberg table
✓ Schema correctly inferred from JSON files
✓ Run metrics accurately collected
✓ Run history shows completed run
✓ Data queryable in destination table
✓ No duplicate records
✓ No data corruption

### Performance Expectations

- Ingestion creation: < 500ms
- Manual run trigger: < 200ms (async)
- Data processing: < 5 minutes for 3 files (1000 records each)
- Status polling: < 100ms per request

## Error Scenarios to Validate

While this is a happy path test, it should gracefully handle:
- Transient Spark Connect connection issues (should retry)
- Temporary S3 unavailability (should retry)
- Database connection timeouts (should retry)

## Dependencies

### Code Components
- `app/api/v1/ingestions.py` - Create and run endpoints
- `app/api/v1/runs.py` - Run history endpoint
- `app/services/ingestion_service.py` - Create and run logic
- `app/spark/connect_client.py` - Spark Connect integration
- `app/spark/executor.py` - Ingestion executor (TODO)
- `app/repositories/ingestion_repository.py` - Data access
- `app/repositories/run_repository.py` - Run tracking

### External Services
- Spark cluster with Spark Connect on port 15002
- MinIO on port 9000 (or LocalStack)
- PostgreSQL test database

## Test Implementation

### File Location
`tests/e2e/test_basic_s3_json_ingestion.py`

### Test Class Structure
```python
@pytest.mark.e2e
@pytest.mark.asyncio
class TestBasicS3JsonIngestion:
    """E2E test for basic S3 JSON ingestion"""

    async def test_create_and_run_ingestion(self, test_environment, api_client):
        """Test complete flow: create → activate → run → verify"""
        pass

    async def test_verify_data_in_iceberg(self, test_environment, ingestion_run):
        """Verify data landed correctly in Iceberg table"""
        pass

    async def test_run_history_recorded(self, api_client, ingestion_id):
        """Verify run history is correctly recorded"""
        pass
```

## Notes

- This test should run in CI/CD pipeline
- Use testcontainers for reproducible environments
- Consider running against embedded Spark for faster feedback
- Mock Spark Connect initially if needed for faster iteration
- This test serves as foundation for other test cases

## Related Test Cases

- E2E-02: Incremental Load (verify only new files are processed)
- E2E-03: Schema Evolution Detection
- E2E-04: Failed Run and Retry
- E2E-05: CSV Format Ingestion
- E2E-06: Scheduled Ingestion
- E2E-07: Multi-cloud Sources (Azure, GCS)

## References

- [README.md](../../README.md) - Architecture overview
- [ingestion-prd-v1.md](../ingestion-prd-v1.md) - Requirements
- [ingestion-prd-using-connect.md](../ingestion-prd-using-connect.md) - Spark Connect architecture
