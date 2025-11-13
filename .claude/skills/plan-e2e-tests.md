# End-to-End Test Planning Strategy

You are creating a comprehensive E2E test strategy from the user's perspective. Focus on complete user journeys using SparkConnect.

## Step 1: Identify User Journey
Ask: "Which user journey or workflow would you like to test end-to-end?"

## Step 2: Map the Complete Journey

1. Define user persona and goals
2. Map all user interactions
3. Identify all system touchpoints
4. List success criteria

## Step 3: Create Strategy Document

Generate a markdown document with this structure:

```markdown
# E2E Test Strategy: [User Journey Name]

## Journey Overview
- **User Persona**: [Who is using the system]
- **User Goal**: [What they want to achieve]
- **Entry Point**: [How they start]
- **Success Criteria**: [How we know they succeeded]
- **Test File Location**: tests/e2e/[journey_name]_test.py

## Environment Setup

### Required Services
- [ ] FastAPI application running
- [ ] PostgreSQL database (with migrations)
- [ ] Spark Connect server (port 15002)
- [ ] Cloud storage configured (S3/Azure/GCS)
- [ ] All environment variables set

### Test User Setup
```python
test_user = {
    "email": "test@example.com",
    "tenant_id": "test_tenant_001",
    "role": "admin"
}
```

## User Journey Scenarios

### Journey 1: [Complete Ingestion Setup and Execution]

#### Pre-conditions
- User is authenticated
- Test data files in cloud storage
- Clean database state

#### User Steps
1. **Login/Authentication**
   - Action: POST /api/v1/auth/login
   - Input: Valid credentials
   - Expected: JWT token received

2. **Create New Ingestion**
   - Action: POST /api/v1/ingestions
   - Input: Source config, destination table, schedule
   - Expected: Ingestion created in DRAFT status

3. **Configure Source**
   - Action: Update source credentials
   - Validation: Test connection successful
   - Expected: Can list files from source

4. **Preview Data**
   - Action: GET /api/v1/ingestions/{id}/preview
   - Expected: Sample data displayed correctly
   - Validation: Schema detected properly

5. **Activate Ingestion**
   - Action: PATCH /api/v1/ingestions/{id}/activate
   - Expected: Status changes to ACTIVE
   - Validation: Spark job scheduled

6. **Monitor Execution**
   - Action: Wait for scheduled run
   - Expected: Run starts automatically
   - Validation: Files processed, data in table

7. **Verify Results**
   - Query destination table via Spark
   - Check processed files marked
   - Verify run history updated

#### Success Validation
- [ ] All files processed successfully
- [ ] Data correctly loaded to Iceberg table
- [ ] Run history shows success
- [ ] Metrics/costs calculated
- [ ] No errors in logs

### Journey 2: [Handle Schema Evolution]

#### User Steps
1. **Initial Setup** (reuse Journey 1 steps 1-6)

2. **Add Files with New Schema**
   - Upload files with additional columns
   - Wait for next scheduled run

3. **Review Schema Change Detection**
   - Check run logs for schema change
   - View proposed evolution

4. **Approve/Apply Evolution**
   - Approve schema change if needed
   - Verify table schema updated

5. **Verify Continued Processing**
   - New files processed with new schema
   - Old data still readable
   - No data loss

### Journey 3: [Error Recovery Flow]

#### User Steps
1. **Simulate Failure**
   - Corrupt source file
   - Invalid credentials
   - Network issues

2. **Review Error State**
   - Check ingestion status
   - Review error details in runs

3. **Fix Issue**
   - Update configuration
   - Fix source files
   - Reset credentials

4. **Retry Operation**
   - Trigger manual retry
   - Monitor recovery

5. **Verify Recovery**
   - Processing continues
   - Only affected files reprocessed
   - State is consistent

## Data Validation Queries

### Via Spark Connect
```python
# Connect to Spark
spark = SparkSession.builder \\
    .remote("sc://localhost:15002") \\
    .appName("E2E-Test") \\
    .getOrCreate()

# Verify data loaded
df = spark.table("catalog.schema.destination_table")
assert df.count() == expected_count
assert df.schema == expected_schema

# Check specific data
result = df.filter(df.column == "test_value").collect()
assert len(result) > 0
```

### Via Database
```python
# Check ingestion state
ingestion = db.query(Ingestion).filter_by(id=test_id).first()
assert ingestion.status == IngestionStatus.ACTIVE

# Verify run history
runs = db.query(Run).filter_by(ingestion_id=test_id).all()
assert all(run.status == RunStatus.COMPLETED for run in runs)
```

## Performance Requirements
- Complete journey: < 5 minutes
- API responses: < 2 seconds
- File processing: > 50 files/minute
- UI responsiveness: < 1 second

## Test Data Management

### Setup
```bash
# Upload test files to cloud storage
aws s3 cp test-data/ s3://test-bucket/input/ --recursive
```

### Cleanup
```bash
# Remove test data
aws s3 rm s3://test-bucket/input/ --recursive
# Drop test tables
spark.sql("DROP TABLE IF EXISTS test_destination_table")
```

## Success Metrics
- [ ] All journeys complete without manual intervention
- [ ] Error scenarios handled gracefully
- [ ] Performance meets requirements
- [ ] No resource leaks
- [ ] Clean state after test

## Test Prioritization

### P0 - Critical Paths
- [ ] Complete ingestion setup and execution
- [ ] Basic error handling
- [ ] Authentication and authorization

### P1 - Important Flows
- [ ] Schema evolution handling
- [ ] Monitoring and alerting
- [ ] Cost tracking

### P2 - Extended Scenarios
- [ ] Complex scheduling patterns
- [ ] Multiple concurrent ingestions
- [ ] Performance under load

## Special Considerations
- Use SparkConnect for all Spark operations (per CLAUDE.md)
- Tests must be idempotent
- Cleanup must be thorough
- Consider test execution costs (cloud storage, compute)
- Mock external services where appropriate

## Validation Checklist
- [ ] User can complete journey without errors
- [ ] All data ends up in correct location
- [ ] System state is consistent
- [ ] Audit trail is complete
- [ ] Performance is acceptable
```

## Step 4: Save and Review
Save to: `docs/test-strategies/e2e/[journey]_e2e_strategy.md`

Ask: "Would you like to explore specific user steps in more detail or proceed with implementation?"

## Important Notes
- E2E tests use real services (slower but more realistic)
- Always use SparkConnect for Spark operations
- Focus on user goals, not implementation details
- Test the "unhappy path" as thoroughly as happy path
- Consider test maintenance cost vs value