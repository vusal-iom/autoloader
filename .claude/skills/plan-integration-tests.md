# Integration Test Planning Strategy

You are creating a comprehensive integration test strategy for component interactions. Follow these steps:

## Step 1: Identify Integration Points
Ask: "Which feature, flow, or component integration would you like to test?"

## Step 2: Analyze the Integration

1. Map all components involved
2. Identify data flow between components
3. List external dependencies (DB, APIs, Spark, etc.)
4. Note potential failure points

## Step 3: Create Strategy Document

Generate a markdown document with this structure:

```markdown
# Integration Test Strategy: [Feature/Flow Name]

## Integration Overview
- **Components Involved**: [List all components]
- **Data Flow**: [Source] → [Processing] → [Destination]
- **External Systems**: [Databases, Spark, Cloud Storage, etc.]
- **Test File Location**: tests/integration/[appropriate_path]

## Test Environment Requirements

### Infrastructure
- [ ] PostgreSQL database (test instance)
- [ ] Spark Connect server (port 15002)
- [ ] Cloud storage access (S3/Azure/GCS)
- [ ] Test data sets prepared

### Configuration
- Database URL: [test database]
- Spark settings: [test cluster config]
- Cloud credentials: [test credentials]

## Integration Test Scenarios

### Scenario 1: [Happy Path - Complete Flow]
- **Purpose**: Validate end-to-end flow works correctly
- **Initial State**: [Database state, file locations]
- **Test Steps**:
  1. [First action]
  2. [Second action]
  3. [Continue...]
- **Verification Points**:
  - [ ] Data correctly written to database
  - [ ] Spark job completed successfully
  - [ ] Files processed and marked
  - [ ] Correct response returned
- **Cleanup**: [What needs to be reset]

### Scenario 2: [Database Connection Failure]
- **Purpose**: Verify graceful handling of database issues
- **Failure Injection**: Disconnect database mid-operation
- **Expected Behavior**:
  - Proper error handling
  - Transaction rollback
  - Meaningful error message
- **Recovery Steps**: [How system should recover]

### Scenario 3: [Spark Job Failure]
- **Purpose**: Handle Spark execution failures
- **Failure Injection**: Invalid Spark configuration
- **Expected Behavior**:
  - Job marked as failed
  - Error details captured
  - Retry mechanism triggered
- **Verification**: Error state properly recorded

### Scenario 4: [Cloud Storage Access Issues]
- **Purpose**: Handle storage permission/access problems
- **Test Conditions**: Invalid credentials, network timeout
- **Expected Behavior**:
  - Appropriate error messages
  - No partial data corruption
  - Graceful degradation

### Scenario 5: [Concurrent Operations]
- **Purpose**: Verify thread safety and race conditions
- **Setup**: Multiple simultaneous requests
- **Verification**:
  - No deadlocks
  - Data consistency maintained
  - Proper isolation between operations

## Data Requirements

### Test Data Sets
- **Small Dataset**: 10 files, 1MB each (quick tests)
- **Medium Dataset**: 100 files, 10MB each (normal tests)
- **Large Dataset**: 1000 files, 100MB each (performance tests)

### Database Fixtures
```sql
-- Initial test data
INSERT INTO ingestions (...) VALUES (...);
INSERT INTO runs (...) VALUES (...);
```

## Performance Criteria
- API response time: < 2 seconds
- Spark job startup: < 10 seconds
- File processing rate: > 100 files/minute
- Database query time: < 100ms

## Test Prioritization

### P0 - Critical
- [ ] Complete happy path flow
- [ ] Database failure handling
- [ ] Spark job execution

### P1 - Important
- [ ] Cloud storage failures
- [ ] Concurrent operations
- [ ] Schema evolution scenarios

### P2 - Nice to Have
- [ ] Performance under load
- [ ] Network timeout scenarios

## Special Considerations
- Use test containers for database isolation
- Mock cloud storage for faster tests where possible
- Ensure Spark Connect cleanup after each test
- Consider test data costs for cloud storage

## Validation Criteria
- All P0 scenarios must pass
- No resource leaks after tests
- Cleanup completes successfully
- Tests are idempotent (can run multiple times)
```

## Step 4: Save and Review
Save to: `docs/test-strategies/integration/[feature]_integration_strategy.md`

Ask: "Would you like to review specific scenarios or proceed with test implementation?"

## Important Notes
- Integration tests are slower - optimize setup/teardown
- Use shared fixtures where possible
- Real dependencies where feasible, mocks where necessary
- Focus on component boundaries and data flow
- Test both success and failure paths