# Integration Tests

## Requirements

Integration tests **MUST** use real Spark Connect (no mocks, no local mode).

**Prerequisites:**
```bash
# Start Spark Connect service
docker-compose -f docker-compose.test.yml up -d

# Wait 1-2 minutes for Spark to fully start
# Verify Spark UI is accessible at http://localhost:4040
```

## Configuration

- **Spark Connect**: `sc://localhost:15002` (from Docker)
- **MinIO**: `http://localhost:9000` (for Iceberg warehouse)
- **Catalog**: `test_catalog.test_db` (same as e2e tests)
- **Database**: SQLite (local, no PostgreSQL needed)
- **Service Check**: Tests will fail-fast if Spark Connect or MinIO are not available

## Running Tests

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run specific test
pytest tests/integration/test_schema_version_tracking.py -v
```

## Test Structure

Integration tests verify business logic with real Spark operations:
- Schema evolution detection and tracking
- Iceberg table operations
- File processing logic
- Repository and service layer integration

Unlike e2e tests, integration tests:
- Use SQLite instead of PostgreSQL
- Don't require MinIO or Prefect
- Focus on component integration, not full system workflows