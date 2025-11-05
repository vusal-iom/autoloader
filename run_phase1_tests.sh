#!/bin/bash
#
# Phase 1 Batch Processing - Test Runner
#
# This script sets up and runs Phase 1 end-to-end tests
#

set -e  # Exit on any error

echo "======================================"
echo "Phase 1 Batch Processing Test Runner"
echo "======================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if docker-compose is running
echo "📋 Step 1: Checking prerequisites..."

if ! docker ps | grep -q "autoloader-test-postgres"; then
    echo -e "${YELLOW}⚠️  PostgreSQL container not running${NC}"
    echo "Starting docker-compose..."
    docker-compose -f docker-compose.test.yml up -d postgres
    echo "Waiting for PostgreSQL to be ready..."
    sleep 5
fi

if ! docker ps | grep -q "autoloader-test-minio"; then
    echo -e "${YELLOW}⚠️  MinIO container not running${NC}"
    echo "Starting docker-compose..."
    docker-compose -f docker-compose.test.yml up -d minio
    echo "Waiting for MinIO to be ready..."
    sleep 5
fi

if ! docker ps | grep -q "autoloader-test-spark-connect"; then
    echo -e "${YELLOW}⚠️  Spark Connect container not running${NC}"
    echo "Starting docker-compose..."
    docker-compose -f docker-compose.test.yml up -d spark-connect
    echo "Waiting for Spark Connect to be ready (this may take 60-90 seconds)..."
    sleep 60
fi

echo -e "${GREEN}✓${NC} All containers running"
echo ""

# Check connectivity
echo "📋 Step 2: Checking connectivity..."

# PostgreSQL
if pg_isready -h localhost -p 5432 -U test_user > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} PostgreSQL is ready"
else
    echo -e "${RED}✗${NC} PostgreSQL not accessible"
    echo "Waiting 10 more seconds..."
    sleep 10
    if ! pg_isready -h localhost -p 5432 -U test_user > /dev/null 2>&1; then
        echo -e "${RED}Error: PostgreSQL still not accessible${NC}"
        echo "Check logs: docker logs autoloader-test-postgres"
        exit 1
    fi
fi

# MinIO
if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} MinIO is ready"
else
    echo -e "${RED}✗${NC} MinIO not accessible"
    exit 1
fi

# Spark Connect (check if port is open)
if nc -z localhost 15002 > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Spark Connect port is open"
else
    echo -e "${YELLOW}⚠️${NC} Spark Connect port not responding (may still be starting)"
fi

echo ""

# Apply migrations
echo "📋 Step 3: Applying database migrations..."
export DATABASE_URL="postgresql://test_user:test_password@localhost:5432/autoloader_test"

if alembic upgrade head; then
    echo -e "${GREEN}✓${NC} Migrations applied"
else
    echo -e "${RED}✗${NC} Migration failed"
    exit 1
fi

echo ""

# Verify table exists
echo "📋 Step 4: Verifying database schema..."
if psql -h localhost -U test_user -d autoloader_test -c "\d processed_files" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} processed_files table exists"
else
    echo -e "${RED}✗${NC} processed_files table not found"
    exit 1
fi

echo ""

# Load environment variables
if [ -f .env.test ]; then
    echo "📋 Step 5: Loading test environment..."
    export $(cat .env.test | grep -v '^#' | xargs)
    echo -e "${GREEN}✓${NC} Environment loaded"
else
    echo -e "${YELLOW}⚠️  .env.test not found, using defaults${NC}"
fi

echo ""

# Run tests
echo "📋 Step 6: Running Phase 1 tests..."
echo "========================================"
echo ""

# Run with verbose output and show print statements
pytest tests/e2e/test_phase1_batch_processing_e2e.py -v -s --tb=short

TEST_EXIT_CODE=$?

echo ""
echo "========================================"

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ All Phase 1 tests PASSED${NC}"
    echo ""
    echo "🎉 Phase 1 batch processing is working correctly!"
    echo ""
    echo "What was tested:"
    echo "  ✓ File discovery from MinIO"
    echo "  ✓ ProcessedFile state tracking in PostgreSQL"
    echo "  ✓ Batch processing with Spark"
    echo "  ✓ Data written to Iceberg tables"
    echo "  ✓ Idempotency (re-running skips processed files)"
    echo "  ✓ Error isolation (one bad file doesn't block others)"
else
    echo -e "${RED}❌ Phase 1 tests FAILED${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  - Check PostgreSQL logs: docker logs autoloader-test-postgres"
    echo "  - Check MinIO logs: docker logs autoloader-test-minio"
    echo "  - Check Spark logs: docker logs autoloader-test-spark-connect"
    echo "  - Verify migrations: alembic current"
    echo "  - Check test output above for specific errors"
fi

echo ""

exit $TEST_EXIT_CODE
