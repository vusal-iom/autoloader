.PHONY: help install test test-unit test-integration test-e2e test-all test-cov test-watch clean setup-test teardown-test

help:
	@echo "IOMETE Autoloader - Available Commands:"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install            - Install all dependencies"
	@echo "  make setup-test         - Start test infrastructure (MinIO, PostgreSQL, Spark)"
	@echo "  make test-spark-connect - Verify Spark Connect connectivity"
	@echo "  make test-spark-logs    - Show Spark Connect logs"
	@echo "  make teardown-test      - Stop and clean test infrastructure"
	@echo ""
	@echo "Testing:"
	@echo "  make test               - Run all tests (except slow/Spark-dependent)"
	@echo "  make test-unit          - Run unit tests only"
	@echo "  make test-integration   - Run integration tests only"
	@echo "  make test-e2e           - Run E2E tests (without Spark)"
	@echo "  make test-e2e-full      - Run E2E tests (with Spark)"
	@echo "  make test-all           - Run ALL tests including slow tests"
	@echo "  make test-cov           - Run tests with coverage report"
	@echo "  make test-watch         - Run tests in watch mode"
	@echo ""
	@echo "Development:"
	@echo "  make run                - Run the application"
	@echo "  make clean              - Clean up generated files"
	@echo "  make format             - Format code (black, isort)"
	@echo "  make lint               - Lint code (flake8, pylint)"
	@echo ""

install:
	pip install -r requirements.txt

setup-test:
	@echo "Starting test infrastructure..."
	docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@echo "Test infrastructure is ready!"
	@echo ""
	@echo "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "PostgreSQL: localhost:5432 (test_user/test_password)"
	@echo "Spark Connect: sc://localhost:15002"
	@echo "Spark UI: http://localhost:4040"
	@echo "Prefect UI: http://localhost:4200"
	@echo ""
	@echo "Run 'make test-spark-connect' to verify Spark connectivity"

test-spark-connect:
	@echo "Testing Spark Connect connectivity..."
	python scripts/test_spark_connect.py

test-spark-logs:
	@echo "Showing Spark Connect logs..."
	docker logs -f autoloader-test-spark-connect

teardown-test:
	@echo "Stopping test infrastructure..."
	docker-compose -f docker-compose.test.yml down -v
	@echo "Test infrastructure stopped and cleaned up."

test:
	@echo "Running tests (excluding slow and Spark-dependent tests)..."
	pytest -m "not slow and not requires_spark"

test-unit:
	@echo "Running unit tests..."
	pytest -m unit

test-integration:
	@echo "Running integration tests..."
	pytest -m integration

test-e2e:
	@echo "Running E2E tests (excluding Spark-dependent)..."
	pytest -m "e2e and not requires_spark"

test-e2e-full:
	@echo "Running ALL E2E tests (including Spark-dependent)..."
	pytest -m e2e

test-all:
	@echo "Running ALL tests..."
	pytest

test-cov:
	@echo "Running tests with coverage..."
	pytest --cov=app --cov-report=html --cov-report=term-missing
	@echo ""
	@echo "Coverage report generated in htmlcov/index.html"

test-watch:
	@echo "Running tests in watch mode..."
	pytest-watch

test-fast:
	@echo "Running fast tests only..."
	pytest -m "not slow" --tb=short

test-verbose:
	@echo "Running tests with verbose output..."
	pytest -v -s

run:
	python run.py

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage coverage.xml test.db
	@echo "Cleanup complete!"

format:
	@echo "Formatting code..."
	black app/ tests/
	isort app/ tests/

lint:
	@echo "Linting code..."
	flake8 app/ tests/
	pylint app/

check: lint test
	@echo "All checks passed!"

# Database migrations
migrate:
	alembic upgrade head

migrate-create:
	@read -p "Enter migration message: " msg; \
	alembic revision --autogenerate -m "$$msg"

migrate-rollback:
	alembic downgrade -1

# Quick test with minimal output
test-quick:
	pytest -x --tb=short -q

# Test specific file
test-file:
	@read -p "Enter test file path: " file; \
	pytest $$file -v

# Show test markers
test-markers:
	pytest --markers

# Show test collection
test-collect:
	pytest --collect-only
