# Pytest Fixtures Guide

## Overview

Pytest fixtures are a powerful feature for setting up test dependencies, managing test data, and handling setup/teardown logic in a reusable and composable way. This guide explains how fixtures work in the IOMETE Autoloader project.

## What Are Fixtures?

Fixtures are functions that run before (and sometimes after) test functions. They:

- **Provide test dependencies** (database connections, API clients, test data)
- **Set up test environments** (create test buckets, initialize services)
- **Clean up after tests** (delete test data, close connections)
- **Share common setup** across multiple tests
- **Enable dependency injection** - tests declare what they need, pytest provides it

## Basic Fixture Example

```python
@pytest.fixture
def sample_data():
    """Provide test data to tests."""
    return {"id": 1, "name": "Test User"}

def test_user_name(sample_data):
    """Test receives sample_data automatically."""
    assert sample_data["name"] == "Test User"
```

## Fixture Scopes

Fixtures can have different scopes that control **when they are created and destroyed**:

### `scope="function"` (default)

- **Created once per test function**
- **Destroyed after each test function**
- Use for: Test-specific data that should be isolated between tests
- Memory impact: Low (created/destroyed frequently)

**Example from our codebase:**

```python
@pytest.fixture(scope="function")
def test_bucket(minio_client) -> Generator[str, None, None]:
    """
    Create a test bucket for each test and clean up after.

    Each test gets its own fresh bucket, ensuring complete isolation.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    bucket_name = f"test-bucket-{timestamp}"

    # Setup: Create bucket
    minio_client.create_bucket(Bucket=bucket_name)
    print(f"✅ Created test bucket: {bucket_name}")

    yield bucket_name  # Test runs here

    # Teardown: Delete bucket
    # ... cleanup code ...
    print(f"🧹 Cleaned up test bucket: {bucket_name}")
```

**Why function scope?**
- Each test needs its own isolated bucket
- Tests don't interfere with each other
- Clean state for every test

### `scope="session"`

- **Created ONCE for entire test session**
- **Destroyed after ALL tests complete**
- Use for: Expensive resources shared across all tests
- Memory impact: High (lives for entire test run)

**Example from our codebase:**

```python
@pytest.fixture(scope="session")
def minio_client(minio_config: Dict[str, str], ensure_services_ready):
    """
    Create boto3 S3 client for MinIO.

    Created once per test session because:
    - Creating S3 clients is relatively expensive
    - Client can be safely shared across tests
    - Connection pooling is more efficient
    """
    client = boto3.client(
        's3',
        endpoint_url=minio_config["endpoint_url"],
        aws_access_key_id=minio_config["aws_access_key_id"],
        aws_secret_access_key=minio_config["aws_secret_access_key"],
        region_name=minio_config["region_name"]
    )

    # Verify connection
    client.list_buckets()

    return client  # Client lives for entire test session
```

**Why session scope?**
- MinIO client creation is expensive
- Same client can serve all tests safely
- Reduces test execution time

### `scope="module"`

- **Created once per test module (file)**
- **Destroyed after all tests in that module complete**
- Use for: Resources shared within a module but isolated between modules

```python
@pytest.fixture(scope="module")
def database_schema():
    """
    Create database schema once per test module.

    All tests in this file share the same schema,
    but different test files get their own schema.
    """
    setup_schema()
    yield
    teardown_schema()
```

### `scope="class"`

- **Created once per test class**
- **Destroyed after all tests in that class complete**
- Use for: Sharing setup among tests in the same class

```python
class TestIngestionAPI:
    @pytest.fixture(scope="class")
    def ingestion_config(self):
        """Shared config for all tests in this class."""
        return {"source": "s3://bucket", "format": "json"}

    def test_create(self, ingestion_config):
        # Uses same config as other tests in this class
        pass
```

## Scope Comparison Table

| Scope | Lifetime | Created | Use Case | Our Examples |
|-------|----------|---------|----------|--------------|
| **function** | Per test | Many times | Test isolation | `test_bucket`, `test_db`, `spark_session` |
| **class** | Per test class | Once per class | Related tests | (not used in our project) |
| **module** | Per file | Once per file | File-level shared setup | (not used in our project) |
| **session** | Entire test run | Once total | Expensive shared resources | `minio_client`, `test_engine`, `ensure_services_ready` |

## Fixture Patterns in Our Project

### 1. Configuration Fixtures (Session Scope)

Provide environment configuration once per test session:

```python
@pytest.fixture(scope="session")
def minio_config() -> Dict[str, str]:
    """MinIO configuration from environment."""
    return {
        "endpoint_url": os.getenv("TEST_MINIO_ENDPOINT", "http://localhost:9000"),
        "aws_access_key_id": os.getenv("TEST_MINIO_ACCESS_KEY", "minioadmin"),
        "aws_secret_access_key": os.getenv("TEST_MINIO_SECRET_KEY", "minioadmin"),
        "region_name": "us-east-1"
    }
```

**Why session scope?**
- Configuration doesn't change during tests
- Reading environment variables once is sufficient
- No cleanup needed

### 2. Resource Fixtures (Session Scope)

Create expensive resources once:

```python
@pytest.fixture(scope="session")
def test_engine(test_database_url: str):
    """Create test database engine."""
    engine = create_engine(test_database_url)

    # Setup: Create all tables
    Base.metadata.create_all(bind=engine)

    yield engine

    # Teardown: Drop all tables after all tests
    Base.metadata.drop_all(bind=engine)
    engine.dispose()
```

**Key points:**
- Database engine created once
- All tables created at session start
- All tables dropped at session end
- Individual tests manage their own transactions

### 3. Isolation Fixtures (Function Scope)

Provide clean state per test:

```python
@pytest.fixture(scope="function")
def test_db(test_engine) -> Generator[Session, None, None]:
    """
    Create a fresh database session for each test.

    Automatically rolls back after each test to ensure isolation.
    """
    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=test_engine
    )

    session = TestingSessionLocal()

    yield session

    # Rollback ensures test changes don't persist
    session.rollback()
    session.close()
```

**Why function scope?**
- Each test gets a fresh database session
- Changes are rolled back after each test
- Perfect test isolation - no test affects another

### 4. Setup/Teardown Fixtures (Function Scope)

Create and clean up test data:

```python
@pytest.fixture(scope="function")
def sample_json_files(minio_client, test_bucket) -> List[Dict[str, any]]:
    """
    Generate and upload sample JSON files to MinIO.

    Creates 3 JSON files with 1000 records each.
    """
    files_created = []

    for file_idx in range(3):
        # Generate data
        records = [...]
        file_content = "\n".join([json.dumps(record) for record in records])

        # Upload to MinIO
        key = f"data/batch_{file_idx}.json"
        minio_client.put_object(
            Bucket=test_bucket,
            Key=key,
            Body=file_content.encode('utf-8')
        )

        files_created.append({"path": f"s3://{test_bucket}/{key}", ...})

    return files_created
    # Note: test_bucket fixture handles cleanup
```

**Why function scope?**
- Each test gets fresh sample files
- Files cleaned up with bucket (via `test_bucket` fixture)
- Tests don't interfere with each other's data

### 5. Service Health Check Fixtures (Session Scope)

Verify services are ready before tests run:

```python
@pytest.fixture(scope="session")
def ensure_services_ready():
    """
    Ensure all required services are ready.

    Runs once per test session and checks:
    - MinIO health
    - PostgreSQL connection
    - Spark Connect availability
    """
    print("\n🔍 Checking service health...")

    # Check MinIO
    if not check_http_service("http://localhost:9000/minio/health/live", 30):
        raise RuntimeError("MinIO not ready. Please start services.")

    # Check PostgreSQL
    if not check_postgres_service(db_url, 30):
        raise RuntimeError("PostgreSQL not ready.")

    # Check Spark
    if not check_http_service("http://localhost:4040", 60):
        raise RuntimeError("Spark Connect not ready.")

    print("✅ All services ready!\n")

    yield True
```

**Why session scope?**
- Services only need checking once
- Fails fast if infrastructure isn't ready
- Avoids repeated health checks

## Fixture Dependencies

Fixtures can depend on other fixtures - pytest handles the dependency graph:

```python
@pytest.fixture(scope="session")
def minio_config():
    """Configuration (no dependencies)"""
    return {...}

@pytest.fixture(scope="session")
def minio_client(minio_config, ensure_services_ready):
    """Depends on minio_config and ensure_services_ready"""
    client = boto3.client('s3', **minio_config)
    return client

@pytest.fixture(scope="function")
def test_bucket(minio_client):
    """Depends on minio_client"""
    # Create bucket using minio_client
    # ...

@pytest.fixture(scope="function")
def sample_json_files(minio_client, test_bucket):
    """Depends on both minio_client and test_bucket"""
    # Upload files to test_bucket using minio_client
    # ...
```

**Dependency chain execution:**
1. `minio_config()` runs (session scope, once)
2. `ensure_services_ready()` runs (session scope, once)
3. `minio_client()` runs (session scope, once)
4. For each test:
   - `test_bucket()` runs (function scope)
   - `sample_json_files()` runs (function scope)
   - Test runs
   - `sample_json_files()` cleanup (implicit)
   - `test_bucket()` cleanup runs
5. After all tests:
   - `minio_client()` cleanup (none in this case)
   - `minio_config()` cleanup (none in this case)

## Yield Fixtures (Setup/Teardown)

Fixtures using `yield` provide both setup and teardown:

```python
@pytest.fixture
def resource():
    # SETUP: Code before yield runs before test
    print("Setting up resource")
    resource = create_resource()

    yield resource  # Provide resource to test

    # TEARDOWN: Code after yield runs after test
    print("Cleaning up resource")
    resource.cleanup()
```

**Execution flow:**

```
1. Setup code runs (before yield)
2. Test function receives yielded value
3. Test function runs
4. Teardown code runs (after yield)
5. Next test begins
```

**Example from our code:**

```python
@pytest.fixture(scope="function")
def spark_session(spark_connect_url: str, ensure_services_ready):
    """Create Spark Connect session for data verification."""
    try:
        # SETUP
        spark = SparkSession.builder \
            .remote(spark_connect_url) \
            .appName("E2E-Test-Verification") \
            .getOrCreate()

        print(f"✅ Connected to Spark: {spark_connect_url}\n")

        yield spark  # Test uses this

        # TEARDOWN
        spark.stop()
        print("🧹 Stopped Spark session")

    except Exception as e:
        raise RuntimeError(f"Failed to connect to Spark: {e}")
```

## Scope Mismatch Rules

**Important:** A fixture can only depend on fixtures with the same or **broader** scope:

✅ **Allowed:**
```python
@pytest.fixture(scope="session")
def session_fixture(): ...

@pytest.fixture(scope="function")
def function_fixture(session_fixture):  # OK: function can use session
    ...
```

❌ **Not allowed:**
```python
@pytest.fixture(scope="function")
def function_fixture(): ...

@pytest.fixture(scope="session")
def session_fixture(function_fixture):  # ERROR: session can't use function
    ...
```

**Scope hierarchy (narrowest to broadest):**
```
function → class → module → session
```

## Best Practices

### 1. Choose the Right Scope

```python
# ✅ Good: Session scope for expensive shared resources
@pytest.fixture(scope="session")
def database_engine():
    """Heavy resource, safe to share"""
    return create_engine(DATABASE_URL)

# ✅ Good: Function scope for test isolation
@pytest.fixture(scope="function")
def test_data(database_engine):
    """Fresh data per test"""
    session = Session(database_engine)
    # ... create test data
    yield session
    session.rollback()  # Isolate tests

# ❌ Bad: Function scope for expensive operation
@pytest.fixture(scope="function")
def database_engine():  # Created for EVERY test!
    return create_engine(DATABASE_URL)  # Slow!
```

### 2. Use Descriptive Names

```python
# ✅ Good: Clear what fixture provides
@pytest.fixture
def test_bucket(): ...

@pytest.fixture
def sample_json_files(): ...

@pytest.fixture
def authenticated_api_client(): ...

# ❌ Bad: Unclear names
@pytest.fixture
def data(): ...

@pytest.fixture
def client(): ...
```

### 3. Add Docstrings

```python
@pytest.fixture(scope="session")
def minio_client(minio_config: Dict[str, str], ensure_services_ready):
    """
    Create boto3 S3 client for MinIO.

    Uses MinIO from docker-compose.test.yml on localhost:9000.

    Scope: session - expensive to create, safe to share
    Dependencies: minio_config, ensure_services_ready
    """
    ...
```

### 4. Keep Fixtures Focused

```python
# ✅ Good: Single responsibility
@pytest.fixture
def test_bucket(): ...

@pytest.fixture
def sample_json_files(test_bucket): ...

# ❌ Bad: Doing too much
@pytest.fixture
def test_environment():
    # Creates bucket, uploads files, creates users, sets up monitoring...
    # Hard to reuse, hard to understand
    ...
```

### 5. Use Type Hints

```python
from typing import Generator, Dict, List

@pytest.fixture(scope="session")
def minio_config() -> Dict[str, str]:
    """Return type is clear"""
    ...

@pytest.fixture(scope="function")
def test_bucket(minio_client) -> Generator[str, None, None]:
    """Yields string, accepts and returns None"""
    ...
```

## Common Patterns

### Pattern 1: Dependency Injection

```python
def test_upload_file(minio_client, test_bucket, sample_json_files):
    """Test declares exactly what it needs."""
    # pytest automatically provides:
    # - minio_client (session scope)
    # - test_bucket (function scope, fresh per test)
    # - sample_json_files (function scope, fresh per test)

    assert len(sample_json_files) == 3
    # Test runs with isolated bucket and files
```

### Pattern 2: Fixture Chains

```python
@pytest.fixture(scope="session")
def config():
    """Base configuration"""
    return load_config()

@pytest.fixture(scope="session")
def client(config):
    """Client using config"""
    return create_client(config)

@pytest.fixture(scope="function")
def resource(client):
    """Resource using client"""
    resource = client.create_resource()
    yield resource
    client.delete_resource(resource.id)
```

### Pattern 3: Conditional Fixtures

```python
@pytest.fixture(scope="session")
def ensure_services_ready():
    """Fail fast if services aren't available."""
    if not check_service_health():
        pytest.skip("Services not ready")
    yield
```

## Fixture Execution Example

Given these fixtures:

```python
@pytest.fixture(scope="session")
def minio_client():
    print("1. Create MinIO client (session)")
    client = boto3.client('s3', ...)
    yield client
    print("6. Cleanup MinIO client (session)")

@pytest.fixture(scope="function")
def test_bucket(minio_client):
    print("2. Create test bucket (function)")
    bucket = create_bucket()
    yield bucket
    print("5. Delete test bucket (function)")

def test_upload(test_bucket):
    print("3. Run test")
    upload_file(test_bucket, "test.txt")
    print("4. Test complete")
```

**Output:**
```
1. Create MinIO client (session)
2. Create test bucket (function)
3. Run test
4. Test complete
5. Delete test bucket (function)
[... next test runs ...]
2. Create test bucket (function)  # Note: reuses session client
3. Run test
4. Test complete
5. Delete test bucket (function)
[... all tests complete ...]
6. Cleanup MinIO client (session)
```

## Debugging Fixtures

### See fixture values

```bash
# List all available fixtures
pytest --fixtures

# Show fixture setup/teardown
pytest -v -s

# Show fixture execution order
pytest --setup-show
```

### Common issues

**Issue: Fixture not found**
```python
def test_something(my_fixture):  # ERROR: fixture 'my_fixture' not found
    ...
```

**Solution:** Ensure fixture is defined in `conftest.py` or same file

**Issue: Scope mismatch**
```python
@pytest.fixture(scope="session")
def session_fixture(function_fixture):  # ERROR
    ...
```

**Solution:** Session fixtures can't depend on function fixtures

**Issue: Fixture not cleaning up**
```python
@pytest.fixture
def resource():
    r = create()
    return r
    # Missing cleanup! Use yield instead:
    # yield r
    # r.cleanup()
```

## Summary

| Concept | Key Point |
|---------|-----------|
| **Purpose** | Reusable setup/teardown, dependency injection |
| **Scopes** | function (default), class, module, session |
| **Session** | Once per test run, expensive shared resources |
| **Function** | Once per test, isolated test data |
| **Yield** | Code before yield = setup, after = teardown |
| **Dependencies** | Fixtures can depend on other fixtures |
| **Scope rules** | Narrower scopes can use broader scopes, not reverse |

## File Locations

- **Shared fixtures:** `tests/conftest.py`
- **E2E fixtures:** `tests/e2e/conftest.py`
- **Unit test fixtures:** Can be in test files or `conftest.py`

## Further Reading

- [Pytest fixtures documentation](https://docs.pytest.org/en/stable/fixture.html)
- [Pytest fixture scopes](https://docs.pytest.org/en/stable/fixture.html#scope-sharing-fixtures-across-classes-modules-packages-or-session)
- Project examples: `/tests/conftest.py`, `/tests/e2e/conftest.py`
