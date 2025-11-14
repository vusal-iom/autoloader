# Test Suite Guidelines

This document provides guidelines for all test types in the IOMETE Autoloader project (unit, integration, and e2e tests).

## Critical Rule: NO EMOJIS

**Absolutely no emojis anywhere in test code, comments, logs, or output.**

This applies to:
- Test files (unit, integration, e2e)
- Fixture files (conftest.py)
- Helper modules
- Print statements
- Comments
- Docstrings
- Assertion messages

### Why?
- Professional output suitable for CI/CD logs
- Consistent across all environments (no rendering issues)
- Easier to search and parse logs
- Better terminal compatibility
- Cleaner git diffs

## Logging Best Practices

### Use the TestLogger Helper

All test types should use the shared `TestLogger` class from `tests.helpers.logger`:

```python
from tests.helpers.logger import TestLogger

def test_my_feature():
    logger = TestLogger()  # Reads TEST_VERBOSE or E2E_VERBOSE env var

    logger.section("TEST: My Feature")
    logger.phase("Phase 1: Setup")
    logger.step("Setting up test data", always=True)
    logger.success("Setup complete")
```

### TestLogger API

- `logger.section(title)` - Test section header (always shown)
- `logger.phase(title)` - Phase description (always shown)
- `logger.step(msg, always=False)` - Step detail (verbose only by default)
- `logger.metric(key, value)` - Log metric (verbose only)
- `logger.success(msg, always=True)` - Success message
- `logger.error(msg)` - Error message (always shown)
- `logger.timed_phase(title)` - Context manager that auto-logs duration

### Verbose Mode

Enable detailed logging with environment variable:

```bash
# All tests
TEST_VERBOSE=true pytest tests/

# E2E tests (backward compatible)
E2E_VERBOSE=true pytest tests/e2e/
```

### Output Style Guidelines

**Keep it clean, professional, and minimal:**

- **No emojis** - Use simple text (✅ → "OK", 🔍 → "Checking", etc.)
- **No decorative separators** - Let TestLogger handle formatting
- **Concise messages** - No trailing punctuation (`...`, `!!!`)
- **No SHOUTING CASE** - Use proper sentence case
- **Professional tone** - Factual, informative

**Good:**
```python
logger.phase("Phase 1: Setup database")
logger.success("Database initialized")
print("Checking service health...")
print("MinIO: Ready")
```

**Bad:**
```python
logger.phase("🚀 PHASE 1: SETTING UP DATABASE!!!")
logger.success("✅ Database is ready!!!")
print("🔍 Checking service health...")
print("MinIO: ✅ Ready")
```

## When to Use Logging

### Unit Tests
- **Minimal or no logging** - Tests should be fast and quiet
- Use assertions with clear messages instead of print statements
- Only use TestLogger for complex test scenarios

### Integration Tests
- **Moderate logging** - Show major phases and operations
- Use TestLogger for multi-phase integration tests
- Log service interactions and state changes

### E2E Tests
- **Comprehensive logging** - Document the entire test flow
- Always use TestLogger for consistent output
- See `tests/e2e/CLAUDE.md` for e2e-specific guidelines

## Fixture Logging

Fixtures in `conftest.py` files should:
- Use TestLogger instead of raw print statements
- Only log important setup/teardown operations
- Avoid verbose output unless necessary for debugging
- Keep messages concise and professional

**Good:**
```python
@pytest.fixture
def setup_database(db_session):
    logger = TestLogger()
    logger.phase("Setting up test database")
    # ... setup code ...
    yield db_session
    logger.step("Cleaning up test database", always=True)
```

**Bad:**
```python
@pytest.fixture
def setup_database(db_session):
    print("🔍 Setting up test database...")
    # ... setup code ...
    yield db_session
    print("🧹 Cleaning up test database...")
```

## Test-Specific Guidelines

Each test type has additional specific guidelines:

- **E2E Tests**: See `tests/e2e/CLAUDE.md`
- **Integration Tests**: See `tests/integration/CLAUDE.md`
- **Unit Tests**: Follow standard pytest best practices

## Summary

1. **NO EMOJIS** anywhere in test code or output
2. Use `TestLogger` from `tests.helpers.logger` for consistent logging
3. Keep output professional and CI/CD-friendly
4. Use verbose mode (`TEST_VERBOSE=true`) for debugging
5. Minimize logging in unit tests, moderate in integration, comprehensive in e2e
