# Test Suite Guidelines

This document provides guidelines for all test types in the IOMETE Autoloader project (unit, integration, and e2e tests).


## Logging and Comments

**Keep it clean, professional, and minimal.**

- **No emojis** - anywhere in code, comments, logs, or test output
- **Simple comments** - single-line phase markers, no decorative separators (`===`, `---`)
- **Concise messages** - no trailing punctuation (`...`, `!!!`), no SHOUTING CASE
- **Log phases, not steps** - let helpers and assertions do the detailed logging
- **Professional tone** - factual, informative, suitable for CI/CD logs

```python
logger.section("E2E TEST: Incremental Load")
logger.phase("Phase 1: Setup")
logger.phase("Phase 2: Execute")
logger.success("Run completed in 5s")
print_test_summary([("Status", "SUCCESS")])
```

### TestLogger API

All test types should use the shared `TestLogger` class from `tests.helpers.logger`

- `logger.section(title)` - Test section header (always shown)
- `logger.phase(title)` - Phase description (always shown)
- `logger.step(msg, always=False)` - Step detail (verbose only by default)
- `logger.metric(key, value)` - Log metric (verbose only)
- `logger.success(msg, always=True)` - Success message
- `logger.error(msg)` - Error message (always shown)
- `logger.timed_phase(title)` - Context manager that auto-logs duration