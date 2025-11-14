"""
E2E Test Logger (Deprecated - Use TestLogger).

This module now imports from the shared test logger.
For new code, import directly from tests.helpers.logger.

Backward compatibility: E2ELogger is an alias for TestLogger.
"""

from tests.helpers.logger import TestLogger

# Backward compatibility: E2ELogger is now an alias for TestLogger
E2ELogger = TestLogger

__all__ = ["E2ELogger", "TestLogger"]
