"""
Test Logger for all test types (unit, integration, e2e).

Provides simple, consistent logging for tests with optional verbose mode.
NO EMOJIS - Keep output professional and CI/CD-friendly.
"""

import os
import time
from typing import Any
from contextlib import contextmanager


class TestLogger:
    """Simple logger for all test types with optional verbosity."""

    def __init__(self, verbose: bool = None):
        """
        Initialize logger.

        Args:
            verbose: If True, logs detailed output. If None, reads from TEST_VERBOSE or E2E_VERBOSE env var.
        """
        if verbose is None:
            # Support both TEST_VERBOSE (preferred) and E2E_VERBOSE (backward compatibility)
            verbose = (
                os.getenv("TEST_VERBOSE", os.getenv("E2E_VERBOSE", "false")).lower() == "true"
            )
        self.verbose = verbose

    def section(self, title: str):
        """Log section header (always shown)."""
        print(f"\n{'='*80}")
        print(title)
        print('='*80)

    def phase(self, title: str):
        """Log phase header (always shown)."""
        print(f"\n{title}")

    def step(self, msg: str, always: bool = False):
        """Log step message (only if verbose or always=True)."""
        if self.verbose or always:
            print(f"  {msg}")

    def metric(self, key: str, value: Any):
        """Log metric (only if verbose)."""
        if self.verbose:
            print(f"  {key}: {value}")

    def success(self, msg: str, always: bool = True):
        """Log success message."""
        if self.verbose or always:
            print(f"  {msg}")

    def error(self, msg: str):
        """Log error message (always shown)."""
        print(f"  ERROR: {msg}")

    @contextmanager
    def timed_phase(self, title: str):
        """
        Context manager that auto-logs phase duration.

        Usage:
            with logger.timed_phase("Phase 1: Loading data..."):
                # do work
                pass
            # Automatically logs: "  Completed in 2.3s"
        """
        start = time.time()
        self.phase(title)
        yield
        duration = time.time() - start
        self.step(f"Completed in {duration:.1f}s", always=True)
