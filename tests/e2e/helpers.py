"""
E2E Test Helper Functions.

This module re-exports all helper functions from the organized submodules.
Provides backward compatibility for existing tests while maintaining
clean separation of concerns.

Organized modules:
- constants.py: Magic numbers, timeouts, status values
- logger.py: E2ELogger class
- factories.py: Ingestion creation, data generation
- run_helpers.py: Run execution and monitoring
- assertions.py: Verification functions
- prefect_helpers.py: Prefect deployment and flow operations
"""

# =============================================================================
# Re-export all public functions for backward compatibility
# =============================================================================

# Constants
from .constants import (
    DEFAULT_RUN_TIMEOUT,
    DEFAULT_POLL_INTERVAL,
    PREFECT_FLOW_TIMEOUT,
    PREFECT_POLL_INTERVAL,
    DEFAULT_NUM_FILES,
    DEFAULT_RECORDS_PER_FILE,
    TEST_TABLE_PREFIX,
    SUCCESS_STATUSES,
    FAILED_STATUSES,
    RUNNING_STATUSES,
    PREFECT_COMPLETED_STATES,
    PREFECT_FAILED_STATES,
    SCHEMA_TYPE_BASE,
    SCHEMA_TYPE_EVOLVED,
    AUTOLOADER_TAG,
)

# Logger
from .logger import E2ELogger

# Factories
from .factories import (
    create_standard_ingestion,
    generate_unique_table_name,
    get_table_identifier,
    upload_json_files,
)

# Run Helpers
from .run_helpers import (
    trigger_run,
    wait_for_run_completion,
    get_run,
    get_latest_run_id,
)

# Assertions
from .assertions import (
    assert_run_metrics,
    verify_table_data,
    verify_schema_evolution,
    print_test_summary,
)

# Prefect Helpers
from .prefect_helpers import (
    wait_for_prefect_flow_completion,
    verify_prefect_deployment_exists,
    verify_prefect_deployment_active,
    verify_prefect_deployment_paused,
    verify_prefect_deployment_deleted,
)

# =============================================================================
# Public API
# =============================================================================

__all__ = [
    # Constants
    "DEFAULT_RUN_TIMEOUT",
    "DEFAULT_POLL_INTERVAL",
    "PREFECT_FLOW_TIMEOUT",
    "PREFECT_POLL_INTERVAL",
    "DEFAULT_NUM_FILES",
    "DEFAULT_RECORDS_PER_FILE",
    "TEST_TABLE_PREFIX",
    "SUCCESS_STATUSES",
    "FAILED_STATUSES",
    "RUNNING_STATUSES",
    "PREFECT_COMPLETED_STATES",
    "PREFECT_FAILED_STATES",
    "SCHEMA_TYPE_BASE",
    "SCHEMA_TYPE_EVOLVED",
    "AUTOLOADER_TAG",
    # Logger
    "E2ELogger",
    # Factories
    "create_standard_ingestion",
    "generate_unique_table_name",
    "get_table_identifier",
    "upload_json_files",
    # Run Helpers
    "trigger_run",
    "wait_for_run_completion",
    "get_run",
    "get_latest_run_id",
    # Assertions
    "assert_run_metrics",
    "verify_table_data",
    "verify_schema_evolution",
    "print_test_summary",
    # Prefect Helpers
    "wait_for_prefect_flow_completion",
    "verify_prefect_deployment_exists",
    "verify_prefect_deployment_active",
    "verify_prefect_deployment_paused",
    "verify_prefect_deployment_deleted",
]
