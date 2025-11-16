"""
E2E Test Helpers Package.

This package contains all helper modules for e2e tests, organized by responsibility:
- constants: Magic numbers, timeouts, status values
- factories: Ingestion creation, data generation
- run_helpers: Run execution and monitoring
- assertions: Verification functions
- prefect_helpers: Prefect deployment and flow operations
"""

# Re-export all public functions for easy importing
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

from .factories import (
    create_standard_ingestion,
    generate_unique_table_name,
    get_table_identifier,
    upload_json_files,
)

from .run_helpers import (
    trigger_run,
    wait_for_run_completion,
    get_run,
    get_latest_run_id,
)

from .assertions import (
    assert_run_metrics,
    verify_table_data,
    verify_schema_evolution,
    print_test_summary,
)

from .prefect_helpers import (
    wait_for_prefect_flow_completion,
    verify_prefect_deployment_exists,
    verify_prefect_deployment_active,
    verify_prefect_deployment_paused,
    verify_prefect_deployment_deleted,
)

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
