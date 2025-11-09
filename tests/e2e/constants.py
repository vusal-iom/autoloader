"""
E2E Test Constants.

Centralized location for all magic numbers, timeouts, and status strings
used across e2e tests. This improves maintainability and makes it easy
to adjust test behavior globally.
"""

# =============================================================================
# Timeouts (in seconds)
# =============================================================================

# Default timeout for run completion (3 minutes)
DEFAULT_RUN_TIMEOUT = 180

# Timeout for Prefect flow run completion (3 minutes)
PREFECT_FLOW_TIMEOUT = 180

# Short timeout for already-completed operations (1 minute)
SHORT_TIMEOUT = 60

# Timeout for service health checks (1 minute)
SERVICE_HEALTH_TIMEOUT = 60

# =============================================================================
# Poll Intervals (in seconds)
# =============================================================================

# Default polling interval for run status checks
DEFAULT_POLL_INTERVAL = 2

# Polling interval for Prefect flow runs
PREFECT_POLL_INTERVAL = 3

# =============================================================================
# Test Data Defaults
# =============================================================================

# Default number of files to generate
DEFAULT_NUM_FILES = 3

# Default number of records per file
DEFAULT_RECORDS_PER_FILE = 1000

# Default bucket prefix for test buckets
TEST_BUCKET_PREFIX = "test-bucket"

# Default table prefix for test tables
TEST_TABLE_PREFIX = "e2e_test"

# =============================================================================
# Run Status Values
# =============================================================================

# Successful completion statuses
SUCCESS_STATUSES = ["success", "completed"]

# Failed run statuses
FAILED_STATUSES = ["failed"]

# Running/in-progress statuses
RUNNING_STATUSES = ["running", "accepted"]

# =============================================================================
# Prefect Flow States
# =============================================================================

# Completed flow states
PREFECT_COMPLETED_STATES = ["Completed", "COMPLETED"]

# Failed flow states
PREFECT_FAILED_STATES = ["Failed", "FAILED", "Crashed", "CRASHED"]

# Running flow states
PREFECT_RUNNING_STATES = ["Running", "RUNNING", "Scheduled", "SCHEDULED"]

# =============================================================================
# Schema Types
# =============================================================================

# Base schema type (5 fields: id, timestamp, user_id, event_type, value)
SCHEMA_TYPE_BASE = "base"

# Evolved schema type (7 fields: base + region, metadata)
SCHEMA_TYPE_EVOLVED = "evolved"

# =============================================================================
# Expected Tags
# =============================================================================

# Default Prefect deployment tag
AUTOLOADER_TAG = "autoloader"
