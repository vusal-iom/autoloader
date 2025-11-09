"""
E2E Test Run Helpers.

Provides functions for triggering and monitoring ingestion runs.
"""

import time
import pytest
from typing import Dict, Any, Optional
from fastapi.testclient import TestClient

from .logger import E2ELogger
from .constants import (
    DEFAULT_RUN_TIMEOUT,
    DEFAULT_POLL_INTERVAL,
    SUCCESS_STATUSES,
    RUNNING_STATUSES,
)


# =============================================================================
# Run Triggering
# =============================================================================

def trigger_run(api_client: TestClient, ingestion_id: str) -> str:
    """
    Trigger a manual ingestion run.

    Args:
        api_client: FastAPI test client
        ingestion_id: Ingestion ID

    Returns:
        Run ID
    """
    response = api_client.post(f"/api/v1/ingestions/{ingestion_id}/run")
    assert response.status_code == 202, f"Failed to trigger run: {response.text}"

    run_response = response.json()
    run_id = run_response["run_id"]

    assert run_id is not None
    assert run_response["status"] in ["accepted", "running", "completed"]

    return run_id


# =============================================================================
# Run Status Monitoring
# =============================================================================

def wait_for_run_completion(
    api_client: TestClient,
    ingestion_id: str,
    run_id: str,
    timeout: int = DEFAULT_RUN_TIMEOUT,
    poll_interval: int = DEFAULT_POLL_INTERVAL,
    logger: Optional[E2ELogger] = None
) -> Dict[str, Any]:
    """
    Poll for run completion.

    Args:
        api_client: FastAPI test client
        ingestion_id: Ingestion ID
        run_id: Run ID
        timeout: Max wait time in seconds
        poll_interval: Poll interval in seconds
        logger: Optional logger for progress output

    Returns:
        Final run data dict

    Raises:
        pytest.fail if run fails or times out
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        run = get_run(api_client, ingestion_id, run_id)
        run_status = run["status"]

        if logger and logger.verbose:
            elapsed = int(time.time() - start_time)
            print(f"  {elapsed}s - Status: {run_status}", end="")

        if run_status in SUCCESS_STATUSES:
            elapsed = int(time.time() - start_time)
            if logger:
                logger.success(f"Run completed in {elapsed}s", always=True)
            return run

        elif run_status == "failed":
            pytest.fail(f"Run failed: {run.get('errors', [])}")

        else:
            time.sleep(poll_interval)

    pytest.fail(
        f"Run did not complete within {timeout}s. "
        f"Last status: {run.get('status', 'unknown')}"
    )


def get_run(api_client: TestClient, ingestion_id: str, run_id: str) -> Dict[str, Any]:
    """
    Get run details.

    Args:
        api_client: FastAPI test client
        ingestion_id: Ingestion ID
        run_id: Run ID

    Returns:
        Run data dict
    """
    response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs/{run_id}")
    assert response.status_code == 200, f"Failed to get run: {response.text}"
    return response.json()


def get_latest_run_id(
    api_client: TestClient,
    ingestion_id: str,
    exclude_run_id: Optional[str] = None
) -> Optional[str]:
    """
    Fetch the latest run ID for an ingestion.

    Args:
        api_client: FastAPI test client
        ingestion_id: Ingestion ID
        exclude_run_id: Optional run ID to exclude from results (e.g., previous run)

    Returns:
        Latest run ID, or None if no runs found
    """
    response = api_client.get(f"/api/v1/ingestions/{ingestion_id}/runs")
    assert response.status_code == 200, f"Failed to get runs: {response.text}"

    runs = response.json()

    if not runs:
        return None

    # Runs are ordered by created_at DESC (newest first)
    if exclude_run_id:
        for run in runs:
            if run["id"] != exclude_run_id:
                return run["id"]
        return None

    return runs[0]["id"]
