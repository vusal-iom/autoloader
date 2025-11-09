"""
E2E Test Prefect Helpers.

Provides functions for Prefect deployment verification and flow run monitoring.
"""

import time
import pytest
from typing import Any, List, Optional
from prefect import get_client
from prefect.exceptions import ObjectNotFound

from .logger import E2ELogger
from .constants import (
    PREFECT_FLOW_TIMEOUT,
    PREFECT_POLL_INTERVAL,
    PREFECT_COMPLETED_STATES,
    PREFECT_FAILED_STATES,
)


# =============================================================================
# Prefect Flow Run Monitoring
# =============================================================================

async def wait_for_prefect_flow_completion(
    flow_run_id: str,
    timeout: int = PREFECT_FLOW_TIMEOUT,
    poll_interval: int = PREFECT_POLL_INTERVAL,
    logger: Optional[E2ELogger] = None
) -> str:
    """
    Wait for Prefect flow run to complete.

    Args:
        flow_run_id: Prefect flow run ID
        timeout: Maximum wait time in seconds
        poll_interval: Poll interval in seconds
        logger: Optional logger for progress output

    Returns:
        Final state name (e.g., "Completed", "Failed")

    Raises:
        pytest.fail if flow run fails or times out
    """
    start_time = time.time()

    async with get_client() as prefect_client:
        while time.time() - start_time < timeout:
            try:
                flow_run = await prefect_client.read_flow_run(flow_run_id)
                state_name = flow_run.state.name if flow_run.state else "unknown"

                if logger and logger.verbose:
                    elapsed = int(time.time() - start_time)
                    print(f"  {elapsed}s - Prefect state: {state_name}")

                if state_name in PREFECT_COMPLETED_STATES:
                    elapsed = int(time.time() - start_time)
                    if logger:
                        logger.success(f"Flow run completed in {elapsed}s")
                    return state_name

                elif state_name in PREFECT_FAILED_STATES:
                    pytest.fail(f"Flow run failed with state: {state_name}")

                time.sleep(poll_interval)

            except Exception as e:
                pytest.fail(f"Error checking flow run status: {e}")

    pytest.fail(f"Flow run did not complete within {timeout}s")


# =============================================================================
# Prefect Deployment Verification
# =============================================================================

async def verify_prefect_deployment_exists(
    deployment_id: str,
    expected_name: Optional[str] = None,
    expected_tags: Optional[List[str]] = None,
    logger: Optional[E2ELogger] = None
) -> Any:
    """
    Verify that a Prefect deployment exists and optionally check its properties.

    Args:
        deployment_id: Prefect deployment ID
        expected_name: Expected deployment name (None to skip)
        expected_tags: List of tags that must be present (None to skip)
        logger: Optional logger for detailed output

    Returns:
        Deployment object from Prefect

    Raises:
        pytest.fail if deployment not found or properties don't match
    """
    async with get_client() as prefect_client:
        try:
            deployment = await prefect_client.read_deployment(deployment_id)

            if logger:
                logger.success(f"Deployment found: {deployment.name}")
                logger.step(f"Paused: {deployment.paused}")
                logger.step(f"Tags: {deployment.tags}")
                has_active_schedule = _has_active_schedules(deployment)
                logger.step(f"Has active schedules: {has_active_schedule}")

            # Verify name if provided
            if expected_name is not None:
                assert deployment.name == expected_name, \
                    f"Expected deployment name '{expected_name}', got '{deployment.name}'"

            # Verify tags if provided
            if expected_tags:
                for tag in expected_tags:
                    assert tag in deployment.tags, \
                        f"Expected tag '{tag}' not found in deployment tags: {deployment.tags}"

            return deployment

        except ObjectNotFound:
            pytest.fail(f"Deployment {deployment_id} not found in Prefect server")


async def verify_prefect_deployment_active(
    deployment_id: str,
    logger: Optional[E2ELogger] = None
):
    """
    Verify that a Prefect deployment is active (not paused, has active schedules).

    Args:
        deployment_id: Prefect deployment ID
        logger: Optional logger for detailed output

    Raises:
        AssertionError if deployment is paused or has no active schedules
    """
    async with get_client() as prefect_client:
        deployment = await prefect_client.read_deployment(deployment_id)

        has_active_schedule = _has_active_schedules(deployment)

        if logger:
            logger.step(f"Deployment paused: {deployment.paused}")
            logger.step(f"Has active schedules: {has_active_schedule}")

        assert deployment.paused is False, \
            f"Expected deployment to be active, but it is paused"
        assert has_active_schedule is True, \
            "Expected at least one active schedule"

        if logger:
            logger.success("Deployment is active")


async def verify_prefect_deployment_paused(
    deployment_id: str,
    logger: Optional[E2ELogger] = None
):
    """
    Verify that a Prefect deployment is paused.

    Args:
        deployment_id: Prefect deployment ID
        logger: Optional logger for detailed output

    Raises:
        AssertionError if deployment is not paused
    """
    async with get_client() as prefect_client:
        deployment = await prefect_client.read_deployment(deployment_id)

        if logger:
            logger.step(f"Deployment paused: {deployment.paused}")

        assert deployment.paused is True, \
            f"Expected deployment to be paused, but it is active"

        if logger:
            logger.success("Deployment is paused")


async def verify_prefect_deployment_deleted(
    deployment_id: str,
    logger: Optional[E2ELogger] = None
):
    """
    Verify that a Prefect deployment has been deleted.

    Args:
        deployment_id: Prefect deployment ID
        logger: Optional logger for detailed output

    Raises:
        pytest.fail if deployment still exists
    """
    async with get_client() as prefect_client:
        try:
            await prefect_client.read_deployment(deployment_id)
            pytest.fail(f"Deployment {deployment_id} should have been deleted but still exists")
        except ObjectNotFound:
            if logger:
                logger.success("Deployment deleted from Prefect server")


# =============================================================================
# Private Helper Functions
# =============================================================================

def _has_active_schedules(deployment) -> bool:
    """Check if deployment has at least one active schedule."""
    return any(s.active for s in deployment.schedules) if deployment.schedules else False
