"""Cluster API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

from app.models.schemas import ClusterInfo
from app.config import get_spark_connect_url

router = APIRouter(prefix="/clusters", tags=["clusters"])


@router.get("", response_model=List[ClusterInfo])
async def list_clusters(
    tenant_id: str = "tenant_123",  # TODO: Extract from auth
):
    """
    List user's available compute clusters.

    Returns clusters with:
    - ID, name, status
    - Spark Connect URL
    - Worker count and DBU information
    - Pricing details
    """
    # TODO: Integrate with IOMETE cluster management API
    # Mock response for now
    return [
        ClusterInfo(
            id="cluster-prod-01",
            name="Production Cluster",
            status="running",
            workers=8,
            dbu_per_worker=8,
            spark_connect_url=get_spark_connect_url("cluster-prod-01"),
            pricing={"dbu_rate": 0.40},
        ),
        ClusterInfo(
            id="cluster-analytics-02",
            name="Analytics Cluster",
            status="running",
            workers=4,
            dbu_per_worker=8,
            spark_connect_url=get_spark_connect_url("cluster-analytics-02"),
            pricing={"dbu_rate": 0.40},
        ),
    ]


@router.get("/{cluster_id}", response_model=ClusterInfo)
async def get_cluster(cluster_id: str):
    """
    Get detailed cluster information.

    Used for cost estimation and cluster selection.
    """
    # TODO: Integrate with IOMETE cluster management API
    if cluster_id == "cluster-prod-01":
        return ClusterInfo(
            id="cluster-prod-01",
            name="Production Cluster",
            status="running",
            workers=8,
            dbu_per_worker=8,
            spark_connect_url=get_spark_connect_url("cluster-prod-01"),
            pricing={"dbu_rate": 0.40},
        )
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Cluster {cluster_id} not found",
    )


@router.post("/{cluster_id}/test-connection")
async def test_connection(cluster_id: str):
    """
    Test Spark Connect connectivity to a cluster.

    Verifies:
    - Cluster is reachable
    - Authentication works
    - Spark version compatibility
    """
    # TODO: Implement Spark Connect connectivity test
    return {
        "status": "success",
        "cluster_id": cluster_id,
        "spark_version": "3.5.0",
        "available_memory_gb": 512,
    }
