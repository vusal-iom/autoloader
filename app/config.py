"""Application configuration."""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings."""

    # Application
    app_name: str = "IOMETE Autoloader"
    app_version: str = "1.0.0"
    debug: bool = False

    # Database
    database_url: str = "sqlite:///./autoloader.db"

    # Security
    secret_key: str = "change-this-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    # Spark Connect
    spark_connect_default_port: int = 15002
    test_spark_connect_token: str = ""  # For testing: leave empty or set token

    # Ingestion
    checkpoint_base_path: str = "s3://iomete-checkpoints/"
    max_preview_files: int = 10
    max_preview_rows: int = 100

    # Cost Estimation
    cost_per_dbu_hour: float = 0.40
    storage_cost_per_gb: float = 0.023
    list_operation_cost_per_1000: float = 0.005

    # Scheduler
    scheduler_check_interval: int = 60  # seconds

    # Prefect Configuration (self-hosted as part of IOMETE stack)
    prefect_api_url: str = "http://prefect-server:4200/api"

    # Work Queues
    prefect_default_work_queue: str = "autoloader-default"
    prefect_high_memory_work_queue: str = "autoloader-high-memory"
    prefect_priority_work_queue: str = "autoloader-priority"

    # Flow Configuration
    prefect_flow_run_timeout_seconds: int = 3600  # 1 hour
    prefect_task_retry_delay_seconds: int = 60
    prefect_max_task_retries: int = 3

    # Resource Thresholds
    high_memory_threshold_gb: int = 10  # Files > 10GB use high-memory queue

    # Monitoring
    metrics_retention_days: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def get_spark_connect_url(cluster_id: str) -> str:
    """
    Get Spark Connect URL for a given cluster ID.

    TODO: Before production, implement proper cluster lookup:
    - Query IOMETE cluster management API
    - Map cluster_id to actual cluster endpoint
    - Handle cluster not found errors
    - Support multiple environments (dev/staging/prod)

    For now it returns local one, with environment variable override support
    for container networking (e.g., Prefect worker needs spark-connect:15002)
    """
    import os
    # Allow override via environment variable for worker containers
    override_url = os.getenv("SPARK_CONNECT_URL_OVERRIDE")
    if override_url:
        return override_url

    return "sc://localhost:15002"


def get_spark_connect_credentials(cluster_id: str) -> tuple[str, str]:
    """
    Get Spark Connect URL and token for a given cluster ID.

    TODO: Before production, implement proper cluster credential lookup:
    - Query IOMETE cluster management API
    - Retrieve authentication token from secure storage
    - Handle token refresh and expiration
    - Support different auth methods per environment

    For now: Returns localhost URL and test token.

    Returns:
        tuple[str, str]: (spark_connect_url, spark_connect_token)
    """
    settings = get_settings()
    url = get_spark_connect_url(cluster_id)
    token = settings.test_spark_connect_token or ""
    return url, token
