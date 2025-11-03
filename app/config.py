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
    spark_session_pool_size: int = 10
    spark_session_idle_timeout: int = 1800  # 30 minutes

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

    For now it returns local one
    """
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
