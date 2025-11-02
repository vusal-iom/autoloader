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
