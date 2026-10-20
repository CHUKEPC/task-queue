"""Application configuration and environment settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration settings loaded from environment variables and .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # General App Config
    environment: Literal["development", "production", "testing"] = "development"
    debug: bool = True
    app_name: str = "Task Queue System"
    app_version: str = "0.1.0"
    api_v1_prefix: str = "/api/v1"
    secret_key: str = "supersecretkey-change-in-production"
    allowed_origins: list[str] = Field(default_factory=lambda: ["*"])

    # Database Settings
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/task_queue"
    database_sync_url: str = "postgresql://postgres:postgres@localhost:5432/task_queue"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # Redis & Celery Settings
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"
    celery_task_default_queue: str = "celery"
    celery_task_always_eager: bool = False
    result_expires_seconds: int = 86400  # 24 hours
    default_task_ttl_hours: int = 24

    # Priority mapping for Celery queues and Redis priority levels
    priority_map: dict[str, int] = Field(
        default_factory=lambda: {
            "low": 0,
            "medium": 3,
            "high": 6,
            "critical": 9,
        }
    )

    @property
    def is_production(self) -> bool:
        """Check if current environment is production."""
        return self.environment == "production"

    @property
    def is_testing(self) -> bool:
        """Check if current environment is testing."""
        return self.environment == "testing"


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton instance of application settings.

    Returns:
        Settings: Application configuration instance.
    """
    return Settings()
