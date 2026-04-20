import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    app_name: str = "scheduler"
    app_env: str = "dev"
    log_level: str = "INFO"
    redis_url: str = "redis://localhost:6379/0"
    checkpoint_database_url: str = "postgresql://formatid:formatid@postgres:5432/formatid"
    poll_interval_seconds: float = 1.0
    task_status_ttl: int = 604800


@lru_cache
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("SCHEDULER_APP_NAME", "scheduler"),
        app_env=os.getenv("SCHEDULER_APP_ENV", "dev"),
        log_level=os.getenv("SCHEDULER_LOG_LEVEL", "INFO"),
        redis_url=os.getenv("SCHEDULER_REDIS_URL", os.getenv("WORKER_REDIS_URL", "redis://localhost:6379/0")),
        checkpoint_database_url=os.getenv(
            "CHECKPOINT_DATABASE_URL",
            "postgresql://formatid:formatid@postgres:5432/formatid",
        ),
        poll_interval_seconds=float(os.getenv("SCHEDULER_POLL_INTERVAL_SECONDS", "1.0")),
        task_status_ttl=int(os.getenv("TASK_STATUS_TTL", "604800")),
    )
