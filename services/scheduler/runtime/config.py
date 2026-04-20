import os
from dataclasses import dataclass
from functools import lru_cache

from shared.postgres_url import get_checkpoint_database_url


@dataclass(frozen=True)
class Settings:
    app_name: str = "scheduler"
    app_env: str = "dev"
    log_level: str = "INFO"
    redis_url: str = "redis://localhost:6379/0"
    checkpoint_database_url: str = ""
    poll_interval_seconds: float = 1.0
    task_status_ttl: int = 604800
    schedule_lock_enabled: bool = True
    schedule_lock_ttl_buffer_seconds: int = 5
    scheduler_heartbeat_interval: int = 10
    scheduler_heartbeat_ttl: int = 30


@lru_cache
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("SCHEDULER_APP_NAME", "scheduler"),
        app_env=os.getenv("SCHEDULER_APP_ENV", "dev"),
        log_level=os.getenv("SCHEDULER_LOG_LEVEL", "INFO"),
        redis_url=os.getenv("SCHEDULER_REDIS_URL", os.getenv("WORKER_REDIS_URL", "redis://localhost:6379/0")),
        checkpoint_database_url=get_checkpoint_database_url(host_default="postgres"),
        poll_interval_seconds=float(os.getenv("SCHEDULER_POLL_INTERVAL_SECONDS", "1.0")),
        task_status_ttl=int(os.getenv("TASK_STATUS_TTL", "604800")),
        schedule_lock_enabled=_parse_bool(os.getenv("SCHEDULER_LOCK_ENABLED", "true")),
        schedule_lock_ttl_buffer_seconds=int(os.getenv("SCHEDULER_LOCK_TTL_BUFFER_SECONDS", "5")),
        scheduler_heartbeat_interval=int(os.getenv("SCHEDULER_HEARTBEAT_INTERVAL", "10")),
        scheduler_heartbeat_ttl=int(os.getenv("SCHEDULER_HEARTBEAT_TTL", "30")),
    )


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
