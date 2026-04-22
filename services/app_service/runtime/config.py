import os
from dataclasses import dataclass
from functools import lru_cache

from shared.postgres_url import get_checkpoint_database_url


@dataclass(frozen=True)
class Settings:
    app_name: str = "app-service"
    app_env: str = "dev"
    log_level: str = "INFO"
    redis_url: str = "redis://localhost:6379/0"
    checkpoint_database_url: str = ""
    task_status_ttl: int = 604800
    service_lock_enabled: bool = True
    service_lock_ttl_seconds: int = 300
    service_heartbeat_interval: int = 10
    service_heartbeat_ttl: int = 30


@lru_cache
def get_settings() -> Settings:
    return Settings(
        app_name=_env("SERVICE_APP_NAME", default="app-service"),
        app_env=_env("SERVICE_APP_ENV", default="dev"),
        log_level=_env("SERVICE_LOG_LEVEL", default="INFO"),
        redis_url=_env(
            "SERVICE_REDIS_URL",
            "WORKER_REDIS_URL",
            default="redis://localhost:6379/0",
        ),
        checkpoint_database_url=get_checkpoint_database_url(host_default="postgres"),
        task_status_ttl=int(os.getenv("TASK_STATUS_TTL", "604800")),
        service_lock_enabled=_parse_bool(_env("SERVICE_LOCK_ENABLED", default="true")),
        service_lock_ttl_seconds=int(_env("SERVICE_LOCK_TTL_SECONDS", default="300")),
        service_heartbeat_interval=int(_env("SERVICE_HEARTBEAT_INTERVAL", default="10")),
        service_heartbeat_ttl=int(_env("SERVICE_HEARTBEAT_TTL", default="30")),
    )


def _env(*names: str, default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
