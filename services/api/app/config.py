import os
from dataclasses import dataclass
from functools import lru_cache

from shared.postgres_url import get_checkpoint_database_url


@dataclass(frozen=True)
class Settings:
    app_name: str = "api"
    app_env: str = "dev"
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 8000
    redis_url: str = "redis://localhost:6379/0"
    checkpoint_database_url: str = ""
    worker_heartbeat_interval: int = 10
    worker_heartbeat_ttl: int = 30
    scheduler_heartbeat_interval: int = 10
    scheduler_heartbeat_ttl: int = 30


@lru_cache
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("API_APP_NAME", "api"),
        app_env=os.getenv("API_APP_ENV", "dev"),
        log_level=os.getenv("API_LOG_LEVEL", "INFO"),
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        redis_url=os.getenv("API_REDIS_URL", os.getenv("WORKER_REDIS_URL", "redis://localhost:6379/0")),
        checkpoint_database_url=get_checkpoint_database_url(host_default="postgres"),
        worker_heartbeat_interval=int(os.getenv("WORKER_HEARTBEAT_INTERVAL", "10")),
        worker_heartbeat_ttl=int(os.getenv("WORKER_HEARTBEAT_TTL", "30")),
        scheduler_heartbeat_interval=int(os.getenv("SCHEDULER_HEARTBEAT_INTERVAL", "10")),
        scheduler_heartbeat_ttl=int(os.getenv("SCHEDULER_HEARTBEAT_TTL", "30")),
    )
