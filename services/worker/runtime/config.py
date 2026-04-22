import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    app_name: str = "worker"
    app_env: str = "dev"
    log_level: str = "INFO"
    queue_poll_interval: float = 1.0
    redis_url: str = "redis://localhost:6379/0"
    worker_queue_name: str = "ingest:api"
    redis_block_timeout: int = 5
    worker_heartbeat_interval: int = 10
    worker_heartbeat_ttl: int = 30
    worker_shutdown_timeout_seconds: int = 30
    task_status_ttl: int = 604800
    task_max_retries: int = 3
    task_retry_delay_seconds: int = 0
    task_timeout_seconds: int = 30
    task_dlq_suffix: str = "dlq"
    worker_log_dir: str = str(Path(__file__).resolve().parents[3] / "logs")
    worker_log_to_file: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("WORKER_APP_NAME", "worker"),
        app_env=os.getenv("WORKER_APP_ENV", "dev"),
        log_level=os.getenv("WORKER_LOG_LEVEL", "INFO"),
        queue_poll_interval=float(os.getenv("WORKER_QUEUE_POLL_INTERVAL", "1.0")),
        redis_url=os.getenv("WORKER_REDIS_URL", "redis://localhost:6379/0"),
        worker_queue_name=os.getenv("WORKER_QUEUE_NAME", "ingest:api"),
        redis_block_timeout=int(os.getenv("WORKER_REDIS_BLOCK_TIMEOUT", "5")),
        worker_heartbeat_interval=int(os.getenv("WORKER_HEARTBEAT_INTERVAL", "10")),
        worker_heartbeat_ttl=int(os.getenv("WORKER_HEARTBEAT_TTL", "30")),
        worker_shutdown_timeout_seconds=int(os.getenv("WORKER_SHUTDOWN_TIMEOUT_SECONDS", "30")),
        task_status_ttl=int(os.getenv("TASK_STATUS_TTL", "604800")),
        task_max_retries=int(os.getenv("TASK_MAX_RETRIES", "3")),
        task_retry_delay_seconds=int(os.getenv("TASK_RETRY_DELAY_SECONDS", "0")),
        task_timeout_seconds=int(os.getenv("TASK_TIMEOUT_SECONDS", "30")),
        task_dlq_suffix=os.getenv("TASK_DLQ_SUFFIX", "dlq"),
        worker_log_dir=os.getenv("WORKER_LOG_DIR", str(Path(__file__).resolve().parents[3] / "logs")),
        worker_log_to_file=os.getenv("WORKER_LOG_TO_FILE", "true").lower() in {"1", "true", "yes", "on"},
    )
