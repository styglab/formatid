import os
from dataclasses import dataclass
from functools import lru_cache

from core.runtime.runtime_db.url import build_postgres_url, get_checkpoint_database_url


@dataclass(frozen=True)
class G2bIngestSettings:
    public_api_key: str = ""
    database_url: str = ""
    checkpoint_database_url: str = ""
    redis_url: str = "redis://localhost:6379/0"
    api_timeout_seconds: float = 30.0
    api_num_of_rows: int = 100
    max_failed_retries: int = 3
    retry_failed_after_seconds: int = 86400
    stale_running_after_seconds: int = 600
    s3_endpoint: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = ""
    s3_region: str = "us-east-1"
    s3_secure: bool = False


@lru_cache
def get_settings() -> G2bIngestSettings:
    return G2bIngestSettings(
        public_api_key=os.getenv("PUBLIC_API_KEY", ""),
        database_url=os.getenv("G2B_INGEST_DATABASE_URL") or build_postgres_url(host_default="localhost"),
        checkpoint_database_url=get_checkpoint_database_url(host_default="localhost"),
        redis_url=os.getenv(
            "WORKER_REDIS_URL",
            os.getenv("SERVICE_REDIS_URL", "redis://localhost:6379/0"),
        ),
        api_timeout_seconds=float(os.getenv("G2B_INGEST_API_TIMEOUT_SECONDS", "30")),
        api_num_of_rows=int(os.getenv("G2B_INGEST_API_NUM_OF_ROWS", "100")),
        max_failed_retries=int(os.getenv("G2B_INGEST_MAX_FAILED_RETRIES", os.getenv("TASK_MAX_RETRIES", "3"))),
        retry_failed_after_seconds=int(os.getenv("G2B_INGEST_RETRY_FAILED_AFTER_SECONDS", "86400")),
        stale_running_after_seconds=int(os.getenv("G2B_INGEST_STALE_RUNNING_AFTER_SECONDS", "600")),
        s3_endpoint=os.getenv("G2B_INGEST_S3_ENDPOINT", ""),
        s3_access_key=os.getenv("G2B_INGEST_S3_ACCESS_KEY", ""),
        s3_secret_key=os.getenv("G2B_INGEST_S3_SECRET_KEY", ""),
        s3_bucket=os.getenv("G2B_INGEST_S3_BUCKET", ""),
        s3_region=os.getenv("G2B_INGEST_S3_REGION", "us-east-1"),
        s3_secure=os.getenv("G2B_INGEST_S3_SECURE", "false").lower() in {"1", "true", "yes", "on"},
    )
