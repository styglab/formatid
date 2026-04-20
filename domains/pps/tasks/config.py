import os
from dataclasses import dataclass
from functools import lru_cache

from shared.postgres_url import build_postgres_url, get_checkpoint_database_url


@dataclass(frozen=True)
class PpsSettings:
    public_api_key: str = ""
    database_url: str = ""
    checkpoint_database_url: str = ""
    redis_url: str = "redis://localhost:6379/0"
    api_timeout_seconds: float = 30.0
    api_num_of_rows: int = 100
    s3_endpoint: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = ""
    s3_region: str = "us-east-1"
    s3_secure: bool = False


@lru_cache
def get_settings() -> PpsSettings:
    return PpsSettings(
        public_api_key=os.getenv("PUBLIC_API_KEY", ""),
        database_url=os.getenv("PPS_DATABASE_URL") or build_postgres_url(host_default="localhost"),
        checkpoint_database_url=get_checkpoint_database_url(host_default="localhost"),
        redis_url=os.getenv(
            "WORKER_REDIS_URL",
            os.getenv("SCHEDULER_REDIS_URL", "redis://localhost:6379/0"),
        ),
        api_timeout_seconds=float(os.getenv("PPS_API_TIMEOUT_SECONDS", "30")),
        api_num_of_rows=int(os.getenv("PPS_API_NUM_OF_ROWS", "100")),
        s3_endpoint=os.getenv("PPS_S3_ENDPOINT", ""),
        s3_access_key=os.getenv("PPS_S3_ACCESS_KEY", ""),
        s3_secret_key=os.getenv("PPS_S3_SECRET_KEY", ""),
        s3_bucket=os.getenv("PPS_S3_BUCKET", ""),
        s3_region=os.getenv("PPS_S3_REGION", "us-east-1"),
        s3_secure=os.getenv("PPS_S3_SECURE", "false").lower() in {"1", "true", "yes", "on"},
    )
