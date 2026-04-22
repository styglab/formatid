from __future__ import annotations

import os
from urllib.parse import quote


def build_postgres_url(*, host_default: str = "localhost") -> str:
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", host_default)
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "postgres")
    return (
        f"postgresql://{quote(user)}:{quote(password)}"
        f"@{host}:{port}/{quote(database)}"
    )


def get_checkpoint_database_url(*, host_default: str = "localhost") -> str:
    return os.getenv("CHECKPOINT_DATABASE_URL") or build_postgres_url(host_default=host_default)


def get_database_url(env_name: str, *, host_default: str = "postgres") -> str:
    value = os.getenv(env_name)
    if value:
        return value
    if env_name == "POSTGRES_DATABASE_URL":
        return build_postgres_url(host_default=host_default)
    raise RuntimeError(f"{env_name} is not configured")
