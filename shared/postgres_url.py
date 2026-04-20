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
