from __future__ import annotations

import os

from pydantic import BaseModel, Field


class SpecRagApiSettings(BaseModel):
    redis_url: str = Field(default_factory=lambda: os.getenv("APP_REDIS_URL", "redis://redis:6379/0"))
    checkpoint_database_url: str = Field(
        default_factory=lambda: os.getenv("CHECKPOINT_DATABASE_URL", _postgres_url())
    )
    graph_queue_name: str = Field(default_factory=lambda: os.getenv("SPEC_RAG_INDEX_QUEUE", "spec-rag:index"))
    upload_schema: str = "spec_rag"
    upload_table: str = "documents"


def get_settings() -> SpecRagApiSettings:
    return SpecRagApiSettings()


def _postgres_url() -> str:
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"
