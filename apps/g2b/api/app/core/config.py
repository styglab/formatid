from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel

from core.runtime.runtime_db.url import get_checkpoint_database_url


class Settings(BaseModel):
    checkpoint_database_url: str


@lru_cache
def get_settings() -> Settings:
    return Settings(checkpoint_database_url=get_checkpoint_database_url(host_default="postgres"))
