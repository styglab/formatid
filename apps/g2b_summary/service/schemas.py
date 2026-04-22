from __future__ import annotations

from pydantic import BaseModel, Field


class CreateSummaryJobRequest(BaseModel):
    bucket: str | None = None
    object_key: str = Field(min_length=1)
    callback_url: str | None = None
