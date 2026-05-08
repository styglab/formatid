from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DocumentChunkPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: dict[str, Any]
    target: dict[str, Any]
    options: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DocumentChunkOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_key: str
    chunk_count: int = Field(ge=0)
