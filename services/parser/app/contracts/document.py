from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DocumentParsePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: dict[str, Any]
    target: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)


class DocumentParseOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_key: str
    bucket: str
    object_key: str
    char_count: int = Field(ge=0)
