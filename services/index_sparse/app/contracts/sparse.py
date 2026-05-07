from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SparseIndexPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: dict[str, Any]
    target: dict[str, Any]
    request: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SparseIndexOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_key: str
    document_count: int = Field(ge=0)
    index: str
