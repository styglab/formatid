from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DenseIndexPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: dict[str, Any]
    target: dict[str, Any]
    request: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DenseIndexOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_key: str
    vector_count: int = Field(ge=0)
    collection: str
    dimensions: int = Field(gt=0)
