from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LlmGenerateStorePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: dict[str, Any] = Field(default_factory=dict)
    source: dict[str, Any]
    target: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)
