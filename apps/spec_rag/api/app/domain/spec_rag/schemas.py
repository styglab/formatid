from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SpecRagRunCreateResponse(BaseModel):
    run_id: str
    graph_name: str
    queue_name: str
    status: str


class SpecRagRunResponse(BaseModel):
    run_id: str
    status: str
    current_node: str | None = None
    result: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None
