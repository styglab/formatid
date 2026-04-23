from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GraphProgress(BaseModel):
    current: int = 0
    total: int | None = None
    percent: float | None = None


class GraphRunResponse(BaseModel):
    run_id: str
    service_name: str
    graph_name: str
    trigger_type: str
    status: str
    current_step: str | None = None
    completed_steps: list[str] = Field(default_factory=list)
    progress: GraphProgress
    params: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    started_at: str | None = None
    updated_at: str
    finished_at: str | None = None
    created_at: str


class GraphRunListResponse(BaseModel):
    graph_runs: list[GraphRunResponse]


class GraphNodeRunResponse(BaseModel):
    id: int
    run_id: str
    graph_name: str
    node_name: str
    status: str
    input_summary: dict[str, Any] = Field(default_factory=dict)
    output_summary: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: float | None = None
    created_at: str
    updated_at: str
