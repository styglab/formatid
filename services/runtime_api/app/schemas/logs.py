from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class LogSourceEntry(BaseModel):
    service_name: str
    source_type: str
    status: str | None = None
    worker_id: str | None = None
    queue_name: str | None = None
    last_seen_at: str | None = None


class LogSourceListResponse(BaseModel):
    sources: list[LogSourceEntry]


class ServiceLogEntry(BaseModel):
    id: int
    service_name: str
    worker_id: str | None = None
    level: str
    event_name: str | None = None
    message: str
    logger_name: str | None = None
    request_id: str | None = None
    run_name: str | None = None
    task_id: str | None = None
    correlation_id: str | None = None
    resource_key: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class ServiceLogListResponse(BaseModel):
    logs: list[ServiceLogEntry]
