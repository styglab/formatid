from typing import Any

from pydantic import BaseModel, Field


class ServiceRunEntry(BaseModel):
    id: int
    service_name: str | None = None
    run_name: str
    status: str
    skip_reason: str | None = None
    payload: dict[str, Any]
    details: dict[str, Any]
    error: dict[str, Any] | None = None
    trigger_type: str | None = None
    trigger_config: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    resource_key: str | None = None
    lock_acquired: bool | None = None
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: float | None = None
    created_at: str


class ServiceRunListResponse(BaseModel):
    service_runs: list[ServiceRunEntry]


class ServiceRequestEntry(BaseModel):
    id: int
    service_name: str
    request_id: str
    method: str | None = None
    path: str | None = None
    correlation_id: str | None = None
    resource_key: str | None = None
    status: str
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    duration_ms: float | None = None
    created_at: str


class ServiceRequestListResponse(BaseModel):
    service_requests: list[ServiceRequestEntry]


class ServiceEventEntry(BaseModel):
    id: int
    service_name: str
    event_name: str
    request_id: str | None = None
    run_name: str | None = None
    correlation_id: str | None = None
    resource_key: str | None = None
    details: dict[str, Any]
    created_at: str


class ServiceEventListResponse(BaseModel):
    service_events: list[ServiceEventEntry]
