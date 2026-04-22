from typing import Any

from pydantic import BaseModel, Field


class ServiceRunEntry(BaseModel):
    id: int
    service_name: str | None = None
    run_name: str
    task_id: str | None = None
    queue_name: str | None = None
    task_name: str | None = None
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


class TaskExecutionEntry(BaseModel):
    task_id: str
    queue_name: str
    service_name: str | None = None
    task_name: str
    dedupe_key: str | None = None
    correlation_id: str | None = None
    resource_key: str | None = None
    status: str
    attempts: int
    worker_id: str | None = None
    enqueued_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: float | None = None
    last_heartbeat_at: str | None = None
    lease_expires_at: str | None = None
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    status_document: dict[str, Any]
    created_at: str
    updated_at: str


class TaskExecutionListResponse(BaseModel):
    task_executions: list[TaskExecutionEntry]


class TaskExecutionEventEntry(BaseModel):
    id: int
    task_id: str
    queue_name: str
    service_name: str | None = None
    task_name: str
    status: str
    attempts: int
    worker_id: str | None = None
    error: dict[str, Any] | None = None
    details: dict[str, Any]
    created_at: str


class TaskExecutionEventListResponse(BaseModel):
    events: list[TaskExecutionEventEntry]
