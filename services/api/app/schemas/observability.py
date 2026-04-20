from typing import Any

from pydantic import BaseModel


class ScheduleRunEntry(BaseModel):
    id: int
    schedule_name: str
    task_id: str | None = None
    queue_name: str | None = None
    task_name: str | None = None
    status: str
    skip_reason: str | None = None
    payload: dict[str, Any]
    details: dict[str, Any]
    created_at: str


class ScheduleRunListResponse(BaseModel):
    schedule_runs: list[ScheduleRunEntry]


class TaskExecutionEntry(BaseModel):
    task_id: str
    queue_name: str
    task_name: str
    status: str
    attempts: int
    worker_id: str | None = None
    enqueued_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: float | None = None
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    status_document: dict[str, Any]
    created_at: str
    updated_at: str


class TaskExecutionListResponse(BaseModel):
    task_executions: list[TaskExecutionEntry]
