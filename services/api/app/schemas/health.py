from typing import Any, Literal

from pydantic import BaseModel


class LivenessResponse(BaseModel):
    status: Literal["ok"]


class RedisHealth(BaseModel):
    ok: bool
    url: str
    error: str | None = None


class ServiceHealthSummary(BaseModel):
    status: Literal["healthy", "degraded", "down"]
    queue_size: int
    workers: int


class SchedulerHealthSummary(BaseModel):
    status: Literal["healthy", "degraded", "down"]
    schedulers: int


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    evaluated_at: str
    redis: RedisHealth
    services: dict[str, ServiceHealthSummary]
    scheduler: SchedulerHealthSummary | None = None


class HealthResponse(BaseModel):
    status: Literal["healthy", "degraded", "down"]
    evaluated_at: str
    redis: RedisHealth
    services: dict[str, ServiceHealthSummary]
    scheduler: SchedulerHealthSummary | None = None


class WorkerHealthEntry(BaseModel):
    worker_id: str
    app_name: str
    queue_name: str
    hostname: str
    pid: int
    status: str
    updated_at: str
    age_seconds: float
    health_status: str


class ServiceHealthDetails(BaseModel):
    size: int
    status: Literal["healthy", "degraded", "down"]
    expected_workers: int
    observed_workers: int
    healthy_workers: int
    stale_workers: int
    down_workers: int


class WorkersHealthResponse(BaseModel):
    evaluated_at: str
    policy: dict[str, Any]
    queues: dict[str, ServiceHealthDetails]
    workers: dict[str, list[WorkerHealthEntry]]
    redis_url: str


class SchedulerHealthEntry(BaseModel):
    scheduler_id: str
    app_name: str
    hostname: str
    pid: int
    status: str
    updated_at: str
    age_seconds: float
    health_status: str


class SchedulerHealthResponse(BaseModel):
    evaluated_at: str
    status: Literal["healthy", "degraded", "down"]
    policy: dict[str, Any]
    scheduler_count: int
    healthy_schedulers: int
    stale_schedulers: int
    down_schedulers: int
    schedulers: list[SchedulerHealthEntry]
    redis_url: str
