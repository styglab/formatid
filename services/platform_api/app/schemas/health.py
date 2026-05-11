from typing import Literal

from pydantic import BaseModel


class LivenessResponse(BaseModel):
    status: Literal["ok"]


class RedisHealth(BaseModel):
    ok: bool
    url: str
    error: str | None = None


class AppServicesHealthSummary(BaseModel):
    status: Literal["healthy", "degraded", "down", "not_configured"]
    services: int


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    evaluated_at: str
    redis: RedisHealth
    app_services: AppServicesHealthSummary | None = None


class HealthResponse(BaseModel):
    status: Literal["healthy", "degraded", "down"]
    evaluated_at: str
    redis: RedisHealth
    app_services: AppServicesHealthSummary | None = None


class AppServiceHealthEntry(BaseModel):
    service_id: str
    app_name: str
    hostname: str
    pid: int
    status: str
    updated_at: str
    age_seconds: float
    health_status: str


class AppServicesHealthResponse(BaseModel):
    evaluated_at: str
    status: Literal["healthy", "degraded", "down", "not_configured"]
    policy: dict
    service_count: int
    healthy_services: int
    stale_services: int
    down_services: int
    services: list[AppServiceHealthEntry]
    redis_url: str
