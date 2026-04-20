from fastapi import APIRouter, HTTPException

from services.api.app.schemas.health import (
    HealthResponse,
    LivenessResponse,
    ReadinessResponse,
    WorkersHealthResponse,
)
from services.api.app.services.health_service import (
    build_health_summary,
    build_readiness,
    get_workers_health_report,
)


router = APIRouter(tags=["health"])


@router.get("/health/live", response_model=LivenessResponse)
async def get_liveness() -> LivenessResponse:
    return LivenessResponse(status="ok")


@router.get("/health/ready", response_model=ReadinessResponse)
async def get_readiness() -> ReadinessResponse:
    readiness = await build_readiness()
    if readiness.status == "not_ready":
        raise HTTPException(status_code=503, detail=readiness.model_dump())
    return readiness


@router.get("/health", response_model=HealthResponse)
async def get_health() -> HealthResponse:
    summary = await build_health_summary()
    if summary.status == "down":
        raise HTTPException(status_code=503, detail=summary.model_dump())
    return summary


@router.get("/health/workers", response_model=WorkersHealthResponse)
async def get_workers_health() -> WorkersHealthResponse:
    report = await get_workers_health_report()
    return WorkersHealthResponse.model_validate(report)
