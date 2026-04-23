from __future__ import annotations

from fastapi import APIRouter, Query

from services.runtime_api.app.schemas.logs import (
    LogSourceEntry,
    LogSourceListResponse,
    ServiceLogEntry,
    ServiceLogListResponse,
)
from services.runtime_api.app.services.log_service import list_log_sources, list_service_logs


router = APIRouter(tags=["logs"])


@router.get("/logs/services", response_model=LogSourceListResponse)
async def get_log_sources() -> LogSourceListResponse:
    rows = await list_log_sources()
    return LogSourceListResponse(sources=[LogSourceEntry.model_validate(row) for row in rows])


@router.get("/logs", response_model=ServiceLogListResponse)
async def get_service_logs(
    limit: int = Query(default=200, ge=1, le=1000),
    service_name: str | None = None,
    worker_id: str | None = None,
    level: str | None = None,
    event_name: str | None = None,
    after_id: int | None = Query(default=None, ge=1),
) -> ServiceLogListResponse:
    rows = await list_service_logs(
        limit=limit,
        service_name=service_name,
        worker_id=worker_id,
        level=level,
        event_name=event_name,
        after_id=after_id,
    )
    return ServiceLogListResponse(logs=[ServiceLogEntry.model_validate(row) for row in rows])
