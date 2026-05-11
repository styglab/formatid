from fastapi import APIRouter, Query

from services.platform_api.app.schemas.observability import (
    ServiceEventEntry,
    ServiceEventListResponse,
    ServiceRequestEntry,
    ServiceRequestListResponse,
    ServiceRunEntry,
    ServiceRunListResponse,
)
from services.platform_api.app.services.observability_service import (
    list_service_events,
    list_service_requests,
    list_service_runs,
)


router = APIRouter(tags=["observability"])


@router.get("/service-runs", response_model=ServiceRunListResponse)
@router.get("/observability/service-runs", response_model=ServiceRunListResponse)
async def get_service_runs(
    limit: int = Query(default=100, ge=1, le=1000),
    run_name: str | None = None,
) -> ServiceRunListResponse:
    rows = await list_service_runs(limit=limit, run_name=run_name)
    return ServiceRunListResponse(service_runs=[ServiceRunEntry.model_validate(row) for row in rows])


@router.get("/service-requests", response_model=ServiceRequestListResponse)
@router.get("/observability/service-requests", response_model=ServiceRequestListResponse)
async def get_service_requests(
    limit: int = Query(default=100, ge=1, le=1000),
    service_name: str | None = None,
    request_id: str | None = None,
    status: str | None = None,
) -> ServiceRequestListResponse:
    rows = await list_service_requests(
        limit=limit,
        service_name=service_name,
        request_id=request_id,
        status=status,
    )
    return ServiceRequestListResponse(
        service_requests=[ServiceRequestEntry.model_validate(row) for row in rows]
    )


@router.get("/service-events", response_model=ServiceEventListResponse)
@router.get("/observability/service-events", response_model=ServiceEventListResponse)
async def get_service_events(
    limit: int = Query(default=100, ge=1, le=1000),
    service_name: str | None = None,
    event_name: str | None = None,
    request_id: str | None = None,
    run_name: str | None = None,
) -> ServiceEventListResponse:
    rows = await list_service_events(
        limit=limit,
        service_name=service_name,
        event_name=event_name,
        request_id=request_id,
        run_name=run_name,
    )
    return ServiceEventListResponse(service_events=[ServiceEventEntry.model_validate(row) for row in rows])
