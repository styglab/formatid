from fastapi import APIRouter, Query

from services.api.app.schemas.observability import (
    ServiceEventEntry,
    ServiceEventListResponse,
    ServiceRequestEntry,
    ServiceRequestListResponse,
    ServiceRunEntry,
    ServiceRunListResponse,
    TaskExecutionEventEntry,
    TaskExecutionEventListResponse,
    TaskExecutionEntry,
    TaskExecutionListResponse,
)
from services.api.app.services.observability_service import (
    list_service_events,
    list_service_requests,
    list_service_runs,
    list_task_execution_events,
    list_task_executions,
)
from services.api.app.config import get_settings
from services.task_runtime.queue_control import get_queue_pause, pause_queue, resume_queue


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


@router.get("/task-executions", response_model=TaskExecutionListResponse)
@router.get("/observability/task-executions", response_model=TaskExecutionListResponse)
async def get_task_executions(
    limit: int = Query(default=100, ge=1, le=1000),
    queue_name: str | None = None,
    task_name: str | None = None,
    service_name: str | None = None,
    dedupe_key: str | None = None,
    correlation_id: str | None = None,
    resource_key: str | None = None,
    error_type: str | None = None,
    updated_after: str | None = None,
    updated_before: str | None = None,
    status: str | None = None,
) -> TaskExecutionListResponse:
    rows = await list_task_executions(
        limit=limit,
        queue_name=queue_name,
        task_name=task_name,
        service_name=service_name,
        dedupe_key=dedupe_key,
        correlation_id=correlation_id,
        resource_key=resource_key,
        error_type=error_type,
        updated_after=updated_after,
        updated_before=updated_before,
        status=status,
    )
    return TaskExecutionListResponse(task_executions=[TaskExecutionEntry.model_validate(row) for row in rows])


@router.get("/task-execution-events", response_model=TaskExecutionEventListResponse)
@router.get("/observability/task-execution-events", response_model=TaskExecutionEventListResponse)
async def get_task_execution_events(
    limit: int = Query(default=100, ge=1, le=1000),
    task_id: str | None = None,
    queue_name: str | None = None,
    task_name: str | None = None,
    service_name: str | None = None,
    error_type: str | None = None,
    status: str | None = None,
) -> TaskExecutionEventListResponse:
    rows = await list_task_execution_events(
        limit=limit,
        task_id=task_id,
        queue_name=queue_name,
        task_name=task_name,
        service_name=service_name,
        error_type=error_type,
        status=status,
    )
    return TaskExecutionEventListResponse(events=[TaskExecutionEventEntry.model_validate(row) for row in rows])


@router.get("/queues/{queue_name}/pause")
@router.get("/observability/queues/{queue_name}/pause")
async def get_queue_pause_state(queue_name: str) -> dict:
    return {"queue_name": queue_name, "pause": await get_queue_pause(redis_url=get_settings().redis_url, queue_name=queue_name)}


@router.post("/queues/{queue_name}/pause")
@router.post("/observability/queues/{queue_name}/pause")
async def post_queue_pause(
    queue_name: str,
    reason: str = "manual",
    ttl_seconds: int | None = Query(default=None, ge=1),
) -> dict:
    pause = await pause_queue(
        redis_url=get_settings().redis_url,
        queue_name=queue_name,
        reason=reason,
        ttl_seconds=ttl_seconds,
    )
    return {"queue_name": queue_name, "pause": pause}


@router.post("/queues/{queue_name}/resume")
@router.post("/observability/queues/{queue_name}/resume")
async def post_queue_resume(queue_name: str) -> dict:
    resumed = await resume_queue(redis_url=get_settings().redis_url, queue_name=queue_name)
    return {"queue_name": queue_name, "resumed": resumed}
