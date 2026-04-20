from fastapi import APIRouter, Query

from services.api.app.schemas.observability import (
    ScheduleRunEntry,
    ScheduleRunListResponse,
    TaskExecutionEntry,
    TaskExecutionListResponse,
)
from services.api.app.services.observability_service import (
    list_schedule_runs,
    list_task_executions,
)


router = APIRouter(tags=["observability"])


@router.get("/schedule-runs", response_model=ScheduleRunListResponse)
async def get_schedule_runs(
    limit: int = Query(default=100, ge=1, le=1000),
    schedule_name: str | None = None,
) -> ScheduleRunListResponse:
    rows = await list_schedule_runs(limit=limit, schedule_name=schedule_name)
    return ScheduleRunListResponse(schedule_runs=[ScheduleRunEntry.model_validate(row) for row in rows])


@router.get("/task-executions", response_model=TaskExecutionListResponse)
async def get_task_executions(
    limit: int = Query(default=100, ge=1, le=1000),
    task_name: str | None = None,
    status: str | None = None,
) -> TaskExecutionListResponse:
    rows = await list_task_executions(limit=limit, task_name=task_name, status=status)
    return TaskExecutionListResponse(task_executions=[TaskExecutionEntry.model_validate(row) for row in rows])
