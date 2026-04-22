from fastapi import APIRouter, Query

from services.api.app.services.dashboard_service import (
    build_app_dashboard_summary,
    build_dashboard_summary,
    list_dashboard_service_runs,
    list_app_dashboard_summaries,
    list_recent_failures,
    list_recent_tasks,
    list_task_duration_stats,
    list_task_trends,
)


router = APIRouter(tags=["dashboard"])


@router.get("/dashboard/summary")
async def get_dashboard_summary() -> dict:
    return await build_dashboard_summary()


@router.get("/dashboard/service-runs")
async def get_dashboard_service_runs() -> dict:
    return {"service_runs": await list_dashboard_service_runs()}


@router.get("/dashboard/task-trends")
async def get_dashboard_task_trends(hours: int = Query(default=24, ge=1, le=168)) -> dict:
    return {"hours": hours, "trends": await list_task_trends(hours=hours)}


@router.get("/dashboard/recent-failures")
async def get_dashboard_recent_failures(limit: int = Query(default=50, ge=1, le=500)) -> dict:
    return {"failures": await list_recent_failures(limit=limit)}


@router.get("/dashboard/recent-tasks")
async def get_dashboard_recent_tasks(limit: int = Query(default=50, ge=1, le=500)) -> dict:
    return {"tasks": await list_recent_tasks(limit=limit)}


@router.get("/dashboard/task-duration-stats")
async def get_dashboard_task_duration_stats(hours: int = Query(default=24, ge=1, le=168)) -> dict:
    return {"hours": hours, "duration_stats": await list_task_duration_stats(hours=hours)}


@router.get("/dashboard/apps")
async def get_dashboard_apps() -> dict:
    return {"apps": await list_app_dashboard_summaries()}


@router.get("/dashboard/apps/{app_name}/summary")
async def get_dashboard_app_summary(app_name: str) -> dict:
    return await build_app_dashboard_summary(app_name)
