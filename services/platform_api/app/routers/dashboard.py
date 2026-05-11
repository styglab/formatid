from fastapi import APIRouter

from services.platform_api.app.services.dashboard_service import (
    build_app_dashboard_summary,
    build_dashboard_summary,
    list_app_dashboard_summaries,
    list_dashboard_service_runs,
)


router = APIRouter(tags=["dashboard"])


@router.get("/dashboard/summary")
async def get_dashboard_summary() -> dict:
    return await build_dashboard_summary()


@router.get("/dashboard/service-runs")
async def get_dashboard_service_runs() -> dict:
    return {"service_runs": await list_dashboard_service_runs()}


@router.get("/dashboard/apps")
async def get_dashboard_apps() -> dict:
    return {"apps": await list_app_dashboard_summaries()}


@router.get("/dashboard/apps/{app_name}/summary")
async def get_dashboard_app_summary(app_name: str) -> dict:
    return await build_app_dashboard_summary(app_name)
