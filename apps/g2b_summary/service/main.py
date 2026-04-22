from __future__ import annotations

import os

from fastapi import FastAPI

from apps.g2b_summary.service.orchestration import (
    create_summary_job as create_summary_job_handler,
    get_summary_job as get_summary_job_handler,
    list_summary_job_events,
)
from apps.g2b_summary.service.schemas import CreateSummaryJobRequest
from apps.g2b_summary.tasks.repository import get_summary_database_url
from services.app_service.runtime.middleware import ServiceRequestMiddleware


app = FastAPI(title="g2b-summary", version="0.1.0")
app.add_middleware(
    ServiceRequestMiddleware,
    service_name="g2b-summary",
    database_url=os.getenv("G2B_SUMMARY_DATABASE_URL") or get_summary_database_url(),
)


@app.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/summary/jobs")
async def create_summary_job(request: CreateSummaryJobRequest) -> dict:
    return await create_summary_job_handler(request)


@app.get("/summary/jobs/{job_id}")
async def get_summary_job(job_id: str) -> dict:
    return await get_summary_job_handler(job_id=job_id)


@app.get("/summary/jobs/{job_id}/events")
async def get_summary_job_events(job_id: str, limit: int = 50) -> dict:
    return await list_summary_job_events(job_id=job_id, limit=limit)
