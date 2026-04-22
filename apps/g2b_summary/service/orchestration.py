from __future__ import annotations

import os
from uuid import uuid4

from fastapi import HTTPException

from apps.g2b_summary.service.constants import (
    EXTRACT_TEXT_QUEUE,
    EXTRACT_TEXT_TASK,
    SERVE_LLM_QUEUE,
    SERVE_LLM_TASK,
)
from apps.g2b_summary.service.payloads import build_llm_generate_payload
from apps.g2b_summary.service.schemas import CreateSummaryJobRequest
from apps.g2b_summary.service.steps import build_summary_plan
from apps.g2b_summary.tasks.repository import SummaryRepository
from services.task_runtime.enqueue import enqueue_task


async def create_summary_job(request: CreateSummaryJobRequest) -> dict:
    job_id = f"sum_{uuid4().hex}"
    bucket = request.bucket or _required_env("G2B_SUMMARY_S3_BUCKET")
    graph_state = await build_summary_plan(
        job_id=job_id,
        bucket=bucket,
        object_key=request.object_key,
        callback_url=request.callback_url,
    )
    repository = SummaryRepository()
    try:
        await repository.create_job(
            job_id=job_id,
            bucket=bucket,
            object_key=request.object_key,
            callback_url=request.callback_url,
        )
        await repository.set_status(job_id=job_id, status="extracting")
    finally:
        await repository.close()

    await enqueue_task(
        redis_url=_redis_url(),
        queue_name=EXTRACT_TEXT_QUEUE,
        task_name=EXTRACT_TEXT_TASK,
        payload=graph_state["extract_payload"],
        status_ttl=_task_status_ttl(),
        dedupe_key=f"g2b_summary_extract:{job_id}",
        correlation_id=job_id,
        resource_key=job_id,
    )
    return {"job_id": job_id, "status": "extracting", "graph": graph_state["graph"]}


async def get_summary_job(*, job_id: str) -> dict:
    await sync_job_progress(job_id=job_id)
    repository = SummaryRepository()
    try:
        job = await repository.get_job(job_id=job_id)
    finally:
        await repository.close()
    if job is None:
        raise HTTPException(status_code=404, detail="summary job not found")
    return job


async def list_summary_job_events(*, job_id: str, limit: int = 50) -> dict:
    await sync_job_progress(job_id=job_id)
    repository = SummaryRepository()
    try:
        events = await repository.list_events(job_id=job_id, limit=limit)
    finally:
        await repository.close()
    return {"job_id": job_id, "events": events}


async def sync_job_progress(*, job_id: str) -> None:
    repository = SummaryRepository()
    try:
        job = await repository.get_job(job_id=job_id)
        if job is None:
            return
        if job.get("summary_text") is not None:
            if job["status"] != "succeeded":
                await repository.set_status(
                    job_id=job_id,
                    status="succeeded",
                    details={"source": "app_polling"},
                )
            return

        failed_task = await repository.get_latest_failed_task(job_id=job_id)
        if failed_task is not None:
            if job["status"] != "failed":
                await repository.set_failed(job_id=job_id, error=failed_task)
            return

        if job.get("char_count") is None:
            if job["status"] not in {"queued", "extracting"}:
                await repository.set_status(job_id=job_id, status="extracting", details={"source": "app_polling"})
            return

        if job["status"] != "summarizing":
            await repository.set_status(
                job_id=job_id,
                status="summarizing",
                details={"source": "app_polling", "char_count": job.get("char_count")},
            )
        await enqueue_task(
            redis_url=_redis_url(),
            queue_name=SERVE_LLM_QUEUE,
            task_name=SERVE_LLM_TASK,
            payload=build_llm_generate_payload(
                job_id=job_id,
                callback_url=job.get("callback_url"),
            ),
            status_ttl=_task_status_ttl(),
            dedupe_key=f"g2b_summary_mock_generate:{job_id}",
            correlation_id=job_id,
            resource_key=job_id,
        )
    finally:
        await repository.close()


def _redis_url() -> str:
    return os.getenv("SERVICE_REDIS_URL", os.getenv("WORKER_REDIS_URL", "redis://redis:6379/0"))


def _task_status_ttl() -> int:
    return int(os.getenv("TASK_STATUS_TTL", "604800"))


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not configured")
    return value
