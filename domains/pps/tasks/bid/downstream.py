from __future__ import annotations

import logging
from typing import Any

from services.worker.runtime.logger import get_logger, log_event
from shared.tasking.catalog import get_task_definition
from shared.tasking.enqueue import enqueue_task
from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult
from domains.pps.tasks.config import get_settings
from domains.pps.tasks.job_keys import build_bid_notice_job_key, build_bid_number_job_key
from domains.pps.tasks.quota import get_quota_block
from domains.pps.tasks.repository import PpsRepository
from domains.pps.tasks.state import PpsTaskStateStore


logger = get_logger("domains.pps.tasks.bid.downstream")


@task("pps.bid.downstream.enqueue")
async def enqueue_bid_downstream(message: TaskMessage) -> TaskResult:
    payload = message.payload
    settings = get_settings()
    repository = PpsRepository(database_url=settings.database_url)
    state_store = PpsTaskStateStore(database_url=settings.database_url)
    limit = int(payload.get("limit", 100))
    max_failed_retries = int(payload.get("max_failed_retries", 3))
    retry_failed_after_seconds = int(payload.get("retry_failed_after_seconds", 86400))

    try:
        quota_blocked = await get_quota_block(redis_url=settings.redis_url) is not None
        notices = await repository.list_bid_notices_for_downstream(
            limit=limit,
            include_result_tasks=not quota_blocked,
            max_failed_retries=max_failed_retries,
            retry_failed_after_seconds=retry_failed_after_seconds,
        )
        enqueued_count = 0
        skipped_result_count = 0
        skipped_retry_limit_count = 0

        for notice in notices:
            bid_ntce_no = str(notice["bid_ntce_no"])
            bid_ntce_ord = str(notice["bid_ntce_ord"])
            enqueued, skipped, retry_limited = await _enqueue_downstream_tasks(
                redis_url=settings.redis_url,
                bid_ntce_no=bid_ntce_no,
                bid_ntce_ord=bid_ntce_ord,
                state_store=state_store,
                status_ttl=604800,
                skip_result_tasks=quota_blocked,
                max_failed_retries=max_failed_retries,
                retry_failed_after_seconds=retry_failed_after_seconds,
            )
            enqueued_count += enqueued
            skipped_result_count += skipped
            skipped_retry_limit_count += retry_limited

        result_payload = {
            "limit": limit,
            "max_failed_retries": max_failed_retries,
            "retry_failed_after_seconds": retry_failed_after_seconds,
            "notice_count": len(notices),
            "enqueued_count": enqueued_count,
            "skipped_result_count": skipped_result_count,
            "skipped_retry_limit_count": skipped_retry_limit_count,
            "quota_blocked": quota_blocked,
        }
        return TaskResult(
            task_id=message.task_id,
            task_name=message.task_name,
            status="succeeded",
            output=result_payload,
        )
    finally:
        await state_store.close()
        await repository.close()


async def _enqueue_downstream_tasks(
    *,
    redis_url: str | None,
    bid_ntce_no: str,
    bid_ntce_ord: str,
    state_store: PpsTaskStateStore,
    status_ttl: int,
    skip_result_tasks: bool,
    max_failed_retries: int,
    retry_failed_after_seconds: int,
) -> tuple[int, int, int]:
    if not redis_url:
        raise RuntimeError("WORKER_REDIS_URL is required for downstream enqueue")

    task_specs: tuple[tuple[str, str, dict[str, Any]], ...] = (
        (
            "attachment",
            "pps.bid.attachment.download",
            {"bidNtceNo": bid_ntce_no, "bidNtceOrd": bid_ntce_ord},
        ),
        (
            "participants",
            "pps.bid_result.participants.collect",
            {"bidNtceNo": bid_ntce_no, "bidNtceOrd": bid_ntce_ord},
        ),
        (
            "winners",
            "pps.bid_result.winners.collect",
            {"bidNtceNo": bid_ntce_no, "bidNtceOrd": bid_ntce_ord},
        ),
    )
    enqueued = 0
    skipped = 0
    retry_limited = 0

    for job_type, task_name, task_payload in task_specs:
        if skip_result_tasks and job_type in {"participants", "winners"}:
            skipped += 1
            continue

        if job_type == "attachment":
            job_key = build_bid_notice_job_key(bid_ntce_no=bid_ntce_no, bid_ntce_ord=bid_ntce_ord)
        else:
            job_key = build_bid_number_job_key(bid_ntce_no=bid_ntce_no)

        state = await state_store.get_state(job_type=job_type, job_key=job_key)
        if state is not None and state.get("status") in {"queued", "running", "succeeded"}:
            continue
        if state is not None and state.get("status") == "failed" and not _is_failed_state_retryable(
            state=state,
            max_failed_retries=max_failed_retries,
            retry_failed_after_seconds=retry_failed_after_seconds,
        ):
            retry_limited += 1
            continue
        if state is not None and state.get("status") == "failed":
            log_event(
                logger,
                logging.INFO,
                "downstream_task_reenqueued_due_to_failed_state",
                job_type=job_type,
                task_name=task_name,
                job_key=job_key,
                bid_ntce_no=bid_ntce_no,
                bid_ntce_ord=bid_ntce_ord,
                previous_status=state.get("status"),
            )

        definition = get_task_definition(task_name)
        await enqueue_task(
            redis_url=redis_url,
            queue_name=definition.queue_name,
            task_name=task_name,
            payload=task_payload,
            status_ttl=status_ttl,
        )
        await state_store.upsert_state(
            job_type=job_type,
            job_key=job_key,
            status="queued",
            payload=task_payload,
        )
        enqueued += 1

    return enqueued, skipped, retry_limited


def _is_failed_state_retryable(
    *,
    state: dict[str, Any],
    max_failed_retries: int,
    retry_failed_after_seconds: int,
) -> bool:
    retry_count = int(state.get("retry_count", 0))
    if retry_count >= max_failed_retries:
        return False
    last_failed_at = state.get("last_failed_at")
    if last_failed_at is None or retry_failed_after_seconds <= 0:
        return True

    from datetime import datetime, timedelta

    from shared.time import now

    if isinstance(last_failed_at, str):
        failed_at = datetime.fromisoformat(last_failed_at)
    else:
        failed_at = last_failed_at
    return failed_at <= now() - timedelta(seconds=retry_failed_after_seconds)
