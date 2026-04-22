from __future__ import annotations

import asyncio

from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult
from apps.g2b_ingest.tasks.config import get_settings
from apps.g2b_ingest.tasks.http import G2bIngestApiClient, G2bIngestDailyQuotaExceededError, extract_items
from apps.g2b_ingest.tasks.job_keys import build_bid_number_job_key
from apps.g2b_ingest.tasks.quota import (
    build_quota_error_detail,
    build_quota_skipped_output,
    get_quota_block,
    mark_quota_blocked,
)
from apps.g2b_ingest.tasks.repository import G2bIngestRepository
from apps.g2b_ingest.tasks.state import G2bIngestTaskStateStore


@task("g2b_ingest.bid_result.participants.collect")
async def collect_bid_result_participants(message: TaskMessage) -> TaskResult:
    payload = message.payload
    settings = get_settings()
    client = G2bIngestApiClient()
    repository = G2bIngestRepository(database_url=settings.database_url)
    state_store = G2bIngestTaskStateStore(database_url=settings.database_url)
    job_key = build_bid_number_job_key(bid_ntce_no=payload["bidNtceNo"])

    try:
        quota_block = await get_quota_block(
            redis_url=settings.redis_url,
            database_url=settings.checkpoint_database_url,
        )
        quota_block = quota_block or await state_store.get_active_quota_block()
        if quota_block is not None:
            error_detail = build_quota_error_detail(
                blocked_until=quota_block.get("blocked_until"),
            )
            await state_store.upsert_state(
                job_type="participants",
                job_key=job_key,
                status="blocked",
                payload=payload,
                error=error_detail,
            )
            return TaskResult(
                task_id=message.task_id,
                task_name=message.task_name,
                status="succeeded",
                output=build_quota_skipped_output(payload=payload, quota_block=quota_block),
            )

        await state_store.upsert_state(job_type="participants", job_key=job_key, status="running", payload=payload)
        rows = []
        page_no = 1
        while True:
            response = await client.fetch_bid_result_participants(
                bid_ntce_no=payload["bidNtceNo"],
                page_no=page_no,
                num_of_rows=settings.api_num_of_rows,
            )
            items = extract_items(response)
            if not items:
                break
            rows.extend(items)
            if len(items) < settings.api_num_of_rows:
                break
            page_no += 1

        await repository.replace_bid_result_participants(
            bid_ntce_no=payload["bidNtceNo"],
            rows=rows,
        )
        result_payload = {
            "bidNtceNo": payload["bidNtceNo"],
            "participant_count": len(rows),
        }
        await state_store.upsert_state(
            job_type="participants",
            job_key=job_key,
            status="succeeded",
            payload=result_payload,
        )
        return TaskResult(
            task_id=message.task_id,
            task_name=message.task_name,
            status="succeeded",
            output=result_payload,
        )
    except G2bIngestDailyQuotaExceededError as exc:
        error_detail = await mark_quota_blocked(
            redis_url=settings.redis_url,
            database_url=settings.checkpoint_database_url,
            error_detail=exc.to_error_detail(),
        )
        await state_store.upsert_state(
            job_type="participants",
            job_key=job_key,
            status="blocked",
            payload=payload,
            error=error_detail,
        )
        return TaskResult(
            task_id=message.task_id,
            task_name=message.task_name,
            status="succeeded",
            output=build_quota_skipped_output(payload=payload, quota_block=error_detail),
        )
    except asyncio.CancelledError as exc:
        await state_store.upsert_state(
            job_type="participants",
            job_key=job_key,
            status="failed",
            payload=payload,
            error={
                "type": type(exc).__name__,
                "message": "task cancelled before participants collection completed",
            },
            max_attempts=settings.max_failed_retries,
            retry_after_seconds=settings.retry_failed_after_seconds,
        )
        raise
    except Exception as exc:
        await state_store.upsert_state(
            job_type="participants",
            job_key=job_key,
            status="failed",
            payload=payload,
            error={"type": type(exc).__name__, "message": str(exc)},
            max_attempts=settings.max_failed_retries,
            retry_after_seconds=settings.retry_failed_after_seconds,
        )
        raise
    finally:
        await state_store.close()
        await repository.close()
