from __future__ import annotations

from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult
from tasks.pps.config import get_settings
from tasks.pps.http import PpsApiClient, PpsDailyQuotaExceededError, extract_items
from tasks.pps.job_keys import build_bid_number_job_key
from tasks.pps.quota import (
    build_quota_error_detail,
    build_quota_skipped_output,
    get_quota_block,
    mark_quota_blocked,
)
from tasks.pps.repository import PpsRepository
from tasks.pps.state import PpsTaskStateStore


@task("pps.bid_result.winners.collect")
async def collect_bid_result_winners(message: TaskMessage) -> TaskResult:
    payload = message.payload
    settings = get_settings()
    client = PpsApiClient()
    repository = PpsRepository(database_url=settings.database_url)
    state_store = PpsTaskStateStore(database_url=settings.database_url)
    job_key = build_bid_number_job_key(bid_ntce_no=payload["bidNtceNo"])

    try:
        quota_block = await get_quota_block(redis_url=settings.redis_url)
        if quota_block is not None:
            error_detail = build_quota_error_detail(
                blocked_until=quota_block.get("blocked_until"),
            )
            await state_store.upsert_state(
                job_type="winners",
                job_key=job_key,
                status="failed",
                payload=payload,
                error=error_detail,
            )
            return TaskResult(
                task_id=message.task_id,
                task_name=message.task_name,
                status="succeeded",
                output=build_quota_skipped_output(payload=payload, quota_block=quota_block),
            )

        await state_store.upsert_state(job_type="winners", job_key=job_key, status="running", payload=payload)
        rows = []
        page_no = 1
        while True:
            response = await client.fetch_bid_result_winners(
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

        await repository.replace_bid_result_winners(
            bid_ntce_no=payload["bidNtceNo"],
            rows=rows,
        )
        result_payload = {
            "bidNtceNo": payload["bidNtceNo"],
            "winner_count": len(rows),
        }
        await state_store.upsert_state(
            job_type="winners",
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
    except PpsDailyQuotaExceededError as exc:
        error_detail = await mark_quota_blocked(
            redis_url=settings.redis_url,
            error_detail=exc.to_error_detail(),
        )
        await state_store.upsert_state(
            job_type="winners",
            job_key=job_key,
            status="failed",
            payload=payload,
            error=error_detail,
        )
        return TaskResult(
            task_id=message.task_id,
            task_name=message.task_name,
            status="succeeded",
            output=build_quota_skipped_output(payload=payload, quota_block=error_detail),
        )
    except Exception as exc:
        await state_store.upsert_state(
            job_type="winners",
            job_key=job_key,
            status="failed",
            payload=payload,
            error={"type": type(exc).__name__, "message": str(exc)},
        )
        raise
    finally:
        await state_store.close()
        await repository.close()
