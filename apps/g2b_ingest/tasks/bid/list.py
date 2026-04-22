from __future__ import annotations

import asyncio
from typing import Any

from shared.checkpoints.postgres import PostgresCheckpointStore
from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult
from apps.g2b_ingest.tasks.checkpoints import set_bid_list_window_checkpoint
from apps.g2b_ingest.tasks.config import get_settings
from apps.g2b_ingest.tasks.http import (
    G2bIngestApiClient,
    G2bIngestDailyQuotaExceededError,
    extract_items,
    extract_total_count,
)
from apps.g2b_ingest.tasks.job_keys import (
    build_bid_list_page_job_key,
    build_bid_list_window_job_key,
)
from apps.g2b_ingest.tasks.quota import (
    build_quota_error_detail,
    build_quota_skipped_output,
    get_quota_block,
    mark_quota_blocked,
)
from apps.g2b_ingest.tasks.repository import G2bIngestRepository
from apps.g2b_ingest.tasks.state import G2bIngestTaskStateStore


@task("g2b_ingest.bid.list.collect")
async def collect_bid_list(message: TaskMessage) -> TaskResult:
    payload = message.payload
    settings = get_settings()
    client = G2bIngestApiClient()
    repository = G2bIngestRepository(database_url=settings.database_url)
    state_store = G2bIngestTaskStateStore(database_url=settings.database_url)
    checkpoint_store = PostgresCheckpointStore(database_url=settings.checkpoint_database_url)
    page_no = int(payload.get("pageNo", 1))
    window_job_key = build_bid_list_window_job_key(
        inqry_bgn_dt=payload["inqryBgnDt"],
        inqry_end_dt=payload["inqryEndDt"],
    )
    page_job_key = build_bid_list_page_job_key(
        inqry_bgn_dt=payload["inqryBgnDt"],
        inqry_end_dt=payload["inqryEndDt"],
        page_no=page_no,
    )

    notice_count = 0

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
            await state_store.upsert_bid_list_window_state(
                job_key=window_job_key,
                status="blocked",
                payload=payload,
                error=error_detail,
            )
            await state_store.upsert_state(
                job_type="bid_list_page",
                job_key=page_job_key,
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

        await state_store.upsert_bid_list_window_state(
            job_key=window_job_key,
            status="running",
            payload=payload,
        )
        await state_store.upsert_state(
            job_type="bid_list_page",
            job_key=page_job_key,
            status="running",
            payload=payload,
        )

        response = await client.fetch_bid_list(
            inqry_bgn_dt=payload["inqryBgnDt"],
            inqry_end_dt=payload["inqryEndDt"],
            page_no=page_no,
            num_of_rows=settings.api_num_of_rows,
        )
        items = extract_items(response)
        total_count = extract_total_count(response)

        for row in items:
            bid_ntce_no = str(row.get("bidNtceNo", "")).strip()
            bid_ntce_ord = str(row.get("bidNtceOrd", "")).strip()
            if not bid_ntce_no or not bid_ntce_ord:
                continue

            await repository.upsert_bid_notice(row=row)
            notice_count += 1

        has_next_page = page_no * settings.api_num_of_rows < total_count
        window_status = "running" if has_next_page else "succeeded"
        next_page_no = page_no + 1 if has_next_page else None

        result_payload: dict[str, Any] = {
            "inqryBgnDt": payload["inqryBgnDt"],
            "inqryEndDt": payload["inqryEndDt"],
            "pageNo": page_no,
            "last_completed_page": page_no,
            "next_page_no": next_page_no,
            "total_count": total_count,
            "num_of_rows": settings.api_num_of_rows,
            "notice_count": notice_count,
        }
        await state_store.upsert_state(
            job_type="bid_list_page",
            job_key=page_job_key,
            status="succeeded",
            payload=result_payload,
        )
        await state_store.upsert_bid_list_window_state(
            job_key=window_job_key,
            status=window_status,
            payload=result_payload,
        )
        await set_bid_list_window_checkpoint(
            checkpoint_store,
            _bid_list_window_checkpoint_name(inqry_bgn_dt=payload["inqryBgnDt"], inqry_end_dt=payload["inqryEndDt"]),
            {
                "inqryBgnDt": payload["inqryBgnDt"],
                "inqryEndDt": payload["inqryEndDt"],
                "last_completed_page": page_no,
                "next_page_no": next_page_no,
                "total_count": total_count,
                "num_of_rows": settings.api_num_of_rows,
                "status": window_status,
            },
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
        await state_store.upsert_bid_list_window_state(
            job_key=window_job_key,
            status="blocked",
            payload=payload,
            error=error_detail,
        )
        await state_store.upsert_state(
            job_type="bid_list_page",
            job_key=page_job_key,
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
        error_detail = {
            "type": type(exc).__name__,
            "message": "task cancelled before bid list collection completed",
        }
        await state_store.upsert_bid_list_window_state(
            job_key=window_job_key,
            status="failed",
            payload=payload,
            error=error_detail,
            max_attempts=settings.max_failed_retries,
            retry_after_seconds=settings.retry_failed_after_seconds,
        )
        await state_store.upsert_state(
            job_type="bid_list_page",
            job_key=page_job_key,
            status="failed",
            payload=payload,
            error=error_detail,
            max_attempts=settings.max_failed_retries,
            retry_after_seconds=settings.retry_failed_after_seconds,
        )
        raise
    except Exception as exc:
        await state_store.upsert_bid_list_window_state(
            job_key=window_job_key,
            status="failed",
            payload=payload,
            error={"type": type(exc).__name__, "message": str(exc)},
            max_attempts=settings.max_failed_retries,
            retry_after_seconds=settings.retry_failed_after_seconds,
        )
        await state_store.upsert_state(
            job_type="bid_list_page",
            job_key=page_job_key,
            status="failed",
            payload=payload,
            error={"type": type(exc).__name__, "message": str(exc)},
            max_attempts=settings.max_failed_retries,
            retry_after_seconds=settings.retry_failed_after_seconds,
        )
        raise
    finally:
        await checkpoint_store.close()
        await state_store.close()
        await repository.close()


def _bid_list_window_checkpoint_name(*, inqry_bgn_dt: str, inqry_end_dt: str) -> str:
    return f"g2b_ingest:bid_list_window:{inqry_bgn_dt}:{inqry_end_dt}"
