from __future__ import annotations

import os
import re
from typing import Any

from services.app_service.runtime.config import get_settings as get_service_settings
from services.app_service.runtime.run_store import ServiceRunStore
from services.task_runtime.enqueue import enqueue_task

from apps.g2b_ingest.service.constants import (
    G2B_INGEST_PARTICIPANTS_URL,
    G2B_INGEST_WINNERS_URL,
    GENERIC_API_QUEUE,
    GENERIC_API_TABLE,
    GENERIC_FILE_QUEUE,
    GENERIC_FILE_TABLE,
)
from apps.g2b_ingest.service.graph import Notice
from apps.g2b_ingest.tasks.config import get_settings as get_g2b_ingest_settings
from apps.g2b_ingest.tasks.job_keys import build_bid_notice_job_key, build_bid_number_job_key
from apps.g2b_ingest.tasks.quota import get_quota_block
from apps.g2b_ingest.tasks.repository import G2bIngestRepository
from apps.g2b_ingest.tasks.state import G2bIngestTaskStateStore


async def build_downstream_plan() -> dict[str, Any]:
    g2b_ingest_settings = get_g2b_ingest_settings()
    repository = G2bIngestRepository(database_url=g2b_ingest_settings.database_url)
    state_store = G2bIngestTaskStateStore(database_url=g2b_ingest_settings.database_url)
    try:
        await state_store.mark_stale_running_as_failed(
            stale_after_seconds=g2b_ingest_settings.stale_running_after_seconds,
        )
        control = await build_downstream_control(state_store=state_store)
        result_tasks_enabled = bool(control["result_tasks_enabled"])
        candidates = {
            "attachment": await _list_downstream_candidates(repository=repository, job_type="attachment"),
            "participants": (
                await _list_downstream_candidates(repository=repository, job_type="participants")
                if result_tasks_enabled
                else []
            ),
            "winners": (
                await _list_downstream_candidates(repository=repository, job_type="winners")
                if result_tasks_enabled
                else []
            ),
        }
        all_notices = _dedupe_notices(
            [
                *candidates["attachment"],
                *candidates["participants"],
                *candidates["winners"],
            ]
        )
        return {
            "notices": all_notices,
            "attachment_candidates": candidates["attachment"],
            "participant_candidates": candidates["participants"],
            "winner_candidates": candidates["winners"],
        }
    finally:
        await state_store.close()
        await repository.close()


async def enqueue_downstream_with_own_run_store(
    *,
    job_type: str,
    candidates: list[Notice],
) -> list[dict[str, Any]]:
    branch_run_store = ServiceRunStore(database_url=get_service_settings().checkpoint_database_url)
    try:
        return await enqueue_downstream_generic_tasks(
            run_store=branch_run_store,
            job_type=job_type,
            candidates=candidates,
        )
    finally:
        await branch_run_store.close()


async def enqueue_downstream_generic_tasks(
    *,
    run_store: ServiceRunStore,
    job_type: str,
    candidates: list[Notice],
) -> list[dict[str, Any]]:
    g2b_ingest_settings = get_g2b_ingest_settings()
    service_settings = get_service_settings()
    repository = G2bIngestRepository(database_url=g2b_ingest_settings.database_url)
    state_store = G2bIngestTaskStateStore(database_url=g2b_ingest_settings.database_url)
    if job_type not in {"attachment", "participants", "winners"}:
        raise ValueError(f"unsupported downstream job_type: {job_type}")
    try:
        control = await build_downstream_control(state_store=state_store)
        if job_type in {"participants", "winners"} and not control.get("result_tasks_enabled", True):
            details = {
                "job_type": job_type,
                "candidate_count": len(candidates),
                **control,
            }
            await run_store.record(
                run_name=f"g2b_ingest_bid_{job_type}_enqueue",
                queue_name=GENERIC_API_QUEUE,
                task_name="ingest.api.fetch",
                status="skipped",
                skip_reason=control.get("skip_result_reason") or "result_tasks_disabled",
                details=details,
            )
            return [
                _branch_summary(
                    job_type=job_type,
                    status="skipped",
                    candidate_count=len(candidates),
                    enqueued_count=0,
                    skip_reason=details["skip_result_reason"],
                )
            ]

        enqueued_count = 0
        for notice in candidates:
            enqueued_count += await _enqueue_notice_downstream_job(
                redis_url=service_settings.redis_url,
                status_ttl=service_settings.task_status_ttl,
                repository=repository,
                state_store=state_store,
                bid_ntce_no=notice["bid_ntce_no"],
                bid_ntce_ord=notice["bid_ntce_ord"],
                job_type=job_type,
            )
        await run_store.record(
            run_name=f"g2b_ingest_bid_{job_type}_enqueue",
            queue_name=GENERIC_FILE_QUEUE if job_type == "attachment" else GENERIC_API_QUEUE,
            task_name="ingest.file.download" if job_type == "attachment" else "ingest.api.fetch",
            status="enqueued" if enqueued_count else "skipped",
            skip_reason=None if enqueued_count else "no_downstream_candidates",
            details={
                "job_type": job_type,
                "candidate_count": len(candidates),
                "enqueued_count": enqueued_count,
                **control,
            },
        )
        return [
            _branch_summary(
                job_type=job_type,
                status="enqueued" if enqueued_count else "skipped",
                candidate_count=len(candidates),
                enqueued_count=enqueued_count,
                skip_reason=None if enqueued_count else "no_downstream_candidates",
            )
        ]
    finally:
        await state_store.close()
        await repository.close()


async def build_downstream_control(*, state_store: G2bIngestTaskStateStore) -> dict[str, Any]:
    g2b_ingest_settings = get_g2b_ingest_settings()
    quota_block = await get_quota_block(
        redis_url=g2b_ingest_settings.redis_url,
        database_url=g2b_ingest_settings.checkpoint_database_url,
    )
    quota_block = quota_block or await state_store.get_active_quota_block()
    missing_public_api_key = not g2b_ingest_settings.public_api_key
    result_tasks_enabled = quota_block is None and not missing_public_api_key
    return {
        "quota_blocked": quota_block is not None,
        "missing_public_api_key": missing_public_api_key,
        "result_tasks_enabled": result_tasks_enabled,
        "skip_result_reason": None
        if result_tasks_enabled
        else ("quota_blocked" if quota_block is not None else "missing_public_api_key"),
    }


async def _list_downstream_candidates(
    *,
    repository: G2bIngestRepository,
    job_type: str,
) -> list[Notice]:
    g2b_ingest_settings = get_g2b_ingest_settings()
    rows = await repository.list_bid_notices_for_downstream_job(
        job_type=job_type,
        limit=int(os.getenv("G2B_INGEST_DOWNSTREAM_LIMIT", "100")),
        max_failed_retries=g2b_ingest_settings.max_failed_retries,
    )
    return [_notice_from_row(row) for row in rows]


def _notice_from_row(row: dict[str, Any]) -> Notice:
    return {
        "bid_ntce_no": str(row["bid_ntce_no"]),
        "bid_ntce_ord": str(row["bid_ntce_ord"]),
    }


def _dedupe_notices(notices: list[Notice]) -> list[Notice]:
    seen: set[tuple[str, str]] = set()
    unique_notices: list[Notice] = []
    for notice in notices:
        key = (notice["bid_ntce_no"], notice["bid_ntce_ord"])
        if key in seen:
            continue
        seen.add(key)
        unique_notices.append(notice)
    return unique_notices


def _branch_summary(
    *,
    job_type: str,
    status: str,
    candidate_count: int,
    enqueued_count: int,
    skip_reason: str | None,
) -> dict[str, Any]:
    return {
        "job_type": job_type,
        "status": status,
        "candidate_count": candidate_count,
        "enqueued_count": enqueued_count,
        "skip_reason": skip_reason,
    }


async def _enqueue_notice_downstream_job(
    *,
    redis_url: str,
    status_ttl: int,
    repository: G2bIngestRepository,
    state_store: G2bIngestTaskStateStore,
    bid_ntce_no: str,
    bid_ntce_ord: str,
    job_type: str,
) -> int:
    notice_payload = await repository.get_bid_notice_raw_payload(
        bid_ntce_no=bid_ntce_no,
        bid_ntce_ord=bid_ntce_ord,
    )
    if notice_payload is None:
        return 0
    if job_type == "attachment":
        return await _enqueue_notice_attachments(
            redis_url=redis_url,
            status_ttl=status_ttl,
            state_store=state_store,
            bid_ntce_no=bid_ntce_no,
            bid_ntce_ord=bid_ntce_ord,
            notice_payload=notice_payload,
        )
    if job_type == "participants":
        return await _enqueue_notice_result_task(
            redis_url=redis_url,
            status_ttl=status_ttl,
            state_store=state_store,
            bid_ntce_no=bid_ntce_no,
            bid_ntce_ord=bid_ntce_ord,
            source="g2b_ingest_participants",
            url=G2B_INGEST_PARTICIPANTS_URL,
            job_type="participants",
        )
    if job_type == "winners":
        return await _enqueue_notice_result_task(
            redis_url=redis_url,
            status_ttl=status_ttl,
            state_store=state_store,
            bid_ntce_no=bid_ntce_no,
            bid_ntce_ord=bid_ntce_ord,
            source="g2b_ingest_winners",
            url=G2B_INGEST_WINNERS_URL,
            job_type="winners",
        )
    raise ValueError(f"unsupported downstream job_type: {job_type}")


async def _enqueue_notice_attachments(
    *,
    redis_url: str,
    status_ttl: int,
    state_store: G2bIngestTaskStateStore,
    bid_ntce_no: str,
    bid_ntce_ord: str,
    notice_payload: dict[str, Any],
) -> int:
    enqueued = 0
    for attachment in _build_attachment_sources(notice_payload):
        g2b_ingest_payload = {
            "bidNtceNo": bid_ntce_no,
            "bidNtceOrd": bid_ntce_ord,
            **attachment,
        }
        await enqueue_task(
            redis_url=redis_url,
            queue_name=GENERIC_FILE_QUEUE,
            task_name="ingest.file.download",
            payload=_build_file_payload(g2b_ingest_payload=g2b_ingest_payload),
            status_ttl=status_ttl,
            dedupe_key=f"g2b_ingest_attachment:{bid_ntce_no}:{bid_ntce_ord}:{attachment['attachment_type']}:{attachment['attachment_index']}",
        )
        await state_store.upsert_state(
            job_type="attachment",
            job_key=build_bid_notice_job_key(bid_ntce_no=bid_ntce_no, bid_ntce_ord=bid_ntce_ord),
            status="queued",
            payload=g2b_ingest_payload,
        )
        enqueued += 1
    return enqueued


async def _enqueue_notice_result_task(
    *,
    redis_url: str,
    status_ttl: int,
    state_store: G2bIngestTaskStateStore,
    bid_ntce_no: str,
    bid_ntce_ord: str,
    source: str,
    url: str,
    job_type: str,
) -> int:
    g2b_ingest_payload = {"bidNtceNo": bid_ntce_no, "bidNtceOrd": bid_ntce_ord}
    await enqueue_task(
        redis_url=redis_url,
        queue_name=GENERIC_API_QUEUE,
        task_name="ingest.api.fetch",
        payload=_build_result_api_payload(url=url, source=source, g2b_ingest_payload=g2b_ingest_payload),
        status_ttl=status_ttl,
        dedupe_key=f"{source}:{bid_ntce_no}",
    )
    await state_store.upsert_state(
        job_type=job_type,
        job_key=build_bid_number_job_key(bid_ntce_no=bid_ntce_no),
        status="queued",
        payload=g2b_ingest_payload,
    )
    return 1


def _build_result_api_payload(*, url: str, source: str, g2b_ingest_payload: dict[str, Any]) -> dict[str, Any]:
    g2b_ingest_settings = get_g2b_ingest_settings()
    if not g2b_ingest_settings.public_api_key:
        raise RuntimeError("PUBLIC_API_KEY is required to enqueue G2B Ingest result API ingest tasks")
    return {
        "request": {
            "method": "GET",
            "url": url,
            "params": {
                "serviceKey": g2b_ingest_settings.public_api_key,
                "type": "json",
                "inqryDiv": 4,
                "bidNtceNo": g2b_ingest_payload["bidNtceNo"],
                "pageNo": 1,
                "numOfRows": g2b_ingest_settings.api_num_of_rows,
            },
            "timeout_seconds": g2b_ingest_settings.api_timeout_seconds,
        },
        "target": {
            "type": "postgres",
            "database_url_env": "G2B_INGEST_DATABASE_URL",
            "schema_name": "raw",
            "table_name": GENERIC_API_TABLE,
            "mode": "append",
            "create_table": True,
        },
        "metadata": {
            "source": source,
            "g2b_ingest_payload": g2b_ingest_payload,
        },
    }


def _build_file_payload(*, g2b_ingest_payload: dict[str, Any]) -> dict[str, Any]:
    file_name = str(g2b_ingest_payload["file_name"])
    return {
        "source": {
            "url": g2b_ingest_payload["source_url"],
            "filename": file_name,
        },
        "target": {
            "type": "s3",
            "endpoint_env": "G2B_INGEST_S3_ENDPOINT",
            "access_key_env": "G2B_INGEST_S3_ACCESS_KEY",
            "secret_key_env": "G2B_INGEST_S3_SECRET_KEY",
            "bucket_env": "G2B_INGEST_S3_BUCKET",
            "secure_env": "G2B_INGEST_S3_SECURE",
            "object_key": f"g2b_ingest/attachments/{g2b_ingest_payload['bidNtceNo']}/{g2b_ingest_payload['bidNtceOrd']}/{file_name.replace('/', '_')}",
        },
        "metadata_target": {
            "type": "postgres",
            "database_url_env": "G2B_INGEST_DATABASE_URL",
            "schema_name": "raw",
            "table_name": GENERIC_FILE_TABLE,
            "mode": "append",
            "create_table": True,
        },
        "metadata": {
            "source": "g2b_ingest_attachment",
            "g2b_ingest_payload": g2b_ingest_payload,
        },
    }


def _build_attachment_sources(notice_payload: dict[str, Any]) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    seen_file_names: set[str] = set()
    std_url = str(notice_payload.get("stdNtceDocUrl", "")).strip()
    if std_url:
        file_name = _normalize_attachment_file_name(_pick_std_notice_file_name(notice_payload, std_url))
        if file_name not in seen_file_names:
            seen_file_names.add(file_name)
            attachments.append(
                {
                    "attachment_type": "std_notice",
                    "attachment_index": 0,
                    "source_url": std_url,
                    "file_name": file_name,
                }
            )

    for index in range(1, 11):
        url = str(notice_payload.get(f"ntceSpecDocUrl{index}", "")).strip()
        file_name = str(notice_payload.get(f"ntceSpecFileNm{index}", "")).strip()
        if not url:
            continue
        resolved_file_name = _normalize_attachment_file_name(file_name) or f"attachment_{index}"
        if resolved_file_name in seen_file_names:
            continue
        seen_file_names.add(resolved_file_name)
        attachments.append(
            {
                "attachment_type": "ntce_spec",
                "attachment_index": index,
                "source_url": url,
                "file_name": resolved_file_name,
            }
        )
    return attachments


def _pick_std_notice_file_name(notice_payload: dict[str, Any], std_url: str) -> str:
    for index in range(1, 11):
        url = str(notice_payload.get(f"ntceSpecDocUrl{index}", "")).strip()
        file_name = str(notice_payload.get(f"ntceSpecFileNm{index}", "")).strip()
        if url == std_url and file_name:
            return file_name
    return "std_notice"


_G2B_INGEST_ATTACHMENT_PREFIX_PATTERN = re.compile(r"^\d+(?:-\d+)?_\d+_(?=.+)")


def _normalize_attachment_file_name(file_name: str) -> str:
    return _G2B_INGEST_ATTACHMENT_PREFIX_PATTERN.sub("", file_name.strip())
