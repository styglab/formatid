from __future__ import annotations

import re

from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult
from domains.pps.tasks.config import get_settings
from domains.pps.tasks.http import PpsApiClient
from domains.pps.tasks.job_keys import build_bid_notice_job_key
from domains.pps.tasks.repository import PpsRepository
from domains.pps.tasks.state import PpsTaskStateStore
from domains.pps.tasks.storage import (
    PpsObjectStorage,
    build_attachment_object_key,
    build_notice_date_path_segment,
)


@task("pps.bid.attachment.download")
async def download_bid_attachment(message: TaskMessage) -> TaskResult:
    payload = message.payload
    settings = get_settings()
    client = PpsApiClient()
    storage = PpsObjectStorage()
    repository = PpsRepository(database_url=settings.database_url)
    state_store = PpsTaskStateStore(database_url=settings.database_url)
    job_key = build_bid_notice_job_key(
        bid_ntce_no=payload["bidNtceNo"],
        bid_ntce_ord=payload["bidNtceOrd"],
    )

    try:
        await state_store.upsert_state(job_type="attachment", job_key=job_key, status="running", payload=payload)
        notice_payload = await repository.get_bid_notice_raw_payload(
            bid_ntce_no=payload["bidNtceNo"],
            bid_ntce_ord=payload["bidNtceOrd"],
        )
        if notice_payload is None:
            raise RuntimeError(
                "bid notice raw payload not found for "
                f"bidNtceNo={payload['bidNtceNo']} bidNtceOrd={payload['bidNtceOrd']}"
            )
        downloaded_count = 0
        notice_date = build_notice_date_path_segment(notice_payload)

        for attachment in _build_attachment_sources(notice_payload):
            content = await client.download_file(url=attachment["source_url"])
            object_key = build_attachment_object_key(
                notice_date=notice_date,
                bid_ntce_no=payload["bidNtceNo"],
                bid_ntce_ord=payload["bidNtceOrd"],
                file_name=attachment["file_name"],
            )
            storage.put_bytes(object_key=object_key, content=content)
            downloaded_count += 1
            await repository.upsert_attachment(
                bid_ntce_no=payload["bidNtceNo"],
                bid_ntce_ord=payload["bidNtceOrd"],
                attachment_type=attachment["attachment_type"],
                attachment_index=attachment["attachment_index"],
                source_url=attachment["source_url"],
                file_name=attachment["file_name"],
                storage_bucket=storage.bucket,
                storage_key=object_key,
                download_status="downloaded",
                raw_payload=attachment,
            )

        result_payload = {
            "bidNtceNo": payload["bidNtceNo"],
            "bidNtceOrd": payload["bidNtceOrd"],
            "downloaded_count": downloaded_count,
        }
        await state_store.upsert_state(
            job_type="attachment",
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
    except Exception as exc:
        await state_store.upsert_state(
            job_type="attachment",
            job_key=job_key,
            status="failed",
            payload=payload,
            error={"type": type(exc).__name__, "message": str(exc)},
        )
        raise
    finally:
        await state_store.close()
        await repository.close()


def _build_attachment_sources(notice_payload: dict) -> list[dict]:
    attachments: list[dict] = []
    seen_file_names: set[str] = set()
    std_url = str(notice_payload.get("stdNtceDocUrl", "")).strip()
    if std_url:
        file_name = _normalize_attachment_file_name(
            _pick_std_notice_file_name(notice_payload, std_url)
        )
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


def _pick_std_notice_file_name(notice_payload: dict, std_url: str) -> str:
    for index in range(1, 11):
        url = str(notice_payload.get(f"ntceSpecDocUrl{index}", "")).strip()
        file_name = str(notice_payload.get(f"ntceSpecFileNm{index}", "")).strip()
        if url == std_url and file_name:
            return file_name
    return "std_notice"


_PPS_ATTACHMENT_PREFIX_PATTERN = re.compile(r"^\d+(?:-\d+)?_\d+_(?=.+)")


def _normalize_attachment_file_name(file_name: str) -> str:
    return _PPS_ATTACHMENT_PREFIX_PATTERN.sub("", file_name.strip())
