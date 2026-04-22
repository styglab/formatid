from __future__ import annotations

from typing import Any

from apps.g2b_ingest.service.bid_list_ingest import mark_bid_list_state
from apps.g2b_ingest.service.constants import GENERIC_API_TABLE, GENERIC_FILE_TABLE
from apps.g2b_ingest.service.generic_rows import fetch_generic_ingest_rows
from apps.g2b_ingest.tasks.config import get_settings as get_g2b_ingest_settings
from apps.g2b_ingest.tasks.http import extract_items
from apps.g2b_ingest.tasks.job_keys import build_bid_notice_job_key, build_bid_number_job_key
from apps.g2b_ingest.tasks.repository import G2bIngestRepository
from apps.g2b_ingest.tasks.state import G2bIngestTaskStateStore


async def normalize_generic_api_ingest(*, sources: set[str] | None = None) -> None:
    g2b_ingest_settings = get_g2b_ingest_settings()
    repository = G2bIngestRepository(database_url=g2b_ingest_settings.database_url)
    state_store = G2bIngestTaskStateStore(database_url=g2b_ingest_settings.database_url)
    rows = await fetch_generic_ingest_rows(
        database_url=g2b_ingest_settings.database_url,
        table_name=GENERIC_API_TABLE,
    )
    try:
        for row in rows:
            metadata = row.get("metadata") or {}
            source = metadata.get("source")
            if sources is not None and source not in sources:
                continue
            raw_payload = row.get("raw_payload") or {}
            g2b_ingest_payload = metadata.get("g2b_ingest_payload") or {}
            if source == "g2b_ingest_bid_list":
                await _normalize_bid_list_response(
                    repository=repository,
                    state_store=state_store,
                    raw_payload=raw_payload,
                    g2b_ingest_payload=g2b_ingest_payload,
                )
            elif source == "g2b_ingest_participants":
                items = extract_items(raw_payload)
                await repository.replace_bid_result_participants(
                    bid_ntce_no=str(g2b_ingest_payload["bidNtceNo"]),
                    rows=items,
                )
                await state_store.upsert_state(
                    job_type="participants",
                    job_key=build_bid_number_job_key(bid_ntce_no=str(g2b_ingest_payload["bidNtceNo"])),
                    status="succeeded",
                    payload={**g2b_ingest_payload, "row_count": len(items)},
                )
            elif source == "g2b_ingest_winners":
                items = extract_items(raw_payload)
                await repository.replace_bid_result_winners(
                    bid_ntce_no=str(g2b_ingest_payload["bidNtceNo"]),
                    rows=items,
                )
                await state_store.upsert_state(
                    job_type="winners",
                    job_key=build_bid_number_job_key(bid_ntce_no=str(g2b_ingest_payload["bidNtceNo"])),
                    status="succeeded",
                    payload={**g2b_ingest_payload, "row_count": len(items)},
                )
    finally:
        await state_store.close()
        await repository.close()


async def normalize_generic_file_ingest() -> None:
    g2b_ingest_settings = get_g2b_ingest_settings()
    repository = G2bIngestRepository(database_url=g2b_ingest_settings.database_url)
    state_store = G2bIngestTaskStateStore(database_url=g2b_ingest_settings.database_url)
    rows = await fetch_generic_ingest_rows(
        database_url=g2b_ingest_settings.database_url,
        table_name=GENERIC_FILE_TABLE,
    )
    try:
        for row in rows:
            raw_payload = row.get("raw_payload") or {}
            metadata = row.get("metadata") or {}
            g2b_ingest_payload = metadata.get("g2b_ingest_payload") or {}
            if metadata.get("source") != "g2b_ingest_attachment" or not g2b_ingest_payload:
                continue
            await repository.upsert_attachment(
                bid_ntce_no=str(g2b_ingest_payload["bidNtceNo"]),
                bid_ntce_ord=str(g2b_ingest_payload["bidNtceOrd"]),
                attachment_type=str(g2b_ingest_payload["attachment_type"]),
                attachment_index=int(g2b_ingest_payload["attachment_index"]),
                source_url=str(raw_payload.get("source_url")),
                file_name=str(raw_payload.get("filename") or ""),
                storage_bucket=str(raw_payload.get("bucket") or ""),
                storage_key=str(raw_payload.get("object_key") or ""),
                download_status="downloaded",
                raw_payload=raw_payload,
            )
            await state_store.upsert_state(
                job_type="attachment",
                job_key=build_bid_notice_job_key(
                    bid_ntce_no=str(g2b_ingest_payload["bidNtceNo"]),
                    bid_ntce_ord=str(g2b_ingest_payload["bidNtceOrd"]),
                ),
                status="succeeded",
                payload=g2b_ingest_payload,
            )
    finally:
        await state_store.close()
        await repository.close()


async def _normalize_bid_list_response(
    *,
    repository: G2bIngestRepository,
    state_store: G2bIngestTaskStateStore,
    raw_payload: dict[str, Any],
    g2b_ingest_payload: dict[str, Any],
) -> None:
    items = extract_items(raw_payload)
    for item in items:
        bid_ntce_no = str(item.get("bidNtceNo", "")).strip()
        bid_ntce_ord = str(item.get("bidNtceOrd", "")).strip()
        if not bid_ntce_no or not bid_ntce_ord:
            continue
        await repository.upsert_bid_notice(row=item)
    await mark_bid_list_state(
        payload=g2b_ingest_payload,
        status="succeeded",
        notice_count=len(items),
        state_store=state_store,
    )
