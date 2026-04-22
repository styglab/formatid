from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

from services.app_service.runtime.config import get_settings as get_service_settings
from services.app_service.runtime.logger import get_logger, log_event
from services.app_service.runtime.run_store import ServiceRunStore
from services.task_runtime.enqueue import enqueue_task
from shared.checkpoints.postgres import PostgresCheckpointStore
from shared.time import iso_now

from apps.g2b_ingest.service.constants import (
    G2B_INGEST_BID_LIST_URL,
    GENERIC_API_QUEUE,
    GENERIC_API_TABLE,
)
from apps.g2b_ingest.tasks.collection_window import build_bid_list_payload
from apps.g2b_ingest.tasks.config import get_settings as get_g2b_ingest_settings
from apps.g2b_ingest.tasks.job_keys import build_bid_list_page_job_key, build_bid_list_window_job_key
from apps.g2b_ingest.tasks.state import G2bIngestTaskStateStore


logger = get_logger("g2b_ingest.service")


async def enqueue_bid_list_if_due(
    *,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
) -> None:
    g2b_ingest_settings = get_g2b_ingest_settings()
    service_settings = get_service_settings()
    if not g2b_ingest_settings.public_api_key:
        await run_store.record(
            run_name="g2b_ingest_bid_list_collect",
            queue_name=GENERIC_API_QUEUE,
            task_name="ingest.api.fetch",
            status="skipped",
            skip_reason="missing_public_api_key",
        )
        log_event(logger, logging.WARNING, "g2b_ingest_bid_list_skipped_missing_public_api_key")
        return

    definition = SimpleNamespace(name="g2b_ingest_bid_list_collect", queue_name=GENERIC_API_QUEUE)
    payload = await build_bid_list_payload(definition=definition, checkpoint_store=checkpoint_store)
    if payload is None:
        await run_store.record(
            run_name="g2b_ingest_bid_list_collect",
            queue_name=GENERIC_API_QUEUE,
            task_name="ingest.api.fetch",
            status="skipped",
            skip_reason="bid_list_payload_not_due",
        )
        return

    await mark_bid_list_state(payload=payload, status="queued")
    generic_payload = _build_bid_list_api_payload(payload=payload)
    message = await enqueue_task(
        redis_url=service_settings.redis_url,
        queue_name=GENERIC_API_QUEUE,
        task_name="ingest.api.fetch",
        payload=generic_payload,
        status_ttl=service_settings.task_status_ttl,
    )
    await checkpoint_store.set(
        "service:g2b_ingest_bid_list_collect",
        {
            "last_enqueued_at": iso_now(),
            "last_task_id": message.task_id,
            "queue_name": GENERIC_API_QUEUE,
            "task_name": "ingest.api.fetch",
            "payload": payload,
        },
    )
    await run_store.record(
        run_name="g2b_ingest_bid_list_collect",
        queue_name=GENERIC_API_QUEUE,
        task_name="ingest.api.fetch",
        task_id=message.task_id,
        status="enqueued",
        payload=generic_payload,
    )
    log_event(
        logger,
        logging.INFO,
        "g2b_ingest_bid_list_generic_enqueued",
        task_id=message.task_id,
        g2b_ingest_payload=payload,
    )


async def mark_bid_list_state(
    *,
    payload: dict[str, Any],
    status: str,
    notice_count: int | None = None,
    state_store: G2bIngestTaskStateStore | None = None,
) -> None:
    own_store = state_store is None
    store = state_store or G2bIngestTaskStateStore(database_url=get_g2b_ingest_settings().database_url)
    page_no = int(payload.get("pageNo", 1))
    state_payload = dict(payload)
    if notice_count is not None:
        state_payload["notice_count"] = notice_count
    try:
        await store.upsert_state(
            job_type="bid_list_page",
            job_key=build_bid_list_page_job_key(
                inqry_bgn_dt=payload["inqryBgnDt"],
                inqry_end_dt=payload["inqryEndDt"],
                page_no=page_no,
            ),
            status=status,
            payload=state_payload,
        )
        await store.upsert_bid_list_window_state(
            job_key=build_bid_list_window_job_key(
                inqry_bgn_dt=payload["inqryBgnDt"],
                inqry_end_dt=payload["inqryEndDt"],
            ),
            status=status,
            payload=state_payload,
        )
    finally:
        if own_store:
            await store.close()


def _build_bid_list_api_payload(*, payload: dict[str, Any]) -> dict[str, Any]:
    g2b_ingest_settings = get_g2b_ingest_settings()
    return {
        "request": {
            "method": "GET",
            "url": G2B_INGEST_BID_LIST_URL,
            "params": {
                "serviceKey": g2b_ingest_settings.public_api_key,
                "type": "json",
                "inqryDiv": 1,
                "inqryBgnDt": payload["inqryBgnDt"],
                "inqryEndDt": payload["inqryEndDt"],
                "pageNo": payload.get("pageNo", 1),
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
            "source": "g2b_ingest_bid_list",
            "g2b_ingest_payload": payload,
        },
    }
