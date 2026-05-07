from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from prefect import task

from apps.shared.data_pipeline.app.repositories.g2b_bid import (
    get_last_succeeded_window_begin as get_last_succeeded_window_begin_repository,
)
from apps.shared.data_pipeline.app.repositories.g2b_bid import (
    mark_window_failed as mark_window_failed_repository,
)
from apps.shared.data_pipeline.app.repositories.g2b_bid import (
    mark_window_running as mark_window_running_repository,
)
from apps.shared.data_pipeline.app.repositories.g2b_bid import (
    mark_window_succeeded as mark_window_succeeded_repository,
)
from apps.shared.data_pipeline.app.repositories.g2b_bid import (
    normalize_raw_notices as normalize_raw_notices_repository,
)
from apps.shared.data_pipeline.app.repositories.g2b_bid import write_raw_records
from apps.shared.data_pipeline.app.steps.g2b_bid import (
    BASE_URLS,
    G2BIngestWindow,
    compute_due_hourly_windows,
    compute_previous_hour_window_value,
    parse_count,
)


@task
def compute_previous_hour_window(now: datetime | None = None) -> dict[str, str]:
    return compute_previous_hour_window_value(now)


@task
def compute_due_windows(last_succeeded_begin: str | None = None, now: datetime | None = None) -> list[dict[str, str]]:
    return compute_due_hourly_windows(
        last_succeeded_begin=last_succeeded_begin,
        now=now,
        default_start=os.getenv("G2B_INGEST_DEFAULT_START", "202605040000"),
        max_windows=int(os.getenv("G2B_INGEST_MAX_WINDOWS", "6")),
    )


@task(retries=2, retry_delay_seconds=5, task_run_name="fetch-{category}")
def fetch_category(category: str, window: G2BIngestWindow) -> list[dict[str, Any]]:
    api_key = os.getenv("G2B_API_KEY") or os.getenv("API_KEY")
    if not api_key:
        raise RuntimeError("G2B_API_KEY is not configured")

    url = BASE_URLS[category]
    page_no = 1
    num_of_rows = int(os.getenv("G2B_INGEST_NUM_OF_ROWS", "100"))
    records: list[dict[str, Any]] = []

    while True:
        params = {
            "ServiceKey": api_key,
            "inqryDiv": 1,
            "inqryBgnDt": window.begin,
            "inqryEndDt": window.end,
            "pageNo": page_no,
            "numOfRows": num_of_rows,
            "type": "json",
        }
        payload = _get_json(url, params)
        body = payload.get("response", {}).get("body", {})
        items = body.get("items", [])
        page_items = items if isinstance(items, list) else []
        records.extend(page_items)

        total_count = parse_count(body.get("totalCount"))
        if not page_items:
            break
        if total_count is not None and len(records) >= total_count:
            break
        if len(page_items) < num_of_rows:
            break
        page_no += 1

    return records


@task(task_run_name="write-{category}")
def write_records(category: str, window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_records_value(category=category, window=window, records=records)


def write_records_value(category: str, window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_raw_records(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_INGEST_SCHEMA", "g2b"),
        table_name=os.getenv("G2B_INGEST_TABLE", "bid_public_notice_raw"),
        category=category,
        window=window,
        records=records,
    )


@task
def normalize_raw_notices(window_begin: str | None = None, window_end: str | None = None) -> dict[str, Any]:
    return normalize_raw_notices_once(window_begin=window_begin, window_end=window_end)


def normalize_raw_notices_once(
    *,
    window_begin: str | None = None,
    window_end: str | None = None,
) -> dict[str, Any]:
    return normalize_raw_notices_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        raw_schema=os.getenv("G2B_INGEST_SCHEMA", "g2b"),
        raw_table=os.getenv("G2B_INGEST_TABLE", "bid_public_notice_raw"),
        target_schema=os.getenv("G2B_NORMALIZED_SCHEMA", "g2b"),
        target_table=os.getenv("G2B_NORMALIZED_TABLE", "bid_public_notice"),
        window_begin=window_begin,
        window_end=window_end,
    )


@task
def get_last_succeeded_window_begin() -> str | None:
    return get_last_succeeded_window_begin_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_CHECKPOINT_SCHEMA", "g2b"),
        table_name=os.getenv("G2B_CHECKPOINT_TABLE", "pipeline_window_checkpoint"),
    )


@task
def mark_window_running(window: G2BIngestWindow) -> None:
    mark_window_running_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_CHECKPOINT_SCHEMA", "g2b"),
        table_name=os.getenv("G2B_CHECKPOINT_TABLE", "pipeline_window_checkpoint"),
        window=window,
    )


@task
def mark_window_succeeded(window: G2BIngestWindow, raw_count: int, normalized_count: int) -> None:
    mark_window_succeeded_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_CHECKPOINT_SCHEMA", "g2b"),
        table_name=os.getenv("G2B_CHECKPOINT_TABLE", "pipeline_window_checkpoint"),
        window=window,
        raw_count=raw_count,
        normalized_count=normalized_count,
    )


@task
def mark_window_failed(window: G2BIngestWindow, error_message: str) -> None:
    mark_window_failed_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_CHECKPOINT_SCHEMA", "g2b"),
        table_name=os.getenv("G2B_CHECKPOINT_TABLE", "pipeline_window_checkpoint"),
        window=window,
        error_message=error_message,
    )


def _get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    request_url = f"{url}?{urlencode(params)}"
    with urlopen(request_url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))
