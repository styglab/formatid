from __future__ import annotations

import os
from typing import Any

from prefect import task

from apps.g2b.pipeline.app.repositories.bid_notices import (
    normalize_raw_notices as normalize_raw_notices_repository,
)
from apps.g2b.pipeline.app.repositories.bid_notices import write_raw_records
from apps.g2b.pipeline.app.steps.bid_notices import BASE_URLS
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.tasks.api import fetch_g2b_items


@task(retries=2, retry_delay_seconds=5, task_run_name="fetch-{category}")
def fetch_category(category: str, window: G2BIngestWindow) -> list[dict[str, Any]]:
    return fetch_g2b_items(BASE_URLS[category], window)


@task(retries=2, retry_delay_seconds=3, task_run_name="write-{category}")
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
