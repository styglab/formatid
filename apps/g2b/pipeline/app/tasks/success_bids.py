from __future__ import annotations

import os
from typing import Any

from prefect import task

from apps.g2b.schema import DEFAULT_SCHEMA, SUCCESSFUL_BID_TABLE
from apps.g2b.pipeline.app.repositories.success_bids import (
    normalize_raw_success_bids as normalize_raw_success_bids_repository,
)
from apps.g2b.pipeline.app.repositories.success_bids import write_success_bid_raw_records
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.success_bids import SUCCESS_BID_URLS
from apps.g2b.pipeline.app.tasks.api import fetch_g2b_items


@task(retries=2, retry_delay_seconds=5, task_run_name="fetch-success-bids-{category}")
def fetch_success_bid_category(category: str, window: G2BIngestWindow) -> list[dict[str, Any]]:
    return fetch_g2b_items(SUCCESS_BID_URLS[category], window)


@task(retries=2, retry_delay_seconds=3, task_run_name="write-success-bids-{category}")
def write_success_bid_records(category: str, window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_success_bid_records_value(category=category, window=window, records=records)


def write_success_bid_records_value(category: str, window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_success_bid_raw_records(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_INGEST_SCHEMA", DEFAULT_SCHEMA),
        table_name=os.getenv("G2B_SUCCESS_BID_RAW_TABLE", SUCCESSFUL_BID_TABLE.raw_table or "successful_bid_raw"),
        category=category,
        window=window,
        records=records,
    )


@task
def normalize_success_bids(window_begin: str | None = None, window_end: str | None = None) -> dict[str, Any]:
    return normalize_success_bids_once(window_begin=window_begin, window_end=window_end)


def normalize_success_bids_once(
    *,
    window_begin: str | None = None,
    window_end: str | None = None,
) -> dict[str, Any]:
    return normalize_raw_success_bids_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        raw_schema=os.getenv("G2B_INGEST_SCHEMA", DEFAULT_SCHEMA),
        raw_table=os.getenv("G2B_SUCCESS_BID_RAW_TABLE", SUCCESSFUL_BID_TABLE.raw_table or "successful_bid_raw"),
        target_schema=os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA),
        target_table=os.getenv("G2B_SUCCESS_BID_NORMALIZED_TABLE", SUCCESSFUL_BID_TABLE.normalized_table),
        window_begin=window_begin,
        window_end=window_end,
    )
