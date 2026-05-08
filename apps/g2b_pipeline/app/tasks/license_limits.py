from __future__ import annotations

import os
from typing import Any

from prefect import task

from apps.g2b_pipeline.app.repositories.license_limits import (
    normalize_raw_license_limits as normalize_raw_license_limits_repository,
)
from apps.g2b_pipeline.app.repositories.license_limits import write_license_limit_raw_records
from apps.g2b_pipeline.app.steps.common import G2BIngestWindow
from apps.g2b_pipeline.app.steps.license_limits import LICENSE_LIMIT_URL
from apps.g2b_pipeline.app.tasks.api import fetch_g2b_items


@task(retries=2, retry_delay_seconds=5, task_run_name="fetch-license-limits")
def fetch_license_limits(window: G2BIngestWindow) -> list[dict[str, Any]]:
    return fetch_g2b_items(LICENSE_LIMIT_URL, window)


@task(retries=2, retry_delay_seconds=3, task_run_name="write-license-limits")
def write_license_limits(window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_license_limits_value(window=window, records=records)


def write_license_limits_value(window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_license_limit_raw_records(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_INGEST_SCHEMA", "g2b"),
        table_name=os.getenv("G2B_LICENSE_LIMIT_RAW_TABLE", "bid_public_notice_license_limit_raw"),
        window=window,
        records=records,
    )


@task
def normalize_license_limits(window_begin: str | None = None, window_end: str | None = None) -> dict[str, Any]:
    return normalize_license_limits_once(window_begin=window_begin, window_end=window_end)


def normalize_license_limits_once(
    *,
    window_begin: str | None = None,
    window_end: str | None = None,
) -> dict[str, Any]:
    return normalize_raw_license_limits_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        raw_schema=os.getenv("G2B_INGEST_SCHEMA", "g2b"),
        raw_table=os.getenv("G2B_LICENSE_LIMIT_RAW_TABLE", "bid_public_notice_license_limit_raw"),
        target_schema=os.getenv("G2B_NORMALIZED_SCHEMA", "g2b"),
        target_table=os.getenv("G2B_LICENSE_LIMIT_NORMALIZED_TABLE", "bid_public_notice_license_limit"),
        window_begin=window_begin,
        window_end=window_end,
    )
