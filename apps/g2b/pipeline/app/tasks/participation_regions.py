from __future__ import annotations

import os
from typing import Any

from prefect import task

from apps.g2b.schema import DEFAULT_SCHEMA, PARTICIPATION_REGION_TABLE
from apps.g2b.pipeline.app.repositories.participation_regions import (
    normalize_raw_participation_regions as normalize_raw_participation_regions_repository,
)
from apps.g2b.pipeline.app.repositories.participation_regions import write_participation_region_raw_records
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.participation_regions import PARTICIPATION_REGION_URL
from apps.g2b.pipeline.app.tasks.api import fetch_g2b_items


@task(retries=2, retry_delay_seconds=5, task_run_name="fetch-participation-regions")
def fetch_participation_regions(window: G2BIngestWindow) -> list[dict[str, Any]]:
    return fetch_g2b_items(PARTICIPATION_REGION_URL, window)


@task(retries=2, retry_delay_seconds=3, task_run_name="write-participation-regions")
def write_participation_regions(window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_participation_regions_value(window=window, records=records)


def write_participation_regions_value(window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_participation_region_raw_records(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_INGEST_SCHEMA", DEFAULT_SCHEMA),
        table_name=os.getenv(
            "G2B_PARTICIPATION_REGION_RAW_TABLE",
            PARTICIPATION_REGION_TABLE.raw_table or "bid_public_notice_participation_region_raw",
        ),
        window=window,
        records=records,
    )


@task
def normalize_participation_regions(window_begin: str | None = None, window_end: str | None = None) -> dict[str, Any]:
    return normalize_participation_regions_once(window_begin=window_begin, window_end=window_end)


def normalize_participation_regions_once(
    *,
    window_begin: str | None = None,
    window_end: str | None = None,
) -> dict[str, Any]:
    return normalize_raw_participation_regions_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        raw_schema=os.getenv("G2B_INGEST_SCHEMA", DEFAULT_SCHEMA),
        raw_table=os.getenv(
            "G2B_PARTICIPATION_REGION_RAW_TABLE",
            PARTICIPATION_REGION_TABLE.raw_table or "bid_public_notice_participation_region_raw",
        ),
        target_schema=os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA),
        target_table=os.getenv("G2B_PARTICIPATION_REGION_NORMALIZED_TABLE", PARTICIPATION_REGION_TABLE.normalized_table),
        window_begin=window_begin,
        window_end=window_end,
    )
