from __future__ import annotations

import os
from typing import Any

from prefect import task

from apps.g2b.schema import CONTRACT_COMPANY_TABLE, CONTRACT_DEMAND_ORG_TABLE, CONTRACT_TABLE, DEFAULT_SCHEMA
from apps.g2b.pipeline.app.repositories.contracts import (
    normalize_raw_contracts as normalize_raw_contracts_repository,
)
from apps.g2b.pipeline.app.repositories.contracts import write_contract_raw_records
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.contracts import CONTRACT_URLS
from apps.g2b.pipeline.app.tasks.api import fetch_g2b_query_items


@task(retries=2, retry_delay_seconds=5, task_run_name="fetch-contracts-{category}")
def fetch_contract_category(category: str, window: G2BIngestWindow) -> list[dict[str, Any]]:
    return fetch_g2b_query_items(
        CONTRACT_URLS[category],
        {
            "inqryDiv": 1,
            "inqryBgnDate": window.begin[:8],
            "inqryEndDate": window.end[:8],
        },
    )


@task(retries=2, retry_delay_seconds=3, task_run_name="write-contracts-{category}")
def write_contract_records(category: str, window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_contract_records_value(category=category, window=window, records=records)


def write_contract_records_value(category: str, window: G2BIngestWindow, records: list[dict[str, Any]]) -> int:
    return write_contract_raw_records(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        schema_name=os.getenv("G2B_INGEST_SCHEMA", DEFAULT_SCHEMA),
        table_name=os.getenv("G2B_CONTRACT_RAW_TABLE", CONTRACT_TABLE.raw_table or "contract_raw"),
        category=category,
        window=window,
        records=records,
    )


@task
def normalize_contracts(window_begin: str | None = None, window_end: str | None = None) -> dict[str, Any]:
    return normalize_contracts_once(window_begin=window_begin, window_end=window_end)


def normalize_contracts_once(
    *,
    window_begin: str | None = None,
    window_end: str | None = None,
) -> dict[str, Any]:
    return normalize_raw_contracts_repository(
        database_url=os.environ["G2B_INGEST_DATABASE_URL"],
        raw_schema=os.getenv("G2B_INGEST_SCHEMA", DEFAULT_SCHEMA),
        raw_table=os.getenv("G2B_CONTRACT_RAW_TABLE", CONTRACT_TABLE.raw_table or "contract_raw"),
        target_schema=os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA),
        target_table=os.getenv("G2B_CONTRACT_NORMALIZED_TABLE", CONTRACT_TABLE.normalized_table),
        company_table=os.getenv("G2B_CONTRACT_COMPANY_TABLE", CONTRACT_COMPANY_TABLE.normalized_table),
        demand_org_table=os.getenv("G2B_CONTRACT_DEMAND_ORG_TABLE", CONTRACT_DEMAND_ORG_TABLE.normalized_table),
        window_begin=window_begin,
        window_end=window_end,
    )
