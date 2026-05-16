from __future__ import annotations

import os
from typing import Any

from prefect import task

from apps.g2b.schema import (
    DEFAULT_SCHEMA,
    PROCUREMENT_COMPANY_INDUSTRY_TABLE,
    PROCUREMENT_COMPANY_TABLE,
    SUCCESSFUL_BID_TABLE,
)
from apps.g2b.pipeline.app.repositories.companies import (
    load_success_bid_winner_business_numbers,
    normalize_company_basic_raw,
    normalize_company_industry_raw,
    write_company_basic_raw_records,
    write_company_industry_raw_records,
)
from apps.g2b.pipeline.app.steps.companies import COMPANY_BASIC_URL, COMPANY_INDUSTRY_URL
from apps.g2b.pipeline.app.tasks.api import fetch_g2b_query_items


@task(retries=2, retry_delay_seconds=5, task_run_name="sync-award-companies")
def sync_award_companies(window_begin: str, window_end: str) -> dict[str, Any]:
    return sync_award_companies_once(window_begin=window_begin, window_end=window_end)


def sync_award_companies_once(window_begin: str, window_end: str) -> dict[str, Any]:
    database_url = os.environ["G2B_INGEST_DATABASE_URL"]
    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA)
    success_bid_table = os.getenv("G2B_SUCCESS_BID_NORMALIZED_TABLE", SUCCESSFUL_BID_TABLE.normalized_table)
    success_bid_raw_table = os.getenv("G2B_SUCCESS_BID_RAW_TABLE", SUCCESSFUL_BID_TABLE.raw_table or "successful_bid_raw")
    company_raw_table = os.getenv("G2B_COMPANY_RAW_TABLE", PROCUREMENT_COMPANY_TABLE.raw_table or "procurement_company_raw")
    company_table = os.getenv("G2B_COMPANY_NORMALIZED_TABLE", PROCUREMENT_COMPANY_TABLE.normalized_table)
    industry_raw_table = os.getenv(
        "G2B_COMPANY_INDUSTRY_RAW_TABLE",
        PROCUREMENT_COMPANY_INDUSTRY_TABLE.raw_table or "procurement_company_industry_raw",
    )
    industry_table = os.getenv(
        "G2B_COMPANY_INDUSTRY_NORMALIZED_TABLE",
        PROCUREMENT_COMPANY_INDUSTRY_TABLE.normalized_table,
    )

    business_numbers = load_success_bid_winner_business_numbers(
        database_url=database_url,
        schema_name=schema_name,
        success_bid_table=success_bid_table,
        success_bid_raw_table=success_bid_raw_table,
        window_begin=window_begin,
        window_end=window_end,
    )
    basic_records: list[dict[str, Any]] = []
    industry_records: list[dict[str, Any]] = []
    for business_no in business_numbers:
        basic_records.extend(_fetch_company_basic(business_no))
        industry_records.extend(_fetch_company_industries(business_no))

    basic_raw_count = write_company_basic_raw_records(
        database_url=database_url,
        schema_name=schema_name,
        table_name=company_raw_table,
        records=basic_records,
    )
    industry_raw_count = write_company_industry_raw_records(
        database_url=database_url,
        schema_name=schema_name,
        table_name=industry_raw_table,
        records=industry_records,
        refresh_business_numbers=business_numbers,
    )
    basic_normalized_count = normalize_company_basic_raw(
        database_url=database_url,
        raw_schema=schema_name,
        raw_table=company_raw_table,
        target_schema=schema_name,
        target_table=company_table,
        business_numbers=business_numbers,
    )
    industry_normalized_count = normalize_company_industry_raw(
        database_url=database_url,
        raw_schema=schema_name,
        raw_table=industry_raw_table,
        target_schema=schema_name,
        target_table=industry_table,
        business_numbers=business_numbers,
    )
    return {
        "source": "successful_bid",
        "window": {"begin": window_begin, "end": window_end},
        "business_numbers": len(business_numbers),
        "basic": {"raw": basic_raw_count, "normalized": basic_normalized_count},
        "industries": {"raw": industry_raw_count, "normalized": industry_normalized_count},
    }


def _fetch_company_basic(business_no: str) -> list[dict[str, Any]]:
    return fetch_g2b_query_items(
        COMPANY_BASIC_URL,
        {
            "inqryDiv": 3,
            "bizno": business_no,
            "numOfRows": 100,
        },
    )


def _fetch_company_industries(business_no: str) -> list[dict[str, Any]]:
    return fetch_g2b_query_items(
        COMPANY_INDUSTRY_URL,
        {
            "inqryDiv": 1,
            "bizno": business_no,
            "numOfRows": 100,
        },
    )
