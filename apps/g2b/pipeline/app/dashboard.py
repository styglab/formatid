from __future__ import annotations

import asyncio
import json
import os
from typing import Any
from urllib.request import Request, urlopen

import psycopg
from psycopg.rows import dict_row

from apps.g2b.schema import (
    BID_NOTICE_TABLE,
    CONTRACT_COMPANY_TABLE,
    CONTRACT_DEMAND_ORG_TABLE,
    CONTRACT_TABLE,
    DEFAULT_SCHEMA,
    LICENSE_CONSTRAINT_TABLE,
    PARTICIPATION_REGION_TABLE,
    PROCUREMENT_COMPANY_INDUSTRY_TABLE,
    PROCUREMENT_COMPANY_TABLE,
    SUCCESSFUL_BID_TABLE,
)


async def build_summary(*, redis_url: str, checkpoint_database_url: str) -> dict[str, Any]:
    del redis_url, checkpoint_database_url
    return await asyncio.to_thread(_build_summary_sync)


def _build_summary_sync() -> dict[str, Any]:
    database_url = os.getenv("G2B_INGEST_DATABASE_URL")
    if not database_url:
        return _unavailable_summary("G2B_INGEST_DATABASE_URL is not configured")

    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA)
    normalized_table = os.getenv("G2B_NORMALIZED_TABLE", BID_NOTICE_TABLE.normalized_table)
    raw_table = os.getenv("G2B_INGEST_TABLE", BID_NOTICE_TABLE.raw_table or "bid_public_notice_raw")
    license_table = os.getenv("G2B_LICENSE_LIMIT_NORMALIZED_TABLE", LICENSE_CONSTRAINT_TABLE.normalized_table)
    region_table = os.getenv("G2B_PARTICIPATION_REGION_NORMALIZED_TABLE", PARTICIPATION_REGION_TABLE.normalized_table)
    success_bid_table = os.getenv("G2B_SUCCESS_BID_NORMALIZED_TABLE", SUCCESSFUL_BID_TABLE.normalized_table)
    company_table = os.getenv("G2B_COMPANY_NORMALIZED_TABLE", PROCUREMENT_COMPANY_TABLE.normalized_table)
    company_industry_table = os.getenv(
        "G2B_COMPANY_INDUSTRY_NORMALIZED_TABLE",
        PROCUREMENT_COMPANY_INDUSTRY_TABLE.normalized_table,
    )
    contract_table = os.getenv("G2B_CONTRACT_NORMALIZED_TABLE", CONTRACT_TABLE.normalized_table)
    contract_company_table = os.getenv("G2B_CONTRACT_COMPANY_TABLE", CONTRACT_COMPANY_TABLE.normalized_table)
    contract_demand_org_table = os.getenv(
        "G2B_CONTRACT_DEMAND_ORG_TABLE",
        CONTRACT_DEMAND_ORG_TABLE.normalized_table,
    )

    table_specs = [
        ("bid_public_notice_raw", raw_table),
        ("bid_public_notice", normalized_table),
        ("bid_public_notice_license_limit_raw", LICENSE_CONSTRAINT_TABLE.raw_table or "bid_public_notice_license_limit_raw"),
        ("bid_public_notice_license_limit", license_table),
        (
            "bid_public_notice_participation_region_raw",
            PARTICIPATION_REGION_TABLE.raw_table or "bid_public_notice_participation_region_raw",
        ),
        ("bid_public_notice_participation_region", region_table),
        ("successful_bid_raw", SUCCESSFUL_BID_TABLE.raw_table or "successful_bid_raw"),
        ("successful_bid", success_bid_table),
        ("procurement_company_raw", PROCUREMENT_COMPANY_TABLE.raw_table or "procurement_company_raw"),
        ("procurement_company", company_table),
        (
            "procurement_company_industry_raw",
            PROCUREMENT_COMPANY_INDUSTRY_TABLE.raw_table or "procurement_company_industry_raw",
        ),
        ("procurement_company_industry", company_industry_table),
        ("contract_raw", CONTRACT_TABLE.raw_table or "contract_raw"),
        ("contract", contract_table),
        ("contract_company", contract_company_table),
        ("contract_demand_organization", contract_demand_org_table),
    ]

    try:
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            table_stats = [
                {
                    "name": display_name,
                    "table": table_name,
                    **_table_stats(conn, schema_name=schema_name, table_name=table_name),
                }
                for display_name, table_name in table_specs
            ]
            pipeline_runs = _prefect_pipeline_runs()
    except Exception as exc:
        return _unavailable_summary(str(exc))

    failed_pipelines = [run for run in pipeline_runs if run["status"] in {"FAILED", "CRASHED", "CANCELLED"}]
    healthy_pipelines = [run for run in pipeline_runs if run["status"] == "COMPLETED"]
    app_status = "degraded" if failed_pipelines else "healthy"

    return {
        "app": "g2b.pipeline",
        "title": "G2B Pipeline",
        "status": app_status,
        "description": "G2B pipeline table freshness and flow status",
        "metrics": [
            {"label": "Tables", "value": len(table_stats)},
            {"label": "Total Rows", "value": sum(row["count"] for row in table_stats)},
            {"label": "Pipelines", "value": len(pipeline_runs)},
            {"label": "Healthy Pipelines", "value": len(healthy_pipelines), "detail": f"{len(failed_pipelines)} failed latest"},
        ],
        "sections": [
            {
                "title": "Data Tables",
                "rows": [
                    {
                        "name": row["name"],
                        "value": row["count"],
                        "freshness": row["latest"],
                    }
                    for row in table_stats
                ],
            },
            {
                "title": "Pipeline Runs",
                "rows": [
                    {
                        "name": run["flow"],
                        "value": run["status"],
                        "last_run_at": run["last_run_at"],
                        "detail": run["detail"],
                    }
                    for run in pipeline_runs
                ],
            },
        ],
    }


def _unavailable_summary(error: str) -> dict[str, Any]:
    return {
        "app": "g2b.pipeline",
        "title": "G2B Pipeline",
        "status": "degraded",
        "description": "G2B canonical and semantic data readiness",
        "error": error,
        "metrics": [],
        "sections": [],
    }


def _table_stats(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> dict[str, Any]:
    if not _table_exists(conn, schema_name=schema_name, table_name=table_name):
        return {"count": 0, "latest": None}
    with conn.cursor() as cursor:
        cursor.execute(
            f'''
            SELECT COUNT(*) AS count, MAX(updated_at) AS latest
            FROM "{schema_name}"."{table_name}"
            '''
        )
        row = cursor.fetchone() or {}
    return {
        "count": int(row.get("count") or 0),
        "latest": _iso(row.get("latest")),
    }


def _prefect_pipeline_runs() -> list[dict[str, Any]]:
    api_url = os.getenv("PREFECT_API_URL")
    if not api_url:
        return []
    flow_names = [
        "g2b-bid-5min-ingest",
        "g2b-success-bid-5min-ingest",
        "g2b-contract-hourly-ingest",
        "g2b-contract-daily-reconcile",
        "g2b-bid-initial-ingest",
        "g2b-success-bid-initial-ingest",
        "g2b-contract-initial-ingest",
    ]
    return [_prefect_latest_flow_run(api_url=api_url, flow_name=flow_name) for flow_name in flow_names]


def _prefect_latest_flow_run(*, api_url: str, flow_name: str) -> dict[str, Any]:
    payload = {
        "flows": {"name": {"any_": [flow_name]}},
        "flow_runs": {
            "state": {
                "type": {
                    "any_": ["COMPLETED", "FAILED", "RUNNING", "CRASHED", "CANCELLED"],
                }
            }
        },
        "sort": "START_TIME_DESC",
        "limit": 1,
    }
    request = Request(
        f"{api_url.rstrip('/')}/flow_runs/filter",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=3) as response:
            rows = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {
            "flow": flow_name,
            "status": "UNKNOWN",
            "last_run_at": None,
            "detail": f"{type(exc).__name__}: {exc}",
        }
    if not rows:
        return {
            "flow": flow_name,
            "status": "UNKNOWN",
            "last_run_at": None,
            "detail": "no finished or running flow run",
        }
    row = rows[0]
    return {
        "flow": flow_name,
        "status": row.get("state_type") or row.get("state_name") or "UNKNOWN",
        "last_run_at": row.get("start_time") or row.get("expected_start_time") or row.get("created"),
        "detail": row.get("name"),
    }


def _table_exists(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> bool:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema_name, table_name),
        )
        row = cursor.fetchone()
    return bool(row and row["exists"])


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None
