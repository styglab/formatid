from __future__ import annotations

import asyncio
import os
from typing import Any

import psycopg
from psycopg.rows import dict_row


async def build_summary(*, redis_url: str, checkpoint_database_url: str) -> dict[str, Any]:
    del redis_url, checkpoint_database_url
    return await asyncio.to_thread(_build_summary_sync)


def _build_summary_sync() -> dict[str, Any]:
    database_url = os.getenv("G2B_INGEST_DATABASE_URL")
    if not database_url:
        return _unavailable_summary("G2B_INGEST_DATABASE_URL is not configured")

    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", "g2b")
    normalized_table = os.getenv("G2B_NORMALIZED_TABLE", "bid_public_notice")
    raw_table = os.getenv("G2B_INGEST_TABLE", "bid_public_notice_raw")
    license_table = os.getenv("G2B_LICENSE_LIMIT_NORMALIZED_TABLE", "bid_public_notice_license_limit")
    region_table = os.getenv("G2B_PARTICIPATION_REGION_NORMALIZED_TABLE", "bid_public_notice_participation_region")
    success_bid_table = os.getenv("G2B_SUCCESS_BID_NORMALIZED_TABLE", "successful_bid")

    try:
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            normalized = _table_stats(conn, schema_name=schema_name, table_name=normalized_table)
            raw = _table_stats(conn, schema_name=schema_name, table_name=raw_table)
            licenses = _table_stats(conn, schema_name=schema_name, table_name=license_table)
            regions = _table_stats(conn, schema_name=schema_name, table_name=region_table)
            success_bids = _table_stats(conn, schema_name=schema_name, table_name=success_bid_table)
            category_counts = _category_counts(conn, schema_name=schema_name, table_name=normalized_table)
            success_bid_category_counts = _category_counts(conn, schema_name=schema_name, table_name=success_bid_table)
            allowed_industries = _allowed_industry_count(conn, schema_name=schema_name, table_name=license_table)
    except Exception as exc:
        return _unavailable_summary(str(exc))

    return {
        "app": "g2b.pipeline",
        "title": "G2B Pipeline",
        "status": "healthy",
        "description": "G2B canonical and semantic data readiness",
        "metrics": [
            {"label": "Raw notices", "value": raw["count"], "detail": raw["latest"]},
            {"label": "Normalized notices", "value": normalized["count"], "detail": normalized["latest"]},
            {"label": "License constraints", "value": licenses["count"], "detail": licenses["latest"]},
            {"label": "Allowed industries", "value": allowed_industries},
            {"label": "Participation regions", "value": regions["count"], "detail": regions["latest"]},
            {"label": "Successful bids", "value": success_bids["count"], "detail": success_bids["latest"]},
        ],
        "sections": [
            {
                "title": "Freshness",
                "rows": [
                    {"name": "Raw latest", "value": raw["latest"] or "-"},
                    {"name": "Normalized latest", "value": normalized["latest"] or "-"},
                    {"name": "License latest", "value": licenses["latest"] or "-"},
                    {"name": "Region latest", "value": regions["latest"] or "-"},
                    {"name": "Success bid latest", "value": success_bids["latest"] or "-"},
                ],
            },
            {
                "title": "Category Counts",
                "rows": [{"name": row["category"] or "unknown", "value": row["count"]} for row in category_counts],
            },
            {
                "title": "Success Bid Category Counts",
                "rows": [{"name": row["category"] or "unknown", "value": row["count"]} for row in success_bid_category_counts],
            },
            {
                "title": "Semantic Coverage",
                "rows": [
                    {"name": "requires", "value": licenses["count"]},
                    {"name": "allows_industry", "value": allowed_industries},
                    {"name": "restricted_to", "value": regions["count"]},
                    {"name": "categorized_as", "value": normalized["count"]},
                    {"name": "awarded_to", "value": success_bids["count"]},
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


def _category_counts(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, schema_name=schema_name, table_name=table_name):
        return []
    with conn.cursor() as cursor:
        cursor.execute(
            f'''
            SELECT category, COUNT(*) AS count
            FROM "{schema_name}"."{table_name}"
            GROUP BY category
            ORDER BY count DESC, category
            '''
        )
        return [{"category": row["category"], "count": int(row["count"])} for row in cursor.fetchall()]


def _allowed_industry_count(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> int:
    if not _table_exists(conn, schema_name=schema_name, table_name=table_name):
        return 0
    with conn.cursor() as cursor:
        cursor.execute(
            f'''
            SELECT COALESCE(SUM(jsonb_array_length(allowed_industries)), 0) AS count
            FROM "{schema_name}"."{table_name}"
            '''
        )
        row = cursor.fetchone() or {}
    return int(row.get("count") or 0)


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
