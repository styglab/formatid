from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg.sql import Identifier, SQL


G2B_TIMEZONE = ZoneInfo(os.getenv("G2B_TIMEZONE", "Asia/Seoul"))


def search_bids(
    *,
    category: str | None = None,
    keyword: str | None = None,
    published_from: str | None = None,
    published_to: str | None = None,
    deadline_from: str | None = None,
    deadline_to: str | None = None,
    organization_name: str | None = None,
    min_budget: int | float | str | None = None,
    max_budget: int | float | str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", "g2b")
    table_name = os.getenv("G2B_NORMALIZED_TABLE", "bid_public_notice")
    limit = max(1, min(int(limit), int(os.getenv("G2B_MCP_MAX_LIMIT", "50"))))

    where_sql, params = _build_filters(
        category=category,
        keyword=keyword,
        published_from=published_from,
        published_to=published_to,
        deadline_from=deadline_from,
        deadline_to=deadline_to,
        organization_name=organization_name,
        min_budget=min_budget,
        max_budget=max_budget,
    )

    table = SQL("{}.{}").format(Identifier(schema_name), Identifier(table_name))
    count_query = SQL("SELECT count(*) FROM {} {}").format(table, where_sql)
    rows_query = SQL(
        """
        SELECT
            resource_key,
            category,
            category_label,
            bid_notice_no,
            bid_notice_order,
            title,
            organization_name,
            demand_org_name,
            budget,
            published_at,
            deadline_at,
            opening_at,
            contract_method,
            bid_method,
            notice_kind,
            detail_url
        FROM {} {}
        ORDER BY published_at DESC NULLS LAST, id DESC
        LIMIT %s
        """
    ).format(table, where_sql)

    with psycopg.connect(_database_url(), row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(count_query, params)
            count = int(cursor.fetchone()["count"])
            cursor.execute(rows_query, (*params, limit))
            rows = cursor.fetchall()

    return {
        "count": count,
        "returned": len(rows),
        "bids": [_format_bid(row) for row in rows],
    }


def _database_url() -> str:
    database_url = os.getenv("G2B_MCP_DATABASE_URL") or os.getenv("G2B_INGEST_DATABASE_URL")
    if not database_url:
        raise RuntimeError("G2B_MCP_DATABASE_URL or G2B_INGEST_DATABASE_URL is not configured")
    return database_url


def _build_filters(
    *,
    category: str | None,
    keyword: str | None,
    published_from: str | None,
    published_to: str | None,
    deadline_from: str | None,
    deadline_to: str | None,
    organization_name: str | None,
    min_budget: int | float | str | None,
    max_budget: int | float | str | None,
) -> tuple[SQL, tuple[Any, ...]]:
    clauses: list[SQL] = []
    params: list[Any] = []

    if category:
        clauses.append(SQL("category = %s"))
        params.append(category.strip().upper())
    if keyword:
        clauses.append(SQL("(title ILIKE %s OR organization_name ILIKE %s OR demand_org_name ILIKE %s)"))
        pattern = f"%{keyword.strip()}%"
        params.extend([pattern, pattern, pattern])
    if organization_name:
        clauses.append(SQL("(organization_name ILIKE %s OR demand_org_name ILIKE %s)"))
        pattern = f"%{organization_name.strip()}%"
        params.extend([pattern, pattern])
    if published_from:
        clauses.append(SQL("published_at >= %s"))
        params.append(_parse_datetime(published_from))
    if published_to:
        clauses.append(SQL("published_at <= %s"))
        params.append(_parse_datetime(published_to))
    if deadline_from:
        clauses.append(SQL("deadline_at >= %s"))
        params.append(_parse_datetime(deadline_from))
    if deadline_to:
        clauses.append(SQL("deadline_at <= %s"))
        params.append(_parse_datetime(deadline_to))
    if min_budget is not None:
        clauses.append(SQL("budget >= %s"))
        params.append(_parse_decimal(min_budget))
    if max_budget is not None:
        clauses.append(SQL("budget <= %s"))
        params.append(_parse_decimal(max_budget))

    if not clauses:
        return SQL(""), tuple(params)
    return SQL("WHERE ") + SQL(" AND ").join(clauses), tuple(params)


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.isdigit():
        if len(normalized) == 8:
            return datetime.strptime(f"{normalized}0000", "%Y%m%d%H%M")
        if len(normalized) == 12:
            return datetime.strptime(normalized, "%Y%m%d%H%M")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid datetime: {value}")


def _parse_decimal(value: int | float | str) -> Decimal:
    return Decimal(str(value).replace(",", "").strip())


def _format_bid(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["resource_key"],
        "category": row["category"],
        "category_label": row["category_label"],
        "title": row["title"],
        "organization_name": row["organization_name"],
        "demand_org_name": row["demand_org_name"],
        "budget": int(row["budget"]) if row["budget"] is not None else None,
        "published_at": _format_datetime(row["published_at"]),
        "deadline_at": _format_datetime(row["deadline_at"]),
        "opening_at": _format_datetime(row["opening_at"]),
        "contract_method": row["contract_method"],
        "bid_method": row["bid_method"],
        "notice_kind": row["notice_kind"],
        "detail_url": row["detail_url"],
        "metadata": {
            "bid_notice_no": row["bid_notice_no"],
            "bid_notice_order": row["bid_notice_order"],
        },
    }


def _format_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is not None:
        return value.astimezone(G2B_TIMEZONE).isoformat()
    return value.replace(tzinfo=G2B_TIMEZONE).isoformat()
