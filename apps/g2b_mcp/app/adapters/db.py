from __future__ import annotations

import os
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg.sql import Identifier, SQL


G2B_TIMEZONE = ZoneInfo(os.getenv("G2B_TIMEZONE", "Asia/Seoul"))
VALID_CATEGORIES = {"SERVICE", "GOODS", "CONSTRUCTION"}
SORT_COLUMNS = {
    "published_at": "published_at",
    "deadline_at": "deadline_at",
    "budget": "budget",
}


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
    offset: int = 0,
    sort_by: str = "published_at",
    sort_order: str = "desc",
) -> dict[str, Any]:
    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", "g2b")
    table_name = os.getenv("G2B_NORMALIZED_TABLE", "bid_public_notice")
    max_limit = _parse_positive_int(os.getenv("G2B_MCP_MAX_LIMIT", "50"), field_name="G2B_MCP_MAX_LIMIT")
    limit = max(1, min(_parse_positive_int(limit, field_name="limit"), max_limit))
    offset = _parse_non_negative_int(offset, field_name="offset")
    order_sql = _build_order_by(sort_by=sort_by, sort_order=sort_order)

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
        {}
        LIMIT %s
        OFFSET %s
        """
    ).format(table, where_sql, order_sql)

    with psycopg.connect(_database_url(), row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(count_query, params)
            count = int(cursor.fetchone()["count"])
            cursor.execute(rows_query, (*params, limit, offset))
            rows = cursor.fetchall()

    return {
        "source": "normalized_db",
        "count": count,
        "returned": len(rows),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rows) < count,
        "sort": {
            "by": sort_by,
            "order": sort_order.lower(),
        },
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
        category = category.strip().upper()
        if category not in VALID_CATEGORIES:
            raise ValueError(f"Invalid category: {category}")
        clauses.append(SQL("category = %s"))
        params.append(category)
    if keyword and keyword.strip():
        clauses.append(SQL("(title ILIKE %s OR organization_name ILIKE %s OR demand_org_name ILIKE %s)"))
        pattern = f"%{keyword.strip()}%"
        params.extend([pattern, pattern, pattern])
    if organization_name and organization_name.strip():
        clauses.append(SQL("(organization_name ILIKE %s OR demand_org_name ILIKE %s)"))
        pattern = f"%{organization_name.strip()}%"
        params.extend([pattern, pattern])
    if published_from:
        clauses.append(SQL("published_at >= %s"))
        params.append(_parse_datetime(published_from))
    if published_to:
        clauses.append(SQL("published_at <= %s"))
        params.append(_parse_datetime(published_to, end_of_day=True))
    if deadline_from:
        clauses.append(SQL("deadline_at >= %s"))
        params.append(_parse_datetime(deadline_from))
    if deadline_to:
        clauses.append(SQL("deadline_at <= %s"))
        params.append(_parse_datetime(deadline_to, end_of_day=True))
    if min_budget is not None:
        clauses.append(SQL("budget >= %s"))
        params.append(_parse_decimal(min_budget))
    if max_budget is not None:
        clauses.append(SQL("budget <= %s"))
        params.append(_parse_decimal(max_budget))

    if not clauses:
        return SQL(""), tuple(params)
    return SQL("WHERE ") + SQL(" AND ").join(clauses), tuple(params)


def _parse_datetime(value: str, *, end_of_day: bool = False) -> datetime:
    normalized = value.strip()
    if normalized.isdigit():
        if len(normalized) == 8:
            parsed = datetime.strptime(normalized, "%Y%m%d")
            if end_of_day:
                parsed = datetime.combine(parsed.date(), time.max.replace(microsecond=0))
            return parsed.replace(tzinfo=G2B_TIMEZONE)
        if len(normalized) == 12:
            return datetime.strptime(normalized, "%Y%m%d%H%M").replace(tzinfo=G2B_TIMEZONE)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=G2B_TIMEZONE)
        except ValueError:
            continue
    try:
        parsed_date = datetime.strptime(normalized, "%Y-%m-%d").date()
        parsed = datetime.combine(parsed_date, time.max.replace(microsecond=0) if end_of_day else time.min)
        return parsed.replace(tzinfo=G2B_TIMEZONE)
    except ValueError:
        pass
    raise ValueError(f"Invalid datetime: {value}")


def _parse_decimal(value: int | float | str) -> Decimal:
    try:
        return Decimal(str(value).replace(",", "").strip())
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal: {value}") from exc


def _parse_positive_int(value: int | str, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{field_name} must be greater than 0")
    return parsed


def _parse_non_negative_int(value: int | str, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be greater than or equal to 0")
    return parsed


def _build_order_by(*, sort_by: str, sort_order: str) -> SQL:
    sort_key = sort_by.strip().lower()
    sort_column = SORT_COLUMNS.get(sort_key)
    if sort_column is None:
        raise ValueError(f"Invalid sort_by: {sort_by}")

    normalized_order = sort_order.strip().lower()
    if normalized_order not in {"asc", "desc"}:
        raise ValueError(f"Invalid sort_order: {sort_order}")

    direction = SQL("ASC") if normalized_order == "asc" else SQL("DESC")
    id_direction = SQL("ASC") if normalized_order == "asc" else SQL("DESC")
    return SQL("ORDER BY {} {} NULLS LAST, id {}").format(Identifier(sort_column), direction, id_direction)


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
