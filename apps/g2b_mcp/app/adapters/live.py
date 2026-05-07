from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

from apps.g2b_mcp.app.adapters import db
from apps.g2b_mcp.app.adapters.g2b import BASE_URLS, fetch_bids_by_category
from apps.shared.data_pipeline.app.steps.g2b_bid import normalize_raw_row, resource_key


def search_live_bids(
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
    categories = _categories(category)
    max_limit = db._parse_positive_int(os.getenv("G2B_MCP_MAX_LIMIT", "50"), field_name="G2B_MCP_MAX_LIMIT")
    limit = max(1, min(db._parse_positive_int(limit, field_name="limit"), max_limit))
    offset = db._parse_non_negative_int(offset, field_name="offset")

    rows: list[dict[str, Any]] = []
    for item_category in categories:
        for raw in fetch_bids_by_category(item_category, published_from=published_from):
            rows.append(_normalize(item_category, raw))

    filtered = [
        row
        for row in rows
        if _matches_text(row, keyword=keyword, organization_name=organization_name)
        and _matches_datetime(row.get("published_at"), lower=published_from, upper=published_to)
        and _matches_datetime(row.get("deadline_at"), lower=deadline_from, upper=deadline_to)
        and _matches_budget(row.get("budget"), lower=min_budget, upper=max_budget)
    ]
    _sort_rows(filtered, sort_by=sort_by, sort_order=sort_order)
    page = filtered[offset : offset + limit]

    return {
        "source": "g2b_api",
        "count": len(filtered),
        "returned": len(page),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(page) < len(filtered),
        "sort": {
            "by": sort_by,
            "order": sort_order.lower(),
        },
        "bids": [_format_bid(row) for row in page],
    }


def _categories(category: str | None) -> list[str]:
    if category is None or not category.strip():
        return sorted(BASE_URLS)
    normalized = category.strip().upper()
    if normalized not in db.VALID_CATEGORIES:
        raise ValueError(f"Invalid category: {normalized}")
    return [normalized]


def _normalize(category: str, raw: dict[str, Any]) -> dict[str, Any]:
    row = {
        "id": 0,
        "category": category,
        "source_url": BASE_URLS[category],
        "resource_key": resource_key(category, raw),
        "raw_payload": raw,
    }
    return normalize_raw_row(row)


def _matches_text(row: dict[str, Any], *, keyword: str | None, organization_name: str | None) -> bool:
    if keyword and keyword.strip():
        lowered = keyword.strip().lower()
        haystack = " ".join(
            str(value or "")
            for value in (row.get("title"), row.get("organization_name"), row.get("demand_org_name"))
        ).lower()
        if lowered not in haystack:
            return False

    if organization_name and organization_name.strip():
        lowered = organization_name.strip().lower()
        haystack = " ".join(str(value or "") for value in (row.get("organization_name"), row.get("demand_org_name"))).lower()
        if lowered not in haystack:
            return False

    return True


def _matches_datetime(value: Any, *, lower: str | None, upper: str | None) -> bool:
    if lower is None and upper is None:
        return True
    if value is None:
        return False
    if lower is not None and value < db._parse_datetime(lower):
        return False
    if upper is not None and value > db._parse_datetime(upper, end_of_day=True):
        return False
    return True


def _matches_budget(value: Any, *, lower: int | float | str | None, upper: int | float | str | None) -> bool:
    if lower is None and upper is None:
        return True
    if value is None:
        return False
    if lower is not None and value < db._parse_decimal(lower):
        return False
    if upper is not None and value > db._parse_decimal(upper):
        return False
    return True


def _sort_rows(rows: list[dict[str, Any]], *, sort_by: str, sort_order: str) -> None:
    normalized = sort_by.strip().lower()
    if normalized not in db.SORT_COLUMNS:
        raise ValueError(f"Invalid sort_by: {sort_by}")
    normalized_order = sort_order.strip().lower()
    if normalized_order not in {"asc", "desc"}:
        raise ValueError(f"Invalid sort_order: {sort_order}")

    non_null_rows = [row for row in rows if row.get(normalized) is not None]
    null_rows = [row for row in rows if row.get(normalized) is None]
    non_null_rows.sort(key=lambda row: row[normalized], reverse=normalized_order == "desc")
    rows[:] = non_null_rows + null_rows


def _format_bid(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["resource_key"],
        "category": row["category"],
        "category_label": row["category_label"],
        "title": row["title"],
        "organization_name": row["organization_name"],
        "demand_org_name": row["demand_org_name"],
        "budget": int(row["budget"]) if isinstance(row.get("budget"), Decimal) else row.get("budget"),
        "published_at": db._format_datetime(row["published_at"]),
        "deadline_at": db._format_datetime(row["deadline_at"]),
        "opening_at": db._format_datetime(row["opening_at"]),
        "contract_method": row["contract_method"],
        "bid_method": row["bid_method"],
        "notice_kind": row["notice_kind"],
        "detail_url": row["detail_url"],
        "metadata": {
            "bid_notice_no": row["bid_notice_no"],
            "bid_notice_order": row["bid_notice_order"],
        },
    }
