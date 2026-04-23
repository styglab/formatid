from __future__ import annotations

from typing import Any


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", [])
    if isinstance(items, dict):
        maybe_item = items.get("item")
        if isinstance(maybe_item, list):
            return [item for item in maybe_item if isinstance(item, dict)]
        if isinstance(maybe_item, dict):
            return [maybe_item]
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    if isinstance(items, dict):
        return [items]
    return []


def extract_total_count(payload: dict[str, Any]) -> int:
    body = payload.get("response", {}).get("body", {})
    total_count = body.get("totalCount", 0)
    try:
        return int(total_count)
    except (TypeError, ValueError):
        return 0
