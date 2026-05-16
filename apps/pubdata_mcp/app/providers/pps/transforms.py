from __future__ import annotations

from datetime import datetime
from typing import Any

from apps.pubdata_mcp.app.common.normalize import normalize_items


def page_response(
    body: dict[str, Any],
    tool_name: str,
    url: str,
    params: dict[str, Any],
    response_spec: dict[str, Any] | None = None,
    evidence_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_items = items(body)
    return {
        "items": normalize_items(raw_items, response_spec),
        "page": {
            "page_no": optional_int(body.get("pageNo")),
            "num_of_rows": optional_int(body.get("numOfRows")),
        },
        "total_count": optional_int(body.get("totalCount")),
        "evidence": evidence(tool_name, url, params, evidence_metadata),
    }


def api_error(
    result_code: str,
    message: str,
    tool_name: str,
    url: str,
    params: dict[str, Any],
    evidence_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "error": {
            "type": "public_api_error",
            "result_code": result_code,
            "message": message,
        },
        "evidence": evidence(tool_name, url, params, evidence_metadata),
    }


def evidence(
    tool_name: str,
    url: str,
    params: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base = {
        "tool": tool_name,
        "provider": "pps",
        "source": "PPS/G2B public API",
        "url": url,
        "params": params,
        "storage": "none",
        "called_at": datetime.now().astimezone().isoformat(),
    }
    if metadata:
        base.update(metadata)
    return base


def items(body: dict[str, Any]) -> list[dict[str, Any]]:
    values = body.get("items", [])
    if isinstance(values, dict):
        item = values.get("item", [])
        if isinstance(item, list):
            return item
        if isinstance(item, dict):
            return [item]
        return []
    if isinstance(values, list):
        return values
    return []


def optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
