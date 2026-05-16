from __future__ import annotations

from datetime import datetime
from typing import Any

from apps.pubdata_mcp.app.common.normalize import normalize_items


def success_response(
    payload: dict[str, Any],
    *,
    tool_name: str,
    url: str,
    request_body: dict[str, Any],
    response_spec: dict[str, Any] | None = None,
    evidence_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_items = _data_items(payload)
    return {
        "items": normalize_items(raw_items, response_spec),
        "total_count": _optional_int(payload.get("totalCount")),
        "match_count": _optional_int(payload.get("match_cnt")),
        "request_count": _optional_int(payload.get("request_cnt")),
        "evidence": evidence(tool_name, url, request_body, evidence_metadata),
    }


def error_response(
    *,
    error_type: str,
    message: str,
    tool_name: str,
    url: str,
    request_body: dict[str, Any],
    status_code: int | None = None,
    evidence_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    error = {
        "type": error_type,
        "message": message,
    }
    if status_code is not None:
        error["status_code"] = status_code
    return {
        "error": error,
        "evidence": evidence(tool_name, url, request_body, evidence_metadata),
    }


def evidence(
    tool_name: str,
    url: str,
    request_body: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base = {
        "tool": tool_name,
        "provider": "nts",
        "source": "NTS business registration API",
        "url": url,
        "request_body": request_body,
        "storage": "none",
        "called_at": datetime.now().astimezone().isoformat(),
    }
    if metadata:
        base.update(metadata)
    return base


def _data_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    values = payload.get("data", [])
    if isinstance(values, list):
        return [value for value in values if isinstance(value, dict)]
    if isinstance(values, dict):
        return [values]
    return []


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
