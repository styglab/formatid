from __future__ import annotations

import os
from typing import Any

import requests

from apps.pubdata_mcp.app.providers.nts.transforms import error_response, success_response


def call_nts_api(
    url: str,
    request_body: dict[str, Any],
    tool_name: str,
    response_spec: dict[str, Any] | None = None,
    evidence_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    service_key = (
        os.getenv("NTS_BUSINESSMAN_API_KEY")
        or os.getenv("ODCLOUD_API_KEY")
        or os.getenv("PUBLIC_API_KEY")
    )
    if not service_key:
        return {
            "error": {
                "type": "missing_config",
                "message": "NTS_BUSINESSMAN_API_KEY, ODCLOUD_API_KEY, or PUBLIC_API_KEY is required.",
            }
        }

    try:
        response = requests.post(
            url,
            params={"serviceKey": service_key, "returnType": "JSON"},
            json=request_body,
            headers={"Accept": "application/json"},
            timeout=20,
        )
    except requests.Timeout:
        return error_response(
            error_type="timeout",
            message="NTS API request timed out.",
            tool_name=tool_name,
            url=url,
            request_body=request_body,
            evidence_metadata=evidence_metadata,
        )
    except requests.RequestException as exc:
        return error_response(
            error_type="transport_error",
            message=str(exc),
            tool_name=tool_name,
            url=url,
            request_body=request_body,
            evidence_metadata=evidence_metadata,
        )

    try:
        payload = response.json()
    except ValueError:
        return error_response(
            error_type="invalid_response",
            message="NTS API response was not valid JSON.",
            tool_name=tool_name,
            url=url,
            request_body=request_body,
            status_code=response.status_code,
            evidence_metadata=evidence_metadata,
        )

    if response.status_code >= 400:
        return error_response(
            error_type=_http_error_type(response.status_code),
            message=_error_message(payload),
            tool_name=tool_name,
            url=url,
            request_body=request_body,
            status_code=response.status_code,
            evidence_metadata=evidence_metadata,
        )

    return success_response(
        payload,
        tool_name=tool_name,
        url=url,
        request_body=request_body,
        response_spec=response_spec,
        evidence_metadata=evidence_metadata,
    )


def _http_error_type(status_code: int) -> str:
    if status_code == 413:
        return "request_too_large"
    if status_code in {429, 411}:
        return "rate_limited"
    if status_code >= 500:
        return "upstream_unavailable"
    return "public_api_error"


def _error_message(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("message", "msg", "error", "detail"):
            value = payload.get(key)
            if value:
                return str(value)
    return "NTS API returned an error."
