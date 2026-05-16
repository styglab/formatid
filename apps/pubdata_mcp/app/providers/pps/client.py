from __future__ import annotations

import os
from typing import Any

import requests

from apps.pubdata_mcp.app.providers.pps.transforms import api_error, page_response


def call_pps_api(
    url: str,
    params: dict[str, Any],
    tool_name: str,
    response_spec: dict[str, Any] | None = None,
    evidence_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    service_key = (
        os.getenv("PPS_PUBLIC_API_KEY")
        or os.getenv("G2B_PUBLIC_API_KEY")
        or os.getenv("PUBLIC_API_KEY")
    )
    if not service_key:
        return {
            "error": {
                "type": "missing_config",
                "message": "PPS_PUBLIC_API_KEY, G2B_PUBLIC_API_KEY, or PUBLIC_API_KEY is required.",
            }
        }

    request_params = {
        **params,
        "serviceKey": service_key,
        "type": "json",
    }
    response = requests.get(url, params=request_params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    envelope = payload.get("response", payload)
    header = envelope.get("header", {}) if isinstance(envelope, dict) else {}
    body = envelope.get("body", {}) if isinstance(envelope, dict) else {}
    result_code = str(header.get("resultCode", ""))
    if result_code and result_code != "00":
        return api_error(
            result_code=result_code,
            message=header.get("resultMsg", "Public API returned an error."),
            tool_name=tool_name,
            url=url,
            params=params,
            evidence_metadata=evidence_metadata,
        )
    return page_response(body, tool_name, url, params, response_spec, evidence_metadata)
