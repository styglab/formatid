from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from apps.g2b_pipeline.app.steps.common import G2BIngestWindow, parse_count


def fetch_g2b_items(url: str, window: G2BIngestWindow) -> list[dict[str, Any]]:
    api_key = os.getenv("G2B_API_KEY") or os.getenv("API_KEY")
    if not api_key:
        raise RuntimeError("G2B_API_KEY is not configured")

    page_no = 1
    num_of_rows = int(os.getenv("G2B_INGEST_NUM_OF_ROWS", "100"))
    records: list[dict[str, Any]] = []

    while True:
        params = {
            "ServiceKey": api_key,
            "inqryDiv": 1,
            "inqryBgnDt": window.begin,
            "inqryEndDt": window.end,
            "pageNo": page_no,
            "numOfRows": num_of_rows,
            "type": "json",
        }
        payload = _get_json(url, params)
        _raise_for_g2b_error(payload)
        body = payload.get("response", {}).get("body", {})
        items = body.get("items", [])
        page_items = items if isinstance(items, list) else []
        records.extend(page_items)

        total_count = parse_count(body.get("totalCount"))
        if not page_items:
            break
        if total_count is not None and len(records) >= total_count:
            break
        if len(page_items) < num_of_rows:
            break
        page_no += 1

    return records


def _get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    request_url = f"{url}?{urlencode(params)}"
    with urlopen(request_url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _raise_for_g2b_error(payload: dict[str, Any]) -> None:
    for key, value in payload.items():
        if key.endswith("ResponseError") and isinstance(value, dict):
            header = value.get("header", {})
            code = header.get("resultCode", "unknown")
            message = header.get("resultMsg", "unknown error")
            raise RuntimeError(f"G2B API error resultCode={code}: {message}")

    header = payload.get("response", {}).get("header", {})
    code = header.get("resultCode") if isinstance(header, dict) else None
    if code and code != "00":
        message = header.get("resultMsg", "unknown error")
        raise RuntimeError(f"G2B API error resultCode={code}: {message}")
