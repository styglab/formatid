import os
from datetime import datetime, timedelta
from math import ceil
from zoneinfo import ZoneInfo

import requests

API_KEY = os.getenv("G2B_API_KEY") or os.getenv("API_KEY")
G2B_TIMEZONE = ZoneInfo(os.getenv("G2B_TIMEZONE", "Asia/Seoul"))

BASE_URLS = {
    "SERVICE": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch",
    "GOODS": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch",
    "CONSTRUCTION": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch"
}



def fetch_bids_by_category(category: str, published_from: str | None = None):
    normalized_category = category.strip().upper()
    url = BASE_URLS.get(normalized_category)

    if not url:
        raise ValueError(f"Invalid category: {category}")
    if not API_KEY:
        raise RuntimeError("G2B_API_KEY is not configured")

    num_of_rows = int(os.getenv("G2B_NUM_OF_ROWS", "100"))
    params = {
        "ServiceKey": API_KEY,
        "type": "json",
        "numOfRows": num_of_rows,
        "inqryDiv": 1,
        "inqryBgnDt": _query_begin_datetime(published_from),
        "inqryEndDt": _query_end_datetime(),
    }

    all_items: list[dict] = []
    total_count: int | None = None
    page_no = 1

    while total_count is None or len(all_items) < total_count:
        payload = _get_page(url, params | {"pageNo": page_no})
        body = payload.get("response", {}).get("body", {})
        items = body.get("items", [])
        page_items = items if isinstance(items, list) else []

        if total_count is None:
            total_count = _parse_total_count(body.get("totalCount"))

        all_items.extend(page_items)

        if not page_items:
            break
        if total_count is None and len(page_items) < num_of_rows:
            break
        if total_count is not None and page_no >= ceil(total_count / num_of_rows):
            break

        page_no += 1

    return all_items


def _get_page(url: str, params: dict) -> dict:
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()
    return res.json()


def _query_begin_datetime(published_from: str | None = None) -> str:
    if published_from:
        return _parse_query_datetime(published_from)

    lookback_days = int(os.getenv("G2B_LOOKBACK_DAYS", "30"))
    return (datetime.now(G2B_TIMEZONE) - timedelta(days=lookback_days)).strftime("%Y%m%d0000")


def _query_end_datetime() -> str:
    return datetime.now(G2B_TIMEZONE).strftime("%Y%m%d%H%M")


def _parse_query_datetime(value: str) -> str:
    normalized = value.strip()

    if normalized.isdigit():
        if len(normalized) == 8:
            return f"{normalized}0000"
        if len(normalized) == 12:
            return normalized

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).strftime("%Y%m%d%H%M")
        except ValueError:
            continue

    raise ValueError(f"Invalid published_from: {value}")


def _parse_total_count(value: object) -> int | None:
    try:
        total_count = int(value)
    except (TypeError, ValueError):
        return None
    return max(total_count, 0)
