from __future__ import annotations

from typing import Any

from apps.pubdata_mcp.app.providers.pps.constants import CATEGORY_ALIASES


def normalize_category(value: str) -> str:
    category = CATEGORY_ALIASES.get(str(value).strip())
    if not category:
        raise ValueError("category must be one of GOODS, SERVICE, CONSTRUCTION, FOREIGN.")
    return category


def build_bid_notice_params(
    date_from: str,
    date_to: str,
    keyword: str | None = None,
    page_no: int = 1,
    num_of_rows: int = 10,
) -> dict[str, Any]:
    params = {
        "inqryDiv": 1,
        "inqryBgnDt": datetime_param(date_from, start=True),
        "inqryEndDt": datetime_param(date_to, start=False),
        "pageNo": page_no,
        "numOfRows": clamp_page_size(num_of_rows),
    }
    if keyword:
        params["bidNtceNm"] = keyword
    return params


def build_contract_params(
    contract_date_from: str,
    contract_date_to: str,
    keyword: str | None = None,
    page_no: int = 1,
    num_of_rows: int = 10,
) -> dict[str, Any]:
    params = {
        "inqryDiv": 1,
        "inqryBgnDate": date_param(contract_date_from),
        "inqryEndDate": date_param(contract_date_to),
        "pageNo": page_no,
        "numOfRows": clamp_page_size(num_of_rows),
    }
    if keyword:
        params["cntrctNm"] = keyword
    return params


def clamp_page_size(value: int) -> int:
    return min(max(value, 1), 100)


def datetime_param(value: str, start: bool) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 8:
        return f"{digits}{'0000' if start else '2359'}"
    if len(digits) == 12:
        return digits
    raise ValueError("date must be YYYYMMDD, YYYYMMDDHHMM, or ISO-like date.")


def date_param(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    raise ValueError("date must be YYYYMMDD or ISO-like date.")
