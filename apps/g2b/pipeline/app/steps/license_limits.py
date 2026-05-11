from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from apps.g2b.pipeline.app.steps.bid_notices import CATEGORY_BY_LABEL
from apps.g2b.pipeline.app.steps.common import (
    clean,
    parse_g2b_datetime,
    parse_int,
    split_name_code,
)


LICENSE_LIMIT_URL = "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoLicenseLimit"


def license_limit_resource_key(record: dict[str, Any]) -> str:
    bid_no = clean(record.get("bidNtceNo")) or "unknown"
    bid_order = clean(record.get("bidNtceOrd")) or "000"
    group_no = clean(record.get("lmtGrpNo")) or "unknown"
    serial_no = clean(record.get("lmtSno")) or "unknown"
    license_limit = clean(record.get("lcnsLmtNm")) or "unknown"
    return f"LICENSE:{bid_no}:{bid_order}:{group_no}:{serial_no}:{license_limit}"


def normalize_license_limit_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["raw_payload"]
    license_name, license_code = split_name_code(raw.get("lcnsLmtNm"))
    business_div_name = clean(raw.get("bsnsDivNm"))
    return {
        "resource_key": row["resource_key"] or license_limit_resource_key(raw),
        "bid_notice_no": clean(raw.get("bidNtceNo")) or "unknown",
        "bid_notice_order": clean(raw.get("bidNtceOrd")) or "000",
        "category": CATEGORY_BY_LABEL.get(business_div_name) if business_div_name else None,
        "business_div_name": business_div_name,
        "registered_at": parse_g2b_datetime(raw.get("rgstDt")),
        "limit_group_no": parse_int(raw.get("lmtGrpNo")),
        "limit_serial_no": parse_int(raw.get("lmtSno")),
        "license_limit_name": license_name,
        "license_limit_code": license_code,
        "allowed_industries": parse_name_code_list(raw.get("permsnIndstrytyList")),
        "main_field_groups": parse_main_field_groups(raw.get("indstrytyMfrcFldList")),
        "raw_id": row["id"],
        "updated_at": datetime.now(UTC),
    }


def parse_name_code_list(value: Any) -> list[dict[str, str | None]]:
    return [
        {"name": name, "code": code}
        for name, code in (split_name_code(item) for item in _bracket_items(value))
        if name or code
    ]


def parse_main_field_groups(value: Any) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for item in _bracket_items(value):
        parts = [part.strip() for part in item.split("^") if part.strip()]
        if not parts:
            continue
        groups.append({"group_seq": parts[0], "all_of": parts[1:]})
    return groups


def _bracket_items(value: Any) -> list[str]:
    cleaned = clean(value)
    if cleaned is None:
        return []
    matches = re.findall(r"\[([^\]]+)\]", cleaned)
    if matches:
        return matches
    return [item.strip() for item in cleaned.split(",") if item.strip()]
