from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from apps.g2b.pipeline.app.steps.bid_notices import CATEGORY_LABELS
from apps.g2b.pipeline.app.steps.common import (
    clean,
    parse_decimal,
    parse_g2b_datetime,
    parse_int,
)


SUCCESS_BID_URLS = {
    "SERVICE": "http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusServcPPSSrch",
    "GOODS": "http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThngPPSSrch",
    "CONSTRUCTION": "http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusCnstwkPPSSrch",
    "FOREIGN": "http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusFrgcptPPSSrch",
}


def success_bid_resource_key(category: str, record: dict[str, Any]) -> str:
    bid_no = clean(record.get("bidNtceNo")) or "unknown"
    bid_order = clean(record.get("bidNtceOrd")) or "000"
    bid_classification_no = clean(record.get("bidClsfcNo")) or "0"
    rebid_no = clean(record.get("rbidNo")) or "000"
    return f"{category}:{bid_no}:{bid_order}:{bid_classification_no}:{rebid_no}"


def normalize_success_bid_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["raw_payload"]
    category = str(row["category"])
    bid_notice_no = clean(raw.get("bidNtceNo")) or "unknown"
    bid_notice_order = clean(raw.get("bidNtceOrd")) or "000"
    bid_classification_no = clean(raw.get("bidClsfcNo")) or "0"
    rebid_no = clean(raw.get("rbidNo")) or "000"
    return {
        "resource_key": row["resource_key"]
        or f"{category}:{bid_notice_no}:{bid_notice_order}:{bid_classification_no}:{rebid_no}",
        "category": category,
        "category_label": CATEGORY_LABELS.get(category, category),
        "bid_notice_no": bid_notice_no,
        "bid_notice_order": bid_notice_order,
        "bid_classification_no": bid_classification_no,
        "rebid_no": rebid_no,
        "notice_division_code": clean(raw.get("ntceDivCd")),
        "title": clean(raw.get("bidNtceNm")) or "(제목 없음)",
        "participant_count": parse_int(raw.get("prtcptCnum")),
        "winner_name": clean(raw.get("bidwinnrNm")),
        "winner_business_no": clean(raw.get("bidwinnrBizno")),
        "winner_ceo_name": clean(raw.get("bidwinnrCeoNm")),
        "winner_address": clean(raw.get("bidwinnrAdrs")),
        "winner_phone_no": clean(raw.get("bidwinnrTelNo")),
        "winning_amount": parse_decimal(raw.get("sucsfbidAmt")),
        "winning_rate": parse_decimal(raw.get("sucsfbidRate")),
        "actual_opening_at": parse_g2b_datetime(raw.get("rlOpengDt")),
        "demand_org_code": clean(raw.get("dminsttCd")),
        "demand_org_name": clean(raw.get("dminsttNm")),
        "registered_at": parse_g2b_datetime(raw.get("rgstDt")),
        "final_success_date": parse_g2b_datetime(raw.get("fnlSucsfDate")),
        "final_success_company_officer": clean(raw.get("fnlSucsfCorpOfcl")),
        "source_url": row["source_url"],
        "raw_id": row["id"],
        "updated_at": datetime.now(UTC),
    }
