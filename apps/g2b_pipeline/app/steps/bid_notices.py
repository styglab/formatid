from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from apps.g2b_pipeline.app.steps.common import (
    G2BIngestWindow,
    G2B_TIMEZONE,
    clean,
    parse_decimal,
    parse_g2b_datetime,
)


BASE_URLS = {
    "SERVICE": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch",
    "GOODS": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch",
    "CONSTRUCTION": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch",
    "FOREIGN": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoFrgcptPPSSrch",
}

CATEGORY_LABELS = {
    "SERVICE": "용역",
    "GOODS": "물품",
    "CONSTRUCTION": "공사",
    "FOREIGN": "외자",
}

CATEGORY_BY_LABEL = {label: category for category, label in CATEGORY_LABELS.items()}


def compute_realtime_window_value(
    now: datetime | None = None,
    *,
    lookback_minutes: int = 90,
) -> dict[str, str]:
    if lookback_minutes < 1:
        raise ValueError("lookback_minutes must be greater than 0")

    current = now or datetime.now(G2B_TIMEZONE)
    current = current.astimezone(G2B_TIMEZONE).replace(second=0, microsecond=0)
    begin = current - timedelta(minutes=lookback_minutes)
    return {
        "begin": begin.strftime("%Y%m%d%H%M"),
        "end": current.strftime("%Y%m%d%H%M"),
    }


def resource_key(category: str, record: dict[str, Any]) -> str:
    bid_no = record.get("bidNtceNo") or "unknown"
    bid_order = record.get("bidNtceOrd") or "000"
    return f"{category}:{bid_no}:{bid_order}"


def normalize_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["raw_payload"]
    category = str(row["category"])
    bid_notice_no = clean(raw.get("bidNtceNo")) or "unknown"
    bid_notice_order = clean(raw.get("bidNtceOrd")) or "000"
    return {
        "resource_key": row["resource_key"] or f"{category}:{bid_notice_no}:{bid_notice_order}",
        "category": category,
        "category_label": CATEGORY_LABELS.get(category, category),
        "bid_notice_no": bid_notice_no,
        "bid_notice_order": bid_notice_order,
        "title": clean(raw.get("bidNtceNm")) or "(제목 없음)",
        "organization_name": clean(raw.get("ntceInsttNm")),
        "demand_org_name": clean(raw.get("dminsttNm")),
        "budget": parse_decimal(raw.get("presmptPrce")) or parse_decimal(raw.get("asignBdgtAmt")),
        "published_at": parse_g2b_datetime(raw.get("bidNtceDt")),
        "deadline_at": parse_g2b_datetime(raw.get("bidClseDt")),
        "opening_at": parse_g2b_datetime(raw.get("opengDt")),
        "contract_method": clean(raw.get("cntrctCnclsMthdNm")),
        "bid_method": clean(raw.get("bidMethdNm")),
        "notice_kind": clean(raw.get("ntceKindNm")),
        "detail_url": clean(raw.get("bidNtceDtlUrl")) or clean(raw.get("bidNtceUrl")),
        "source_url": row["source_url"],
        "raw_id": row["id"],
        "updated_at": datetime.now(UTC),
    }
