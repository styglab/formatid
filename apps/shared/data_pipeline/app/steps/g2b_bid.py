from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo


BASE_URLS = {
    "SERVICE": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch",
    "GOODS": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch",
    "CONSTRUCTION": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch",
}

CATEGORY_LABELS = {
    "SERVICE": "용역",
    "GOODS": "물품",
    "CONSTRUCTION": "공사",
}

G2B_TIMEZONE = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class G2BIngestWindow:
    begin: str
    end: str


def compute_previous_hour_window_value(now: datetime | None = None) -> dict[str, str]:
    current = now or datetime.now(G2B_TIMEZONE)
    current = current.astimezone(G2B_TIMEZONE)
    hour_start = current.replace(minute=0, second=0, microsecond=0)
    previous_hour_start = hour_start - timedelta(hours=1)
    previous_hour_end = hour_start - timedelta(minutes=1)
    return {
        "begin": previous_hour_start.strftime("%Y%m%d%H%M"),
        "end": previous_hour_end.strftime("%Y%m%d%H%M"),
    }


def compute_due_hourly_windows(
    *,
    last_succeeded_begin: str | None,
    now: datetime | None = None,
    default_start: str = "202605040000",
    max_windows: int = 6,
) -> list[dict[str, str]]:
    latest = compute_previous_hour_window_value(now)
    latest_start = parse_window_begin(latest["begin"])

    if last_succeeded_begin:
        start = parse_window_begin(last_succeeded_begin) + timedelta(hours=1)
    else:
        start = parse_window_begin(default_start)

    windows: list[dict[str, str]] = []
    current = start
    while current <= latest_start and len(windows) < max_windows:
        windows.append(
            {
                "begin": current.strftime("%Y%m%d%H%M"),
                "end": (current + timedelta(minutes=59)).strftime("%Y%m%d%H%M"),
            }
        )
        current += timedelta(hours=1)
    return windows


def parse_window_begin(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=G2B_TIMEZONE)


def parse_count(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def parse_decimal(value: Any) -> Decimal | None:
    cleaned = clean(value)
    if cleaned is None:
        return None
    try:
        return Decimal(cleaned.replace(",", ""))
    except InvalidOperation:
        return None


def parse_g2b_datetime(value: Any) -> datetime | None:
    cleaned = clean(value)
    if cleaned is None:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return parsed.replace(tzinfo=G2B_TIMEZONE)
        except ValueError:
            continue
    return None
