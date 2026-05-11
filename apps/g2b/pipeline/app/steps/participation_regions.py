from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from apps.g2b.pipeline.app.steps.bid_notices import CATEGORY_BY_LABEL
from apps.g2b.pipeline.app.steps.common import (
    clean,
    first_present,
    parse_g2b_datetime,
    parse_int,
    split_name_code,
)


PARTICIPATION_REGION_URL = "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoPrtcptPsblRgn"


def participation_region_resource_key(record: dict[str, Any]) -> str:
    bid_no = clean(record.get("bidNtceNo")) or "unknown"
    bid_order = clean(record.get("bidNtceOrd")) or "000"
    group_no = clean(record.get("lmtGrpNo")) or "unknown"
    serial_no = clean(record.get("lmtSno")) or "unknown"
    region = clean(first_present(record, "prtcptPsblRgnNm", "prtcptPsblRgn", "rgnNm")) or "unknown"
    return f"REGION:{bid_no}:{bid_order}:{group_no}:{serial_no}:{region}"


def normalize_participation_region_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["raw_payload"]
    region_name, region_code = split_name_code(first_present(raw, "prtcptPsblRgnNm", "prtcptPsblRgn", "rgnNm"))
    region_code = region_code or clean(first_present(raw, "prtcptPsblRgnCd", "rgnCd"))
    business_div_name = clean(raw.get("bsnsDivNm"))
    return {
        "resource_key": row["resource_key"] or participation_region_resource_key(raw),
        "bid_notice_no": clean(raw.get("bidNtceNo")) or "unknown",
        "bid_notice_order": clean(raw.get("bidNtceOrd")) or "000",
        "category": CATEGORY_BY_LABEL.get(business_div_name) if business_div_name else None,
        "business_div_name": business_div_name,
        "registered_at": parse_g2b_datetime(raw.get("rgstDt")),
        "limit_group_no": parse_int(raw.get("lmtGrpNo")),
        "limit_serial_no": parse_int(raw.get("lmtSno")),
        "region_name": region_name,
        "region_code": region_code,
        "raw_id": row["id"],
        "updated_at": datetime.now(UTC),
    }
