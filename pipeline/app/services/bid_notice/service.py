from typing import Literal
#
from app.core.external.g2b.endpoints import BID_THNG_PPS, BID_CNST_PPS
from app.core.logger import get_logger
from app.clients.g2b.bid_notice_client import fetch_all_bid_pages
from app.services.bid_notice.preprocessor import is_valid_notice
from app.services.bid_notice.mapper import to_bid_notice_row
from app.services.attachment.parser import parse_attachments
from app.db.repositories.bid_notice_repo import (
    bulk_upsert_bid_notices,
    set_latest_flags,
)
from app.db.repositories.attachment_repo import bulk_insert_attachments
from app.resources.loaders.bid_group_catalog import load_bid_group_catalog

logger = get_logger("service.bid_notice")

async def collect_bids_by_endpoint(
    *,
    endpoint,
    service_key: str,
    inqry_bgn_dt: str,
    inqry_end_dt: str,
    inqry_div: str,
    num_of_rows: int,
) -> list[dict]:
    """
    공통 입찰 공고 수집 (물품 / 공사)
    """
    bid_type = endpoint.name
    groups = load_bid_group_catalog()
    notices: dict[str, dict] = {}  # bidNtceNo 기준 dedup

    for group in groups:
        keywords = group.get("keywords", [])
        if not keywords:
            continue

        for keyword in keywords:
            raw_items = await fetch_all_bid_pages(
                endpoint=endpoint,
                service_key=service_key,
                inqry_bgn_dt=inqry_bgn_dt,
                inqry_end_dt=inqry_end_dt,
                inqry_div=inqry_div,
                bid_ntce_nm=keyword,
                num_of_rows=num_of_rows,
            )

            for item in raw_items:
                # 1️⃣ 규격서 URL 필터
                if not is_valid_notice(item):
                    continue

                # 2️⃣ dedup key
                bid_key = item.get("bidNtceNo")
                if not bid_key:
                    continue

                # 3️⃣ 메타데이터 주입
                item["bid_type"] = bid_type
                item["_group_id"] = group["group_id"]
                item["_matched_keyword"] = keyword

                notices[bid_key] = item

    return list(notices.values())       


async def collect_thng_bids(**kwargs) -> list[dict]:
    return await collect_bids_by_endpoint(
        endpoint=BID_THNG_PPS,
        **kwargs,
    )


async def collect_cnst_bids(**kwargs) -> list[dict]:
    return await collect_bids_by_endpoint(
        endpoint=BID_CNST_PPS,
        **kwargs,
    )

async def save_bid_notices(notices: list[dict]):
    rows = [
        r for r in (to_bid_notice_row(n) for n in notices)
        if r
    ]
    if not rows:
        return

    bid_ntce_nos = list({r["bid_ntce_no"] for r in rows})

    await bulk_upsert_bid_notices(rows)
    await set_latest_flags(bid_ntce_nos)


async def save_attachments(notices: list[dict]):
    rows = []
    for n in notices:
        rows.extend(parse_attachments(n))

    if rows:
        await bulk_insert_attachments(rows)
        
    return len(rows)





