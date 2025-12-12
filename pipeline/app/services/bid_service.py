from app.core.logger import get_logger
from app.db.repositories.bid_notice_repo import (
    bulk_upsert_bid_notices,
    set_latest_flags,
)
from app.db.repositories.attachment_repo import bulk_insert_attachments
from app.services.attachment_extractor import parse_attachments
from app.services.g2b_client import fetch_bid_notices
from app.services.notice_mapper import to_bid_notice_row
#
#
logger = get_logger("service")

async def collect_bid_notices(service_key: str,
                              inqry_bgn_dt: str,
                              inqry_end_dt: str,
                              inqry_div: int,
                              page_no: int,
                              num_of_rows: int,
):
    """
    공고 수집 + DB 저장 (첨부파일 메타까지)
    """
    logger.info(f"collect bid notices: page={page_no}, rows={num_of_rows}")

    notices = await fetch_bid_notices(
        service_key=service_key,
        inqry_bgn_dt=inqry_bgn_dt,
        inqry_end_dt=inqry_end_dt,
        inqry_div=inqry_div,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )
    
    for notice in notices:
        rows = [to_bid_notice_row(n) for n in notices]
        bid_ntce_nos = list({r["bid_ntce_no"] for r in rows})
        # 1) 공고 저장
        """
        notice_row = to_bid_notice_row(notice)
        await upsert_bid_notice(notice_row)
        """        
        await bulk_upsert_bid_notices(rows)
        
        # 2) 공고 업데이트
        await set_latest_flags(bid_ntce_nos)
        
        # 3) 첨부파일 파싱 + 저장
        attachment_rows = parse_attachments(notice)
        await bulk_insert_attachments(attachment_rows)

    logger.info(f"✔ collected {len(notices)} notices")

