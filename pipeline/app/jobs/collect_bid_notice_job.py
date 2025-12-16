from app.core.logger import get_logger
from app.core.deps import get_config
from app.services.bid_notice.service import (
    collect_thng_bids,
    collect_cnst_bids,
    save_bid_notices,
    save_attachments
)

logger = get_logger("job.collect")

async def run(
    *,
    inqry_bgn_dt: str,
    inqry_end_dt: str,
    inqry_div: str,
    num_of_rows: int,
):
    settings = get_config()
    service_key = settings.PUBLIC_API_KEY
    if not service_key:
        raise RuntimeError("PUBLIC_API_KEY is not set.")
    
    logger.info("start collect bid job.")
    
    notices = []
    # 물품 공고 수집
    notices += await collect_thng_bids(
        service_key=service_key,
        inqry_bgn_dt=inqry_bgn_dt,
        inqry_end_dt=inqry_end_dt,
        inqry_div=inqry_div,     
        num_of_rows=num_of_rows, 
    )
    # 공사 공고 수집
    notices += await collect_cnst_bids(
        service_key=service_key,
        inqry_bgn_dt=inqry_bgn_dt,
        inqry_end_dt=inqry_end_dt,
        inqry_div=inqry_div,      
        num_of_rows=num_of_rows,
    )
    
    if not notices:
        logger.info("no notices collected.")
        return
    
    try:
        await save_bid_notices(notices)
    except Exception:
        logger.exception("failed to save bid notices.")
        return
    
    try:
        num_attachments = await save_attachments(notices)
    except Exception:
        logger.exception("failed to save attachments.")
        return
    
    logger.info(f"✔ collected {len(notices)} notices.")
    logger.info(f"✔ collected {num_attachments} attachments.")     



