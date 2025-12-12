import os
from dotenv import load_dotenv
#
from app.services.bid_service import collect_bid_notices
from app.core.logger import get_logger
#
#
load_dotenv(".env.pipeline")
logger = get_logger("job.collect")

async def run():
    service_key = os.getenv("PUBLIC_API_KEY")
    if not service_key:
        raise RuntimeError("PUBLIC_API_KEY is not set.")

    logger.info("start sample collect job.")

    await collect_bid_notices(
        service_key=service_key,
        inqry_bgn_dt="202001020000",
        inqry_end_dt="202001022359",
        inqry_div=1, # 1: 등록일자 기준
        page_no=1,   # 샘플
        num_of_rows=10,   # 샘플
    )

    logger.info("finish sample collect job.")

