import sys
import asyncio
#
from app.core.logger import get_logger
from app.db.init_db import init_db
from app.jobs.sample_collect_job import run as collect_job
from app.jobs.download_attachment_job import run as download_job
#
#
logger = get_logger("main")

async def main(mode: str):
    logger.info("▶ initializing database...")
    await init_db()

    if mode == "collect":
        logger.info("▶ running collect job...")
        await collect_job()

    elif mode == "download":
        logger.info("▶ running download job...")
        await download_job()

    else:
        raise ValueError(f"unknown mode: {mode}")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "collect"
    asyncio.run(main(mode))

    
