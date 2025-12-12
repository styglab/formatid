import asyncio
import aiohttp
from pathlib import Path
#
from app.db.repositories.attachment_repo import (
    recover_stuck_attachments,
    claim_attachments,
    mark_downloaded,
    mark_failed,
)
from app.services.file_downloader import download_file
from app.core.logger import get_logger
#
#
logger = get_logger("job.download")

BASE_DIR = Path(__file__).resolve().parents[3]
DOWNLOAD_DIR = BASE_DIR / "data/00_raw/attachments"

CONCURRENCY = 5  # DB에서 한 번에 가져오는 attachment 개수
BATCH_SIZE = 10  # 동시에 다운로드 가능한 최대 개수

async def run():
    # 1) 재시작 시 stuck recovery
    recovered = await recover_stuck_attachments()
    if recovered:
        logger.warning(f"recovered {recovered} stuck attachments")    
    
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        while True:
            rows = await claim_attachments(BATCH_SIZE)
            if not rows:
                logger.info("no attachments to download, sleep...")
                await asyncio.sleep(3)
                continue

            stats = {"success": 0, "failed": 0}
            
            async def handle(row: dict):
                async with semaphore:
                    try:
                        file_name = row["file_name"] or f"{row['id']}.bin"
                        save_path = (
                            DOWNLOAD_DIR
                            / row["bid_ntce_no"]
                            / row["bid_ntce_ord"]
                            / file_name
                        )

                        size, sha = await download_file(
                            session,
                            row["download_url"],
                            save_path,
                        )

                        await mark_downloaded(
                            id_=row["id"],
                            storage_path=str(save_path),
                            file_size=size,
                            file_hash=sha,
                        )
                        
                        stats["success"] += 1

                    except Exception as e:
                        stats["failed"] += 1
                        await mark_failed(row["id"], repr(e))
                        logger.error(
                            f"download failed id={row['id']} url={row['download_url']} err={e}"
                        )

            await asyncio.gather(*(handle(r) for r in rows))

            # 배치 단위 요약 로그
            total = len(rows)
            success = stats["success"]
            failed = stats["failed"]

            logger.info(
                f"download batch finished: total={total} "
                f"success={success} failed={failed}"
            )

