import asyncio
from pathlib import Path
#
from app.core.logger import get_logger
from app.services.attachment.downloader import download_file
from app.db.repositories.attachment_repo import (
    claim_attachments,
    mark_downloaded,
    mark_failed,
)


logger = get_logger("service.attachment")

BASE_DIR = Path(__file__).resolve().parents[3]
ATTACHMENT_DOWNLOAD_DIR = BASE_DIR / "data/00_raw/attachments"
ATTACHMENT_FETCH_SIZE = 10  # DB에서 한 번에 가져오는 attachment 개수

async def process_attachment_batch(session, semaphore):
    rows = await claim_attachments(ATTACHMENT_FETCH_SIZE)
    
    if not rows:
        logger.info("no attachments to download, sleep...")
        return
    
    async def handle(row: dict):
        async with semaphore:
            try:
                file_name = row.get("file_name") or f"{row['id']}.bin"
                save_path = (
                    ATTACHMENT_DOWNLOAD_DIR
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
                
                return True

            except Exception as e:
                await mark_failed(row["id"], f"{type(e).__name__}: {e}")
                logger.error(
                    f"download failed id={row['id']} url={row['download_url']} err={e}"
                )
                
                return False

    results = await asyncio.gather(*(handle(r) for r in rows))

    # 배치 단위 요약 로그
    total = len(rows)
    success = sum(results)
    failed = len(results) - success

    logger.info(
        f"download batch finished: total={total} "
        f"success={success} failed={failed}"
    )


