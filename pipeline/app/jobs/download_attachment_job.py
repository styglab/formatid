import asyncio
import aiohttp
#
from app.services.attachment.service import process_attachment_batch
from app.db.repositories.attachment_repo import recover_stuck_attachments
from app.core.logger import get_logger
#
#
logger = get_logger("job.download_attachment")
ATTACHMNT_MAX_CONCURRENCY = 5  # 동시에 다운로드 가능한 최대 개수

async def run():
    # 1) 재시작 시 stuck recovery
    recovered = await recover_stuck_attachments()
    if recovered:
        logger.warning(f"recovered {recovered} stuck attachments")    
    
    semaphore = asyncio.Semaphore(ATTACHMNT_MAX_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        while True:
            await process_attachment_batch(
                session=session,
                semaphore=semaphore,
            )
            
            await asyncio.sleep(0.5)


