import aiohttp
import aiofiles
import hashlib
from pathlib import Path
#
#
async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    save_path: Path,
) -> tuple[int, str]:
    """
    return: (file_size, sha256)
    """
    save_path.parent.mkdir(parents=True, exist_ok=True)

    sha256 = hashlib.sha256()
    size = 0

    async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as resp:
        resp.raise_for_status()

        async with aiofiles.open(save_path, "wb") as f:
            async for chunk in resp.content.iter_chunked(1024 * 128):
                await f.write(chunk)
                sha256.update(chunk)
                size += len(chunk)

    return size, sha256.hexdigest()


