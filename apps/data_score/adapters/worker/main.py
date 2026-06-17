from __future__ import annotations

import asyncio
import os

from apps.data_score.domain.repositories.evaluation_runs import DataScoreRunRepository
from apps.data_score.domain.service.evaluation_runs import process_next_pending_run
from core.runtime.runtime_db.url import get_database_url


POLL_INTERVAL_SECONDS = float(os.getenv("DATA_SCORE_WORKER_POLL_INTERVAL_SECONDS", "2"))


async def run_worker() -> None:
    repository = DataScoreRunRepository(
        database_url=get_database_url("DATA_SCORE_DATABASE_URL", host_default="postgres")
    )
    while True:
        processed = await process_next_pending_run(repository)
        if processed is None:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
