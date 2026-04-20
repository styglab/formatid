from __future__ import annotations

import os

from shared.tasking.schemas import TaskMessage
from scripts.ops.common import get_redis_url


async def enqueue(queue_name: str, task_name: str, payload: dict, attempts: int) -> TaskMessage:
    from shared.tasking.enqueue import enqueue_task

    return await enqueue_task(
        redis_url=get_redis_url(),
        queue_name=queue_name,
        task_name=task_name,
        payload=payload,
        attempts=attempts,
        status_ttl=int(os.getenv("TASK_STATUS_TTL", "604800")),
    )


async def fetch_task(task_id: str) -> dict | None:
    from shared.tasking.status_store import TaskStatusStore

    store = TaskStatusStore(redis_url=get_redis_url())
    try:
        return await store.get(task_id)
    finally:
        await store.close()
