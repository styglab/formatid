from __future__ import annotations

import os

from shared.tasking.schemas import TaskMessage
from scripts.ops.common import get_redis_url


async def enqueue(
    queue_name: str,
    task_name: str,
    payload: dict,
    attempts: int,
    *,
    dedupe_key: str | None = None,
    correlation_id: str | None = None,
    resource_key: str | None = None,
) -> TaskMessage:
    from services.task_runtime.enqueue import enqueue_task

    return await enqueue_task(
        redis_url=get_redis_url(),
        queue_name=queue_name,
        task_name=task_name,
        payload=payload,
        attempts=attempts,
        dedupe_key=dedupe_key,
        correlation_id=correlation_id,
        resource_key=resource_key,
        status_ttl=int(os.getenv("TASK_STATUS_TTL", "604800")),
    )


async def fetch_task(task_id: str) -> dict | None:
    from services.task_runtime.status_store import TaskStatusStore

    store = TaskStatusStore(redis_url=get_redis_url())
    try:
        return await store.get(task_id)
    finally:
        await store.close()
