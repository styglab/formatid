from __future__ import annotations

import os

from core.runtime.task_runtime.schemas import TaskMessage
from scripts.ops.common import get_redis_url


async def enqueue(
    task_name: str,
    payload: dict,
    attempts: int,
    *,
    queue_name: str | None = None,
    dedupe_key: str | None = None,
    correlation_id: str | None = None,
    resource_key: str | None = None,
) -> TaskMessage:
    from core.runtime.task_runtime.enqueue import enqueue_task

    return await enqueue_task(
        redis_url=get_redis_url(),
        task_name=task_name,
        payload=payload,
        queue_name=queue_name,
        attempts=attempts,
        dedupe_key=dedupe_key,
        correlation_id=correlation_id,
        resource_key=resource_key,
        status_ttl=int(os.getenv("TASK_STATUS_TTL", "604800")),
    )


async def fetch_task(task_id: str) -> dict | None:
    from core.runtime.task_runtime.status_store import TaskStatusStore

    store = TaskStatusStore(redis_url=get_redis_url())
    try:
        return await store.get(task_id)
    finally:
        await store.close()
