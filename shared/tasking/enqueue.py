from typing import Any

from shared.queue.redis import RedisTaskQueue
from shared.tasking.catalog import get_task_definition
from shared.tasking.schemas import TaskMessage
from shared.tasking.status_store import TaskStatusStore
from shared.tasking.routing import validate_task_route
from shared.tasking.validation import validate_task_payload


async def enqueue_task(
    *,
    redis_url: str,
    queue_name: str,
    task_name: str,
    payload: dict[str, Any],
    attempts: int = 0,
    status_ttl: int = 604800,
) -> TaskMessage:
    validate_task_route(queue_name=queue_name, task_name=task_name)
    validated_payload = validate_task_payload(task_name=task_name, payload=payload)
    queue = RedisTaskQueue(redis_url=redis_url, queue_name=queue_name)
    status_store = TaskStatusStore(redis_url=redis_url, ttl_seconds=status_ttl)
    definition = get_task_definition(task_name)
    message = TaskMessage(
        queue_name=queue_name,
        task_name=task_name,
        payload=validated_payload,
        attempts=attempts,
    )

    try:
        await queue.put(message)
        await status_store.mark_queued(
            message,
            policy_snapshot={
                "queue_name": definition.queue_name,
                "payload_schema": definition.payload_schema,
                "max_retries": definition.max_retries,
                "retryable": definition.retryable,
                "backoff_seconds": definition.backoff_seconds,
                "timeout_seconds": definition.timeout_seconds,
                "dlq_enabled": definition.dlq_enabled,
                "dlq_requeue_limit": definition.dlq_requeue_limit,
                "dlq_requeue_keep_attempts": definition.dlq_requeue_keep_attempts,
            },
        )
    finally:
        await status_store.close()
        await queue.close()

    return message
