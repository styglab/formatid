from typing import Any

from core.runtime.task_runtime.queue import RedisTaskQueue
from core.runtime.runtime_db.url import get_checkpoint_database_url
from core.observability.safe_record import safe_record
from core.runtime.task_runtime.catalog import get_task_definition
from core.runtime.task_runtime.execution_store import PostgresTaskExecutionStore
from core.runtime.task_runtime.schemas import TaskMessage
from core.runtime.task_runtime.status_store import TaskStatusStore
from core.runtime.task_runtime.routing import validate_task_route
from core.runtime.task_runtime.validation import validate_task_payload


def _log_event(logger, level, event, **fields) -> None:
    logger.log(level, "%s %s", event, fields)


async def enqueue_task(
    *,
    redis_url: str,
    task_name: str,
    payload: dict[str, Any],
    queue_name: str | None = None,
    attempts: int = 0,
    status_ttl: int = 604800,
    dedupe_key: str | None = None,
    dedupe_ttl: int | None = None,
    correlation_id: str | None = None,
    resource_key: str | None = None,
) -> TaskMessage:
    definition = get_task_definition(task_name)
    resolved_queue_name = definition.queue_name
    if queue_name is not None:
        validate_task_route(queue_name=queue_name, task_name=task_name)
    validated_payload = validate_task_payload(task_name=task_name, payload=payload)
    queue = RedisTaskQueue(redis_url=redis_url, queue_name=resolved_queue_name)
    status_store = TaskStatusStore(redis_url=redis_url, ttl_seconds=status_ttl)
    execution_store = PostgresTaskExecutionStore(database_url=get_checkpoint_database_url(host_default="postgres"))
    message = TaskMessage(
        queue_name=resolved_queue_name,
        task_name=task_name,
        payload=validated_payload,
        attempts=attempts,
        dedupe_key=dedupe_key,
        correlation_id=correlation_id,
        resource_key=resource_key,
    )

    try:
        if dedupe_key:
            existing = await _claim_dedupe_key(
                redis_url=redis_url,
                service_name=definition.service_name,
                task_name=task_name,
                dedupe_key=dedupe_key,
                task_id=message.task_id,
                ttl_seconds=dedupe_ttl or status_ttl,
            )
            if existing is not None:
                existing_status = await status_store.get(existing)
                if existing_status is not None:
                    return TaskMessage(
                        queue_name=existing_status["queue_name"],
                        task_name=existing_status["task_name"],
                        payload=existing_status.get("payload", {}),
                        attempts=int(existing_status.get("attempts", 0)),
                        task_id=existing_status["task_id"],
                        dedupe_key=existing_status.get("dedupe_key"),
                        correlation_id=existing_status.get("correlation_id"),
                        resource_key=existing_status.get("resource_key"),
                    )
                return TaskMessage(
                    queue_name=resolved_queue_name,
                    task_name=task_name,
                    payload=validated_payload,
                    attempts=attempts,
                    task_id=existing,
                    dedupe_key=dedupe_key,
                    correlation_id=correlation_id,
                    resource_key=resource_key,
                )
        status_document = await status_store.mark_queued(
            message,
            policy_snapshot={
                "service_name": definition.service_name,
                "queue_name": definition.queue_name,
                "payload_schema": definition.payload_schema,
                "output_schema": definition.output_schema,
                "max_retries": definition.max_retries,
                "retryable": definition.retryable,
                "backoff_seconds": definition.backoff_seconds,
                "timeout_seconds": definition.timeout_seconds,
                "dlq_enabled": definition.dlq_enabled,
                "dlq_requeue_limit": definition.dlq_requeue_limit,
                "dlq_requeue_keep_attempts": definition.dlq_requeue_keep_attempts,
            },
        )
        import logging

        await safe_record(
            execution_store.upsert(status_document),
            logger=logging.getLogger("tasking.enqueue"),
            log_event=_log_event,
            event="task_execution_history_record_failed",
            task_id=status_document.get("task_id"),
            task_name=status_document.get("task_name"),
            status=status_document.get("status"),
        )
        await queue.put(message)
    finally:
        await execution_store.close()
        await status_store.close()
        await queue.close()

    return message


async def _claim_dedupe_key(
    *,
    redis_url: str,
    service_name: str,
    task_name: str,
    dedupe_key: str,
    task_id: str,
    ttl_seconds: int,
) -> str | None:
    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url, decode_responses=True)
    key = f"task:dedupe:{service_name}:{task_name}:{dedupe_key}"
    try:
        claimed = await redis.set(key, task_id, nx=True, ex=ttl_seconds)
        if claimed:
            return None
        return await redis.get(key)
    finally:
        await redis.aclose()
