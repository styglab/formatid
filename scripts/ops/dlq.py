from __future__ import annotations

import json
from typing import Any

from core.runtime.runtime_db.url import get_checkpoint_database_url
from core.observability.safe_record import safe_record
from core.runtime.task_runtime.catalog import get_task_definition
from core.runtime.task_runtime.execution_store import PostgresTaskExecutionStore
from core.runtime.task_runtime.schemas import TaskMessage
from core.runtime.time import iso_now
from scripts.ops.common import build_dlq_queue_name, get_redis_url


def _log_event(logger, level, event, **fields) -> None:
    logger.log(level, "%s %s", event, fields)


async def inspect_dlq(queue_names: list[str], *, limit: int) -> dict:
    from redis.asyncio import Redis
    from core.runtime.task_runtime.status_store import TaskStatusStore

    redis_url = get_redis_url()
    redis = Redis.from_url(redis_url, decode_responses=True)
    status_store = TaskStatusStore(redis_url=redis_url)

    try:
        report: dict[str, dict] = {}
        for queue_name in queue_names:
            dlq_queue_name = build_dlq_queue_name(queue_name)
            raw_messages = await redis.lrange(dlq_queue_name, 0, max(limit - 1, -1))
            messages = []
            for raw_message in raw_messages:
                message = TaskMessage.from_dict(json.loads(raw_message))
                status = await status_store.get(message.task_id)
                messages.append(
                    {
                        **message.to_dict(),
                        "dlq_requeue_count": 0 if status is None else status.get("dlq_requeue_count", 0),
                        "last_error": None if status is None else status.get("last_error"),
                    }
                )
            report[queue_name] = {
                "dlq_queue_name": dlq_queue_name,
                "size": int(await redis.llen(dlq_queue_name)),
                "messages": messages,
            }
        return {
            "redis_url": redis_url,
            "queues": report,
        }
    finally:
        await status_store.close()
        await redis.aclose()


async def requeue_dlq_messages(
    *,
    queue_name: str,
    task_id: str | None,
    count: int,
    keep_attempts: bool,
    force: bool,
) -> dict:
    from redis.asyncio import Redis
    from core.runtime.task_runtime.status_store import TaskStatusStore

    if count < 1:
        raise ValueError("count must be >= 1")

    redis_url = get_redis_url()
    redis = Redis.from_url(redis_url, decode_responses=True)
    status_store = TaskStatusStore(redis_url=redis_url)
    execution_store = PostgresTaskExecutionStore(database_url=get_checkpoint_database_url(host_default="localhost"))
    dlq_queue_name = build_dlq_queue_name(queue_name)
    requeued_messages: list[dict] = []
    skipped_messages: list[dict] = []

    try:
        if task_id is not None:
            raw_messages = await redis.lrange(dlq_queue_name, 0, -1)
            for raw_message in raw_messages:
                message = TaskMessage.from_dict(json.loads(raw_message))
                if message.task_id != task_id:
                    continue
                maybe_requeued = await _maybe_requeue_message(
                    redis=redis,
                    status_store=status_store,
                    execution_store=execution_store,
                    message=message,
                    queue_name=queue_name,
                    dlq_queue_name=dlq_queue_name,
                    keep_attempts=keep_attempts,
                    force=force,
                )
                if "skipped" in maybe_requeued:
                    skipped_messages.append(maybe_requeued)
                    break
                await redis.lrem(dlq_queue_name, 1, raw_message)
                requeued_message = maybe_requeued["message"]
                requeued_messages.append(requeued_message.to_dict())
                break
        else:
            for _ in range(count):
                raw_message = await redis.lpop(dlq_queue_name)
                if raw_message is None:
                    break
                message = TaskMessage.from_dict(json.loads(raw_message))
                maybe_requeued = await _maybe_requeue_message(
                    redis=redis,
                    status_store=status_store,
                    execution_store=execution_store,
                    message=message,
                    queue_name=queue_name,
                    dlq_queue_name=dlq_queue_name,
                    keep_attempts=keep_attempts,
                    force=force,
                )
                if "skipped" in maybe_requeued:
                    skipped_messages.append(maybe_requeued)
                    await redis.rpush(dlq_queue_name, raw_message)
                    continue
                requeued_message = maybe_requeued["message"]
                requeued_messages.append(requeued_message.to_dict())

        return {
            "redis_url": redis_url,
            "queue_name": queue_name,
            "dlq_queue_name": dlq_queue_name,
            "requeued_count": len(requeued_messages),
            "skipped_count": len(skipped_messages),
            "messages": requeued_messages,
            "skipped_messages": skipped_messages,
        }
    finally:
        await status_store.close()
        await execution_store.close()
        await redis.aclose()


async def _maybe_requeue_message(
    *,
    redis,
    status_store,
    execution_store,
    message: TaskMessage,
    queue_name: str,
    dlq_queue_name: str,
    keep_attempts: bool,
    force: bool,
) -> dict[str, Any]:
    definition = get_task_definition(message.task_name)
    preserve_attempts = keep_attempts or definition.dlq_requeue_keep_attempts
    policy_snapshot = {
        "service_name": definition.service_name,
        "queue_name": definition.queue_name,
        "max_retries": definition.max_retries,
        "retryable": definition.retryable,
        "backoff_seconds": definition.backoff_seconds,
        "timeout_seconds": definition.timeout_seconds,
        "dlq_enabled": definition.dlq_enabled,
        "dlq_requeue_limit": definition.dlq_requeue_limit,
        "dlq_requeue_keep_attempts": definition.dlq_requeue_keep_attempts,
    }
    current_status = await status_store.get(message.task_id) or {}
    current_requeue_count = int(current_status.get("dlq_requeue_count", 0))
    requeue_limit = definition.dlq_requeue_limit

    if not force and requeue_limit is not None and current_requeue_count >= requeue_limit:
        return {
            "skipped": True,
            "task_id": message.task_id,
            "task_name": message.task_name,
            "queue_name": queue_name,
            "reason": "dlq_requeue_limit_exceeded",
            "dlq_requeue_count": current_requeue_count,
            "dlq_requeue_limit": requeue_limit,
        }

    requeued_message = TaskMessage(
        queue_name=queue_name,
        task_name=message.task_name,
        payload=message.payload,
        attempts=message.attempts if preserve_attempts else 0,
        task_id=message.task_id,
        dedupe_key=message.dedupe_key,
        correlation_id=message.correlation_id,
        resource_key=message.resource_key,
    )
    requeue_entry = {
        "requeued_at": iso_now(),
        "from_queue_name": dlq_queue_name,
        "to_queue_name": queue_name,
        "preserved_attempts": preserve_attempts,
        "requeue_number": current_requeue_count + 1,
        "forced": force,
    }
    status_document = await status_store.mark_requeued_from_dlq(
        requeued_message,
        queue_name=queue_name,
        dlq_queue_name=dlq_queue_name,
        requeue_entry=requeue_entry,
        policy_snapshot=policy_snapshot,
    )
    import logging

    await safe_record(
        execution_store.upsert(status_document),
        logger=logging.getLogger("ops.dlq"),
        log_event=_log_event,
        event="task_execution_history_record_failed",
        task_id=status_document.get("task_id"),
        task_name=status_document.get("task_name"),
        status=status_document.get("status"),
    )
    await redis.rpush(queue_name, json.dumps(requeued_message.to_dict()))
    return {"message": requeued_message}
