import json
from typing import Any

from shared.tasking.schemas import TaskMessage, TaskResult
from shared.tasking.status_documents import (
    build_dead_lettered_document,
    build_failed_document,
    build_interrupted_document,
    build_queued_document,
    build_requeued_from_dlq_document,
    build_retrying_document,
    build_running_document,
    build_succeeded_document,
)


class TaskStatusStore:
    def __init__(self, *, redis_url: str, ttl_seconds: int = 604800, key_prefix: str = "task:status") -> None:
        from redis.asyncio import Redis

        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix

    def build_key(self, task_id: str) -> str:
        return f"{self.key_prefix}:{task_id}"

    async def mark_queued(
        self,
        message: TaskMessage,
        *,
        policy_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        document = build_queued_document(message, policy_snapshot=policy_snapshot)
        await self._set(message.task_id, document)
        return document

    async def mark_requeued_from_dlq(
        self,
        message: TaskMessage,
        *,
        queue_name: str,
        dlq_queue_name: str,
        requeue_entry: dict[str, Any],
        policy_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        document = build_requeued_from_dlq_document(
            await self._get(message.task_id),
            message,
            queue_name=queue_name,
            dlq_queue_name=dlq_queue_name,
            requeue_entry=requeue_entry,
            policy_snapshot=policy_snapshot,
        )
        await self._set(message.task_id, document)
        return document

    async def mark_running(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        policy_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        document = build_running_document(
            await self._get(message.task_id),
            message,
            worker_id=worker_id,
            policy_snapshot=policy_snapshot,
        )
        await self._set(message.task_id, document)
        return document

    async def mark_succeeded(
        self,
        message: TaskMessage,
        result: TaskResult,
        *,
        worker_id: str,
    ) -> dict[str, Any]:
        document = build_succeeded_document(
            await self._get(message.task_id),
            result,
            worker_id=worker_id,
        )
        await self._set(message.task_id, document)
        return document

    async def mark_retrying(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        next_attempts: int,
        max_retries: int,
        policy_snapshot: dict[str, Any],
        error: dict[str, Any],
    ) -> dict[str, Any]:
        document = build_retrying_document(
            await self._get(message.task_id),
            message,
            worker_id=worker_id,
            next_attempts=next_attempts,
            max_retries=max_retries,
            policy_snapshot=policy_snapshot,
            error=error,
        )
        await self._set(message.task_id, document)
        return document

    async def mark_failed(
        self,
        message: TaskMessage,
        error: dict[str, Any],
        *,
        worker_id: str,
        policy_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        document = build_failed_document(
            await self._get(message.task_id),
            message,
            worker_id=worker_id,
            policy_snapshot=policy_snapshot,
            error=error,
        )
        await self._set(message.task_id, document)
        return document

    async def mark_dead_lettered(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        dlq_queue_name: str,
        max_retries: int,
        policy_snapshot: dict[str, Any],
        error: dict[str, Any],
    ) -> dict[str, Any]:
        document = build_dead_lettered_document(
            await self._get(message.task_id),
            message,
            worker_id=worker_id,
            dlq_queue_name=dlq_queue_name,
            max_retries=max_retries,
            policy_snapshot=policy_snapshot,
            error=error,
        )
        await self._set(message.task_id, document)
        return document

    async def mark_interrupted(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        policy_snapshot: dict[str, Any],
        error: dict[str, Any],
    ) -> dict[str, Any]:
        document = build_interrupted_document(
            await self._get(message.task_id),
            message,
            worker_id=worker_id,
            policy_snapshot=policy_snapshot,
            error=error,
        )
        await self._set(message.task_id, document)
        return document

    async def get(self, task_id: str) -> dict[str, Any] | None:
        raw = await self._redis.get(self.build_key(task_id))
        if raw is None:
            return None
        return json.loads(raw)

    async def close(self) -> None:
        await self._redis.aclose()

    async def _get(self, task_id: str) -> dict[str, Any]:
        return await self.get(task_id) or {}

    async def _set(self, task_id: str, payload: dict[str, Any]) -> None:
        await self._redis.set(
            self.build_key(task_id),
            json.dumps(payload),
            ex=self.ttl_seconds,
        )
