import json
from datetime import datetime
from typing import Any

from shared.tasking.schemas import TaskMessage, TaskResult
from shared.time import iso_now


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
    ) -> None:
        payload = {
            "task_id": message.task_id,
            "queue_name": message.queue_name,
            "task_name": message.task_name,
            "status": "queued",
            "attempts": message.attempts,
            "retry_count": message.attempts,
            "payload": message.payload,
            "enqueued_at": message.enqueued_at.isoformat(),
            "updated_at": _now(),
        }
        if policy_snapshot is not None:
            payload["policy_snapshot"] = policy_snapshot
        await self._set(message.task_id, payload)

    async def mark_requeued_from_dlq(
        self,
        message: TaskMessage,
        *,
        queue_name: str,
        dlq_queue_name: str,
        requeue_entry: dict[str, Any],
        policy_snapshot: dict[str, Any],
    ) -> None:
        payload = await self._get(message.task_id)
        requeue_history = list(payload.get("dlq_requeue_history", []))
        requeue_history.append(requeue_entry)
        payload.update(
            {
                "task_id": message.task_id,
                "queue_name": queue_name,
                "task_name": message.task_name,
                "status": "queued",
                "attempts": message.attempts,
                "retry_count": message.attempts,
                "payload": message.payload,
                "enqueued_at": message.enqueued_at.isoformat(),
                "updated_at": _now(),
                "dlq_queue_name": dlq_queue_name,
                "dlq_requeue_count": len(requeue_history),
                "dlq_requeue_history": requeue_history,
                "last_requeued_from_dlq_at": requeue_entry["requeued_at"],
                "policy_snapshot": policy_snapshot,
            }
        )
        await self._set(message.task_id, payload)

    async def mark_running(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        policy_snapshot: dict[str, Any],
    ) -> None:
        payload = await self._get(message.task_id)
        started_at = _now()
        payload.update(
            {
                "task_id": message.task_id,
                "queue_name": message.queue_name,
                "task_name": message.task_name,
                "status": "running",
                "attempts": message.attempts,
                "retry_count": message.attempts,
                "payload": message.payload,
                "enqueued_at": message.enqueued_at.isoformat(),
                "started_at": started_at,
                "updated_at": _now(),
                "worker_id": worker_id,
                "policy_snapshot": policy_snapshot,
                "timeout_seconds": policy_snapshot.get("timeout_seconds"),
                "backoff_seconds": policy_snapshot.get("backoff_seconds"),
                "max_retries": policy_snapshot.get("max_retries"),
                "dlq_enabled": policy_snapshot.get("dlq_enabled"),
                "retryable": policy_snapshot.get("retryable"),
            }
        )
        await self._set(message.task_id, payload)

    async def mark_succeeded(
        self,
        message: TaskMessage,
        result: TaskResult,
        *,
        worker_id: str,
    ) -> None:
        payload = await self._get(message.task_id)
        finished_at = _now()
        payload.update(
            {
                "status": "succeeded",
                "result": result.output,
                "finished_at": finished_at,
                "updated_at": _now(),
                "worker_id": worker_id,
                "duration_ms": _duration_ms(payload.get("started_at"), finished_at),
            }
        )
        await self._set(message.task_id, payload)

    async def mark_retrying(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        next_attempts: int,
        max_retries: int,
        policy_snapshot: dict[str, Any],
        error: dict[str, Any],
    ) -> None:
        payload = await self._get(message.task_id)
        payload.update(
            {
                "task_id": message.task_id,
                "queue_name": message.queue_name,
                "task_name": message.task_name,
                "status": "retrying",
                "attempts": next_attempts,
                "retry_count": next_attempts,
                "max_retries": max_retries,
                "payload": message.payload,
                "enqueued_at": message.enqueued_at.isoformat(),
                "updated_at": _now(),
                "last_error": error,
                "last_failed_at": _now(),
                "worker_id": worker_id,
                "policy_snapshot": policy_snapshot,
            }
        )
        await self._set(message.task_id, payload)

    async def mark_failed(
        self,
        message: TaskMessage,
        error: dict[str, Any],
        *,
        worker_id: str,
        policy_snapshot: dict[str, Any],
    ) -> None:
        payload = await self._get(message.task_id)
        finished_at = _now()
        payload.update(
            {
                "task_id": message.task_id,
                "queue_name": message.queue_name,
                "task_name": message.task_name,
                "status": "failed",
                "attempts": message.attempts,
                "retry_count": message.attempts,
                "payload": message.payload,
                "enqueued_at": message.enqueued_at.isoformat(),
                "finished_at": finished_at,
                "updated_at": _now(),
                "error": error,
                "last_error": error,
                "last_failed_at": finished_at,
                "worker_id": worker_id,
                "policy_snapshot": policy_snapshot,
                "duration_ms": _duration_ms(payload.get("started_at"), finished_at),
            }
        )
        await self._set(message.task_id, payload)

    async def mark_dead_lettered(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        dlq_queue_name: str,
        max_retries: int,
        policy_snapshot: dict[str, Any],
        error: dict[str, Any],
    ) -> None:
        payload = await self._get(message.task_id)
        finished_at = _now()
        payload.update(
            {
                "task_id": message.task_id,
                "queue_name": message.queue_name,
                "task_name": message.task_name,
                "status": "dead_lettered",
                "attempts": message.attempts,
                "retry_count": message.attempts,
                "max_retries": max_retries,
                "payload": message.payload,
                "enqueued_at": message.enqueued_at.isoformat(),
                "updated_at": _now(),
                "finished_at": finished_at,
                "dlq_queue_name": dlq_queue_name,
                "last_error": error,
                "last_failed_at": _now(),
                "worker_id": worker_id,
                "policy_snapshot": policy_snapshot,
                "duration_ms": _duration_ms(payload.get("started_at"), finished_at),
            }
        )
        await self._set(message.task_id, payload)

    async def mark_interrupted(
        self,
        message: TaskMessage,
        *,
        worker_id: str,
        policy_snapshot: dict[str, Any],
        error: dict[str, Any],
    ) -> None:
        payload = await self._get(message.task_id)
        finished_at = _now()
        payload.update(
            {
                "task_id": message.task_id,
                "queue_name": message.queue_name,
                "task_name": message.task_name,
                "status": "interrupted",
                "attempts": message.attempts,
                "retry_count": message.attempts,
                "payload": message.payload,
                "enqueued_at": message.enqueued_at.isoformat(),
                "finished_at": finished_at,
                "updated_at": finished_at,
                "last_error": error,
                "last_failed_at": finished_at,
                "worker_id": worker_id,
                "policy_snapshot": policy_snapshot,
                "duration_ms": _duration_ms(payload.get("started_at"), finished_at),
            }
        )
        await self._set(message.task_id, payload)

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


def _now() -> str:
    return iso_now()


def _duration_ms(started_at: str | None, finished_at: str) -> float | None:
    if started_at is None:
        return None

    try:
        started = datetime.fromisoformat(started_at)
        finished = datetime.fromisoformat(finished_at)
    except ValueError:
        return None

    return round(max((finished - started).total_seconds(), 0.0) * 1000, 3)
