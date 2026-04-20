from __future__ import annotations

from datetime import datetime
from typing import Any

from shared.tasking.schemas import TaskMessage, TaskResult
from shared.time import iso_now


def build_queued_document(
    message: TaskMessage,
    *,
    policy_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _base_message_fields(message)
    payload.update(
        {
            "status": "queued",
            "updated_at": _now(),
        }
    )
    if policy_snapshot is not None:
        payload["policy_snapshot"] = policy_snapshot
    return payload


def build_requeued_from_dlq_document(
    existing: dict[str, Any],
    message: TaskMessage,
    *,
    queue_name: str,
    dlq_queue_name: str,
    requeue_entry: dict[str, Any],
    policy_snapshot: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(existing)
    requeue_history = list(payload.get("dlq_requeue_history", []))
    requeue_history.append(requeue_entry)
    payload.update(
        {
            **_base_message_fields(message, queue_name=queue_name),
            "status": "queued",
            "updated_at": _now(),
            "dlq_queue_name": dlq_queue_name,
            "dlq_requeue_count": len(requeue_history),
            "dlq_requeue_history": requeue_history,
            "last_requeued_from_dlq_at": requeue_entry["requeued_at"],
            "policy_snapshot": policy_snapshot,
        }
    )
    return payload


def build_running_document(
    existing: dict[str, Any],
    message: TaskMessage,
    *,
    worker_id: str,
    policy_snapshot: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(existing)
    started_at = _now()
    payload.update(
        {
            **_base_message_fields(message),
            "status": "running",
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
    return payload


def build_succeeded_document(
    existing: dict[str, Any],
    result: TaskResult,
    *,
    worker_id: str,
) -> dict[str, Any]:
    payload = dict(existing)
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
    return payload


def build_retrying_document(
    existing: dict[str, Any],
    message: TaskMessage,
    *,
    worker_id: str,
    next_attempts: int,
    max_retries: int,
    policy_snapshot: dict[str, Any],
    error: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(existing)
    payload.update(
        {
            **_base_message_fields(message, attempts=next_attempts),
            "status": "retrying",
            "max_retries": max_retries,
            "updated_at": _now(),
            "last_error": error,
            "last_failed_at": _now(),
            "worker_id": worker_id,
            "policy_snapshot": policy_snapshot,
        }
    )
    return payload


def build_failed_document(
    existing: dict[str, Any],
    message: TaskMessage,
    *,
    worker_id: str,
    policy_snapshot: dict[str, Any],
    error: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(existing)
    finished_at = _now()
    payload.update(
        {
            **_base_message_fields(message),
            "status": "failed",
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
    return payload


def build_dead_lettered_document(
    existing: dict[str, Any],
    message: TaskMessage,
    *,
    worker_id: str,
    dlq_queue_name: str,
    max_retries: int,
    policy_snapshot: dict[str, Any],
    error: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(existing)
    finished_at = _now()
    payload.update(
        {
            **_base_message_fields(message),
            "status": "dead_lettered",
            "max_retries": max_retries,
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
    return payload


def build_interrupted_document(
    existing: dict[str, Any],
    message: TaskMessage,
    *,
    worker_id: str,
    policy_snapshot: dict[str, Any],
    error: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(existing)
    finished_at = _now()
    payload.update(
        {
            **_base_message_fields(message),
            "status": "interrupted",
            "finished_at": finished_at,
            "updated_at": finished_at,
            "last_error": error,
            "last_failed_at": finished_at,
            "worker_id": worker_id,
            "policy_snapshot": policy_snapshot,
            "duration_ms": _duration_ms(payload.get("started_at"), finished_at),
        }
    )
    return payload


def _base_message_fields(
    message: TaskMessage,
    *,
    queue_name: str | None = None,
    attempts: int | None = None,
) -> dict[str, Any]:
    resolved_attempts = message.attempts if attempts is None else attempts
    return {
        "task_id": message.task_id,
        "queue_name": message.queue_name if queue_name is None else queue_name,
        "task_name": message.task_name,
        "attempts": resolved_attempts,
        "retry_count": resolved_attempts,
        "payload": message.payload,
        "enqueued_at": message.enqueued_at.isoformat(),
    }


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
