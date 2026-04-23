from __future__ import annotations

import json
from typing import Any

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_task_execution_events_table, ensure_task_executions_table
from core.runtime.task_runtime.redaction import redact


class PostgresTaskExecutionStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def upsert(self, document: dict[str, Any]) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO task_executions (
                    task_id,
                    queue_name,
                    service_name,
                    task_name,
                    dedupe_key,
                    correlation_id,
                    resource_key,
                    status,
                    attempts,
                    worker_id,
                    enqueued_at,
                    started_at,
                    finished_at,
                    duration_ms,
                    last_heartbeat_at,
                    lease_expires_at,
                    payload,
                    result,
                    error,
                    status_document
                )
                VALUES (
                    %(task_id)s,
                    %(queue_name)s,
                    %(service_name)s,
                    %(task_name)s,
                    %(dedupe_key)s,
                    %(correlation_id)s,
                    %(resource_key)s,
                    %(status)s,
                    %(attempts)s,
                    %(worker_id)s,
                    %(enqueued_at)s::timestamptz,
                    %(started_at)s::timestamptz,
                    %(finished_at)s::timestamptz,
                    %(duration_ms)s,
                    %(last_heartbeat_at)s::timestamptz,
                    %(lease_expires_at)s::timestamptz,
                    %(payload)s::jsonb,
                    %(result)s::jsonb,
                    %(error)s::jsonb,
                    %(status_document)s::jsonb
                )
                ON CONFLICT (task_id)
                DO UPDATE SET
                    queue_name = EXCLUDED.queue_name,
                    service_name = EXCLUDED.service_name,
                    task_name = EXCLUDED.task_name,
                    dedupe_key = EXCLUDED.dedupe_key,
                    correlation_id = EXCLUDED.correlation_id,
                    resource_key = EXCLUDED.resource_key,
                    status = EXCLUDED.status,
                    attempts = EXCLUDED.attempts,
                    worker_id = EXCLUDED.worker_id,
                    enqueued_at = COALESCE(task_executions.enqueued_at, EXCLUDED.enqueued_at),
                    started_at = COALESCE(EXCLUDED.started_at, task_executions.started_at),
                    finished_at = EXCLUDED.finished_at,
                    duration_ms = EXCLUDED.duration_ms,
                    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    payload = EXCLUDED.payload,
                    result = EXCLUDED.result,
                    error = EXCLUDED.error,
                    status_document = EXCLUDED.status_document,
                    updated_at = NOW()
                """,
                _document_params(document),
            )
            await cursor.execute(
                """
                INSERT INTO task_execution_events (
                    task_id,
                    queue_name,
                    service_name,
                    task_name,
                    status,
                    attempts,
                    worker_id,
                    error,
                    details
                )
                VALUES (
                    %(task_id)s,
                    %(queue_name)s,
                    %(service_name)s,
                    %(task_name)s,
                    %(status)s,
                    %(attempts)s,
                    %(worker_id)s,
                    %(error)s::jsonb,
                    %(details)s::jsonb
                )
                """,
                _event_params(document),
            )
        await conn.commit()

    async def record_event(
        self,
        *,
        task_id: str,
        queue_name: str,
        service_name: str | None,
        task_name: str,
        status: str,
        attempts: int,
        worker_id: str | None = None,
        error: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO task_execution_events (
                    task_id,
                    queue_name,
                    service_name,
                    task_name,
                    status,
                    attempts,
                    worker_id,
                    error,
                    details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    task_id,
                    queue_name,
                    service_name,
                    task_name,
                    status,
                    attempts,
                    worker_id,
                    _json_or_none(error),
                    json.dumps(details or {}),
                ),
            )
        await conn.commit()

    async def refresh_lease(self, *, task_id: str, lease_expires_at: str | None) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE task_executions
                SET last_heartbeat_at = NOW(),
                    lease_expires_at = COALESCE(%s::timestamptz, lease_expires_at),
                    updated_at = NOW()
                WHERE task_id = %s
                  AND status = 'running'
                """,
                (lease_expires_at, task_id),
            )
        await conn.commit()

    async def interrupt_expired_leases(self, *, queue_name: str | None = None) -> int:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        params: list[Any] = []
        queue_filter = ""
        if queue_name is not None:
            queue_filter = "AND queue_name = %s"
            params.append(queue_name)
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"""
                UPDATE task_executions
                SET status = 'interrupted',
                    error = jsonb_build_object(
                        'type', 'TaskLeaseExpired',
                        'message', 'running task lease expired'
                    ),
                    status_document = jsonb_set(
                        status_document,
                        '{{last_error}}',
                        jsonb_build_object(
                            'type', 'TaskLeaseExpired',
                            'message', 'running task lease expired'
                        ),
                        true
                    ),
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE status = 'running'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < NOW()
                  {queue_filter}
                """,
                tuple(params),
            )
            count = max(cursor.rowcount, 0)
        await conn.commit()
        return count

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> Any:
        if self._conn is None:
            self._conn = await connect(self._database_url)
        return self._conn

    async def _ensure_table(self, conn: Any) -> None:
        if self._initialized:
            return
        await ensure_task_executions_table(conn)
        await ensure_task_execution_events_table(conn)
        self._initialized = True


def _document_params(document: dict[str, Any]) -> dict[str, Any]:
    error = document.get("error") or document.get("last_error")
    return {
        "task_id": document["task_id"],
        "queue_name": document["queue_name"],
        "service_name": document.get("service_name") or (document.get("policy_snapshot") or {}).get("service_name"),
        "task_name": document["task_name"],
        "dedupe_key": document.get("dedupe_key"),
        "correlation_id": document.get("correlation_id"),
        "resource_key": document.get("resource_key"),
        "status": document["status"],
        "attempts": int(document.get("attempts", 0)),
        "worker_id": document.get("worker_id"),
        "enqueued_at": document.get("enqueued_at"),
        "started_at": document.get("started_at"),
        "finished_at": document.get("finished_at"),
        "duration_ms": document.get("duration_ms"),
        "last_heartbeat_at": document.get("last_heartbeat_at"),
        "lease_expires_at": document.get("lease_expires_at"),
        "payload": json.dumps(redact(document.get("payload", {}))),
        "result": _json_or_none(redact(document.get("result"))),
        "error": _json_or_none(redact(error)),
        "status_document": json.dumps(redact(document)),
    }


def _event_params(document: dict[str, Any]) -> dict[str, Any]:
    error = document.get("error") or document.get("last_error")
    return {
        "task_id": document["task_id"],
        "queue_name": document["queue_name"],
        "service_name": document.get("service_name") or (document.get("policy_snapshot") or {}).get("service_name"),
        "task_name": document["task_name"],
        "status": document["status"],
        "attempts": int(document.get("attempts", 0)),
        "worker_id": document.get("worker_id"),
        "error": _json_or_none(redact(error)),
        "details": json.dumps(
            {
                "dedupe_key": document.get("dedupe_key"),
                "correlation_id": document.get("correlation_id"),
                "resource_key": document.get("resource_key"),
                "duration_ms": document.get("duration_ms"),
                "last_heartbeat_at": document.get("last_heartbeat_at"),
                "lease_expires_at": document.get("lease_expires_at"),
            }
        ),
    }


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value)
