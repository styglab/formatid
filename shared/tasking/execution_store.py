from __future__ import annotations

import json
from typing import Any


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
                    task_name,
                    status,
                    attempts,
                    worker_id,
                    enqueued_at,
                    started_at,
                    finished_at,
                    duration_ms,
                    payload,
                    result,
                    error,
                    status_document
                )
                VALUES (
                    %(task_id)s,
                    %(queue_name)s,
                    %(task_name)s,
                    %(status)s,
                    %(attempts)s,
                    %(worker_id)s,
                    %(enqueued_at)s::timestamptz,
                    %(started_at)s::timestamptz,
                    %(finished_at)s::timestamptz,
                    %(duration_ms)s,
                    %(payload)s::jsonb,
                    %(result)s::jsonb,
                    %(error)s::jsonb,
                    %(status_document)s::jsonb
                )
                ON CONFLICT (task_id)
                DO UPDATE SET
                    queue_name = EXCLUDED.queue_name,
                    task_name = EXCLUDED.task_name,
                    status = EXCLUDED.status,
                    attempts = EXCLUDED.attempts,
                    worker_id = EXCLUDED.worker_id,
                    enqueued_at = COALESCE(task_executions.enqueued_at, EXCLUDED.enqueued_at),
                    started_at = COALESCE(EXCLUDED.started_at, task_executions.started_at),
                    finished_at = EXCLUDED.finished_at,
                    duration_ms = EXCLUDED.duration_ms,
                    payload = EXCLUDED.payload,
                    result = EXCLUDED.result,
                    error = EXCLUDED.error,
                    status_document = EXCLUDED.status_document,
                    updated_at = NOW()
                """,
                _document_params(document),
            )
        await conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> Any:
        if self._conn is None:
            from psycopg import AsyncConnection

            self._conn = await AsyncConnection.connect(self._database_url)
        return self._conn

    async def _ensure_table(self, conn: Any) -> None:
        if self._initialized:
            return
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS task_executions (
                    task_id TEXT PRIMARY KEY,
                    queue_name TEXT NOT NULL,
                    task_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    worker_id TEXT,
                    enqueued_at TIMESTAMPTZ,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_ms DOUBLE PRECISION,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    result JSONB,
                    error JSONB,
                    status_document JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_task_executions_status_updated_at
                    ON task_executions (status, updated_at DESC)
                """
            )
            await cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_task_executions_task_name_updated_at
                    ON task_executions (task_name, updated_at DESC)
                """
            )
        await conn.commit()
        self._initialized = True


def _document_params(document: dict[str, Any]) -> dict[str, Any]:
    error = document.get("error") or document.get("last_error")
    return {
        "task_id": document["task_id"],
        "queue_name": document["queue_name"],
        "task_name": document["task_name"],
        "status": document["status"],
        "attempts": int(document.get("attempts", 0)),
        "worker_id": document.get("worker_id"),
        "enqueued_at": document.get("enqueued_at"),
        "started_at": document.get("started_at"),
        "finished_at": document.get("finished_at"),
        "duration_ms": document.get("duration_ms"),
        "payload": json.dumps(document.get("payload", {})),
        "result": _json_or_none(document.get("result")),
        "error": _json_or_none(error),
        "status_document": json.dumps(document),
    }


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value)
