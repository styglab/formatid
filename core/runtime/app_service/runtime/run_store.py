from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_service_runs_table


class ServiceRunStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def record(
        self,
        *,
        run_name: str,
        status: str,
        service_name: str | None = None,
        queue_name: str | None = None,
        task_name: str | None = None,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
        skip_reason: str | None = None,
        details: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        trigger_type: str | None = None,
        trigger_config: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        resource_key: str | None = None,
        lock_acquired: bool | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_ms: float | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO service_runs (
                    service_name,
                    run_name,
                    task_id,
                    queue_name,
                    task_name,
                    status,
                    skip_reason,
                    payload,
                    details,
                    error,
                    trigger_type,
                    trigger_config,
                    correlation_id,
                    resource_key,
                    lock_acquired,
                    started_at,
                    finished_at,
                    duration_ms
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                    %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    service_name,
                    run_name,
                    task_id,
                    queue_name,
                    task_name,
                    status,
                    skip_reason,
                    json.dumps(payload or {}),
                    json.dumps(details or {}),
                    json.dumps(error) if error is not None else None,
                    trigger_type,
                    json.dumps(trigger_config or {}),
                    correlation_id,
                    resource_key,
                    lock_acquired,
                    started_at,
                    finished_at,
                    duration_ms,
                ),
            )
        await conn.commit()

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
        await ensure_service_runs_table(conn)
        self._initialized = True
