from __future__ import annotations

import json
from typing import Any


class ScheduleRunStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def record(
        self,
        *,
        schedule_name: str,
        status: str,
        queue_name: str | None = None,
        task_name: str | None = None,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
        skip_reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO schedule_runs (
                    schedule_name,
                    task_id,
                    queue_name,
                    task_name,
                    status,
                    skip_reason,
                    payload,
                    details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    schedule_name,
                    task_id,
                    queue_name,
                    task_name,
                    status,
                    skip_reason,
                    json.dumps(payload or {}),
                    json.dumps(details or {}),
                ),
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
                CREATE TABLE IF NOT EXISTS schedule_runs (
                    id BIGSERIAL PRIMARY KEY,
                    schedule_name TEXT NOT NULL,
                    task_id TEXT,
                    queue_name TEXT,
                    task_name TEXT,
                    status TEXT NOT NULL,
                    skip_reason TEXT,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    details JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_created_at
                    ON schedule_runs (schedule_name, created_at DESC)
                """
            )
            await cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_created_at
                    ON schedule_runs (status, created_at DESC)
                """
            )
        await conn.commit()
        self._initialized = True
