from __future__ import annotations

import json
from typing import Any

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_service_events_table, ensure_service_requests_table


class ServiceRequestStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def record(
        self,
        *,
        service_name: str,
        request_id: str,
        status: str,
        method: str | None = None,
        path: str | None = None,
        correlation_id: str | None = None,
        resource_key: str | None = None,
        payload: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        duration_ms: float | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO service_requests (
                    service_name,
                    request_id,
                    method,
                    path,
                    correlation_id,
                    resource_key,
                    status,
                    payload,
                    result,
                    error,
                    duration_ms
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    service_name,
                    request_id,
                    method,
                    path,
                    correlation_id,
                    resource_key,
                    status,
                    json.dumps(payload or {}),
                    json.dumps(result) if result is not None else None,
                    json.dumps(error) if error is not None else None,
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
        await ensure_service_requests_table(conn)
        self._initialized = True


class ServiceEventStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def record(
        self,
        *,
        service_name: str,
        event_name: str,
        request_id: str | None = None,
        run_name: str | None = None,
        correlation_id: str | None = None,
        resource_key: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO service_events (
                    service_name,
                    event_name,
                    request_id,
                    run_name,
                    correlation_id,
                    resource_key,
                    details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    service_name,
                    event_name,
                    request_id,
                    run_name,
                    correlation_id,
                    resource_key,
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
            self._conn = await connect(self._database_url)
        return self._conn

    async def _ensure_table(self, conn: Any) -> None:
        if self._initialized:
            return
        await ensure_service_events_table(conn)
        self._initialized = True
