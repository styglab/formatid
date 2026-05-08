from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_service_logs_table
from core.runtime.runtime_db.url import get_checkpoint_database_url
from core.runtime.time import now


class ServiceLogStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def record(
        self,
        *,
        service_name: str,
        level: str,
        message: str,
        worker_id: str | None = None,
        event_name: str | None = None,
        logger_name: str | None = None,
        request_id: str | None = None,
        run_name: str | None = None,
        task_id: str | None = None,
        correlation_id: str | None = None,
        resource_key: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO service_logs (
                    service_name, worker_id, level, event_name, message, logger_name,
                    request_id, run_name, task_id, correlation_id, resource_key, details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    service_name,
                    worker_id,
                    level.lower(),
                    event_name,
                    message,
                    logger_name,
                    request_id,
                    run_name,
                    task_id,
                    correlation_id,
                    resource_key,
                    json.dumps(details or {}, ensure_ascii=True, default=str),
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
        await ensure_service_logs_table(conn)
        self._initialized = True


def record_service_log_best_effort(
    *,
    service_name: str,
    level: str,
    message: str,
    database_url: str | None = None,
    worker_id: str | None = None,
    event_name: str | None = None,
    logger_name: str | None = None,
    request_id: str | None = None,
    run_name: str | None = None,
    task_id: str | None = None,
    correlation_id: str | None = None,
    resource_key: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(
        _record_and_close(
            database_url=database_url or get_checkpoint_database_url(host_default="postgres"),
            service_name=service_name,
            worker_id=worker_id,
            level=level,
            event_name=event_name,
            message=message,
            logger_name=logger_name,
            request_id=request_id,
            run_name=run_name,
            task_id=task_id,
            correlation_id=correlation_id,
            resource_key=resource_key,
            details=details,
        )
    )


async def _record_and_close(*, database_url: str, **fields: Any) -> None:
    store = ServiceLogStore(database_url=database_url)
    try:
        await store.record(**fields)
    except Exception as exc:
        sys.stderr.write(
            json.dumps(
                {
                    "timestamp": now().isoformat(),
                    "level": "warning",
                    "message": "service_log_record_failed",
                    "event": "service_log_record_failed",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
                ensure_ascii=True,
                default=str,
            )
            + "\n"
        )
    finally:
        await store.close()
