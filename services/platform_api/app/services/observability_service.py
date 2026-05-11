from __future__ import annotations

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import (
    ensure_service_events_table,
    ensure_service_requests_table,
    ensure_service_runs_table,
)
from services.platform_api.app.config import get_settings


async def list_service_runs(*, limit: int = 100, run_name: str | None = None) -> list[dict]:
    from psycopg.rows import dict_row

    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_service_runs_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            if run_name is None:
                await cursor.execute(
                    """
                    SELECT id, service_name, run_name, status, skip_reason, payload,
                           details, error, trigger_type, trigger_config, correlation_id,
                           resource_key, lock_acquired, started_at, finished_at,
                           duration_ms, created_at
                    FROM service_runs
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                await cursor.execute(
                    """
                    SELECT id, service_name, run_name, status, skip_reason, payload,
                           details, error, trigger_type, trigger_config, correlation_id,
                           resource_key, lock_acquired, started_at, finished_at,
                           duration_ms, created_at
                    FROM service_runs
                    WHERE run_name = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (run_name, limit),
                )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]
    finally:
        await conn.close()


async def list_service_requests(
    *,
    limit: int = 100,
    service_name: str | None = None,
    request_id: str | None = None,
    status: str | None = None,
) -> list[dict]:
    from psycopg.rows import dict_row

    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_service_requests_table(conn)
        conditions = []
        params: list[object] = []
        if service_name is not None:
            conditions.append("service_name = %s")
            params.append(service_name)
        if request_id is not None:
            conditions.append("request_id = %s")
            params.append(request_id)
        if status is not None:
            conditions.append("status = %s")
            params.append(status)
        where_clause = "" if not conditions else "WHERE " + " AND ".join(conditions)
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT id, service_name, request_id, method, path, correlation_id,
                       resource_key, status, payload, result, error, duration_ms, created_at
                FROM service_requests
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]
    finally:
        await conn.close()


async def list_service_events(
    *,
    limit: int = 100,
    service_name: str | None = None,
    event_name: str | None = None,
    request_id: str | None = None,
    run_name: str | None = None,
) -> list[dict]:
    from psycopg.rows import dict_row

    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_service_events_table(conn)
        conditions = []
        params: list[object] = []
        if service_name is not None:
            conditions.append("service_name = %s")
            params.append(service_name)
        if event_name is not None:
            conditions.append("event_name = %s")
            params.append(event_name)
        if request_id is not None:
            conditions.append("request_id = %s")
            params.append(request_id)
        if run_name is not None:
            conditions.append("run_name = %s")
            params.append(run_name)
        where_clause = "" if not conditions else "WHERE " + " AND ".join(conditions)
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT id, service_name, event_name, request_id, run_name,
                       correlation_id, resource_key, details, created_at
                FROM service_events
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]
    finally:
        await conn.close()


def _serialize_row(row: dict) -> dict:
    serialized = {}
    for key, value in dict(row).items():
        serialized[key] = value.isoformat() if hasattr(value, "isoformat") else value
    return serialized
