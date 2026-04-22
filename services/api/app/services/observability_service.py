from __future__ import annotations

from services.api.app.config import get_settings
from services.runtime_db.connection import connect
from services.runtime_db.schema import (
    ensure_service_events_table,
    ensure_service_requests_table,
    ensure_service_runs_table,
    ensure_task_execution_events_table,
    ensure_task_executions_table,
)


async def list_service_runs(*, limit: int = 100, run_name: str | None = None) -> list[dict]:
    settings = get_settings()
    from psycopg.rows import dict_row

    conn = await connect(settings.checkpoint_database_url)
    try:
        await ensure_service_runs_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            if run_name is None:
                await cursor.execute(
                    """
                    SELECT id, service_name, run_name, task_id, queue_name, task_name,
                           status, skip_reason, payload, details, error, trigger_type,
                           trigger_config, correlation_id, resource_key, lock_acquired,
                           started_at, finished_at, duration_ms, created_at
                    FROM service_runs
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                await cursor.execute(
                    """
                    SELECT id, service_name, run_name, task_id, queue_name, task_name,
                           status, skip_reason, payload, details, error, trigger_type,
                           trigger_config, correlation_id, resource_key, lock_acquired,
                           started_at, finished_at, duration_ms, created_at
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
    settings = get_settings()
    from psycopg.rows import dict_row

    conn = await connect(settings.checkpoint_database_url)
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
    settings = get_settings()
    from psycopg.rows import dict_row

    conn = await connect(settings.checkpoint_database_url)
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


async def list_task_executions(
    *,
    limit: int = 100,
    queue_name: str | None = None,
    task_name: str | None = None,
    service_name: str | None = None,
    dedupe_key: str | None = None,
    correlation_id: str | None = None,
    resource_key: str | None = None,
    error_type: str | None = None,
    updated_after: str | None = None,
    updated_before: str | None = None,
    status: str | None = None,
) -> list[dict]:
    settings = get_settings()
    from psycopg.rows import dict_row

    conn = await connect(settings.checkpoint_database_url)
    try:
        await ensure_task_executions_table(conn)
        conditions = []
        params: list[object] = []
        if queue_name is not None:
            conditions.append("queue_name = %s")
            params.append(queue_name)
        if task_name is not None:
            conditions.append("task_name = %s")
            params.append(task_name)
        if service_name is not None:
            conditions.append("service_name = %s")
            params.append(service_name)
        if dedupe_key is not None:
            conditions.append("dedupe_key = %s")
            params.append(dedupe_key)
        if correlation_id is not None:
            conditions.append("correlation_id = %s")
            params.append(correlation_id)
        if resource_key is not None:
            conditions.append("resource_key = %s")
            params.append(resource_key)
        if status is not None:
            conditions.append("status = %s")
            params.append(status)
        if error_type is not None:
            conditions.append("(error->>'type' = %s OR status_document->'last_error'->>'type' = %s)")
            params.extend([error_type, error_type])
        if updated_after is not None:
            conditions.append("updated_at >= %s::timestamptz")
            params.append(updated_after)
        if updated_before is not None:
            conditions.append("updated_at <= %s::timestamptz")
            params.append(updated_before)
        where_clause = "" if not conditions else "WHERE " + " AND ".join(conditions)
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT task_id, queue_name, service_name, task_name, dedupe_key,
                       correlation_id, resource_key, status, attempts, worker_id,
                       enqueued_at, started_at, finished_at, duration_ms,
                       last_heartbeat_at, lease_expires_at,
                       payload, result, error, status_document, created_at, updated_at
                FROM task_executions
                {where_clause}
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]
    finally:
        await conn.close()


async def list_task_execution_events(
    *,
    limit: int = 100,
    task_id: str | None = None,
    queue_name: str | None = None,
    task_name: str | None = None,
    service_name: str | None = None,
    error_type: str | None = None,
    status: str | None = None,
) -> list[dict]:
    settings = get_settings()
    from psycopg.rows import dict_row

    conn = await connect(settings.checkpoint_database_url)
    try:
        await ensure_task_execution_events_table(conn)
        conditions = []
        params: list[object] = []
        if task_id is not None:
            conditions.append("task_id = %s")
            params.append(task_id)
        if queue_name is not None:
            conditions.append("queue_name = %s")
            params.append(queue_name)
        if task_name is not None:
            conditions.append("task_name = %s")
            params.append(task_name)
        if service_name is not None:
            conditions.append("service_name = %s")
            params.append(service_name)
        if status is not None:
            conditions.append("status = %s")
            params.append(status)
        if error_type is not None:
            conditions.append("error->>'type' = %s")
            params.append(error_type)
        where_clause = "" if not conditions else "WHERE " + " AND ".join(conditions)
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT id, task_id, queue_name, service_name, task_name, status,
                       attempts, worker_id, error, details, created_at
                FROM task_execution_events
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
    return {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in row.items()
    }
