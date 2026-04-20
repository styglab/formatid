from __future__ import annotations

from services.api.app.config import get_settings


async def list_schedule_runs(*, limit: int = 100, schedule_name: str | None = None) -> list[dict]:
    settings = get_settings()
    from psycopg import AsyncConnection
    from psycopg.rows import dict_row

    conn = await AsyncConnection.connect(settings.checkpoint_database_url)
    try:
        await _ensure_schedule_runs_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            if schedule_name is None:
                await cursor.execute(
                    """
                    SELECT id, schedule_name, task_id, queue_name, task_name, status, skip_reason,
                           payload, details, created_at
                    FROM schedule_runs
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                await cursor.execute(
                    """
                    SELECT id, schedule_name, task_id, queue_name, task_name, status, skip_reason,
                           payload, details, created_at
                    FROM schedule_runs
                    WHERE schedule_name = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (schedule_name, limit),
                )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]
    finally:
        await conn.close()


async def list_task_executions(
    *,
    limit: int = 100,
    task_name: str | None = None,
    status: str | None = None,
) -> list[dict]:
    settings = get_settings()
    from psycopg import AsyncConnection
    from psycopg.rows import dict_row

    conn = await AsyncConnection.connect(settings.checkpoint_database_url)
    try:
        await _ensure_task_executions_table(conn)
        conditions = []
        params: list[object] = []
        if task_name is not None:
            conditions.append("task_name = %s")
            params.append(task_name)
        if status is not None:
            conditions.append("status = %s")
            params.append(status)
        where_clause = "" if not conditions else "WHERE " + " AND ".join(conditions)
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT task_id, queue_name, task_name, status, attempts, worker_id,
                       enqueued_at, started_at, finished_at, duration_ms,
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


def _serialize_row(row: dict) -> dict:
    return {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in row.items()
    }


async def _ensure_schedule_runs_table(conn) -> None:
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
    await conn.commit()


async def _ensure_task_executions_table(conn) -> None:
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
    await conn.commit()
