from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_graph_runs_tables


class GraphRunStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def mark_running(
        self,
        *,
        run_id: str,
        service_name: str,
        graph_name: str,
        trigger_type: str,
        params: dict[str, Any] | None = None,
        started_at: datetime | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO graph_runs (
                    run_id, service_name, graph_name, trigger_type, status,
                    params, started_at, updated_at
                )
                VALUES (%s, %s, %s, %s, 'running', %s::jsonb, %s, NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    service_name = EXCLUDED.service_name,
                    graph_name = EXCLUDED.graph_name,
                    trigger_type = EXCLUDED.trigger_type,
                    status = 'running',
                    params = EXCLUDED.params,
                    started_at = COALESCE(graph_runs.started_at, EXCLUDED.started_at),
                    updated_at = NOW()
                """,
                (
                    run_id,
                    service_name,
                    graph_name,
                    trigger_type,
                    json.dumps(params or {}),
                    started_at,
                ),
            )
        await conn.commit()

    async def mark_succeeded(
        self,
        *,
        run_id: str,
        result: dict[str, Any] | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        await self._mark_finished(
            run_id=run_id,
            status="succeeded",
            result=result,
            error=None,
            finished_at=finished_at,
        )

    async def mark_failed(
        self,
        *,
        run_id: str,
        error: dict[str, Any],
        finished_at: datetime | None = None,
    ) -> None:
        await self._mark_finished(
            run_id=run_id,
            status="failed",
            result=None,
            error=error,
            finished_at=finished_at,
        )

    async def mark_skipped(
        self,
        *,
        run_id: str,
        service_name: str,
        graph_name: str,
        trigger_type: str,
        params: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO graph_runs (
                    run_id, service_name, graph_name, trigger_type, status,
                    params, result, started_at, finished_at, updated_at
                )
                VALUES (%s, %s, %s, %s, 'skipped', %s::jsonb, %s::jsonb, %s, %s, NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'skipped',
                    result = EXCLUDED.result,
                    finished_at = EXCLUDED.finished_at,
                    updated_at = NOW()
                """,
                (
                    run_id,
                    service_name,
                    graph_name,
                    trigger_type,
                    json.dumps(params or {}),
                    json.dumps(result or {}),
                    started_at,
                    finished_at,
                ),
            )
        await conn.commit()

    async def mark_node_started(
        self,
        *,
        run_id: str,
        graph_name: str,
        node_name: str,
        progress_total: int | None = None,
        input_summary: dict[str, Any] | None = None,
        started_at: datetime | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO graph_node_runs (
                    run_id, graph_name, node_name, status, input_summary, started_at, updated_at
                )
                VALUES (%s, %s, %s, 'running', %s::jsonb, %s, NOW())
                ON CONFLICT (run_id, node_name) DO UPDATE SET
                    status = 'running',
                    input_summary = EXCLUDED.input_summary,
                    error = NULL,
                    started_at = COALESCE(graph_node_runs.started_at, EXCLUDED.started_at),
                    finished_at = NULL,
                    duration_ms = NULL,
                    updated_at = NOW()
                """,
                (
                    run_id,
                    graph_name,
                    node_name,
                    json.dumps(input_summary or {}),
                    started_at,
                ),
            )
            await cursor.execute(
                """
                UPDATE graph_runs
                SET status = 'running',
                    current_node = %s,
                    progress_total = COALESCE(%s, progress_total),
                    progress_percent = CASE
                        WHEN COALESCE(%s, progress_total) IS NULL OR COALESCE(%s, progress_total) = 0 THEN NULL
                        ELSE ROUND((progress_current::numeric / COALESCE(%s, progress_total)) * 100, 3)::double precision
                    END,
                    updated_at = NOW()
                WHERE run_id = %s
                """,
                (node_name, progress_total, progress_total, progress_total, progress_total, run_id),
            )
        await conn.commit()

    async def mark_node_succeeded(
        self,
        *,
        run_id: str,
        graph_name: str,
        node_name: str,
        progress_total: int | None = None,
        output_summary: dict[str, Any] | None = None,
        finished_at: datetime | None = None,
        duration_ms: float | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE graph_node_runs
                SET status = 'succeeded',
                    output_summary = %s::jsonb,
                    finished_at = %s,
                    duration_ms = %s,
                    updated_at = NOW()
                WHERE run_id = %s AND node_name = %s
                """,
                (json.dumps(output_summary or {}), finished_at, duration_ms, run_id, node_name),
            )
            await cursor.execute(
                """
                WITH updated_nodes AS (
                    SELECT CASE
                        WHEN completed_nodes ? %s THEN completed_nodes
                        ELSE completed_nodes || to_jsonb(ARRAY[%s]::text[])
                    END AS nodes
                    FROM graph_runs
                    WHERE run_id = %s
                )
                UPDATE graph_runs
                SET completed_nodes = updated_nodes.nodes,
                    progress_current = jsonb_array_length(updated_nodes.nodes),
                    progress_total = COALESCE(%s, progress_total),
                    progress_percent = CASE
                        WHEN COALESCE(%s, progress_total) IS NULL OR COALESCE(%s, progress_total) = 0 THEN NULL
                        ELSE ROUND((jsonb_array_length(updated_nodes.nodes)::numeric / COALESCE(%s, progress_total)) * 100, 3)::double precision
                    END,
                    updated_at = NOW()
                FROM updated_nodes
                WHERE graph_runs.run_id = %s
                """,
                (
                    node_name,
                    node_name,
                    run_id,
                    progress_total,
                    progress_total,
                    progress_total,
                    progress_total,
                    run_id,
                ),
            )
        await conn.commit()

    async def mark_node_failed(
        self,
        *,
        run_id: str,
        node_name: str,
        error: dict[str, Any],
        finished_at: datetime | None = None,
        duration_ms: float | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE graph_node_runs
                SET status = 'failed',
                    error = %s::jsonb,
                    finished_at = %s,
                    duration_ms = %s,
                    updated_at = NOW()
                WHERE run_id = %s AND node_name = %s
                """,
                (json.dumps(error), finished_at, duration_ms, run_id, node_name),
            )
            await cursor.execute(
                """
                UPDATE graph_runs
                SET status = 'failed',
                    current_node = %s,
                    error = %s::jsonb,
                    updated_at = NOW()
                WHERE run_id = %s
                """,
                (node_name, json.dumps(error), run_id),
            )
        await conn.commit()

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        from psycopg.rows import dict_row

        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT run_id, service_name, graph_name, trigger_type, status,
                       current_node, completed_nodes, progress_current, progress_total,
                       progress_percent, params, result, error, started_at, updated_at,
                       finished_at, created_at
                FROM graph_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = await cursor.fetchone()
        return None if row is None else _serialize_row(row)

    async def list_runs(
        self,
        *,
        limit: int = 100,
        graph_name: str | None = None,
        status: str | None = None,
        service_name: str | None = None,
    ) -> list[dict[str, Any]]:
        from psycopg.rows import dict_row

        conn = await self._get_connection()
        await self._ensure_tables(conn)
        conditions = []
        params: list[object] = []
        if graph_name is not None:
            conditions.append("graph_name = %s")
            params.append(graph_name)
        if status is not None:
            conditions.append("status = %s")
            params.append(status)
        if service_name is not None:
            conditions.append("service_name = %s")
            params.append(service_name)
        where_clause = "" if not conditions else "WHERE " + " AND ".join(conditions)
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT run_id, service_name, graph_name, trigger_type, status,
                       current_node, completed_nodes, progress_current, progress_total,
                       progress_percent, params, result, error, started_at, updated_at,
                       finished_at, created_at
                FROM graph_runs
                {where_clause}
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]

    async def list_nodes(self, run_id: str) -> list[dict[str, Any]]:
        from psycopg.rows import dict_row

        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT id, run_id, graph_name, node_name, status, input_summary,
                       output_summary, error, started_at, finished_at, duration_ms,
                       created_at, updated_at
                FROM graph_node_runs
                WHERE run_id = %s
                ORDER BY created_at, id
                """,
                (run_id,),
            )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _mark_finished(
        self,
        *,
        run_id: str,
        status: str,
        result: dict[str, Any] | None,
        error: dict[str, Any] | None,
        finished_at: datetime | None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_tables(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE graph_runs
                SET status = %s,
                    current_node = NULL,
                    result = %s::jsonb,
                    error = %s::jsonb,
                    finished_at = %s,
                    updated_at = NOW()
                WHERE run_id = %s
                """,
                (
                    status,
                    json.dumps(result or {}),
                    json.dumps(error) if error is not None else None,
                    finished_at,
                    run_id,
                ),
            )
        await conn.commit()

    async def _get_connection(self) -> Any:
        if self._conn is None:
            self._conn = await connect(self._database_url)
        return self._conn

    async def _ensure_tables(self, conn: Any) -> None:
        if self._initialized:
            return
        await ensure_graph_runs_tables(conn)
        self._initialized = True


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in dict(row).items():
        serialized[key] = value.isoformat() if hasattr(value, "isoformat") else value
    return serialized
