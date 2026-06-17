from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import uuid4

from core.runtime.runtime_db.connection import connect
from core.runtime.time import iso_now


class DataScoreRunRepository:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: Any | None = None
        self._initialized = False

    async def enqueue_run(
        self,
        *,
        dataset_name: str,
        llm_mode: str,
        business_context: str | None,
        request_payload: dict[str, Any],
    ) -> dict[str, Any]:
        run_id = f"dsrun_{uuid4().hex}"
        now = datetime.utcnow()
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO data_score_runs (
                    run_id,
                    dataset_name,
                    llm_mode,
                    business_context,
                    status,
                    request_payload,
                    created_at,
                    started_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                """,
                (
                    run_id,
                    dataset_name,
                    llm_mode,
                    business_context,
                    "pending",
                    json.dumps(request_payload),
                    now,
                    now,
                    now,
                ),
            )
        await conn.commit()
        return {
            "run_id": run_id,
            "dataset_name": dataset_name,
            "llm_mode": llm_mode,
            "business_context": business_context,
            "status": "pending",
            "created_at": iso_now(now),
            "started_at": None,
            "updated_at": iso_now(now),
        }

    async def claim_next_pending_run(self) -> dict[str, Any] | None:
        from psycopg.rows import dict_row

        now = datetime.utcnow()
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT run_id
                FROM data_score_runs
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row = await cursor.fetchone()
            if row is None:
                await conn.commit()
                return None
            await cursor.execute(
                """
                UPDATE data_score_runs
                SET status = %s,
                    started_at = %s,
                    updated_at = %s
                WHERE run_id = %s
                RETURNING
                    run_id,
                    dataset_name,
                    llm_mode,
                    business_context,
                    status,
                    request_payload,
                    summary,
                    report,
                    error,
                    created_at,
                    started_at,
                    finished_at,
                    updated_at,
                    duration_ms
                """,
                ("running", now, now, str(row["run_id"])),
            )
            claimed = await cursor.fetchone()
        await conn.commit()
        if claimed is None:
            return None
        return _serialize_row(claimed)

    async def complete_run(
        self,
        *,
        run_id: str,
        report: dict[str, Any],
        duration_ms: float,
    ) -> None:
        finished_at = datetime.utcnow()
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE data_score_runs
                SET status = %s,
                    report = %s::jsonb,
                    summary = %s::jsonb,
                    duration_ms = %s,
                    finished_at = %s,
                    updated_at = %s
                WHERE run_id = %s
                """,
                (
                    "completed",
                    json.dumps(report),
                    json.dumps(report.get("summary", {})),
                    duration_ms,
                    finished_at,
                    finished_at,
                    run_id,
                ),
            )
        await conn.commit()

    async def fail_run(
        self,
        *,
        run_id: str,
        error: dict[str, Any],
        duration_ms: float,
    ) -> None:
        finished_at = datetime.utcnow()
        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE data_score_runs
                SET status = %s,
                    error = %s::jsonb,
                    duration_ms = %s,
                    finished_at = %s,
                    updated_at = %s
                WHERE run_id = %s
                """,
                (
                    "failed",
                    json.dumps(error),
                    duration_ms,
                    finished_at,
                    finished_at,
                    run_id,
                ),
            )
        await conn.commit()

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        from psycopg.rows import dict_row

        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT
                    run_id,
                    dataset_name,
                    llm_mode,
                    business_context,
                    status,
                    request_payload,
                    summary,
                    report,
                    error,
                    created_at,
                    started_at,
                    finished_at,
                    updated_at,
                    duration_ms
                FROM data_score_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return _serialize_row(row)

    async def list_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        from psycopg.rows import dict_row

        conn = await self._get_connection()
        await self._ensure_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT
                    run_id,
                    dataset_name,
                    llm_mode,
                    business_context,
                    status,
                    request_payload,
                    summary,
                    report,
                    error,
                    created_at,
                    started_at,
                    finished_at,
                    updated_at,
                    duration_ms
                FROM data_score_runs
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (max(limit, 1),),
            )
            rows = await cursor.fetchall()
        return [_serialize_row(row) for row in rows]

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
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS data_score_runs (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL UNIQUE,
                    dataset_name TEXT NOT NULL,
                    llm_mode TEXT NOT NULL,
                    business_context TEXT,
                    status TEXT NOT NULL,
                    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                    report JSONB,
                    error JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    duration_ms DOUBLE PRECISION
                )
                """
            )
            await cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_data_score_runs_created_at
                    ON data_score_runs (created_at DESC)
                """
            )
            await cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_data_score_runs_status_created_at
                    ON data_score_runs (status, created_at DESC)
                """
            )
        await conn.commit()
        self._initialized = True


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for key in ("created_at", "started_at", "finished_at", "updated_at"):
        value = result.get(key)
        result[key] = iso_now(value) if isinstance(value, datetime) else None
    return result
