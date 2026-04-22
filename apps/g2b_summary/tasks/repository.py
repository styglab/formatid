from __future__ import annotations

import json
import os
from typing import Any

from psycopg.rows import dict_row

from services.runtime_db.connection import connect
from services.runtime_db.schema import ensure_task_executions_table
from shared.postgres_url import get_checkpoint_database_url


def get_summary_database_url() -> str:
    return os.getenv("G2B_SUMMARY_DATABASE_URL") or get_checkpoint_database_url(host_default="postgres")


class SummaryRepository:
    def __init__(self, *, database_url: str | None = None) -> None:
        self._database_url = database_url or get_summary_database_url()
        self._conn: Any | None = None

    async def ensure_schema(self) -> None:
        conn = await self._get_connection()
        schema_path = os.path.join(os.getcwd(), "apps", "g2b_summary", "sql", "schema.sql")
        with open(schema_path, encoding="utf-8") as file:
            sql = file.read()
        async with conn.cursor() as cursor:
            await cursor.execute(sql)
        await conn.commit()

    async def create_job(
        self,
        *,
        job_id: str,
        bucket: str,
        object_key: str,
        callback_url: str | None,
    ) -> None:
        await self.ensure_schema()
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO summary.jobs (job_id, status, bucket, object_key, callback_url)
                VALUES (%s, 'queued', %s, %s, %s)
                """,
                (job_id, bucket, object_key, callback_url),
            )
            await cursor.execute(
                """
                INSERT INTO summary.job_events (job_id, event_name, details)
                VALUES (%s, 'job.created', %s::jsonb)
                """,
                (job_id, json.dumps({"bucket": bucket, "object_key": object_key})),
            )
        await conn.commit()

    async def set_status(self, *, job_id: str, status: str, details: dict[str, Any] | None = None) -> None:
        await self.ensure_schema()
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE summary.jobs SET status = %s, updated_at = NOW() WHERE job_id = %s",
                (status, job_id),
            )
            await cursor.execute(
                """
                INSERT INTO summary.job_events (job_id, event_name, details)
                VALUES (%s, %s, %s::jsonb)
                """,
                (job_id, f"job.{status}", json.dumps(details or {})),
            )
        await conn.commit()

    async def set_failed(self, *, job_id: str, error: dict[str, Any]) -> None:
        await self.ensure_schema()
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE summary.jobs SET status = 'failed', error = %s::jsonb, updated_at = NOW() WHERE job_id = %s",
                (json.dumps(error), job_id),
            )
            await cursor.execute(
                """
                INSERT INTO summary.job_events (job_id, event_name, details)
                VALUES (%s, 'job.failed', %s::jsonb)
                """,
                (job_id, json.dumps(error)),
            )
        await conn.commit()

    async def get_job(self, *, job_id: str) -> dict[str, Any] | None:
        await self.ensure_schema()
        conn = await self._get_connection()
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT
                    j.job_id,
                    j.status,
                    j.bucket,
                    j.object_key,
                    j.callback_url,
                    j.error,
                    j.created_at,
                    j.updated_at,
                    e.char_count,
                    r.summary_text,
                    r.model,
                    r.prompt_version
                FROM summary.jobs j
                LEFT JOIN summary.extracted_texts e ON e.job_id = j.job_id
                LEFT JOIN summary.results r ON r.job_id = j.job_id
                WHERE j.job_id = %s
                """,
                (job_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return _serialize_row(dict(row))

    async def list_events(self, *, job_id: str, limit: int = 50) -> list[dict[str, Any]]:
        await self.ensure_schema()
        conn = await self._get_connection()
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT id, job_id, event_name, details, created_at
                FROM summary.job_events
                WHERE job_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (job_id, limit),
            )
            rows = await cursor.fetchall()
        return [_serialize_row(dict(row)) for row in rows]

    async def get_latest_failed_task(self, *, job_id: str) -> dict[str, Any] | None:
        conn = await self._get_connection()
        await ensure_task_executions_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT task_id, task_name, status, attempts, error, updated_at
                FROM task_executions
                WHERE correlation_id = %s
                  AND status IN ('failed', 'dead_lettered', 'interrupted')
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (job_id,),
            )
            row = await cursor.fetchone()
        await conn.commit()
        if row is None:
            return None
        return _serialize_row(dict(row))

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = await connect(self._database_url)
        return self._conn


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in row.items()
    }
