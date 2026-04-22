from __future__ import annotations

import os
from typing import Any

from apps.g2b_ingest.tasks.config import get_settings
from apps.g2b_ingest.tasks.quota import get_quota_block


async def build_g2b_ingest_dashboard_summary(*, redis_url: str, checkpoint_database_url: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    quota_block = None
    quota_error = None
    try:
        quota_block = await get_quota_block(redis_url=redis_url, database_url=checkpoint_database_url)
    except Exception as exc:
        quota_error = f"{type(exc).__name__}: {exc}"
    summary: dict[str, Any] = {
        "app": "g2b_ingest",
        "database_url_configured": bool(settings.database_url),
        "quota_block": quota_block,
        "quota_error": quota_error,
        "counts": {},
        "task_states": {},
        "recent_tasks": [],
        "recent_failures": [],
        "error": None,
    }
    if not settings.database_url:
        summary["error"] = "G2B_INGEST_DATABASE_URL is not configured"
        return summary

    try:
        from psycopg import AsyncConnection
        from psycopg.rows import dict_row

        conn = await AsyncConnection.connect(settings.database_url)
        try:
            summary["quota_block"] = quota_block or await _fetch_active_quota_block(conn)
            summary["counts"] = await _fetch_counts(conn)
            summary["task_states"] = await _fetch_task_state_counts(conn)
            summary["failed_by_reason"] = await _fetch_failed_by_reason(conn)
            summary["retry_due"] = await _fetch_retry_due(conn)
            summary["oldest_open_tasks"] = await _fetch_oldest_open_tasks(conn)
            summary["recent_tasks"] = await _fetch_recent_task_states(conn)
            summary["recent_failures"] = await _fetch_recent_task_state_failures(conn)
        finally:
            await conn.close()
        if checkpoint_database_url:
            summary["backfill"] = await _fetch_backfill_progress(
                checkpoint_database_url=checkpoint_database_url,
            )
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
    return summary


async def _fetch_counts(conn) -> dict[str, int]:
    table_names = {
        "notices": "raw.g2b_ingest_bid_notices",
        "attachments": "raw.g2b_ingest_bid_attachments",
        "participants": "raw.g2b_ingest_bid_result_participants",
        "winners": "raw.g2b_ingest_bid_result_winners",
    }
    counts: dict[str, int] = {}
    async with conn.cursor() as cursor:
        for key, table_name in table_names.items():
            if not await _table_exists(cursor, table_name):
                counts[key] = 0
                continue
            await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row = await cursor.fetchone()
            counts[key] = int(row[0])
    return counts


async def _fetch_task_state_counts(conn) -> dict[str, int]:
    if not await _table_exists_for_conn(conn, "raw.g2b_ingest_task_states"):
        return {}
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT status, COUNT(*)
            FROM raw.g2b_ingest_task_states
            GROUP BY status
            ORDER BY status
            """
        )
        rows = await cursor.fetchall()
    return {state: int(count) for state, count in rows}


async def _fetch_active_quota_block(conn) -> dict[str, Any] | None:
    if not await _table_exists_for_conn(conn, "raw.g2b_ingest_task_states"):
        return None
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT blocked_until, last_error, updated_at
            FROM raw.g2b_ingest_task_states
            WHERE status = 'blocked'
              AND last_error_reason = 'daily_quota_exceeded'
              AND blocked_until > NOW()
            ORDER BY blocked_until DESC
            LIMIT 1
            """
        )
        row = await cursor.fetchone()
    if row is None:
        return None
    blocked_until, last_error, updated_at = row
    detail = dict(last_error or {})
    detail.setdefault("reason", "daily_quota_exceeded")
    detail["blocked_until"] = blocked_until.isoformat()
    detail["updated_at"] = updated_at.isoformat()
    return detail


async def _fetch_recent_task_state_failures(conn) -> list[dict[str, Any]]:
    if not await _table_exists_for_conn(conn, "raw.g2b_ingest_task_states"):
        return []
    from psycopg.rows import dict_row

    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """
            SELECT *
            FROM raw.g2b_ingest_task_states
            WHERE status IN ('failed', 'blocked', 'dead_lettered')
            ORDER BY updated_at DESC
            LIMIT 20
            """
        )
        rows = await cursor.fetchall()
    return [_serialize_row(row) for row in rows]


async def _fetch_recent_task_states(conn) -> list[dict[str, Any]]:
    if not await _table_exists_for_conn(conn, "raw.g2b_ingest_task_states"):
        return []
    from psycopg.rows import dict_row

    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """
            SELECT id, job_type, job_key, status, payload, last_enqueued_at,
                   last_started_at, last_completed_at, last_failed_at, last_error,
                   retry_count, next_run_at, max_attempts, blocked_until,
                   last_error_code, last_error_reason, created_at, updated_at
            FROM raw.g2b_ingest_task_states
            ORDER BY updated_at DESC
            LIMIT 30
            """
        )
        rows = await cursor.fetchall()
    return [_serialize_row(row) for row in rows]


async def _fetch_failed_by_reason(conn) -> list[dict[str, Any]]:
    if not await _table_exists_for_conn(conn, "raw.g2b_ingest_task_states"):
        return []
    from psycopg.rows import dict_row

    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """
            SELECT
                COALESCE(last_error_reason, last_error->>'reason', last_error->>'type', 'unknown') AS reason,
                COUNT(*) AS count,
                MAX(updated_at) AS latest_at
            FROM raw.g2b_ingest_task_states
            WHERE status IN ('failed', 'blocked', 'dead_lettered')
            GROUP BY reason
            ORDER BY count DESC, reason ASC
            LIMIT 12
            """
        )
        rows = await cursor.fetchall()
    return [_serialize_row(row) for row in rows]


async def _fetch_retry_due(conn) -> dict[str, Any]:
    if not await _table_exists_for_conn(conn, "raw.g2b_ingest_task_states"):
        return {"due_now": 0, "future": 0}
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE status IN ('failed', 'blocked')
                      AND retry_count < max_attempts
                      AND COALESCE(blocked_until, next_run_at, NOW()) <= NOW()
                ) AS due_now,
                COUNT(*) FILTER (
                    WHERE status IN ('failed', 'blocked')
                      AND retry_count < max_attempts
                      AND COALESCE(blocked_until, next_run_at) > NOW()
                ) AS future
            FROM raw.g2b_ingest_task_states
            """
        )
        row = await cursor.fetchone()
    return {"due_now": int(row[0] or 0), "future": int(row[1] or 0)}


async def _fetch_oldest_open_tasks(conn) -> list[dict[str, Any]]:
    if not await _table_exists_for_conn(conn, "raw.g2b_ingest_task_states"):
        return []
    from psycopg.rows import dict_row

    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """
            SELECT id, job_type, job_key, status, retry_count, next_run_at,
                   blocked_until, last_error_reason, updated_at
            FROM raw.g2b_ingest_task_states
            WHERE status IN ('pending', 'queued', 'running', 'failed', 'blocked')
            ORDER BY updated_at ASC
            LIMIT 12
            """
        )
        rows = await cursor.fetchall()
    return [_serialize_row(row) for row in rows]


async def _fetch_backfill_progress(*, checkpoint_database_url: str) -> dict[str, Any]:
    from services.runtime_db.connection import connect

    conn = await connect(checkpoint_database_url)
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT name, value, updated_at
                FROM checkpoints
                WHERE name = 'service:g2b_ingest_bid_list_collect'
                   OR name LIKE 'g2b_ingest:bid_list_window:%'
                ORDER BY updated_at DESC
                LIMIT 10
                """
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    checkpoints = [
        {
            "name": name,
            "value": value,
            "updated_at": updated_at.isoformat(),
        }
        for name, value, updated_at in rows
    ]
    service_checkpoint = next(
        (item for item in checkpoints if item["name"] == "service:g2b_ingest_bid_list_collect"),
        None,
    )
    payload = {}
    if service_checkpoint and isinstance(service_checkpoint.get("value"), dict):
        payload = service_checkpoint["value"].get("payload", {}) or {}
    return {
        "start": os.getenv("G2B_INGEST_BACKFILL_START"),
        "window_minutes": os.getenv("G2B_INGEST_WINDOW_MINUTES"),
        "current_window": payload,
        "recent_checkpoints": checkpoints,
    }


async def _table_exists_for_conn(conn, table_name: str) -> bool:
    async with conn.cursor() as cursor:
        return await _table_exists(cursor, table_name)


async def _table_exists(cursor, table_name: str) -> bool:
    await cursor.execute("SELECT to_regclass(%s)", (table_name,))
    row = await cursor.fetchone()
    return bool(row and row[0])


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in row.items()
    }
