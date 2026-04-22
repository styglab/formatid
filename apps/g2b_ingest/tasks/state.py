from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from shared.time import now
from apps.g2b_ingest.tasks.schema import ensure_g2b_ingest_raw_schema


class G2bIngestTaskStateStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: AsyncConnection | None = None

    async def upsert_state(
        self,
        *,
        job_type: str,
        job_key: str,
        status: str,
        payload: dict[str, Any],
        error: dict[str, Any] | None = None,
        max_attempts: int = 3,
        retry_after_seconds: int | None = None,
    ) -> None:
        conn = await self._get_connection()
        await self._ensure_schema(conn)
        await self._rollback_if_needed(conn)
        next_run_at = _calculate_next_run_at(
            status=status,
            error=error,
            retry_after_seconds=retry_after_seconds,
        )
        blocked_until = _extract_blocked_until(error)
        error_code = _extract_error_code(error)
        error_reason = _extract_error_reason(error)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT INTO raw.g2b_ingest_task_states AS current_state (
                        job_type,
                        job_key,
                        status,
                        payload,
                        last_enqueued_at,
                        last_started_at,
                        last_completed_at,
                        last_failed_at,
                        last_error,
                        retry_count,
                        next_run_at,
                        max_attempts,
                        blocked_until,
                        last_error_code,
                        last_error_reason
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s::jsonb,
                        CASE WHEN %s = 'queued' THEN NOW() ELSE NULL END,
                        CASE WHEN %s = 'running' THEN NOW() ELSE NULL END,
                        CASE WHEN %s = 'succeeded' THEN NOW() ELSE NULL END,
                        CASE WHEN %s IN ('failed', 'blocked') THEN NOW() ELSE NULL END,
                        %s::jsonb,
                        CASE WHEN %s IN ('failed', 'blocked') THEN 1 ELSE 0 END,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    ON CONFLICT (job_type, job_key)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        payload = EXCLUDED.payload,
                        last_enqueued_at = CASE
                            WHEN EXCLUDED.status = 'queued' THEN NOW()
                            ELSE current_state.last_enqueued_at
                        END,
                        last_started_at = CASE
                            WHEN EXCLUDED.status = 'running' THEN NOW()
                            ELSE current_state.last_started_at
                        END,
                        last_completed_at = CASE
                            WHEN EXCLUDED.status = 'succeeded' THEN NOW()
                            ELSE current_state.last_completed_at
                        END,
                        last_failed_at = CASE
                            WHEN EXCLUDED.status IN ('failed', 'blocked') THEN NOW()
                            ELSE current_state.last_failed_at
                        END,
                        last_error = EXCLUDED.last_error,
                        retry_count = CASE
                            WHEN EXCLUDED.status IN ('failed', 'blocked') THEN current_state.retry_count + 1
                            WHEN EXCLUDED.status = 'succeeded' THEN 0
                            ELSE current_state.retry_count
                        END,
                        next_run_at = EXCLUDED.next_run_at,
                        max_attempts = EXCLUDED.max_attempts,
                        blocked_until = EXCLUDED.blocked_until,
                        last_error_code = EXCLUDED.last_error_code,
                        last_error_reason = EXCLUDED.last_error_reason,
                        updated_at = NOW()
                    """,
                    (
                        job_type,
                        job_key,
                        status,
                        json.dumps(payload),
                        status,
                        status,
                        status,
                        status,
                        json.dumps(error) if error is not None else None,
                        status,
                        next_run_at,
                        max_attempts,
                        blocked_until,
                        error_code,
                        error_reason,
                    ),
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def get_state(self, *, job_type: str, job_key: str) -> dict[str, Any] | None:
        conn = await self._get_connection()
        await self._ensure_schema(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT job_type, job_key, status, payload, last_enqueued_at, last_started_at,
                       last_completed_at, last_failed_at, last_error, retry_count,
                       next_run_at, max_attempts, blocked_until, last_error_code,
                       last_error_reason, created_at, updated_at
                FROM raw.g2b_ingest_task_states
                WHERE job_type = %s AND job_key = %s
                """,
                (job_type, job_key),
            )
            row = await cursor.fetchone()
        await conn.commit()
        if row is None:
            return None
        return dict(row)

    async def get_active_quota_block(self) -> dict[str, Any] | None:
        conn = await self._get_connection()
        await self._ensure_schema(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
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
        await conn.commit()
        if row is None:
            return None
        detail = dict(row["last_error"] or {})
        detail.setdefault("reason", "daily_quota_exceeded")
        detail["blocked_until"] = row["blocked_until"].isoformat()
        detail["updated_at"] = row["updated_at"].isoformat()
        return detail

    async def mark_stale_running_as_failed(self, *, stale_after_seconds: int) -> int:
        if stale_after_seconds <= 0:
            return 0
        conn = await self._get_connection()
        await self._ensure_schema(conn)
        await self._rollback_if_needed(conn)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE raw.g2b_ingest_task_states
                    SET
                        status = 'failed',
                        last_failed_at = NOW(),
                        last_error = jsonb_build_object(
                            'type', 'StaleRunningTask',
                            'message', 'running task exceeded stale threshold',
                            'stale_after_seconds', %s
                        ),
                        retry_count = retry_count + 1,
                        next_run_at = NOW(),
                        blocked_until = NULL,
                        last_error_code = NULL,
                        last_error_reason = 'stale_running',
                        updated_at = NOW()
                    WHERE status = 'running'
                      AND updated_at < NOW() - (%s * INTERVAL '1 second')
                    """,
                    (stale_after_seconds, stale_after_seconds),
                )
                row_count = cursor.rowcount
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        return int(row_count or 0)

    async def upsert_bid_list_window_state(
        self,
        *,
        job_key: str,
        status: str,
        payload: dict[str, Any],
        error: dict[str, Any] | None = None,
        max_attempts: int = 3,
        retry_after_seconds: int | None = None,
    ) -> None:
        existing = await self.get_state(job_type="bid_list_window", job_key=job_key)
        merged_status = _merge_bid_list_window_status(
            existing_status=None if existing is None else existing.get("status"),
            incoming_status=status,
        )
        merged_payload = _merge_bid_list_window_payload(
            existing_payload=None if existing is None else existing.get("payload"),
            incoming_payload=payload,
            merged_status=merged_status,
        )
        await self.upsert_state(
            job_type="bid_list_window",
            job_key=job_key,
            status=merged_status,
            payload=merged_payload,
            error=error,
            max_attempts=max_attempts,
            retry_after_seconds=retry_after_seconds,
        )

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> AsyncConnection:
        if self._conn is None:
            self._conn = await AsyncConnection.connect(self._database_url)
        return self._conn

    async def _ensure_schema(self, conn: AsyncConnection) -> None:
        await ensure_g2b_ingest_raw_schema(conn, database_url=self._database_url)

    async def _rollback_if_needed(self, conn: AsyncConnection) -> None:
        if conn.info.transaction_status.name == "INERROR":
            await conn.rollback()


def _merge_bid_list_window_status(*, existing_status: Any, incoming_status: str) -> str:
    if existing_status == "succeeded" and incoming_status in {"running", "failed", "blocked"}:
        return "succeeded"
    return incoming_status


def _merge_bid_list_window_payload(
    *,
    existing_payload: Any,
    incoming_payload: dict[str, Any],
    merged_status: str,
) -> dict[str, Any]:
    if not isinstance(existing_payload, dict):
        payload = dict(incoming_payload)
    else:
        payload = dict(existing_payload)
        payload.update(incoming_payload)

    existing_page = _optional_int(
        existing_payload.get("last_completed_page")
        if isinstance(existing_payload, dict)
        else None
    )
    incoming_page = _optional_int(incoming_payload.get("last_completed_page", incoming_payload.get("pageNo")))
    if existing_page is not None or incoming_page is not None:
        payload["last_completed_page"] = max(
            page for page in (existing_page, incoming_page) if page is not None
        )

    total_count = _optional_int(incoming_payload.get("total_count", payload.get("total_count")))
    num_of_rows = _optional_int(incoming_payload.get("num_of_rows", payload.get("num_of_rows")))
    last_completed_page = _optional_int(payload.get("last_completed_page"))
    if merged_status == "succeeded":
        payload["next_page_no"] = None
    elif total_count is not None and num_of_rows and last_completed_page is not None:
        payload["next_page_no"] = (
            last_completed_page + 1
            if last_completed_page * num_of_rows < total_count
            else None
        )

    payload["status"] = merged_status
    return payload


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _calculate_next_run_at(
    *,
    status: str,
    error: dict[str, Any] | None,
    retry_after_seconds: int | None,
):
    if status == "blocked":
        return _extract_blocked_until(error)
    if status != "failed":
        return None
    if retry_after_seconds is None or retry_after_seconds <= 0:
        return None
    return now() + timedelta(seconds=retry_after_seconds)


def _extract_blocked_until(error: dict[str, Any] | None):
    if not isinstance(error, dict):
        return None
    return error.get("blocked_until")


def _extract_error_code(error: dict[str, Any] | None) -> str | None:
    if not isinstance(error, dict):
        return None
    value = error.get("result_code") or error.get("code")
    return None if value is None else str(value)


def _extract_error_reason(error: dict[str, Any] | None) -> str | None:
    if not isinstance(error, dict):
        return None
    value = error.get("reason") or error.get("type")
    return None if value is None else str(value)
