from __future__ import annotations

import json
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row


class PpsTaskStateStore:
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
    ) -> None:
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO raw.pps_task_states AS current_state (
                    job_type,
                    job_key,
                    status,
                    payload,
                    last_enqueued_at,
                    last_started_at,
                    last_completed_at,
                    last_failed_at,
                    last_error,
                    retry_count
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s::jsonb,
                    CASE WHEN %s = 'queued' THEN NOW() ELSE NULL END,
                    CASE WHEN %s = 'running' THEN NOW() ELSE NULL END,
                    CASE WHEN %s = 'succeeded' THEN NOW() ELSE NULL END,
                    CASE WHEN %s = 'failed' THEN NOW() ELSE NULL END,
                    %s::jsonb,
                    CASE WHEN %s = 'failed' THEN 1 ELSE 0 END
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
                        WHEN EXCLUDED.status = 'failed' THEN NOW()
                        ELSE current_state.last_failed_at
                    END,
                    last_error = EXCLUDED.last_error,
                    retry_count = CASE
                        WHEN EXCLUDED.status = 'failed' THEN current_state.retry_count + 1
                        ELSE current_state.retry_count
                    END,
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
                ),
            )
        await conn.commit()

    async def get_state(self, *, job_type: str, job_key: str) -> dict[str, Any] | None:
        conn = await self._get_connection()
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT job_type, job_key, status, payload, last_enqueued_at, last_started_at,
                       last_completed_at, last_failed_at, last_error, retry_count, created_at, updated_at
                FROM raw.pps_task_states
                WHERE job_type = %s AND job_key = %s
                """,
                (job_type, job_key),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    async def upsert_bid_list_window_state(
        self,
        *,
        job_key: str,
        status: str,
        payload: dict[str, Any],
        error: dict[str, Any] | None = None,
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
        )

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> AsyncConnection:
        if self._conn is None:
            self._conn = await AsyncConnection.connect(self._database_url)
        return self._conn


def _merge_bid_list_window_status(*, existing_status: Any, incoming_status: str) -> str:
    if existing_status == "succeeded" and incoming_status in {"running", "failed"}:
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
