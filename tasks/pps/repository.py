from __future__ import annotations

import json
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row


class PpsRepository:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: AsyncConnection | None = None

    async def upsert_bid_notice(self, *, row: dict[str, Any]) -> None:
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO raw.pps_bid_notices (
                    bid_ntce_no,
                    bid_ntce_ord,
                    bid_ntce_nm,
                    ntce_instt_nm,
                    bid_ntce_dt,
                    bid_begin_dt,
                    bid_clse_dt,
                    openg_dt,
                    rgst_dt,
                    chg_dt,
                    raw_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (bid_ntce_no, bid_ntce_ord)
                DO UPDATE SET
                    bid_ntce_nm = EXCLUDED.bid_ntce_nm,
                    ntce_instt_nm = EXCLUDED.ntce_instt_nm,
                    bid_ntce_dt = EXCLUDED.bid_ntce_dt,
                    bid_begin_dt = EXCLUDED.bid_begin_dt,
                    bid_clse_dt = EXCLUDED.bid_clse_dt,
                    openg_dt = EXCLUDED.openg_dt,
                    rgst_dt = EXCLUDED.rgst_dt,
                    chg_dt = EXCLUDED.chg_dt,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()
                """,
                (
                    row.get("bidNtceNo"),
                    row.get("bidNtceOrd"),
                    row.get("bidNtceNm"),
                    row.get("ntceInsttNm"),
                    _normalize_timestamp(row.get("bidNtceDt")),
                    _normalize_timestamp(row.get("bidBeginDt")),
                    _normalize_timestamp(row.get("bidClseDt")),
                    _normalize_timestamp(row.get("opengDt")),
                    _normalize_timestamp(row.get("rgstDt")),
                    _normalize_timestamp(row.get("chgDt")),
                    json.dumps(row),
                ),
            )
        await conn.commit()

    async def get_bid_notice_raw_payload(
        self,
        *,
        bid_ntce_no: str,
        bid_ntce_ord: str,
    ) -> dict[str, Any] | None:
        conn = await self._get_connection()
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT raw_payload
                FROM raw.pps_bid_notices
                WHERE bid_ntce_no = %s AND bid_ntce_ord = %s
                """,
                (bid_ntce_no, bid_ntce_ord),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        raw_payload = row.get("raw_payload")
        return raw_payload if isinstance(raw_payload, dict) else None

    async def list_bid_notices_for_downstream(
        self,
        *,
        limit: int,
        include_result_tasks: bool = True,
        max_failed_retries: int,
        retry_failed_after_seconds: int,
    ) -> list[dict[str, Any]]:
        conn = await self._get_connection()
        result_state_filter = """
                   OR (
                        participants_state.id IS NULL
                        OR (
                            participants_state.status = 'failed'
                            AND participants_state.retry_count < %(max_failed_retries)s
                            AND (
                                participants_state.last_failed_at IS NULL
                                OR participants_state.last_failed_at <= NOW() - (%(retry_failed_after_seconds)s * INTERVAL '1 second')
                            )
                        )
                   )
                   OR (
                        winners_state.id IS NULL
                        OR (
                            winners_state.status = 'failed'
                            AND winners_state.retry_count < %(max_failed_retries)s
                            AND (
                                winners_state.last_failed_at IS NULL
                                OR winners_state.last_failed_at <= NOW() - (%(retry_failed_after_seconds)s * INTERVAL '1 second')
                            )
                        )
                   )
        """ if include_result_tasks else ""
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT n.bid_ntce_no, n.bid_ntce_ord
                FROM raw.pps_bid_notices AS n
                LEFT JOIN raw.pps_task_states AS attachment_state
                    ON attachment_state.job_type = 'attachment'
                   AND attachment_state.job_key = n.bid_ntce_no || ':' || n.bid_ntce_ord
                LEFT JOIN raw.pps_task_states AS participants_state
                    ON participants_state.job_type = 'participants'
                   AND participants_state.job_key = n.bid_ntce_no
                LEFT JOIN raw.pps_task_states AS winners_state
                    ON winners_state.job_type = 'winners'
                   AND winners_state.job_key = n.bid_ntce_no
                WHERE (
                        attachment_state.id IS NULL
                        OR (
                            attachment_state.status = 'failed'
                            AND attachment_state.retry_count < %(max_failed_retries)s
                            AND (
                                attachment_state.last_failed_at IS NULL
                                OR attachment_state.last_failed_at <= NOW() - (%(retry_failed_after_seconds)s * INTERVAL '1 second')
                            )
                        )
                )
                {result_state_filter}
                ORDER BY n.created_at ASC, n.bid_ntce_no ASC, n.bid_ntce_ord ASC
                LIMIT %(limit)s
                """,
                {
                    "limit": limit,
                    "max_failed_retries": max_failed_retries,
                    "retry_failed_after_seconds": retry_failed_after_seconds,
                },
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def upsert_attachment(
        self,
        *,
        bid_ntce_no: str,
        bid_ntce_ord: str,
        attachment_type: str,
        attachment_index: int,
        source_url: str,
        file_name: str,
        storage_bucket: str | None,
        storage_key: str | None,
        download_status: str,
        raw_payload: dict[str, Any],
    ) -> None:
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO raw.pps_bid_attachments (
                    bid_ntce_no,
                    bid_ntce_ord,
                    attachment_type,
                    attachment_index,
                    source_url,
                    file_name,
                    storage_bucket,
                    storage_key,
                    download_status,
                    raw_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (bid_ntce_no, bid_ntce_ord, attachment_type, attachment_index)
                DO UPDATE SET
                    source_url = EXCLUDED.source_url,
                    file_name = EXCLUDED.file_name,
                    storage_bucket = EXCLUDED.storage_bucket,
                    storage_key = EXCLUDED.storage_key,
                    download_status = EXCLUDED.download_status,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()
                """,
                (
                    bid_ntce_no,
                    bid_ntce_ord,
                    attachment_type,
                    attachment_index,
                    source_url,
                    file_name,
                    storage_bucket,
                    storage_key,
                    download_status,
                    json.dumps(raw_payload),
                ),
            )
        await conn.commit()

    async def replace_bid_result_participants(
        self,
        *,
        bid_ntce_no: str,
        rows: list[dict[str, Any]],
    ) -> None:
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM raw.pps_bid_result_participants WHERE bid_ntce_no = %s",
                (bid_ntce_no,),
            )
            for row in rows:
                bid_ntce_ord = str(row.get("bidNtceOrd", "")).strip()
                await cursor.execute(
                    """
                    INSERT INTO raw.pps_bid_result_participants (
                        bid_ntce_no,
                        bid_ntce_ord,
                        openg_rank,
                        prcbdr_bizno,
                        prcbdr_nm,
                        bidprc_amt,
                        bidprcrt,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        bid_ntce_no,
                        bid_ntce_ord,
                        row.get("opengRank"),
                        row.get("prcbdrBizno"),
                        row.get("prcbdrNm"),
                        _normalize_numeric(row.get("bidprcAmt")),
                        _normalize_numeric(row.get("bidprcrt")),
                        json.dumps(row),
                    ),
                )
        await conn.commit()

    async def replace_bid_result_winners(
        self,
        *,
        bid_ntce_no: str,
        rows: list[dict[str, Any]],
    ) -> None:
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM raw.pps_bid_result_winners WHERE bid_ntce_no = %s",
                (bid_ntce_no,),
            )
            for row in rows:
                bid_ntce_ord = str(row.get("bidNtceOrd", "")).strip()
                await cursor.execute(
                    """
                    INSERT INTO raw.pps_bid_result_winners (
                        bid_ntce_no,
                        bid_ntce_ord,
                        bidwinnr_bizno,
                        bidwinnr_nm,
                        sucsfbid_amt,
                        sucsfbid_rate,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        bid_ntce_no,
                        bid_ntce_ord,
                        row.get("bidwinnrBizno"),
                        row.get("bidwinnrNm"),
                        _normalize_numeric(row.get("sucsfbidAmt")),
                        _normalize_numeric(row.get("sucsfbidRate")),
                        json.dumps(row),
                    ),
                )
        await conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> AsyncConnection:
        if self._conn is None:
            self._conn = await AsyncConnection.connect(self._database_url)
        return self._conn

def _normalize_numeric(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip()


def _normalize_timestamp(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip()
