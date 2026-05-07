from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from apps.shared.data_pipeline.app.steps.g2b_bid import BASE_URLS, G2BIngestWindow, normalize_raw_row, resource_key


PIPELINE_NAME = "g2b_bid_hourly_ingest"


def write_raw_records(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
    category: str,
    window: G2BIngestWindow,
    records: list[dict[str, Any]],
) -> int:
    if not records:
        return 0

    now = datetime.now(UTC)
    with psycopg.connect(database_url) as conn:
        _ensure_raw_table(conn, schema_name=schema_name, table_name=table_name)
        statement = (
            f'INSERT INTO "{schema_name}"."{table_name}" '
            "(category, source_url, resource_key, bid_notice_no, bid_notice_order, "
            "notice_published_at, raw_payload, metadata, fetched_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s) "
            "ON CONFLICT (resource_key) DO UPDATE SET "
            "raw_payload = EXCLUDED.raw_payload, "
            "metadata = EXCLUDED.metadata, "
            "updated_at = EXCLUDED.updated_at"
        )
        with conn.cursor() as cursor:
            for record in records:
                cursor.execute(
                    statement,
                    (
                        category,
                        BASE_URLS[category],
                        resource_key(category, record),
                        record.get("bidNtceNo"),
                        record.get("bidNtceOrd"),
                        record.get("bidNtceDt"),
                        json.dumps(record, ensure_ascii=False),
                        json.dumps(
                            {
                                "window": {"begin": window.begin, "end": window.end},
                                "ingest_name": "g2b-bid-ingest",
                            },
                            ensure_ascii=False,
                        ),
                        now,
                        now,
                    ),
                )
        conn.commit()
    return len(records)


def normalize_raw_notices(
    *,
    database_url: str,
    raw_schema: str,
    raw_table: str,
    target_schema: str,
    target_table: str,
    window_begin: str | None = None,
    window_end: str | None = None,
) -> dict[str, Any]:
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        _ensure_normalized_table(
            conn,
            schema_name=target_schema,
            table_name=target_table,
            raw_schema=raw_schema,
            raw_table=raw_table,
        )
        rows = _load_raw_rows(
            conn,
            schema_name=raw_schema,
            table_name=raw_table,
            window_begin=window_begin,
            window_end=window_end,
        )
        written = _write_normalized_rows(
            conn,
            schema_name=target_schema,
            table_name=target_table,
            rows=[normalize_raw_row(row) for row in rows],
        )
        conn.commit()

    return {
        "source": {
            "schema": raw_schema,
            "table": raw_table,
            "count": len(rows),
            "window": {"begin": window_begin, "end": window_end} if window_begin and window_end else None,
        },
        "target": {"schema": target_schema, "table": target_table, "count": written},
    }


def get_last_succeeded_window_begin(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
    pipeline_name: str = PIPELINE_NAME,
) -> str | None:
    with psycopg.connect(database_url) as conn:
        _ensure_checkpoint_table(conn, schema_name=schema_name, table_name=table_name)
        with conn.cursor() as cursor:
            cursor.execute(
                f'''
                SELECT window_begin
                FROM "{schema_name}"."{table_name}"
                WHERE pipeline_name = %s AND status = 'succeeded'
                ORDER BY window_begin DESC
                LIMIT 1
                ''',
                (pipeline_name,),
            )
            row = cursor.fetchone()
        conn.commit()
    return str(row[0]) if row else None


def mark_window_running(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
    window: G2BIngestWindow,
    pipeline_name: str = PIPELINE_NAME,
) -> None:
    with psycopg.connect(database_url) as conn:
        _ensure_checkpoint_table(conn, schema_name=schema_name, table_name=table_name)
        with conn.cursor() as cursor:
            cursor.execute(
                f'''
                INSERT INTO "{schema_name}"."{table_name}"
                    (pipeline_name, window_begin, window_end, status, started_at, error_message, updated_at)
                VALUES (%s, %s, %s, 'running', now(), NULL, now())
                ON CONFLICT (pipeline_name, window_begin, window_end) DO UPDATE SET
                    status = 'running',
                    started_at = EXCLUDED.started_at,
                    error_message = NULL,
                    updated_at = EXCLUDED.updated_at
                ''',
                (pipeline_name, window.begin, window.end),
            )
        conn.commit()


def mark_window_succeeded(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
    window: G2BIngestWindow,
    raw_count: int,
    normalized_count: int,
    pipeline_name: str = PIPELINE_NAME,
) -> None:
    with psycopg.connect(database_url) as conn:
        _ensure_checkpoint_table(conn, schema_name=schema_name, table_name=table_name)
        with conn.cursor() as cursor:
            cursor.execute(
                f'''
                INSERT INTO "{schema_name}"."{table_name}"
                    (pipeline_name, window_begin, window_end, status, raw_count, normalized_count, finished_at, updated_at)
                VALUES (%s, %s, %s, 'succeeded', %s, %s, now(), now())
                ON CONFLICT (pipeline_name, window_begin, window_end) DO UPDATE SET
                    status = 'succeeded',
                    raw_count = EXCLUDED.raw_count,
                    normalized_count = EXCLUDED.normalized_count,
                    finished_at = EXCLUDED.finished_at,
                    error_message = NULL,
                    updated_at = EXCLUDED.updated_at
                ''',
                (pipeline_name, window.begin, window.end, raw_count, normalized_count),
            )
        conn.commit()


def mark_window_failed(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
    window: G2BIngestWindow,
    error_message: str,
    pipeline_name: str = PIPELINE_NAME,
) -> None:
    with psycopg.connect(database_url) as conn:
        _ensure_checkpoint_table(conn, schema_name=schema_name, table_name=table_name)
        with conn.cursor() as cursor:
            cursor.execute(
                f'''
                INSERT INTO "{schema_name}"."{table_name}"
                    (pipeline_name, window_begin, window_end, status, error_message, finished_at, updated_at)
                VALUES (%s, %s, %s, 'failed', %s, now(), now())
                ON CONFLICT (pipeline_name, window_begin, window_end) DO UPDATE SET
                    status = 'failed',
                    error_message = EXCLUDED.error_message,
                    finished_at = EXCLUDED.finished_at,
                    updated_at = EXCLUDED.updated_at
                ''',
                (pipeline_name, window.begin, window.end, error_message[:2000]),
            )
        conn.commit()


def _ensure_raw_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                category TEXT NOT NULL,
                source_url TEXT NOT NULL,
                resource_key TEXT NOT NULL,
                bid_notice_no TEXT,
                bid_notice_order TEXT,
                notice_published_at TEXT,
                raw_payload JSONB NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            '''
        )
        cursor.execute(
            f'''
            CREATE UNIQUE INDEX IF NOT EXISTS "{table_name}_resource_key_uidx"
            ON "{schema_name}"."{table_name}" (resource_key)
            '''
        )


def _ensure_checkpoint_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                window_begin TEXT NOT NULL,
                window_end TEXT NOT NULL,
                status TEXT NOT NULL,
                raw_count INTEGER NOT NULL DEFAULT 0,
                normalized_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (pipeline_name, window_begin, window_end)
            )
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_pipeline_status_idx"
            ON "{schema_name}"."{table_name}" (pipeline_name, status, window_begin DESC)
            '''
        )


def _ensure_normalized_table(
    conn: psycopg.Connection,
    *,
    schema_name: str,
    table_name: str,
    raw_schema: str,
    raw_table: str,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                resource_key TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL,
                category_label TEXT NOT NULL,
                bid_notice_no TEXT NOT NULL,
                bid_notice_order TEXT NOT NULL DEFAULT '000',
                title TEXT NOT NULL,
                organization_name TEXT,
                demand_org_name TEXT,
                budget NUMERIC,
                published_at TIMESTAMPTZ,
                deadline_at TIMESTAMPTZ,
                opening_at TIMESTAMPTZ,
                contract_method TEXT,
                bid_method TEXT,
                notice_kind TEXT,
                detail_url TEXT,
                source_url TEXT,
                raw_id BIGINT REFERENCES "{raw_schema}"."{raw_table}"(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (category, bid_notice_no, bid_notice_order)
            )
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_category_published_idx"
            ON "{schema_name}"."{table_name}" (category, published_at DESC)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_deadline_idx"
            ON "{schema_name}"."{table_name}" (deadline_at)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_organization_idx"
            ON "{schema_name}"."{table_name}" (organization_name)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_budget_idx"
            ON "{schema_name}"."{table_name}" (budget)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_title_trgm_idx"
            ON "{schema_name}"."{table_name}" USING gin (title public.gin_trgm_ops)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_organization_trgm_idx"
            ON "{schema_name}"."{table_name}" USING gin (organization_name public.gin_trgm_ops)
            '''
        )


def _load_raw_rows(
    conn: psycopg.Connection,
    *,
    schema_name: str,
    table_name: str,
    window_begin: str | None,
    window_end: str | None,
) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        query = f'''
        SELECT id, category, source_url, resource_key, raw_payload
        FROM "{schema_name}"."{table_name}"
        '''
        params: tuple[Any, ...] = ()
        if window_begin and window_end:
            query += "WHERE metadata->'window'->>'begin' = %s AND metadata->'window'->>'end' = %s "
            params = (window_begin, window_end)
        query += "ORDER BY id"
        cursor.execute(query, params)
        return list(cursor.fetchall())


def _write_normalized_rows(
    conn: psycopg.Connection,
    *,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0

    statement = (
        f'INSERT INTO "{schema_name}"."{table_name}" '
        "(resource_key, category, category_label, bid_notice_no, bid_notice_order, title, "
        "organization_name, demand_org_name, budget, published_at, deadline_at, opening_at, "
        "contract_method, bid_method, notice_kind, detail_url, source_url, raw_id, updated_at) "
        "VALUES (%(resource_key)s, %(category)s, %(category_label)s, %(bid_notice_no)s, "
        "%(bid_notice_order)s, %(title)s, %(organization_name)s, %(demand_org_name)s, "
        "%(budget)s, %(published_at)s, %(deadline_at)s, %(opening_at)s, %(contract_method)s, "
        "%(bid_method)s, %(notice_kind)s, %(detail_url)s, %(source_url)s, %(raw_id)s, %(updated_at)s) "
        "ON CONFLICT (resource_key) DO UPDATE SET "
        "category = EXCLUDED.category, "
        "category_label = EXCLUDED.category_label, "
        "bid_notice_no = EXCLUDED.bid_notice_no, "
        "bid_notice_order = EXCLUDED.bid_notice_order, "
        "title = EXCLUDED.title, "
        "organization_name = EXCLUDED.organization_name, "
        "demand_org_name = EXCLUDED.demand_org_name, "
        "budget = EXCLUDED.budget, "
        "published_at = EXCLUDED.published_at, "
        "deadline_at = EXCLUDED.deadline_at, "
        "opening_at = EXCLUDED.opening_at, "
        "contract_method = EXCLUDED.contract_method, "
        "bid_method = EXCLUDED.bid_method, "
        "notice_kind = EXCLUDED.notice_kind, "
        "detail_url = EXCLUDED.detail_url, "
        "source_url = EXCLUDED.source_url, "
        "raw_id = EXCLUDED.raw_id, "
        "updated_at = EXCLUDED.updated_at"
    )
    with conn.cursor() as cursor:
        cursor.executemany(statement, rows)
    return len(rows)
