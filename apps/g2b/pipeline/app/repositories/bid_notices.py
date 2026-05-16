from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from apps.g2b.pipeline.app.repositories.common import acquire_write_lock, load_raw_rows
from apps.g2b.pipeline.app.steps.bid_notices import (
    BASE_URLS,
    normalize_raw_row,
    resource_key,
)
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow


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
        acquire_write_lock(conn)
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
        acquire_write_lock(conn)
        _ensure_normalized_table(
            conn,
            schema_name=target_schema,
            table_name=target_table,
            raw_schema=raw_schema,
            raw_table=raw_table,
        )
        rows = load_raw_rows(
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
                notice_agency_code TEXT,
                notice_agency_name TEXT,
                demand_agency_code TEXT,
                demand_agency_name TEXT,
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
        cursor.execute(f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN IF NOT EXISTS notice_agency_code TEXT')
        cursor.execute(f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN IF NOT EXISTS notice_agency_name TEXT')
        cursor.execute(f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN IF NOT EXISTS demand_agency_code TEXT')
        cursor.execute(f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN IF NOT EXISTS demand_agency_name TEXT')
        for suffix, expression in {
            "category_published": "(category, published_at DESC)",
            "deadline": "(deadline_at)",
            "notice_agency": "(notice_agency_name)",
            "demand_agency": "(demand_agency_name)",
            "budget": "(budget)",
        }.items():
            cursor.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "{table_name}_{suffix}_idx"
                ON "{schema_name}"."{table_name}" {expression}
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
            CREATE INDEX IF NOT EXISTS "{table_name}_notice_agency_trgm_idx"
            ON "{schema_name}"."{table_name}" USING gin (notice_agency_name public.gin_trgm_ops)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_demand_agency_trgm_idx"
            ON "{schema_name}"."{table_name}" USING gin (demand_agency_name public.gin_trgm_ops)
            '''
        )


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
        "notice_agency_code, notice_agency_name, demand_agency_code, demand_agency_name, "
        "organization_name, demand_org_name, budget, published_at, deadline_at, opening_at, "
        "contract_method, bid_method, notice_kind, detail_url, source_url, raw_id, updated_at) "
        "VALUES (%(resource_key)s, %(category)s, %(category_label)s, %(bid_notice_no)s, "
        "%(bid_notice_order)s, %(title)s, %(notice_agency_code)s, %(notice_agency_name)s, "
        "%(demand_agency_code)s, %(demand_agency_name)s, %(organization_name)s, %(demand_org_name)s, "
        "%(budget)s, %(published_at)s, %(deadline_at)s, %(opening_at)s, %(contract_method)s, "
        "%(bid_method)s, %(notice_kind)s, %(detail_url)s, %(source_url)s, %(raw_id)s, %(updated_at)s) "
        "ON CONFLICT (resource_key) DO UPDATE SET "
        "category = EXCLUDED.category, "
        "category_label = EXCLUDED.category_label, "
        "bid_notice_no = EXCLUDED.bid_notice_no, "
        "bid_notice_order = EXCLUDED.bid_notice_order, "
        "title = EXCLUDED.title, "
        "notice_agency_code = EXCLUDED.notice_agency_code, "
        "notice_agency_name = EXCLUDED.notice_agency_name, "
        "demand_agency_code = EXCLUDED.demand_agency_code, "
        "demand_agency_name = EXCLUDED.demand_agency_name, "
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
