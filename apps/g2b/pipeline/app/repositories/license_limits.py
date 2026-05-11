from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from apps.g2b.pipeline.app.repositories.common import acquire_write_lock, load_raw_rows
from apps.g2b.pipeline.app.steps.bid_notices import CATEGORY_BY_LABEL
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.license_limits import (
    LICENSE_LIMIT_URL,
    license_limit_resource_key,
    normalize_license_limit_raw_row,
)


def write_license_limit_raw_records(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
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
            "(category, source_url, resource_key, bid_notice_no, bid_notice_order, registered_at_text, "
            "raw_payload, metadata, fetched_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s) "
            "ON CONFLICT (resource_key) DO UPDATE SET "
            "category = EXCLUDED.category, "
            "raw_payload = EXCLUDED.raw_payload, "
            "metadata = EXCLUDED.metadata, "
            "updated_at = EXCLUDED.updated_at"
        )
        with conn.cursor() as cursor:
            for record in records:
                cursor.execute(
                    statement,
                    (
                        CATEGORY_BY_LABEL.get(str(record.get("bsnsDivNm", "")).strip()),
                        LICENSE_LIMIT_URL,
                        license_limit_resource_key(record),
                        record.get("bidNtceNo"),
                        record.get("bidNtceOrd"),
                        record.get("rgstDt"),
                        json.dumps(record, ensure_ascii=False),
                        json.dumps(
                            {
                                "window": {"begin": window.begin, "end": window.end},
                                "ingest_name": "g2b-bid-license-limit-ingest",
                            },
                            ensure_ascii=False,
                        ),
                        now,
                        now,
                    ),
                )
        conn.commit()
    return len(records)


def normalize_raw_license_limits(
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
            rows=[normalize_license_limit_raw_row(row) for row in rows],
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
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                category TEXT,
                source_url TEXT NOT NULL,
                resource_key TEXT NOT NULL,
                bid_notice_no TEXT,
                bid_notice_order TEXT,
                registered_at_text TEXT,
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
                bid_notice_no TEXT NOT NULL,
                bid_notice_order TEXT NOT NULL DEFAULT '000',
                category TEXT,
                business_div_name TEXT,
                registered_at TIMESTAMPTZ,
                limit_group_no INTEGER,
                limit_serial_no INTEGER,
                license_limit_name TEXT,
                license_limit_code TEXT,
                allowed_industries JSONB NOT NULL DEFAULT '[]'::jsonb,
                main_field_groups JSONB NOT NULL DEFAULT '[]'::jsonb,
                raw_id BIGINT REFERENCES "{raw_schema}"."{raw_table}"(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_notice_idx"
            ON "{schema_name}"."{table_name}" (bid_notice_no, bid_notice_order)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_category_idx"
            ON "{schema_name}"."{table_name}" (category)
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_allowed_industries_idx"
            ON "{schema_name}"."{table_name}" USING gin (allowed_industries)
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
        "(resource_key, bid_notice_no, bid_notice_order, category, business_div_name, "
        "registered_at, limit_group_no, limit_serial_no, license_limit_name, "
        "license_limit_code, allowed_industries, main_field_groups, raw_id, updated_at) "
        "VALUES (%(resource_key)s, %(bid_notice_no)s, %(bid_notice_order)s, %(category)s, "
        "%(business_div_name)s, %(registered_at)s, %(limit_group_no)s, %(limit_serial_no)s, "
        "%(license_limit_name)s, %(license_limit_code)s, %(allowed_industries)s::jsonb, "
        "%(main_field_groups)s::jsonb, %(raw_id)s, %(updated_at)s) "
        "ON CONFLICT (resource_key) DO UPDATE SET "
        "bid_notice_no = EXCLUDED.bid_notice_no, "
        "bid_notice_order = EXCLUDED.bid_notice_order, "
        "category = EXCLUDED.category, "
        "business_div_name = EXCLUDED.business_div_name, "
        "registered_at = EXCLUDED.registered_at, "
        "limit_group_no = EXCLUDED.limit_group_no, "
        "limit_serial_no = EXCLUDED.limit_serial_no, "
        "license_limit_name = EXCLUDED.license_limit_name, "
        "license_limit_code = EXCLUDED.license_limit_code, "
        "allowed_industries = EXCLUDED.allowed_industries, "
        "main_field_groups = EXCLUDED.main_field_groups, "
        "raw_id = EXCLUDED.raw_id, "
        "updated_at = EXCLUDED.updated_at"
    )
    encoded_rows = [
        {
            **row,
            "allowed_industries": json.dumps(row["allowed_industries"], ensure_ascii=False),
            "main_field_groups": json.dumps(row["main_field_groups"], ensure_ascii=False),
        }
        for row in rows
    ]
    with conn.cursor() as cursor:
        cursor.executemany(statement, encoded_rows)
    return len(rows)
