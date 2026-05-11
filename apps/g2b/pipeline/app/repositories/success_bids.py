from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from apps.g2b.pipeline.app.repositories.common import acquire_write_lock, load_raw_rows
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.success_bids import (
    SUCCESS_BID_URLS,
    normalize_success_bid_raw_row,
    success_bid_resource_key,
)


def write_success_bid_raw_records(
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
            "bid_classification_no, rebid_no, registered_at, raw_payload, metadata, fetched_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s) "
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
                        SUCCESS_BID_URLS[category],
                        success_bid_resource_key(category, record),
                        record.get("bidNtceNo"),
                        record.get("bidNtceOrd"),
                        record.get("bidClsfcNo"),
                        record.get("rbidNo"),
                        record.get("rgstDt"),
                        json.dumps(record, ensure_ascii=False),
                        json.dumps(
                            {
                                "window": {"begin": window.begin, "end": window.end},
                                "ingest_name": "g2b-success-bid-ingest",
                            },
                            ensure_ascii=False,
                        ),
                        now,
                        now,
                    ),
                )
        conn.commit()
    return len(records)


def normalize_raw_success_bids(
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
            rows=[normalize_success_bid_raw_row(row) for row in rows],
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
                bid_classification_no TEXT,
                rebid_no TEXT,
                registered_at TEXT,
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
                bid_classification_no TEXT NOT NULL DEFAULT '0',
                rebid_no TEXT NOT NULL DEFAULT '000',
                notice_division_code TEXT,
                title TEXT NOT NULL,
                participant_count INTEGER,
                winner_name TEXT,
                winner_business_no TEXT,
                winner_ceo_name TEXT,
                winner_address TEXT,
                winner_phone_no TEXT,
                winning_amount NUMERIC,
                winning_rate NUMERIC,
                actual_opening_at TIMESTAMPTZ,
                demand_org_code TEXT,
                demand_org_name TEXT,
                registered_at TIMESTAMPTZ,
                final_success_date TIMESTAMPTZ,
                final_success_company_officer TEXT,
                source_url TEXT,
                raw_id BIGINT REFERENCES "{raw_schema}"."{raw_table}"(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (category, bid_notice_no, bid_notice_order, bid_classification_no, rebid_no)
            )
            '''
        )
        for suffix, expression in {
            "category_registered": "(category, registered_at DESC)",
            "notice": "(bid_notice_no, bid_notice_order)",
            "winner": "(winner_name)",
            "demand_org": "(demand_org_name)",
            "winning_amount": "(winning_amount)",
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
        "(resource_key, category, category_label, bid_notice_no, bid_notice_order, "
        "bid_classification_no, rebid_no, notice_division_code, title, participant_count, "
        "winner_name, winner_business_no, winner_ceo_name, winner_address, winner_phone_no, "
        "winning_amount, winning_rate, actual_opening_at, demand_org_code, demand_org_name, "
        "registered_at, final_success_date, final_success_company_officer, source_url, raw_id, updated_at) "
        "VALUES (%(resource_key)s, %(category)s, %(category_label)s, %(bid_notice_no)s, "
        "%(bid_notice_order)s, %(bid_classification_no)s, %(rebid_no)s, %(notice_division_code)s, "
        "%(title)s, %(participant_count)s, %(winner_name)s, %(winner_business_no)s, %(winner_ceo_name)s, "
        "%(winner_address)s, %(winner_phone_no)s, %(winning_amount)s, %(winning_rate)s, "
        "%(actual_opening_at)s, %(demand_org_code)s, %(demand_org_name)s, %(registered_at)s, "
        "%(final_success_date)s, %(final_success_company_officer)s, %(source_url)s, %(raw_id)s, %(updated_at)s) "
        "ON CONFLICT (resource_key) DO UPDATE SET "
        "category = EXCLUDED.category, "
        "category_label = EXCLUDED.category_label, "
        "bid_notice_no = EXCLUDED.bid_notice_no, "
        "bid_notice_order = EXCLUDED.bid_notice_order, "
        "bid_classification_no = EXCLUDED.bid_classification_no, "
        "rebid_no = EXCLUDED.rebid_no, "
        "notice_division_code = EXCLUDED.notice_division_code, "
        "title = EXCLUDED.title, "
        "participant_count = EXCLUDED.participant_count, "
        "winner_name = EXCLUDED.winner_name, "
        "winner_business_no = EXCLUDED.winner_business_no, "
        "winner_ceo_name = EXCLUDED.winner_ceo_name, "
        "winner_address = EXCLUDED.winner_address, "
        "winner_phone_no = EXCLUDED.winner_phone_no, "
        "winning_amount = EXCLUDED.winning_amount, "
        "winning_rate = EXCLUDED.winning_rate, "
        "actual_opening_at = EXCLUDED.actual_opening_at, "
        "demand_org_code = EXCLUDED.demand_org_code, "
        "demand_org_name = EXCLUDED.demand_org_name, "
        "registered_at = EXCLUDED.registered_at, "
        "final_success_date = EXCLUDED.final_success_date, "
        "final_success_company_officer = EXCLUDED.final_success_company_officer, "
        "source_url = EXCLUDED.source_url, "
        "raw_id = EXCLUDED.raw_id, "
        "updated_at = EXCLUDED.updated_at"
    )
    with conn.cursor() as cursor:
        cursor.executemany(statement, rows)
    return len(rows)
