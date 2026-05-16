from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from apps.g2b.pipeline.app.repositories.common import acquire_write_lock
from apps.g2b.pipeline.app.steps.companies import (
    company_basic_resource_key,
    company_industry_resource_key,
    normalize_company_basic_raw_row,
    normalize_company_industry_raw_row,
)


def load_success_bid_winner_business_numbers(
    *,
    database_url: str,
    schema_name: str,
    success_bid_table: str,
    success_bid_raw_table: str,
    window_begin: str,
    window_end: str,
) -> list[str]:
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f'''
                SELECT DISTINCT success_bid.winner_business_no
                FROM "{schema_name}"."{success_bid_table}" AS success_bid
                JOIN "{schema_name}"."{success_bid_raw_table}" AS raw
                  ON raw.id = success_bid.raw_id
                WHERE raw.metadata->'window'->>'begin' = %s
                  AND raw.metadata->'window'->>'end' = %s
                  AND success_bid.winner_business_no IS NOT NULL
                  AND success_bid.winner_business_no <> ''
                ORDER BY success_bid.winner_business_no
                ''',
                (window_begin, window_end),
            )
            return [row["winner_business_no"] for row in cursor.fetchall()]


def write_company_basic_raw_records(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
    records: list[dict[str, Any]],
) -> int:
    if not records:
        return 0
    now = datetime.now(UTC)
    with psycopg.connect(database_url) as conn:
        acquire_write_lock(conn)
        _ensure_company_basic_raw_table(conn, schema_name=schema_name, table_name=table_name)
        statement = (
            f'INSERT INTO "{schema_name}"."{table_name}" '
            "(resource_key, business_no, raw_payload, fetched_at, updated_at) "
            "VALUES (%s, %s, %s::jsonb, %s, %s) "
            "ON CONFLICT (resource_key) DO UPDATE SET "
            "business_no = EXCLUDED.business_no, raw_payload = EXCLUDED.raw_payload, "
            "fetched_at = EXCLUDED.fetched_at, updated_at = EXCLUDED.updated_at"
        )
        with conn.cursor() as cursor:
            for record in records:
                cursor.execute(
                    statement,
                    (
                        company_basic_resource_key(record),
                        record.get("bizno"),
                        json.dumps(record, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
        conn.commit()
    return len(records)


def write_company_industry_raw_records(
    *,
    database_url: str,
    schema_name: str,
    table_name: str,
    records: list[dict[str, Any]],
    refresh_business_numbers: list[str] | None = None,
) -> int:
    now = datetime.now(UTC)
    with psycopg.connect(database_url) as conn:
        acquire_write_lock(conn)
        _ensure_company_industry_raw_table(conn, schema_name=schema_name, table_name=table_name)
        if refresh_business_numbers:
            with conn.cursor() as cursor:
                cursor.execute(
                    f'DELETE FROM "{schema_name}"."{table_name}" WHERE business_no = ANY(%s)',
                    (refresh_business_numbers,),
                )
        if not records:
            conn.commit()
            return 0
        statement = (
            f'INSERT INTO "{schema_name}"."{table_name}" '
            "(resource_key, business_no, industry_code, raw_payload, fetched_at, updated_at) "
            "VALUES (%s, %s, %s, %s::jsonb, %s, %s) "
            "ON CONFLICT (resource_key) DO UPDATE SET "
            "business_no = EXCLUDED.business_no, industry_code = EXCLUDED.industry_code, "
            "raw_payload = EXCLUDED.raw_payload, fetched_at = EXCLUDED.fetched_at, updated_at = EXCLUDED.updated_at"
        )
        with conn.cursor() as cursor:
            for record in records:
                cursor.execute(
                    statement,
                    (
                        company_industry_resource_key(record),
                        record.get("bizno"),
                        record.get("indstrytyCd"),
                        json.dumps(record, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
        conn.commit()
    return len(records)


def normalize_company_basic_raw(
    *,
    database_url: str,
    raw_schema: str,
    raw_table: str,
    target_schema: str,
    target_table: str,
    business_numbers: list[str],
) -> int:
    if not business_numbers:
        return 0
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        acquire_write_lock(conn)
        _ensure_company_basic_normalized_table(conn, schema_name=target_schema, table_name=target_table)
        rows = _load_raw_rows_by_business_no(conn, schema_name=raw_schema, table_name=raw_table, business_numbers=business_numbers)
        written = _write_company_basic_rows(
            conn,
            schema_name=target_schema,
            table_name=target_table,
            rows=[normalize_company_basic_raw_row(row) for row in rows],
        )
        conn.commit()
    return written


def normalize_company_industry_raw(
    *,
    database_url: str,
    raw_schema: str,
    raw_table: str,
    target_schema: str,
    target_table: str,
    business_numbers: list[str],
) -> int:
    if not business_numbers:
        return 0
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        acquire_write_lock(conn)
        _ensure_company_industry_normalized_table(conn, schema_name=target_schema, table_name=target_table)
        rows = _load_raw_rows_by_business_no(conn, schema_name=raw_schema, table_name=raw_table, business_numbers=business_numbers)
        normalized_rows = [normalize_company_industry_raw_row(row) for row in rows]
        with conn.cursor() as cursor:
            cursor.execute(
                f'DELETE FROM "{target_schema}"."{target_table}" WHERE business_no = ANY(%s)',
                (business_numbers,),
            )
        written = _insert_company_industry_rows(
            conn,
            schema_name=target_schema,
            table_name=target_table,
            rows=normalized_rows,
        )
        conn.commit()
    return written


def _load_raw_rows_by_business_no(
    conn: psycopg.Connection,
    *,
    schema_name: str,
    table_name: str,
    business_numbers: list[str],
) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            f'''
            SELECT id, raw_payload
            FROM "{schema_name}"."{table_name}"
            WHERE business_no = ANY(%s)
            ORDER BY id
            ''',
            (business_numbers,),
        )
        return list(cursor.fetchall())


def _ensure_company_basic_raw_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                resource_key TEXT NOT NULL UNIQUE,
                business_no TEXT NOT NULL,
                raw_payload JSONB NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            '''
        )


def _ensure_company_industry_raw_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                resource_key TEXT NOT NULL UNIQUE,
                business_no TEXT NOT NULL,
                industry_code TEXT,
                raw_payload JSONB NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            '''
        )


def _ensure_company_basic_normalized_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                business_no TEXT NOT NULL UNIQUE,
                company_name TEXT NOT NULL,
                english_company_name TEXT,
                ceo_name TEXT,
                opened_at TIMESTAMPTZ,
                region_code TEXT,
                region_name TEXT,
                zip_code TEXT,
                address TEXT,
                detail_address TEXT,
                phone_no TEXT,
                fax_no TEXT,
                country_name TEXT,
                homepage_url TEXT,
                manufacturing_division_code TEXT,
                manufacturing_division_name TEXT,
                employee_count INTEGER,
                business_division_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
                business_division_names JSONB NOT NULL DEFAULT '[]'::jsonb,
                head_office_division_name TEXT,
                source_registered_at TIMESTAMPTZ,
                source_changed_at TIMESTAMPTZ,
                essential_no_cert_registered TEXT,
                last_checked_at TIMESTAMPTZ NOT NULL,
                raw_id BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            '''
        )


def _ensure_company_industry_normalized_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                business_no TEXT NOT NULL,
                industry_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                registered_at TIMESTAMPTZ,
                valid_until TIMESTAMPTZ,
                system_registered_at TIMESTAMPTZ,
                source_changed_at TIMESTAMPTZ,
                status_name TEXT,
                is_representative BOOLEAN NOT NULL DEFAULT false,
                system_changed_at TIMESTAMPTZ,
                last_checked_at TIMESTAMPTZ NOT NULL,
                raw_id BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (business_no, industry_code)
            )
            '''
        )


def _write_company_basic_rows(
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
        "(business_no, company_name, english_company_name, ceo_name, opened_at, region_code, region_name, "
        "zip_code, address, detail_address, phone_no, fax_no, country_name, homepage_url, "
        "manufacturing_division_code, manufacturing_division_name, employee_count, "
        "business_division_codes, business_division_names, head_office_division_name, "
        "source_registered_at, source_changed_at, essential_no_cert_registered, last_checked_at, raw_id, updated_at) "
        "VALUES (%(business_no)s, %(company_name)s, %(english_company_name)s, %(ceo_name)s, %(opened_at)s, "
        "%(region_code)s, %(region_name)s, %(zip_code)s, %(address)s, %(detail_address)s, %(phone_no)s, "
        "%(fax_no)s, %(country_name)s, %(homepage_url)s, %(manufacturing_division_code)s, "
        "%(manufacturing_division_name)s, %(employee_count)s, %(business_division_codes)s::jsonb, "
        "%(business_division_names)s::jsonb, %(head_office_division_name)s, %(source_registered_at)s, "
        "%(source_changed_at)s, %(essential_no_cert_registered)s, %(last_checked_at)s, %(raw_id)s, %(updated_at)s) "
        "ON CONFLICT (business_no) DO UPDATE SET "
        "company_name = EXCLUDED.company_name, english_company_name = EXCLUDED.english_company_name, "
        "ceo_name = EXCLUDED.ceo_name, opened_at = EXCLUDED.opened_at, region_code = EXCLUDED.region_code, "
        "region_name = EXCLUDED.region_name, zip_code = EXCLUDED.zip_code, address = EXCLUDED.address, "
        "detail_address = EXCLUDED.detail_address, phone_no = EXCLUDED.phone_no, fax_no = EXCLUDED.fax_no, "
        "country_name = EXCLUDED.country_name, homepage_url = EXCLUDED.homepage_url, "
        "manufacturing_division_code = EXCLUDED.manufacturing_division_code, "
        "manufacturing_division_name = EXCLUDED.manufacturing_division_name, employee_count = EXCLUDED.employee_count, "
        "business_division_codes = EXCLUDED.business_division_codes, business_division_names = EXCLUDED.business_division_names, "
        "head_office_division_name = EXCLUDED.head_office_division_name, source_registered_at = EXCLUDED.source_registered_at, "
        "source_changed_at = EXCLUDED.source_changed_at, essential_no_cert_registered = EXCLUDED.essential_no_cert_registered, "
        "last_checked_at = EXCLUDED.last_checked_at, raw_id = EXCLUDED.raw_id, updated_at = EXCLUDED.updated_at"
    )
    with conn.cursor() as cursor:
        cursor.executemany(statement, [_json_ready(row) for row in rows])
    return len(rows)


def _insert_company_industry_rows(
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
        "(business_no, industry_code, industry_name, registered_at, valid_until, system_registered_at, "
        "source_changed_at, status_name, is_representative, system_changed_at, last_checked_at, raw_id, updated_at) "
        "VALUES (%(business_no)s, %(industry_code)s, %(industry_name)s, %(registered_at)s, %(valid_until)s, "
        "%(system_registered_at)s, %(source_changed_at)s, %(status_name)s, %(is_representative)s, "
        "%(system_changed_at)s, %(last_checked_at)s, %(raw_id)s, %(updated_at)s) "
        "ON CONFLICT (business_no, industry_code) DO UPDATE SET "
        "industry_name = EXCLUDED.industry_name, registered_at = EXCLUDED.registered_at, "
        "valid_until = EXCLUDED.valid_until, system_registered_at = EXCLUDED.system_registered_at, "
        "source_changed_at = EXCLUDED.source_changed_at, status_name = EXCLUDED.status_name, "
        "is_representative = EXCLUDED.is_representative, system_changed_at = EXCLUDED.system_changed_at, "
        "last_checked_at = EXCLUDED.last_checked_at, raw_id = EXCLUDED.raw_id, updated_at = EXCLUDED.updated_at"
    )
    with conn.cursor() as cursor:
        cursor.executemany(statement, rows)
    return len(rows)


def _json_ready(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "business_division_codes": json.dumps(row["business_division_codes"], ensure_ascii=False),
        "business_division_names": json.dumps(row["business_division_names"], ensure_ascii=False),
    }
