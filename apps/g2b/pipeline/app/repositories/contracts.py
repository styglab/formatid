from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from apps.g2b.pipeline.app.repositories.common import acquire_write_lock, load_raw_rows
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.contracts import (
    CONTRACT_URLS,
    contract_resource_key,
    normalize_contract_raw_row,
    parse_contract_company_list,
    parse_contract_demand_org_list,
)


def write_contract_raw_records(
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
            "(category, source_url, resource_key, unified_contract_no, decision_contract_no, "
            "contract_ref_no, registered_at, raw_payload, metadata, fetched_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s) "
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
                        CONTRACT_URLS[category],
                        contract_resource_key(category, record),
                        record.get("untyCntrctNo"),
                        record.get("dcsnCntrctNo"),
                        record.get("cntrctRefNo"),
                        record.get("rgstDt"),
                        json.dumps(record, ensure_ascii=False),
                        json.dumps(
                            {
                                "window": {"begin": window.begin, "end": window.end},
                                "ingest_name": "g2b-contract-ingest",
                            },
                            ensure_ascii=False,
                        ),
                        now,
                        now,
                    ),
                )
        conn.commit()
    return len(records)


def normalize_raw_contracts(
    *,
    database_url: str,
    raw_schema: str,
    raw_table: str,
    target_schema: str,
    target_table: str,
    company_table: str,
    demand_org_table: str,
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
        _ensure_company_table(conn, schema_name=target_schema, table_name=company_table)
        _ensure_demand_org_table(conn, schema_name=target_schema, table_name=demand_org_table)
        rows = load_raw_rows(
            conn,
            schema_name=raw_schema,
            table_name=raw_table,
            window_begin=window_begin,
            window_end=window_end,
        )
        normalized_rows = [normalize_contract_raw_row(row) for row in rows]
        written = _write_normalized_rows(
            conn,
            schema_name=target_schema,
            table_name=target_table,
            rows=normalized_rows,
        )
        company_count = _replace_contract_companies(
            conn,
            schema_name=target_schema,
            table_name=company_table,
            rows=rows,
        )
        demand_org_count = _replace_contract_demand_orgs(
            conn,
            schema_name=target_schema,
            table_name=demand_org_table,
            rows=rows,
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
        "relations": {
            "companies": {"schema": target_schema, "table": company_table, "count": company_count},
            "demand_organizations": {"schema": target_schema, "table": demand_org_table, "count": demand_org_count},
        },
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
                unified_contract_no TEXT,
                decision_contract_no TEXT,
                contract_ref_no TEXT,
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
                unified_contract_no TEXT,
                decision_contract_no TEXT,
                contract_ref_no TEXT,
                contract_name TEXT NOT NULL,
                business_div_name TEXT,
                is_common_contract BOOLEAN,
                long_term_division_name TEXT,
                contract_concluded_date TIMESTAMPTZ,
                contract_period TEXT,
                base_law_name TEXT,
                total_contract_amount NUMERIC,
                current_contract_amount NUMERIC,
                guarantee_rate NUMERIC,
                contract_info_url TEXT,
                payment_division_name TEXT,
                request_no TEXT,
                bid_notice_no TEXT,
                contract_org_code TEXT,
                contract_org_name TEXT,
                contract_org_jurisdiction_name TEXT,
                contract_org_department_name TEXT,
                contract_org_officer_name TEXT,
                contract_org_officer_phone_no TEXT,
                contract_org_officer_fax_no TEXT,
                detail_url TEXT,
                creditor_name TEXT,
                base_details TEXT,
                contract_method TEXT,
                registered_at TIMESTAMPTZ,
                changed_at TIMESTAMPTZ,
                delay_compensation_rate NUMERIC,
                public_procurement_classification_no TEXT,
                public_procurement_classification_name TEXT,
                public_procurement_mid_classification_name TEXT,
                public_procurement_large_classification_name TEXT,
                contract_date TIMESTAMPTZ,
                is_info_business BOOLEAN,
                source_url TEXT,
                raw_id BIGINT REFERENCES "{raw_schema}"."{raw_table}"(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            '''
        )
        for suffix, expression in {
            "category_registered": "(category, registered_at DESC)",
            "contract_date": "(contract_date DESC)",
            "contract_org": "(contract_org_name)",
            "bid_notice": "(bid_notice_no)",
            "amount": "(current_contract_amount)",
        }.items():
            cursor.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "{table_name}_{suffix}_idx"
                ON "{schema_name}"."{table_name}" {expression}
                '''
            )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_name_trgm_idx"
            ON "{schema_name}"."{table_name}" USING gin (contract_name public.gin_trgm_ops)
            '''
        )


def _ensure_company_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                contract_resource_key TEXT NOT NULL,
                sequence_no TEXT NOT NULL,
                role_name TEXT,
                contract_type_name TEXT,
                company_name TEXT,
                display_company_name TEXT,
                ceo_name TEXT,
                country_name TEXT,
                share_rate NUMERIC,
                business_no TEXT,
                raw_item JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (contract_resource_key, sequence_no)
            )
            '''
        )
        for suffix, expression in {
            "business_no": "(business_no)",
            "company_name": "(company_name)",
        }.items():
            cursor.execute(
                f'''
                CREATE INDEX IF NOT EXISTS "{table_name}_{suffix}_idx"
                ON "{schema_name}"."{table_name}" {expression}
                '''
            )


def _ensure_demand_org_table(conn: psycopg.Connection, *, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                contract_resource_key TEXT NOT NULL,
                sequence_no TEXT NOT NULL,
                organization_code TEXT,
                organization_name TEXT,
                jurisdiction_name TEXT,
                department_name TEXT,
                officer_name TEXT,
                raw_item JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (contract_resource_key, sequence_no)
            )
            '''
        )
        cursor.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "{table_name}_organization_idx"
            ON "{schema_name}"."{table_name}" (organization_code, organization_name)
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
    columns = (
        "resource_key, category, category_label, unified_contract_no, decision_contract_no, contract_ref_no, "
        "contract_name, business_div_name, is_common_contract, long_term_division_name, contract_concluded_date, "
        "contract_period, base_law_name, total_contract_amount, current_contract_amount, guarantee_rate, "
        "contract_info_url, payment_division_name, request_no, bid_notice_no, contract_org_code, contract_org_name, "
        "contract_org_jurisdiction_name, contract_org_department_name, contract_org_officer_name, "
        "contract_org_officer_phone_no, contract_org_officer_fax_no, detail_url, creditor_name, base_details, "
        "contract_method, registered_at, changed_at, delay_compensation_rate, public_procurement_classification_no, "
        "public_procurement_classification_name, public_procurement_mid_classification_name, "
        "public_procurement_large_classification_name, contract_date, is_info_business, source_url, raw_id, updated_at"
    )
    placeholders = ", ".join(f"%({column.strip()})s" for column in columns.split(","))
    update_columns = [column.strip() for column in columns.split(",") if column.strip() not in {"resource_key"}]
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    statement = (
        f'INSERT INTO "{schema_name}"."{table_name}" ({columns}) '
        f"VALUES ({placeholders}) "
        "ON CONFLICT (resource_key) DO UPDATE SET "
        f"{updates}"
    )
    with conn.cursor() as cursor:
        cursor.executemany(statement, rows)
    return len(rows)


def _replace_contract_companies(
    conn: psycopg.Connection,
    *,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> int:
    resource_keys = [row["resource_key"] for row in rows]
    if not resource_keys:
        return 0
    with conn.cursor() as cursor:
        cursor.execute(
            f'DELETE FROM "{schema_name}"."{table_name}" WHERE contract_resource_key = ANY(%s)',
            (resource_keys,),
        )
        relation_rows = []
        for row in rows:
            for company in parse_contract_company_list(row["raw_payload"].get("corpList")):
                relation_rows.append({**company, "contract_resource_key": row["resource_key"]})
        if not relation_rows:
            return 0
        statement = (
            f'INSERT INTO "{schema_name}"."{table_name}" '
            "(contract_resource_key, sequence_no, role_name, contract_type_name, company_name, display_company_name, "
            "ceo_name, country_name, share_rate, business_no, raw_item, updated_at) "
            "VALUES (%(contract_resource_key)s, %(sequence_no)s, %(role_name)s, %(contract_type_name)s, "
            "%(company_name)s, %(display_company_name)s, %(ceo_name)s, %(country_name)s, %(share_rate)s, "
            "%(business_no)s, %(raw_item)s::jsonb, %(updated_at)s) "
            "ON CONFLICT (contract_resource_key, sequence_no) DO UPDATE SET "
            "role_name = EXCLUDED.role_name, contract_type_name = EXCLUDED.contract_type_name, "
            "company_name = EXCLUDED.company_name, display_company_name = EXCLUDED.display_company_name, "
            "ceo_name = EXCLUDED.ceo_name, country_name = EXCLUDED.country_name, share_rate = EXCLUDED.share_rate, "
            "business_no = EXCLUDED.business_no, raw_item = EXCLUDED.raw_item, updated_at = EXCLUDED.updated_at"
        )
        now = datetime.now(UTC)
        cursor.executemany(statement, [_relation_json_ready(row, now) for row in relation_rows])
    return len(relation_rows)


def _replace_contract_demand_orgs(
    conn: psycopg.Connection,
    *,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> int:
    resource_keys = [row["resource_key"] for row in rows]
    if not resource_keys:
        return 0
    with conn.cursor() as cursor:
        cursor.execute(
            f'DELETE FROM "{schema_name}"."{table_name}" WHERE contract_resource_key = ANY(%s)',
            (resource_keys,),
        )
        relation_rows = []
        for row in rows:
            for organization in parse_contract_demand_org_list(row["raw_payload"].get("dminsttList")):
                relation_rows.append({**organization, "contract_resource_key": row["resource_key"]})
        if not relation_rows:
            return 0
        statement = (
            f'INSERT INTO "{schema_name}"."{table_name}" '
            "(contract_resource_key, sequence_no, organization_code, organization_name, jurisdiction_name, "
            "department_name, officer_name, raw_item, updated_at) "
            "VALUES (%(contract_resource_key)s, %(sequence_no)s, %(organization_code)s, %(organization_name)s, "
            "%(jurisdiction_name)s, %(department_name)s, %(officer_name)s, %(raw_item)s::jsonb, %(updated_at)s) "
            "ON CONFLICT (contract_resource_key, sequence_no) DO UPDATE SET "
            "organization_code = EXCLUDED.organization_code, organization_name = EXCLUDED.organization_name, "
            "jurisdiction_name = EXCLUDED.jurisdiction_name, department_name = EXCLUDED.department_name, "
            "officer_name = EXCLUDED.officer_name, raw_item = EXCLUDED.raw_item, updated_at = EXCLUDED.updated_at"
        )
        now = datetime.now(UTC)
        cursor.executemany(statement, [_relation_json_ready(row, now) for row in relation_rows])
    return len(relation_rows)


def _relation_json_ready(row: dict[str, Any], updated_at: datetime) -> dict[str, Any]:
    return {
        **row,
        "raw_item": json.dumps(row["raw_parts"], ensure_ascii=False),
        "updated_at": updated_at,
    }
