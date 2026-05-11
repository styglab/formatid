from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg


@contextmanager
def ingest_run_lock(*, database_url: str, lock_name: str) -> Iterator[bool]:
    conn = psycopg.connect(database_url)
    acquired = False
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_name,))
            row = cursor.fetchone()
            acquired = bool(row[0]) if row is not None else False
        yield acquired
    finally:
        if acquired:
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_name,))
        conn.close()


def acquire_write_lock(conn: psycopg.Connection) -> None:
    lock_name = os.getenv("G2B_INGEST_WRITE_LOCK_NAME", "g2b_bid_write")
    with conn.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_name,))


def load_raw_rows(
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
