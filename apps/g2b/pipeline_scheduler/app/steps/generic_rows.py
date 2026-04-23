from __future__ import annotations

import os
from typing import Any

from psycopg.rows import dict_row

from core.runtime.runtime_db.connection import connect


async def fetch_generic_ingest_rows(*, database_url: str, table_name: str) -> list[dict[str, Any]]:
    conn = await connect(database_url)
    try:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'raw' AND table_name = %s
                )
                """,
                (table_name,),
            )
            exists = (await cursor.fetchone())["exists"]
            if not exists:
                return []
            await cursor.execute(
                f'''
                SELECT id, source_url, resource_key, raw_payload, metadata, fetched_at
                FROM raw."{table_name}"
                ORDER BY id DESC
                LIMIT %s
                ''',
                (int(os.getenv("G2B_INGEST_GENERIC_NORMALIZE_LIMIT", "500")),),
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return [dict(row) for row in rows]
