from __future__ import annotations

from pathlib import Path

from psycopg import AsyncConnection


_SCHEMA_READY_DATABASE_URLS: set[str] = set()
_SCHEMA_LOCK_KEY = 4_803_001
_RAW_SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "raw_schema.sql"


async def ensure_g2b_ingest_raw_schema(conn: AsyncConnection, *, database_url: str) -> None:
    if database_url in _SCHEMA_READY_DATABASE_URLS:
        return

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK_KEY,))
            await cursor.execute(_RAW_SCHEMA_SQL.read_text(encoding="utf-8"))
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise

    _SCHEMA_READY_DATABASE_URLS.add(database_url)
