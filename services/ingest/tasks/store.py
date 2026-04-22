from __future__ import annotations

import json
from typing import Any

from psycopg import AsyncConnection

from shared.postgres_url import get_database_url


def get_value_at_path(payload: Any, path: str | None) -> Any:
    if not path:
        return payload
    current = payload
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            current = current[index] if index < len(current) else None
        else:
            return None
    return current


def ensure_record_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


class GenericPostgresIngestStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: AsyncConnection | None = None

    async def write_records(
        self,
        *,
        schema_name: str,
        table_name: str,
        records: list[Any],
        source_url: str | None,
        resource_keys: list[str | None],
        metadata: dict[str, Any],
        mode: str,
        create_table: bool,
    ) -> int:
        if not records:
            return 0
        conn = await self._get_connection()
        if create_table:
            await self._ensure_table(conn, schema_name=schema_name, table_name=table_name, mode=mode)

        statement = self._build_insert_statement(
            schema_name=schema_name,
            table_name=table_name,
            mode=mode,
        )
        async with conn.cursor() as cursor:
            for index, record in enumerate(records):
                resource_key = resource_keys[index] if index < len(resource_keys) else None
                await cursor.execute(
                    statement,
                    (
                        source_url,
                        resource_key,
                        json.dumps(record),
                        json.dumps(metadata),
                    ),
                )
        await conn.commit()
        return len(records)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> AsyncConnection:
        if self._conn is None or self._conn.closed:
            self._conn = await AsyncConnection.connect(self._database_url)
        return self._conn

    async def _ensure_table(self, conn: AsyncConnection, *, schema_name: str, table_name: str, mode: str) -> None:
        async with conn.cursor() as cursor:
            await cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            await cursor.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                    id BIGSERIAL PRIMARY KEY,
                    source_url TEXT,
                    resource_key TEXT,
                    raw_payload JSONB NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                '''
            )
            if mode == "upsert":
                await cursor.execute(
                    f'''
                    CREATE UNIQUE INDEX IF NOT EXISTS "{table_name}_resource_key_uidx"
                    ON "{schema_name}"."{table_name}" (resource_key)
                    WHERE resource_key IS NOT NULL
                    '''
                )
        await conn.commit()

    def _build_insert_statement(self, *, schema_name: str, table_name: str, mode: str) -> str:
        base = (
            f'INSERT INTO "{schema_name}"."{table_name}" '
            "(source_url, resource_key, raw_payload, metadata) "
            "VALUES (%s, %s, %s::jsonb, %s::jsonb)"
        )
        if mode == "upsert":
            return (
                base
                + " ON CONFLICT (resource_key) WHERE resource_key IS NOT NULL "
                "DO UPDATE SET "
                "source_url = EXCLUDED.source_url, "
                "raw_payload = EXCLUDED.raw_payload, "
                "metadata = EXCLUDED.metadata, "
                "updated_at = NOW()"
            )
        return base
