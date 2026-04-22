import json
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row


class PostgresCheckpointStore:
    def __init__(self, *, database_url: str) -> None:
        self._database_url = database_url
        self._conn: AsyncConnection | None = None

    async def get(self, name: str) -> dict[str, Any] | None:
        conn = await self._get_connection()
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT name, value, created_at, updated_at
                FROM checkpoints
                WHERE name = %s
                """,
                (name,),
            )
            row = await cursor.fetchone()
        await conn.commit()
        if row is None:
            return None
        return {
            "name": row["name"],
            "value": row["value"],
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
        }

    async def list(self) -> list[dict[str, Any]]:
        conn = await self._get_connection()
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT name, value, created_at, updated_at
                FROM checkpoints
                ORDER BY name ASC
                """
            )
            rows = await cursor.fetchall()
        await conn.commit()
        return [
            {
                "name": row["name"],
                "value": row["value"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
            }
            for row in rows
        ]

    async def set(self, name: str, value: dict[str, Any]) -> None:
        conn = await self._get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO checkpoints (name, value)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (name)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                (name, json.dumps(value)),
            )
        await conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> AsyncConnection:
        if self._conn is None:
            self._conn = await AsyncConnection.connect(self._database_url)
        return self._conn
