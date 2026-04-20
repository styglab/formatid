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

    async def set_bid_list_window(self, name: str, value: dict[str, Any]) -> None:
        existing = await self.get(name)
        merged_value = _merge_bid_list_window_checkpoint(
            existing_value=None if existing is None else existing.get("value"),
            incoming_value=value,
        )
        await self.set(name, merged_value)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_connection(self) -> AsyncConnection:
        if self._conn is None:
            self._conn = await AsyncConnection.connect(self._database_url)
        return self._conn


def _merge_bid_list_window_checkpoint(
    *,
    existing_value: Any,
    incoming_value: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(existing_value, dict):
        value = dict(incoming_value)
    else:
        value = dict(existing_value)
        value.update(incoming_value)

    existing_status = existing_value.get("status") if isinstance(existing_value, dict) else None
    incoming_status = incoming_value.get("status")
    if existing_status == "succeeded" and incoming_status in {"running", "failed"}:
        value["status"] = "succeeded"
    elif incoming_status is not None:
        value["status"] = incoming_status

    existing_page = _optional_int(
        existing_value.get("last_completed_page")
        if isinstance(existing_value, dict)
        else None
    )
    incoming_page = _optional_int(incoming_value.get("last_completed_page"))
    if existing_page is not None or incoming_page is not None:
        value["last_completed_page"] = max(
            page for page in (existing_page, incoming_page) if page is not None
        )

    total_count = _optional_int(value.get("total_count"))
    num_of_rows = _optional_int(value.get("num_of_rows"))
    last_completed_page = _optional_int(value.get("last_completed_page"))
    if value.get("status") == "succeeded":
        value["next_page_no"] = None
    elif total_count is not None and num_of_rows and last_completed_page is not None:
        value["next_page_no"] = (
            last_completed_page + 1
            if last_completed_page * num_of_rows < total_count
            else None
        )
    return value


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
