from __future__ import annotations

from typing import Any


async def connect(database_url: str) -> Any:
    from psycopg import AsyncConnection

    return await AsyncConnection.connect(database_url)
