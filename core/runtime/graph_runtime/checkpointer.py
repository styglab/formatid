from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from core.runtime.runtime_db.connection import connect


class AsyncGraphCheckpointer:
    def __init__(self, *, database_url: str, schema_name: str = "langgraph_checkpoints") -> None:
        self._database_url = database_url
        self._schema_name = schema_name
        self._checkpointer_cm: Any | None = None
        self._checkpointer: Any | None = None

    async def start(self) -> None:
        if self._checkpointer is not None:
            return
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

        await self._ensure_schema()
        self._checkpointer_cm = AsyncPostgresSaver.from_conn_string(
            _with_search_path(self._database_url, self._schema_name),
            pipeline=False,
        )
        self._checkpointer = await self._checkpointer_cm.__aenter__()
        await self._checkpointer.setup()

    def get(self) -> Any:
        if self._checkpointer is None:
            raise RuntimeError("AsyncGraphCheckpointer.start() must be called before use")
        return self._checkpointer

    async def close(self) -> None:
        if self._checkpointer_cm is not None:
            await self._checkpointer_cm.__aexit__(None, None, None)
        self._checkpointer_cm = None
        self._checkpointer = None

    async def _ensure_schema(self) -> None:
        conn = await connect(self._database_url)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema_name}"')
            await conn.commit()
        finally:
            await conn.close()


def _with_search_path(database_url: str, schema_name: str) -> str:
    parts = urlsplit(database_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema_name},public"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
