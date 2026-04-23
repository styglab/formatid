from __future__ import annotations

from core.runtime.runtime_db.url import get_checkpoint_database_url


async def fetch_checkpoints(name: str | None = None) -> dict:
    from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore

    database_url = get_checkpoint_database_url(host_default="localhost")
    store = PostgresCheckpointStore(database_url=database_url)
    try:
        if name is None:
            checkpoints = await store.list()
            return {
                "database_url": database_url,
                "checkpoints": checkpoints,
            }
        checkpoint = await store.get(name)
        return {
            "database_url": database_url,
            "checkpoint": checkpoint,
        }
    finally:
        await store.close()
