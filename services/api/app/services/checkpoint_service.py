from services.api.app.config import get_settings
from shared.checkpoints.postgres import PostgresCheckpointStore


async def list_checkpoints() -> list[dict]:
    settings = get_settings()
    store = PostgresCheckpointStore(database_url=settings.checkpoint_database_url)
    try:
        return await store.list()
    finally:
        await store.close()


async def get_checkpoint(name: str) -> dict | None:
    settings = get_settings()
    store = PostgresCheckpointStore(database_url=settings.checkpoint_database_url)
    try:
        return await store.get(name)
    finally:
        await store.close()
