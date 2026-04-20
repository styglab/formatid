import os

from shared.checkpoints.postgres import PostgresCheckpointStore


def get_checkpoint_database_url() -> str:
    return os.getenv(
        "CHECKPOINT_DATABASE_URL",
        "postgresql://formatid:formatid@postgres:5432/formatid",
    )


def build_checkpoint_store() -> PostgresCheckpointStore:
    return PostgresCheckpointStore(database_url=get_checkpoint_database_url())
