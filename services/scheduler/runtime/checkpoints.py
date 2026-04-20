from shared.checkpoints.postgres import PostgresCheckpointStore
from shared.postgres_url import get_checkpoint_database_url as build_checkpoint_database_url


def get_checkpoint_database_url() -> str:
    return build_checkpoint_database_url(host_default="postgres")


def build_checkpoint_store() -> PostgresCheckpointStore:
    return PostgresCheckpointStore(database_url=get_checkpoint_database_url())
