from __future__ import annotations

import os

from services.observability.retention import prune_observability
from shared.postgres_url import get_checkpoint_database_url


def get_observability_retention_days() -> int:
    return int(os.getenv("OBSERVABILITY_RETENTION_DAYS", "30"))


async def prune_observability_data(*, days: int | None = None) -> dict:
    retention_days = get_observability_retention_days() if days is None else days
    result = await prune_observability(
        database_url=get_checkpoint_database_url(host_default="localhost"),
        retention_days=retention_days,
    )
    return result.to_dict()
