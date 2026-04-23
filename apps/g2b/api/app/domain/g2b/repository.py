from __future__ import annotations

from typing import Any

from apps.g2b.api.app.core.config import get_settings
from core.runtime.graph_runtime import GraphRunStore


G2B_PIPELINE_SERVICES = ("g2b-pipeline-scheduler", "g2b-pipeline-worker")


class G2bGraphStatusRepository:
    def __init__(self) -> None:
        self._store = GraphRunStore(database_url=get_settings().checkpoint_database_url)

    async def list_graph_runs(
        self,
        *,
        limit: int,
        graph_name: str | None,
        status: str | None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for service_name in G2B_PIPELINE_SERVICES:
            rows.extend(
                await self._store.list_runs(
                    limit=limit,
                    graph_name=graph_name,
                    status=status,
                    service_name=service_name,
                )
            )
        return sorted(rows, key=lambda row: row["updated_at"], reverse=True)[:limit]

    async def get_graph_run(self, run_id: str) -> dict[str, Any] | None:
        row = await self._store.get_run(run_id)
        if row is None or row["service_name"] not in G2B_PIPELINE_SERVICES:
            return None
        return row

    async def list_graph_run_nodes(self, run_id: str) -> list[dict[str, Any]]:
        graph_run = await self.get_graph_run(run_id)
        if graph_run is None:
            return []
        return await self._store.list_nodes(run_id)

    async def close(self) -> None:
        await self._store.close()
