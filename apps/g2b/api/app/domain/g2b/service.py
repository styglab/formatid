from __future__ import annotations

from typing import Any

from apps.g2b.api.app.domain.g2b.repository import G2bGraphStatusRepository
from apps.g2b.api.app.domain.g2b.schemas import GraphNodeRunResponse, GraphProgress, GraphRunResponse


class G2bGraphStatusService:
    async def list_graph_runs(
        self,
        *,
        limit: int,
        graph_name: str | None,
        status: str | None,
    ) -> list[GraphRunResponse]:
        repository = G2bGraphStatusRepository()
        try:
            rows = await repository.list_graph_runs(limit=limit, graph_name=graph_name, status=status)
            return [_to_graph_run_response(row) for row in rows]
        finally:
            await repository.close()

    async def get_graph_run(self, run_id: str) -> GraphRunResponse | None:
        repository = G2bGraphStatusRepository()
        try:
            row = await repository.get_graph_run(run_id)
            return None if row is None else _to_graph_run_response(row)
        finally:
            await repository.close()

    async def list_graph_run_nodes(self, run_id: str) -> list[GraphNodeRunResponse]:
        repository = G2bGraphStatusRepository()
        try:
            rows = await repository.list_graph_run_nodes(run_id)
            return [GraphNodeRunResponse.model_validate(row) for row in rows]
        finally:
            await repository.close()


def _to_graph_run_response(row: dict[str, Any]) -> GraphRunResponse:
    return GraphRunResponse(
        run_id=row["run_id"],
        service_name=row["service_name"],
        graph_name=row["graph_name"],
        trigger_type=row["trigger_type"],
        status=row["status"],
        current_step=row.get("current_node"),
        completed_steps=row.get("completed_nodes") or [],
        progress=GraphProgress(
            current=row.get("progress_current") or 0,
            total=row.get("progress_total"),
            percent=row.get("progress_percent"),
        ),
        params=row.get("params") or {},
        result=row.get("result"),
        error=row.get("error"),
        started_at=row.get("started_at"),
        updated_at=row["updated_at"],
        finished_at=row.get("finished_at"),
        created_at=row["created_at"],
    )
