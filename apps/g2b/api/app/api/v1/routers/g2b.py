from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from apps.g2b.api.app.domain.g2b.schemas import GraphNodeRunResponse, GraphRunListResponse, GraphRunResponse
from apps.g2b.api.app.domain.g2b.service import G2bGraphStatusService


router = APIRouter(prefix="/api/v1/g2b", tags=["g2b"])


@router.get("/graph-runs", response_model=GraphRunListResponse)
async def list_graph_runs(
    limit: int = Query(default=100, ge=1, le=500),
    graph_name: str | None = None,
    status: str | None = None,
) -> GraphRunListResponse:
    service = G2bGraphStatusService()
    return GraphRunListResponse(
        graph_runs=await service.list_graph_runs(limit=limit, graph_name=graph_name, status=status)
    )


@router.get("/graph-runs/{run_id}", response_model=GraphRunResponse)
async def get_graph_run(run_id: str) -> GraphRunResponse:
    service = G2bGraphStatusService()
    graph_run = await service.get_graph_run(run_id)
    if graph_run is None:
        raise HTTPException(status_code=404, detail="graph run not found")
    return graph_run


@router.get("/graph-runs/{run_id}/nodes", response_model=list[GraphNodeRunResponse])
async def list_graph_run_nodes(run_id: str) -> list[GraphNodeRunResponse]:
    service = G2bGraphStatusService()
    return await service.list_graph_run_nodes(run_id)
