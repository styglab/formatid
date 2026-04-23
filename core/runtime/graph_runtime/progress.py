from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from core.runtime.graph_runtime.state_store import GraphRunStore
from core.runtime.time import now


ResultT = TypeVar("ResultT")
GraphNodeWork = Callable[[], Awaitable[ResultT]]
OutputSummary = Callable[[ResultT], dict[str, Any]]


async def run_tracked_node(
    state: dict[str, Any],
    *,
    graph_name: str,
    node_name: str,
    progress_total: int,
    work: GraphNodeWork[ResultT],
    input_summary: dict[str, Any] | None = None,
    output_summary: OutputSummary[ResultT] | None = None,
) -> ResultT:
    store = _get_store(state)
    run_id = state.get("graph_run_id")
    started_at = now()
    monotonic_started_at = time.perf_counter()
    if store is not None and isinstance(run_id, str):
        await store.mark_node_started(
            run_id=run_id,
            graph_name=graph_name,
            node_name=node_name,
            progress_total=progress_total,
            input_summary=input_summary,
            started_at=started_at,
        )
    try:
        result = await work()
    except Exception as exc:
        if store is not None and isinstance(run_id, str):
            await store.mark_node_failed(
                run_id=run_id,
                node_name=node_name,
                error={"type": type(exc).__name__, "message": str(exc)},
                finished_at=now(),
                duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
            )
        raise

    if store is not None and isinstance(run_id, str):
        await store.mark_node_succeeded(
            run_id=run_id,
            graph_name=graph_name,
            node_name=node_name,
            progress_total=progress_total,
            output_summary=output_summary(result) if output_summary is not None else {},
            finished_at=now(),
            duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
        )
    return result


def _get_store(state: dict[str, Any]) -> GraphRunStore | None:
    store = state.get("graph_run_store")
    if isinstance(store, GraphRunStore):
        return store
    return None
