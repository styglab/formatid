from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from core.runtime.graph_runtime.queue import TriggeredGraphQueue, TriggeredGraphRequest

if TYPE_CHECKING:
    from core.runtime.graph_runtime.state_store import GraphRunStore


async def enqueue_graph_resumes_for_task(
    *,
    redis_url: str,
    graph_run_store: GraphRunStore,
    task_id: str,
    resume_value: object,
    requested_by: str | None = None,
) -> int:
    runs = await graph_run_store.list_suspended_runs_for_task(task_id=task_id)
    grouped_runs: dict[str, list[dict[str, object]]] = defaultdict(list)
    for run in runs:
        params = dict(run.get("params") or {})
        runtime_params = dict(params.get("__runtime") or {})
        resume_queue = runtime_params.get("resume_queue")
        if isinstance(resume_queue, str) and resume_queue:
            grouped_runs[resume_queue].append(run)

    total = 0
    for resume_queue, queued_runs in grouped_runs.items():
        queue = TriggeredGraphQueue(redis_url=redis_url, queue_name=resume_queue)
        try:
            for run in queued_runs:
                params = dict(run.get("params") or {})
                runtime_params = dict(params.get("__runtime") or {})
                identity = runtime_params.get("identity")
                if not isinstance(identity, dict):
                    identity = {}
                await queue.enqueue(
                    TriggeredGraphRequest(
                        graph_name=str(run["graph_name"]),
                        run_id=str(run["run_id"]),
                        request_kind="resume",
                        resume_value=resume_value,
                        requested_by=requested_by,
                        request_id=_string_or_none(identity.get("request_id")),
                        correlation_id=_string_or_none(identity.get("correlation_id")),
                        resource_key=_string_or_none(identity.get("resource_key")),
                        session_id=_string_or_none(identity.get("session_id")),
                    )
                )
                total += 1
        finally:
            await queue.close()
    return total


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
