from __future__ import annotations

import asyncio
import os
import queue
import threading
from typing import Any

from prefect.deployments import run_deployment

from services.context_platform.adapters.worker.flows.onboarding_pipeline import (
    run_onboarding_pipeline,
)


ONBOARDING_FLOW_NAME = "run-context-platform-ingestion"
ONBOARDING_DEPLOYMENT_NAME = "context-platform-ingestion"


def submit_ingestion_run(run_id: str) -> dict[str, Any]:
    if not run_id:
        return {"status": "skipped", "reason": "missing_run_id"}
    deployment_name = f"{ONBOARDING_FLOW_NAME}/{ONBOARDING_DEPLOYMENT_NAME}"
    try:
        flow_run = run_deployment(name=deployment_name, parameters={"run_id": run_id}, timeout=0)
        if asyncio.iscoroutine(flow_run):
            try:
                flow_run = asyncio.run(flow_run)
            except RuntimeError:
                flow_run = _run_coro_in_thread(flow_run)
        return {
            "status": "submitted",
            "deployment": deployment_name,
            "flow_run_id": str(getattr(flow_run, "id", "")),
        }
    except Exception as exc:  # pragma: no cover - runtime integration fallback
        return {
            "status": "not_submitted",
            "deployment": deployment_name,
            "reason": str(exc),
        }


def main() -> None:
    limit = int(os.getenv("CONTEXT_PLATFORM_PREFECT_LIMIT", "1"))
    run_onboarding_pipeline.serve(
        name=ONBOARDING_DEPLOYMENT_NAME,
        pause_on_shutdown=False,
        limit=limit,
    )


if __name__ == "__main__":
    main()


def _run_coro_in_thread(coro: Any) -> Any:
    result_queue: queue.Queue[Any] = queue.Queue(maxsize=1)

    def _runner() -> None:
        try:
            result_queue.put(asyncio.run(coro))
        except Exception as exc:  # pragma: no cover - runtime integration fallback
            result_queue.put(exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    result = result_queue.get()
    if isinstance(result, Exception):
        raise result
    return result
