from __future__ import annotations

import asyncio
from time import perf_counter
from typing import Any

from apps.data_score.domain.flows.evaluation import run_evaluation
from apps.data_score.domain.repositories.evaluation_runs import DataScoreRunRepository


async def execute_and_store_evaluation(
    *,
    repository: DataScoreRunRepository,
    dataset_name: str,
    csv_text: str,
    business_context: str | None,
    llm_mode: str,
    manual_judge_result: dict[str, Any] | None,
) -> dict[str, Any]:
    created = await repository.enqueue_run(
        dataset_name=dataset_name,
        llm_mode=llm_mode,
        business_context=business_context,
        request_payload={
            "dataset_name": dataset_name,
            "csv_text": csv_text,
            "business_context": business_context,
            "llm_mode": llm_mode,
            "manual_judge_result": manual_judge_result,
        },
    )
    return created


async def process_run(repository: DataScoreRunRepository, run: dict[str, Any]) -> dict[str, Any]:
    started = perf_counter()
    run_id = str(run["run_id"])
    payload = run.get("request_payload") or {}
    dataset_name = str(payload.get("dataset_name") or run.get("dataset_name") or "")
    csv_text = str(payload.get("csv_text") or "")
    business_context = payload.get("business_context")
    llm_mode = str(payload.get("llm_mode") or run.get("llm_mode") or "disabled")
    manual_judge_result = payload.get("manual_judge_result")
    try:
        report = await asyncio.to_thread(
            run_evaluation,
            dataset_name=dataset_name,
            csv_text=csv_text,
            business_context=business_context,
            llm_mode=llm_mode,
            manual_judge_result=manual_judge_result,
        )
    except Exception as exc:
        duration_ms = round((perf_counter() - started) * 1000.0, 2)
        await repository.fail_run(
            run_id=run_id,
            error={
                "type": exc.__class__.__name__,
                "message": str(exc),
            },
            duration_ms=duration_ms,
        )
        failed = await repository.get_run(run_id)
        if failed is None:
            raise
        return failed

    duration_ms = round((perf_counter() - started) * 1000.0, 2)
    await repository.complete_run(
        run_id=run_id,
        report=report,
        duration_ms=duration_ms,
    )
    completed = await repository.get_run(run_id)
    if completed is None:
        raise RuntimeError("evaluation run was not persisted")
    return completed


async def process_next_pending_run(repository: DataScoreRunRepository) -> dict[str, Any] | None:
    run = await repository.claim_next_pending_run()
    if run is None:
        return None
    return await process_run(repository, run)
