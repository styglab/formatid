from __future__ import annotations

from typing import Any

from apps.g2b_summary.service.graph import SummaryDagSteps, SummaryGraphState, run_summary_dag
from apps.g2b_summary.service.payloads import build_llm_generate_payload, build_text_extract_payload


def build_summary_dag_steps() -> SummaryDagSteps:
    return SummaryDagSteps(
        extract_text=plan_extract_text_step,
        serve_llm=plan_serve_llm_step,
        load_result=plan_load_result_step,
    )


async def build_summary_plan(
    *,
    job_id: str,
    bucket: str,
    object_key: str,
    callback_url: str | None,
) -> dict[str, Any]:
    return await run_summary_dag(
        job_id=job_id,
        bucket=bucket,
        object_key=object_key,
        callback_url=callback_url,
        steps=build_summary_dag_steps(),
    )


async def plan_extract_text_step(state: SummaryGraphState) -> dict[str, Any]:
    return {
        "extract_payload": build_text_extract_payload(
            job_id=state["job_id"],
            bucket=state["bucket"],
            object_key=state["object_key"],
            callback_url=state.get("callback_url"),
        )
    }


async def plan_serve_llm_step(state: SummaryGraphState) -> dict[str, Any]:
    return {
        "llm_payload": build_llm_generate_payload(
            job_id=state["job_id"],
            callback_url=state.get("callback_url"),
        )
    }


async def plan_load_result_step(state: SummaryGraphState) -> dict[str, Any]:
    return {
        "result": {
            "job_id": state["job_id"],
            "status_source": "summary.results",
        }
    }
