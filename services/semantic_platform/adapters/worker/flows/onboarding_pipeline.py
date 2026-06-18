from __future__ import annotations

from prefect import flow, task

from services.semantic_platform.adapters.worker.flows.stages import (
    run_stage_asset_discovery,
    run_stage_binding_drafting,
    run_stage_semantic_model_drafting,
    run_stage_source_review,
    run_stage_structure_review,
)
from services.semantic_platform.internal.storage import SemanticLayerRepository


@task
def execute_source_review(run_id: str) -> dict[str, str]:
    result = run_stage_source_review(SemanticLayerRepository(), run_id)
    return {
        "stage": result.stage,
        "current_stage": result.current_stage,
        "stage_status": result.stage_status,
        "status": result.run_status,
    }


@task
def execute_asset_discovery(run_id: str) -> dict[str, str]:
    result = run_stage_asset_discovery(SemanticLayerRepository(), run_id)
    return {
        "stage": result.stage,
        "current_stage": result.current_stage,
        "stage_status": result.stage_status,
        "status": result.run_status,
    }


@task
def execute_structure_review(run_id: str) -> dict[str, str]:
    result = run_stage_structure_review(SemanticLayerRepository(), run_id)
    return {
        "stage": result.stage,
        "current_stage": result.current_stage,
        "stage_status": result.stage_status,
        "status": result.run_status,
    }


@task
def execute_semantic_model_drafting(run_id: str) -> dict[str, str]:
    result = run_stage_semantic_model_drafting(SemanticLayerRepository(), run_id)
    return {
        "stage": result.stage,
        "current_stage": result.current_stage,
        "stage_status": result.stage_status,
        "status": result.run_status,
    }


@task
def execute_binding_drafting(run_id: str) -> dict[str, str]:
    result = run_stage_binding_drafting(SemanticLayerRepository(), run_id)
    return {
        "stage": result.stage,
        "current_stage": result.current_stage,
        "stage_status": result.stage_status,
        "status": result.run_status,
    }


@flow(name="run-onboarding-pipeline")
def run_onboarding_pipeline(run_id: str) -> dict[str, str]:
    repository = SemanticLayerRepository()
    run = repository.get_onboarding_run(run_id)
    if run is None:
        return {"run_id": run_id, "status": "not_found"}
    current_stage = str(run.get("current_stage") or "source_review")

    if current_stage in {"source_review", "asset_discovery", "structure_review", "semantic_model_drafting"}:
        execute_source_review(run_id)
        execute_asset_discovery(run_id)
        execute_structure_review(run_id)
        semantic_model_result = execute_semantic_model_drafting(run_id)
        status = str(semantic_model_result.get("status") or "")
        if status == "paused":
            return {"run_id": run_id, "status": "waiting_manual_llm"}
        return {"run_id": run_id, "status": "semantic_model_drafts_ready"}

    if current_stage == "binding_drafting":
        binding_result = execute_binding_drafting(run_id)
        status = str(binding_result.get("status") or "")
        if status == "paused":
            return {"run_id": run_id, "status": "waiting_manual_llm"}
        return {"run_id": run_id, "status": "binding_drafts_ready"}

    return {"run_id": run_id, "status": "noop"}
