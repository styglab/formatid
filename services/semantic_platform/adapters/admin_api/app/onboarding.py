from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.semantic_platform.adapters.worker.deployments import submit_onboarding_run
from services.semantic_platform.internal.onboarding import (
    advance_run_after_task_completion,
    build_onboarding_run_detail,
    build_onboarding_runs,
    build_proposal_bundle,
    build_task_draft,
    build_workspace_progress_summary,
    cancel_onboarding_run as cancel_onboarding_run_service,
    pause_onboarding_run as pause_onboarding_run_service,
    resume_onboarding_run as resume_onboarding_run_service,
)
from services.semantic_platform.internal.storage import SemanticLayerRepository


router = APIRouter()


def _auto_approve_stage_proposals(
    repository: SemanticLayerRepository,
    task: dict[str, Any],
    reviewer: str,
) -> list[str]:
    task_type = str(task.get("task_type") or "")
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
    if task_type == "approve_semantic_model":
        proposal_ids = payload.get("semantic_model_proposal_ids")
    elif task_type == "approve_source_binding":
        proposal_ids = payload.get("binding_proposal_ids")
    else:
        return []

    normalized_ids = [str(item) for item in proposal_ids or [] if str(item)]
    if not normalized_ids:
        return []

    proposals = {str(item.get("id") or ""): item for item in repository.list_proposals()}
    approved_ids: list[str] = []
    for proposal_id in normalized_ids:
        proposal = proposals.get(proposal_id)
        if proposal is None:
            continue
        status = str(proposal.get("status") or "")
        if status == "approved":
            approved_ids.append(proposal_id)
            continue
        if status != "pending_review":
            continue
        repository.review_proposal(proposal_id, "approved", reviewer=reviewer)
        approved_ids.append(proposal_id)
    return approved_ids


def _validate_mapping_task_completion(repository: SemanticLayerRepository, task: dict[str, Any]) -> str | None:
    if str(task.get("task_type") or "") != "approve_source_binding":
        return None
    draft_payload = task.get("draft_payload") if isinstance(task.get("draft_payload"), dict) else {}
    suggestions = draft_payload.get("suggestions") if isinstance(draft_payload.get("suggestions"), list) else []
    if not suggestions and int(draft_payload.get("unresolved_count") or 0) == 0:
        return None

    proposals = {str(item.get("id") or ""): item for item in repository.list_proposals()}
    blocking_reasons: list[str] = []
    for suggestion in suggestions:
        if not isinstance(suggestion, dict):
            continue
        field_path = str(suggestion.get("field_path") or suggestion.get("raw_name") or "field")
        if str(suggestion.get("status") or "") != "matched":
            blocking_reasons.append(f"{field_path}: unresolved semantic mapping candidate")
            continue
        dependency_status = str(suggestion.get("dependency_status") or "blocked")
        resolution_basis = str(suggestion.get("resolution_basis") or "missing")
        depends_on = [str(item) for item in suggestion.get("depends_on_proposal_ids") or [] if str(item)]
        if dependency_status == "needs_rebase":
            blocking_reasons.append(f"{field_path}: dependency needs rebase")
            continue
        if dependency_status == "blocked":
            if resolution_basis == "missing" and not depends_on:
                blocking_reasons.append(f"{field_path}: missing approved semantic type or canonical attribute")
                continue
            unresolved_dependencies = [
                proposal_id
                for proposal_id in depends_on
                if str((proposals.get(proposal_id) or {}).get("status") or "") != "approved"
            ]
            if unresolved_dependencies:
                blocking_reasons.append(
                    f"{field_path}: waiting for proposal approval ({', '.join(unresolved_dependencies)})"
                )
                continue
        if resolution_basis == "missing":
            blocking_reasons.append(f"{field_path}: missing approved semantic type or canonical attribute")
    if not blocking_reasons:
        return None
    return "; ".join(blocking_reasons[:5])


class OnboardingTaskDraftPayload(BaseModel):
    reviewer: str = "dashboard"


class OnboardingTaskCompletePayload(BaseModel):
    reviewer: str = "dashboard"


class OnboardingRunResumePayload(BaseModel):
    reviewer: str = "dashboard"


class OnboardingManualLlmResponsePayload(BaseModel):
    stage: str
    manual_llm_response: dict[str, Any]
    reviewer: str = "dashboard"


@router.get("/api/onboarding-runs")
def list_onboarding_runs() -> list[dict[str, Any]]:
    repository = SemanticLayerRepository()
    records = repository.list_onboarding_runs()
    sources = repository.list_execution_sources()
    operations = repository.list_execution_operations()
    fields = repository.list_operation_fields()
    mappings = repository.list_field_mappings()
    proposals = repository.list_proposals()
    derived = build_onboarding_runs(sources, operations, fields, mappings, proposals)
    real_source_ids = {str(item.get("source_id") or "") for item in records}
    merged = records + [item for item in derived if str(item.get("source_id") or "") not in real_source_ids]
    summarized = []
    for item in merged:
        tasks = repository.list_work_queue_tasks(run_id=str(item.get("id") or ""))
        summarized.append({**item, **build_workspace_progress_summary(item, tasks)})
    return sorted(summarized, key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)


@router.get("/api/onboarding-runs/{run_id}")
def get_onboarding_run(run_id: str) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    records = repository.list_onboarding_runs()
    selected = next((item for item in records if item.get("id") == run_id), None)
    if selected is not None:
        try:
            return build_onboarding_run_detail(repository, selected)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    sources = repository.list_execution_sources()
    operations = repository.list_execution_operations()
    fields = repository.list_operation_fields()
    mappings = repository.list_field_mappings()
    proposals = repository.list_proposals()
    derived = build_onboarding_runs(sources, operations, fields, mappings, proposals)
    selected = next((item for item in derived if item.get("id") == run_id), None)
    if selected is None:
        raise HTTPException(status_code=404, detail="onboarding run not found")
    try:
        return build_onboarding_run_detail(repository, selected)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/execution-sources/{source_id}/start-workspace")
def start_workspace(source_id: str, payload: OnboardingRunResumePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    source = repository.get_execution_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="execution source not found")
    upload_metadata: dict[str, Any] = {}
    config = source.get("config") or {}
    if isinstance(config, dict):
        upload = config.get("upload")
        if isinstance(upload, dict):
            upload_metadata = upload
    created = repository.create_onboarding_run_for_source(
        source=source,
        upload_metadata=upload_metadata,
        trigger_type="source_workspace_start",
        created_by=payload.reviewer,
    )
    run = created["onboarding_run"]
    trigger = submit_onboarding_run(str(run.get("id") or ""))
    return {"run": repository.get_onboarding_run(str(run.get("id") or "")) or run, "trigger": trigger}


@router.post("/api/onboarding-runs/{run_id}/resume")
def resume_onboarding_run(run_id: str, payload: OnboardingRunResumePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    run = repository.get_onboarding_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="onboarding run not found")
    updated = resume_onboarding_run_service(repository, run_id, payload.reviewer)
    trigger = submit_onboarding_run(run_id)
    return {"run": updated, "trigger": trigger}


@router.post("/api/onboarding-runs/{run_id}/manual-llm-response")
def submit_manual_llm_response(run_id: str, payload: OnboardingManualLlmResponsePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    run = repository.get_onboarding_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="onboarding run not found")
    metadata = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
    responses = metadata.get("manual_llm_responses") if isinstance(metadata.get("manual_llm_responses"), dict) else {}
    responses = {**responses, payload.stage: payload.manual_llm_response}
    updated = repository.update_onboarding_run_metadata(
        run_id,
        {
            "manual_llm_responses": responses,
            "manual_llm_last_stage": payload.stage,
            "manual_llm_last_reviewer": payload.reviewer,
        },
    )
    trigger = submit_onboarding_run(run_id)
    return {"run": updated, "trigger": trigger}


@router.post("/api/onboarding-runs/{run_id}/pause")
def pause_onboarding_run(run_id: str, payload: OnboardingRunResumePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    run = repository.get_onboarding_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="onboarding run not found")
    updated = pause_onboarding_run_service(repository, run_id, payload.reviewer)
    return {"run": updated}


@router.post("/api/onboarding-runs/{run_id}/cancel")
def cancel_onboarding_run(run_id: str, payload: OnboardingRunResumePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    run = repository.get_onboarding_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="onboarding run not found")
    updated = cancel_onboarding_run_service(repository, run_id, payload.reviewer)
    return {"run": updated}


@router.post("/api/onboarding-tasks/{task_id}/generate-draft")
def generate_onboarding_task_draft(task_id: str, payload: OnboardingTaskDraftPayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    task = repository.get_work_queue_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="onboarding task not found")
    draft = build_task_draft(task)
    updated = repository.update_work_queue_task(
        task_id,
        draft_status="ai_drafted",
        draft_payload=draft["draft_payload"],
        draft_rationale=draft["draft_rationale"],
        draft_confidence=draft["draft_confidence"],
        recommended_action=draft["recommended_action"],
        status="needs_review",
        assigned_to=payload.reviewer,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="onboarding task not found")
    return {"task": updated}


@router.post("/api/onboarding-tasks/{task_id}/complete")
def complete_onboarding_task(task_id: str, payload: OnboardingTaskCompletePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    task = repository.get_work_queue_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="onboarding task not found")
    validation_error = _validate_mapping_task_completion(repository, task)
    if validation_error is not None:
        raise HTTPException(status_code=409, detail=validation_error)
    approved_proposal_ids = _auto_approve_stage_proposals(repository, task, payload.reviewer)
    updated_task = repository.update_work_queue_task(
        task_id,
        status="completed",
        assigned_to=payload.reviewer,
        recommended_action=(
            "Task completed and related proposals were applied to the authoring registry. Review next stage task or resume worker."
            if approved_proposal_ids
            else "Task completed. Review next stage task or resume worker."
        ),
    )
    if updated_task is None:
        raise HTTPException(status_code=404, detail="onboarding task not found")
    updated_run = advance_run_after_task_completion(repository, updated_task["run_id"])
    trigger = None
    if isinstance(updated_run, dict) and str(updated_run.get("current_stage") or "").endswith("_drafting"):
        trigger = submit_onboarding_run(str(updated_task["run_id"]))
    return {
        "task": updated_task,
        "run": updated_run,
        "trigger": trigger,
        "applied_proposal_ids": approved_proposal_ids,
    }


@router.get("/api/proposal-bundles")
def list_proposal_bundles() -> list[dict[str, Any]]:
    repository = SemanticLayerRepository()
    records = repository.list_proposal_bundles()
    sources = repository.list_execution_sources()
    operations = repository.list_execution_operations()
    fields = repository.list_operation_fields()
    mappings = repository.list_field_mappings()
    proposals = repository.list_proposals()
    runs = build_onboarding_runs(sources, operations, fields, mappings, proposals)
    derived = [build_proposal_bundle(run, proposals) for run in runs]
    real_source_ids = {str(item.get("source_id") or "") for item in records}
    merged = records + [item for item in derived if str(item.get("source_id") or "") not in real_source_ids]
    return sorted(merged, key=lambda item: str(item.get("updated_at") or ""), reverse=True)
