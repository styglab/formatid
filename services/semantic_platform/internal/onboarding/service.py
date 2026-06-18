from __future__ import annotations

from typing import TYPE_CHECKING, Any

from services.semantic_platform.internal.onboarding.stages import next_onboarding_stage

if TYPE_CHECKING:
    from services.semantic_platform.internal.storage.repository import SemanticLayerRepository


def initialize_onboarding_run(repository: SemanticLayerRepository, run_id: str) -> dict[str, Any] | None:
    run = repository.get_onboarding_run(run_id)
    if run is None:
        return None
    current_stage = str(run.get("current_stage") or "source_review")
    return repository.update_onboarding_run_stage(
        run_id,
        current_stage=current_stage,
        stage_status="in_progress",
        next_action="Review source evidence and complete the current stage tasks.",
        status="started",
    )


def resume_onboarding_run(repository: SemanticLayerRepository, run_id: str, reviewer: str) -> dict[str, Any] | None:
    return repository.update_onboarding_run_stage(
        run_id,
        stage_status="in_progress",
        next_action=f"Run resumed by {reviewer}. Waiting for worker orchestration.",
        status="started",
    )


def pause_onboarding_run(repository: SemanticLayerRepository, run_id: str, reviewer: str) -> dict[str, Any] | None:
    return repository.update_onboarding_run_stage(
        run_id,
        stage_status="paused",
        next_action=f"Workspace paused by {reviewer}. Resume when ready.",
        status="paused",
    )


def cancel_onboarding_run(repository: SemanticLayerRepository, run_id: str, reviewer: str) -> dict[str, Any] | None:
    return repository.update_onboarding_run_stage(
        run_id,
        stage_status="cancelled",
        next_action=f"Workspace cancelled by {reviewer}. Start a new workspace from the source to continue onboarding.",
        status="cancelled",
    )


def advance_run_after_task_completion(repository: SemanticLayerRepository, run_id: str) -> dict[str, Any] | None:
    run = repository.get_onboarding_run(run_id)
    if run is None:
        return None
    tasks = repository.list_work_queue_tasks(run_id=run_id)
    current_stage = str(run.get("current_stage") or "source_review")
    current_stage_tasks = [task for task in tasks if str(task.get("stage") or "") == current_stage]
    if not current_stage_tasks or any(task.get("status") != "completed" for task in current_stage_tasks):
        return repository.update_onboarding_run_stage(
            run_id,
            current_stage=current_stage,
            stage_status="in_progress",
            next_action="Complete remaining tasks in the current stage.",
            status="started",
        )

    next_stage = next_onboarding_stage(current_stage)
    if next_stage is None:
        return repository.update_onboarding_run_stage(
            run_id,
            current_stage=current_stage,
            stage_status="completed",
            next_action="All onboarding stages completed. Ready for publish review.",
            status="ready_to_publish",
        )

    next_stage_tasks = [task for task in tasks if str(task.get("stage") or "") == next_stage]
    for next_task in next_stage_tasks:
        if next_task.get("status") == "blocked":
            repository.update_work_queue_task(
                str(next_task.get("id") or ""),
                status="open",
                recommended_action="Generate AI draft or complete this task to continue onboarding.",
            )
    return repository.update_onboarding_run_stage(
        run_id,
        current_stage=next_stage,
        stage_status="pending",
        next_action=(
            f"Resume worker to generate {next_stage.replace('_', ' ')}."
            if next_stage.endswith("_drafting")
            else f"Continue with {next_stage.replace('_', ' ')} tasks."
        ),
        status="in_review",
    )
