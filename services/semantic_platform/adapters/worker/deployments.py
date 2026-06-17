from __future__ import annotations

import os
from prefect import flow, task

from services.semantic_platform.internal.storage import SemanticLayerRepository


ONBOARDING_FLOW_NAME = "run-onboarding-pipeline"
ONBOARDING_DEPLOYMENT_NAME = "semantic-platform-onboarding"


@task
def bootstrap_source_review(run_id: str) -> None:
    repository = SemanticLayerRepository()
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="source_review",
        stage_status="in_progress",
        next_action="Review source evidence and complete source review tasks.",
        status="started",
    )


@task
def discover_operations_and_fields(run_id: str) -> None:
    repository = SemanticLayerRepository()
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="asset_discovery",
        stage_status="pending",
        next_action="Generate AI drafts for source assets and access paths before structure review.",
        status="started",
    )


@task
def prepare_semantic_mapping_tasks(run_id: str) -> None:
    repository = SemanticLayerRepository()
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="structure_review",
        stage_status="pending",
        next_action="Review extracted structures and fields, then continue to semantic mapping.",
        status="started",
    )


@flow(name=ONBOARDING_FLOW_NAME)
def run_onboarding_pipeline(run_id: str) -> dict[str, str]:
    bootstrap_source_review(run_id)
    discover_operations_and_fields(run_id)
    prepare_semantic_mapping_tasks(run_id)
    return {"run_id": run_id, "status": "stage_scaffold_ready"}


def main() -> None:
    limit = int(os.getenv("SEMANTIC_PLATFORM_PREFECT_LIMIT", "1"))
    run_onboarding_pipeline.serve(
        name=ONBOARDING_DEPLOYMENT_NAME,
        pause_on_shutdown=False,
        limit=limit,
    )


if __name__ == "__main__":
    main()
