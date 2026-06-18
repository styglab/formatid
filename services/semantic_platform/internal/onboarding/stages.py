from __future__ import annotations

from typing import Any
from uuid import uuid4


ONBOARDING_STAGE_ORDER = [
    "source_review",
    "asset_discovery",
    "structure_review",
    "semantic_model_drafting",
    "semantic_model_approval",
    "binding_drafting",
    "binding_approval",
    "proposal_review",
    "publish_readiness",
]

_ONBOARDING_TASK_DEFINITIONS = [
    {
        "stage": "source_review",
        "task_type": "confirm_source_metadata",
        "title": "Confirm source metadata and evidence package",
        "priority": 10,
        "recommended_action": "Review uploaded source metadata, provenance, and evidence snapshot.",
    },
    {
        "stage": "asset_discovery",
        "task_type": "discover_assets",
        "title": "Discover source assets and access paths",
        "priority": 20,
        "recommended_action": "Generate AI draft for source assets, access paths, and execution surfaces.",
    },
    {
        "stage": "structure_review",
        "task_type": "review_extracted_structures",
        "title": "Review extracted structures and fields",
        "priority": 30,
        "recommended_action": "Generate AI draft for structure classification and validate extracted fields.",
    },
    {
        "stage": "semantic_model_drafting",
        "task_type": "draft_semantic_model",
        "title": "Draft semantic model candidates",
        "priority": 40,
        "recommended_action": "Generate semantic meaning, canonical entity, and canonical attribute candidates from extracted structures and fields.",
    },
    {
        "stage": "semantic_model_approval",
        "task_type": "approve_semantic_model",
        "title": "Approve semantic model",
        "priority": 50,
        "recommended_action": "Review and approve semantic meaning plus canonical model before source binding.",
    },
    {
        "stage": "binding_drafting",
        "task_type": "draft_source_binding",
        "title": "Draft source bindings",
        "priority": 60,
        "recommended_action": "Generate field mappings, control semantics, variants, and binding candidates from approved semantic model.",
    },
    {
        "stage": "binding_approval",
        "task_type": "approve_source_binding",
        "title": "Approve source bindings",
        "priority": 70,
        "recommended_action": "Review and approve source bindings before bundle review.",
    },
    {
        "stage": "proposal_review",
        "task_type": "approve_proposal_bundle",
        "title": "Review proposal bundle",
        "priority": 80,
        "recommended_action": "Inspect generated proposals, approve or reject changes, and capture reviewer notes.",
    },
    {
        "stage": "publish_readiness",
        "task_type": "publish_runtime_snapshot",
        "title": "Validate publish readiness",
        "priority": 90,
        "recommended_action": "Confirm required tasks are approved before publishing runtime snapshot.",
    },
]


def next_onboarding_stage(stage: str) -> str | None:
    try:
        current_index = ONBOARDING_STAGE_ORDER.index(stage)
    except ValueError:
        return ONBOARDING_STAGE_ORDER[0]
    next_index = current_index + 1
    if next_index >= len(ONBOARDING_STAGE_ORDER):
        return None
    return ONBOARDING_STAGE_ORDER[next_index]


def build_onboarding_stage_task_records(
    *,
    run_id: str,
    source_id: str,
    source_name: str,
    evidence_snapshot_id: str,
    proposal_id: str | None,
    created_at: str,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    previous_task_id = ""
    payload = {
        "source_id": source_id,
        "source_name": source_name,
        "asset_id": None,
        "access_path_id": None,
    }
    for definition in _ONBOARDING_TASK_DEFINITIONS:
        task_id = f"task_{uuid4().hex}"
        task = {
            "id": task_id,
            "run_id": run_id,
            "source_id": source_id,
            "evidence_snapshot_id": evidence_snapshot_id,
            "operation_id": None,
            "field_id": None,
            "stage": definition["stage"],
            "task_type": definition["task_type"],
            "status": "open" if not previous_task_id else "blocked",
            "supports_ai_draft": True,
            "draft_status": "not_started",
            "depends_on": [previous_task_id] if previous_task_id else [],
            "recommended_action": definition["recommended_action"],
            "draft_payload": {},
            "draft_rationale": "",
            "draft_confidence": 0.0,
            "priority": definition["priority"],
            "title": definition["title"],
            "payload": payload.copy(),
            "proposal_id": proposal_id,
            "assigned_to": "",
            "created_at": created_at,
            "updated_at": created_at,
        }
        tasks.append(task)
        previous_task_id = task_id
    return tasks
