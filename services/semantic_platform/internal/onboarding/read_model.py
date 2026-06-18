from __future__ import annotations

from typing import Any


def build_onboarding_runs(
    sources: list[dict[str, Any]],
    operations: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    proposals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    operations_by_source: dict[str, list[dict[str, Any]]] = {}
    for operation in operations:
        source_id = str(operation.get("source_id") or "")
        operations_by_source.setdefault(source_id, []).append(operation)

    fields_by_operation: dict[str, list[dict[str, Any]]] = {}
    for field in fields:
        fields_by_operation.setdefault(str(field.get("operation_id") or ""), []).append(field)

    mappings_by_source: dict[str, list[dict[str, Any]]] = {}
    for mapping in mappings:
        source_id = str(mapping.get("source_id") or "")
        mappings_by_source.setdefault(source_id, []).append(mapping)

    runs: list[dict[str, Any]] = []
    for source in sources:
        source_id = source["id"]
        source_operations = operations_by_source.get(source_id, [])
        operation_ids = {item["id"] for item in source_operations}
        source_fields = [field for operation_id in operation_ids for field in fields_by_operation.get(operation_id, [])]
        source_mappings = mappings_by_source.get(source_id, [])
        source_proposals = _filter_proposals_for_source(proposals, source_id, operation_ids)
        config = source.get("config") or {}
        upload = config.get("upload") if isinstance(config, dict) else {}
        suggestion_generation = upload.get("suggestion_generation") if isinstance(upload, dict) else None
        runs.append(
            {
                "id": f"run_{source_id}",
                "source_id": source_id,
                "source_name": source.get("name") or source_id,
                "status": "pending_review" if any(item.get("status") == "pending_review" for item in source_proposals) else source.get("status") or "draft",
                "stage": "source_uploaded",
                "current_stage": "source_review",
                "stage_status": "pending",
                "run_mode": "ai_assisted",
                "next_action": "Review source evidence and generate onboarding drafts.",
                "evidence_snapshot_id": f"evidence_{source_id}",
                "operation_count": len(source_operations),
                "field_count": len(source_fields),
                "mapping_count": len(source_mappings),
                "proposal_count": len(source_proposals),
                "pending_proposal_count": len([item for item in source_proposals if item.get("status") == "pending_review"]),
                "suggestion_status": (suggestion_generation or {}).get("status") or "derived_on_read",
                "created_at": source.get("created_at"),
                "updated_at": source.get("updated_at"),
            }
        )
    return sorted(runs, key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)


def build_proposal_bundle(run: dict[str, Any], proposals: list[dict[str, Any]]) -> dict[str, Any]:
    source_id = str(run.get("source_id") or "")
    source_proposals = _filter_proposals_for_source(proposals, source_id)
    entity_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for proposal in source_proposals:
        entity_type = str(proposal.get("entity_type") or "unknown")
        status = str(proposal.get("status") or "unknown")
        entity_counts[entity_type] = entity_counts.get(entity_type, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "id": f"bundle_{run['id']}",
        "run_id": run["id"],
        "source_id": source_id,
        "source_name": run.get("source_name") or source_id,
        "status": "pending_review" if status_counts.get("pending_review") else "ready",
        "proposal_count": len(source_proposals),
        "pending_count": status_counts.get("pending_review", 0),
        "approved_count": status_counts.get("approved", 0),
        "rejected_count": status_counts.get("rejected", 0),
        "entity_counts": entity_counts,
        "evidence_snapshot_id": run.get("evidence_snapshot_id"),
        "proposal_ids": [proposal["id"] for proposal in source_proposals],
        "updated_at": run.get("updated_at"),
    }


def build_onboarding_run_detail(repository: Any, run: dict[str, Any]) -> dict[str, Any]:
    source_id = str(run.get("source_id") or "")
    source = next((item for item in repository.list_execution_sources() if item.get("id") == source_id), None)
    if source is None:
        raise KeyError(f"source not found for onboarding run: {source_id}")

    operations = repository.list_execution_operations(source_id=source_id)
    operation_ids = {str(item.get("id") or "") for item in operations}
    all_fields = repository.list_operation_fields()
    fields = [item for item in all_fields if str(item.get("operation_id") or "") in operation_ids]
    mappings = [item for item in repository.list_field_mappings() if str(item.get("source_id") or "") == source_id]
    proposals = _filter_proposals_for_source(repository.list_proposals(), source_id, operation_ids)

    actual_evidence = repository.list_evidence_snapshots(run_id=str(run.get("id") or ""))
    actual_tasks = repository.list_work_queue_tasks(run_id=str(run.get("id") or ""))
    bundle = next((item for item in repository.list_proposal_bundles() if item.get("run_id") == run.get("id")), None)

    evidence = actual_evidence[:1]
    if not evidence:
        config = source.get("config") or {}
        upload = config.get("upload") if isinstance(config, dict) else {}
        if not isinstance(upload, dict):
            upload = {}
        evidence = [
            {
                "id": str(run.get("evidence_snapshot_id") or f"evidence_{source_id}"),
                "run_id": run.get("id"),
                "source_id": source_id,
                "snapshot_type": "derived_on_read",
                "content_hash": str(upload.get("sha256") or ""),
                "source_ref": {
                    "reference_uri": config.get("reference_uri") if isinstance(config, dict) else "",
                    "upload": upload,
                },
                "operation_evidence": [
                    {
                        "operation_id": item.get("id"),
                        "operation_name": item.get("name"),
                        "http_method": item.get("http_method"),
                        "access_path_locator": item.get("access_path_locator"),
                    }
                    for item in operations
                ],
                "schema_evidence": [
                    {
                        "field_id": item.get("id"),
                        "raw_name": item.get("raw_name"),
                        "field_path": item.get("field_path"),
                        "scope": item.get("scope"),
                        "data_type": item.get("data_type"),
                        "evidence": item.get("evidence") or [],
                    }
                    for item in fields
                ],
                "sample_values": {
                    item.get("field_path") or item.get("raw_name") or item.get("id"): (item.get("evidence") or [])[:2]
                    for item in fields[:20]
                },
                "ai_context": {
                    "suggestion_status": run.get("suggestion_status") or "derived_on_read",
                },
                "created_at": run.get("created_at"),
            }
        ]

    work_queue = actual_tasks
    if not work_queue:
        mapped_field_ids = {str(item.get("field_id") or "") for item in mappings if item.get("field_id")}
        work_queue = []
        if not operations:
            work_queue.append(
                {
                    "id": f"task_discover_{source_id}",
                    "run_id": run.get("id"),
                    "source_id": source_id,
                    "evidence_snapshot_id": evidence[0].get("id"),
                    "operation_id": None,
                    "operation_name": "",
                    "field_id": None,
                    "field_name": "",
                    "field_path": "",
                    "stage": "asset_discovery",
                    "task_type": "discover_assets",
                    "status": "open",
                    "supports_ai_draft": True,
                    "draft_status": "not_started",
                    "depends_on": [],
                    "recommended_action": "Generate AI draft for assets, access paths, and structures from uploaded source.",
                    "draft_payload": {},
                    "draft_rationale": "",
                    "draft_confidence": None,
                    "priority": 10,
                    "title": "Discover assets and access paths from uploaded source",
                    "payload": {"source_id": source_id},
                    "proposal_id": None,
                    "assigned_to": None,
                    "created_at": run.get("created_at"),
                    "updated_at": run.get("updated_at"),
                }
            )
        for item in fields:
            field_id = str(item.get("id") or "")
            if field_id and field_id not in mapped_field_ids:
                work_queue.append(
                    {
                        "id": f"task_map_{field_id}",
                        "run_id": run.get("id"),
                        "source_id": source_id,
                        "evidence_snapshot_id": evidence[0].get("id"),
                        "operation_id": item.get("operation_id"),
                        "operation_name": next((op.get("name") for op in operations if op.get("id") == item.get("operation_id")), ""),
                        "field_id": item.get("id"),
                        "field_name": item.get("raw_name") or "",
                        "field_path": item.get("field_path") or "",
                        "stage": "semantic_mapping",
                        "task_type": "map_field",
                        "status": "open",
                        "supports_ai_draft": True,
                        "draft_status": "not_started",
                        "depends_on": [],
                        "recommended_action": "Generate AI mapping draft and confirm semantic type link.",
                        "draft_payload": {},
                        "draft_rationale": "",
                        "draft_confidence": None,
                        "priority": 100,
                        "title": f"Map {item.get('raw_name') or item.get('field_path') or item.get('id')}",
                        "payload": {"field_id": item.get("id"), "field_path": item.get("field_path"), "scope": item.get("scope")},
                        "proposal_id": None,
                        "assigned_to": None,
                        "created_at": run.get("created_at"),
                        "updated_at": run.get("updated_at"),
                    }
                )

    if bundle is None:
        bundle = build_proposal_bundle(run, proposals)

    run_with_progress = {
        **run,
        **build_workspace_progress_summary(run, work_queue),
    }

    return {
        "run": run_with_progress,
        "source": source,
        "evidence_snapshots": evidence,
        "operations": operations,
        "fields": fields,
        "mappings": mappings,
        "work_queue_tasks": work_queue,
        "proposal_bundle": bundle,
        "proposals": proposals,
    }


def build_task_draft(task: dict[str, Any]) -> dict[str, Any]:
    stage = str(task.get("stage") or "source_review")
    subject = str(task.get("field_path") or task.get("operation_name") or task.get("payload", {}).get("source_name") or task.get("source_id") or "")
    return {
        "draft_payload": {
            "mode": "ai_assist_scaffold",
            "stage": stage,
            "task_type": task.get("task_type"),
            "subject": subject,
            "notes": [
                f"Review subject: {subject or 'source evidence'}",
                f"Stage: {stage}",
                "This is an AI draft scaffold. Replace with reviewed semantic decisions before publish.",
            ],
        },
        "draft_rationale": f"AI scaffold created for {task.get('task_type')} during {stage}.",
        "draft_confidence": 0.51,
        "recommended_action": "Inspect AI draft, edit if needed, then complete the task.",
    }


def build_workspace_progress_summary(run: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, Any]:
    draft_ready_statuses = {"ai_drafted", "draft_ready", "ready"}
    draft_failed_statuses = {"draft_failed", "failed"}
    draft_active_statuses = {"queued", "running", "drafting", "generating", "in_progress"}

    draftable_tasks = [task for task in tasks if bool(task.get("supports_ai_draft", True))]
    current_stage = str(run.get("current_stage") or "source_review")
    current_stage_tasks = [task for task in tasks if str(task.get("stage") or "") == current_stage]

    def _count_ready(items: list[dict[str, Any]]) -> int:
        return sum(
            1
            for task in items
            if str(task.get("status") or "") == "completed"
            or str(task.get("draft_status") or "") in draft_ready_statuses
        )

    def _count_failed(items: list[dict[str, Any]]) -> int:
        return sum(1 for task in items if str(task.get("draft_status") or "") in draft_failed_statuses)

    def _count_active(items: list[dict[str, Any]]) -> int:
        return sum(1 for task in items if str(task.get("draft_status") or "") in draft_active_statuses)

    total_tasks = len(draftable_tasks)
    completed_tasks = sum(1 for task in tasks if str(task.get("status") or "") == "completed")
    ready_tasks = _count_ready(draftable_tasks)
    failed_tasks = _count_failed(draftable_tasks)
    active_tasks = _count_active(draftable_tasks)
    queued_tasks = max(total_tasks - ready_tasks - failed_tasks - active_tasks, 0)
    percent = int(round((ready_tasks / total_tasks) * 100)) if total_tasks else None
    current_stage_task_count = len(current_stage_tasks)
    current_stage_ready_count = _count_ready(current_stage_tasks)
    current_stage_failed_count = _count_failed(current_stage_tasks)
    current_stage_completed_count = sum(1 for task in current_stage_tasks if str(task.get("status") or "") == "completed")

    stage_status = str(run.get("stage_status") or "")
    preparation_status = "ready"
    if stage_status == "completed":
        preparation_status = "completed"
    elif failed_tasks > 0 and current_stage.endswith("_drafting"):
        preparation_status = "blocked"
    elif current_stage in {"source_review", "asset_discovery", "structure_review", "semantic_model_drafting", "binding_drafting"}:
        preparation_status = "preparing"
    elif current_stage in {"semantic_model_approval", "binding_approval", "proposal_review", "publish_readiness"}:
        preparation_status = "ready"
    elif total_tasks and ready_tasks < total_tasks:
        preparation_status = "preparing"

    next_worker_task = next(
        (
            task
            for task in draftable_tasks
            if str(task.get("status") or "") != "completed"
            and str(task.get("draft_status") or "") not in draft_ready_statuses
        ),
        None,
    )

    return {
        "preparation_status": preparation_status,
        "worker_progress_percent": percent,
        "task_count": total_tasks,
        "completed_task_count": completed_tasks,
        "draft_ready_count": ready_tasks,
        "draft_failed_count": failed_tasks,
        "draft_active_count": active_tasks,
        "draft_queued_count": queued_tasks,
        "current_stage_task_count": current_stage_task_count,
        "current_stage_ready_count": current_stage_ready_count,
        "current_stage_failed_count": current_stage_failed_count,
        "current_stage_completed_count": current_stage_completed_count,
        "worker_current_task": str(next_worker_task.get("title") or next_worker_task.get("recommended_action") or "") if next_worker_task else "",
    }


def _filter_proposals_for_source(
    proposals: list[dict[str, Any]],
    source_id: str,
    operation_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    scoped_operation_ids = operation_ids or set()
    return [
        proposal
        for proposal in proposals
        if proposal.get("entity_id") == source_id
        or str((proposal.get("payload") or {}).get("source_id") or "") == source_id
        or (
            scoped_operation_ids
            and str((proposal.get("payload") or {}).get("operation_id") or "") in scoped_operation_ids
        )
    ]
