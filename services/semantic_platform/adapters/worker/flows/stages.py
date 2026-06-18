from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.semantic_platform.internal.ingestion.parsers import (
    determine_ingestion_strategy,
    discover_assets,
    discover_structures,
    load_source_payload,
)
from services.semantic_platform.internal.ingestion.langgraph import (
    generate_semantic_mapping_drafts,
)
from services.semantic_platform.internal.ingestion.langgraph.semantic_model import (
    generate_semantic_model_drafts,
)
from services.semantic_platform.internal.ingestion.llm import (
    build_manual_semantic_model_request,
    build_manual_semantic_mapping_request,
)
from services.semantic_platform.internal.onboarding import build_task_draft
from services.semantic_platform.internal.storage import SemanticLayerRepository


@dataclass
class StageResult:
    stage: str
    current_stage: str
    stage_status: str
    next_action: str
    run_status: str


def run_stage_source_review(repository: SemanticLayerRepository, run_id: str) -> StageResult:
    run, source, stage_task = _load_stage_context(repository, run_id, "source_review")
    loaded = load_source_payload(source)
    strategy = determine_ingestion_strategy(loaded)
    repository.update_onboarding_run_metadata(
        run_id,
        {
            "ingestion_strategy": strategy,
            "worker_current_step": "source_review",
            "loaded_source": {
                "filename": loaded.filename,
                "media_type": loaded.media_type,
                "reference_uri": loaded.reference_uri,
                "stored_path": loaded.stored_path,
            },
        },
    )
    draft = build_task_draft(stage_task)
    draft["draft_payload"] = {
        **draft["draft_payload"],
        "source_summary": {
            "source_name": loaded.source_name,
            "source_type": loaded.source_type,
            "filename": loaded.filename,
            "media_type": loaded.media_type,
            "ingestion_strategy": strategy,
        },
        "preview_excerpt": loaded.content_text[:500],
    }
    repository.update_work_queue_task(
        stage_task["id"],
        status="open",
        draft_status="draft_ready",
        draft_payload=draft["draft_payload"],
        draft_rationale=draft["draft_rationale"],
        draft_confidence=0.82,
        recommended_action="Review source summary and generated ingestion strategy before semantic review.",
        payload={
            **(stage_task.get("payload") or {}),
            "source_id": source["id"],
            "ingestion_strategy": strategy,
        },
    )
    _open_stage_tasks(repository, run_id, "asset_discovery")
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="asset_discovery",
        stage_status="in_progress",
        next_action="Generate assets and access paths from uploaded source content.",
        status="started",
    )
    return StageResult(
        stage="source_review",
        current_stage="asset_discovery",
        stage_status="in_progress",
        next_action="Generate assets and access paths from uploaded source content.",
        run_status="started",
    )


def run_stage_asset_discovery(repository: SemanticLayerRepository, run_id: str) -> StageResult:
    run, source, stage_task = _load_stage_context(repository, run_id, "asset_discovery")
    loaded = load_source_payload(source)
    strategy = str((run.get("metadata") or {}).get("ingestion_strategy") or determine_ingestion_strategy(loaded))
    assets = discover_assets(loaded, strategy)

    created_assets: list[dict[str, Any]] = []
    created_operations: list[dict[str, Any]] = []
    for asset_index, asset in enumerate(assets):
        saved_asset = repository.save_execution_asset(
            {
                "source_id": source["id"],
                "name": asset.name,
                "asset_type": asset.asset_type,
                "locator": asset.locator,
                "description": asset.description,
                "status": "draft",
                "lifecycle": "review",
                "metadata": {"worker_stage": "asset_discovery", **asset.metadata},
                "evidence": [{"kind": "asset_discovery", "source": loaded.filename}],
                "confidence": 0.76 if strategy != "structured" else 0.9,
            }
        )
        created_assets.append(saved_asset)
        for path_index, access_path in enumerate(asset.access_paths):
            saved_access_path = repository.save_execution_access_path(
                {
                    "asset_id": saved_asset["id"],
                    "name": str(access_path.get("name") or f"path_{path_index + 1}"),
                    "access_type": str(access_path.get("access_type") or "other"),
                    "locator": str(access_path.get("locator") or saved_asset.get("locator") or ""),
                    "http_method": str(access_path.get("http_method") or ""),
                    "description": str(access_path.get("description") or ""),
                    "status": "draft",
                    "lifecycle": "review",
                    "request_shape": {},
                    "response_shape": {},
                    "execution_hints": {},
                    "evidence": [{"kind": "access_path_discovery", "locator": str(access_path.get("locator") or "")}],
                    "confidence": 0.75 if strategy != "structured" else 0.9,
                }
            )
            source_operation_key = str(access_path.get("operation_key") or saved_access_path["name"])
            operation_key = _namespaced_operation_key(source["id"], source_operation_key)
            saved_operation = repository.save_execution_operation(
                {
                    "access_path_id": saved_access_path["id"],
                    "operation_key": operation_key,
                    "name": str(access_path.get("operation_name") or saved_access_path["name"]),
                    "description": str(access_path.get("operation_description") or access_path.get("description") or ""),
                    "namespace": "public",
                    "status": "draft",
                    "lifecycle": "review",
                    "metadata": {
                        "worker_stage": "asset_discovery",
                        "asset_index": asset_index,
                        "path_index": path_index,
                        "source_operation_key": source_operation_key,
                        **(access_path.get("metadata") if isinstance(access_path.get("metadata"), dict) else {}),
                    },
                    "evidence": [{"kind": "operation_discovery", "operation_key": operation_key}],
                    "confidence": 0.72 if strategy != "structured" else 0.88,
                }
            )
            created_operations.append(saved_operation)

    draft = build_task_draft(stage_task)
    draft["draft_payload"] = {
        **draft["draft_payload"],
        "ingestion_strategy": strategy,
        "asset_count": len(created_assets),
        "operation_count": len(created_operations),
        "assets": [
            {
                "asset_id": item["id"],
                "name": item["name"],
                "asset_type": item.get("asset_type"),
                "locator": item.get("locator"),
            }
            for item in created_assets
        ],
        "operations": [
            {
                "operation_id": item["id"],
                "operation_key": item.get("metadata", {}).get("source_operation_key") or item.get("operation_key"),
                "name": item.get("name"),
                "access_path_locator": item.get("access_path_locator"),
                "http_method": item.get("http_method"),
            }
            for item in created_operations
        ],
    }
    repository.update_work_queue_task(
        stage_task["id"],
        status="open",
        draft_status="draft_ready",
        draft_payload=draft["draft_payload"],
        draft_rationale=draft["draft_rationale"],
        draft_confidence=0.84 if strategy == "structured" else 0.68,
        recommended_action="Review generated assets, access paths, and operation candidates.",
        payload={
            **(stage_task.get("payload") or {}),
            "source_id": source["id"],
            "asset_ids": [item["id"] for item in created_assets],
            "operation_ids": [item["id"] for item in created_operations],
        },
    )
    _open_stage_tasks(repository, run_id, "structure_review")
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="structure_review",
        stage_status="in_progress",
        next_action="Extract fields and structural hints from discovered assets and operations.",
        status="started",
    )
    return StageResult(
        stage="asset_discovery",
        current_stage="structure_review",
        stage_status="in_progress",
        next_action="Extract fields and structural hints from discovered assets and operations.",
        run_status="started",
    )


def run_stage_structure_review(repository: SemanticLayerRepository, run_id: str) -> StageResult:
    run, source, stage_task = _load_stage_context(repository, run_id, "structure_review")
    loaded = load_source_payload(source)
    strategy = str((run.get("metadata") or {}).get("ingestion_strategy") or determine_ingestion_strategy(loaded))
    operations = repository.list_execution_operations(source_id=source["id"])
    drafts = discover_structures(loaded, operations, strategy)
    operations_by_key: dict[str, dict[str, Any]] = {}
    for operation in operations:
        operations_by_key[str(operation.get("operation_key") or "")] = operation
        metadata = operation.get("metadata") if isinstance(operation.get("metadata"), dict) else {}
        source_operation_key = str(metadata.get("source_operation_key") or "")
        if source_operation_key:
            operations_by_key[source_operation_key] = operation

    total_fields = 0
    operation_summaries: list[dict[str, Any]] = []
    for draft in drafts:
        operation = operations_by_key.get(draft.operation_key)
        if operation is None:
            continue
        created_for_operation = 0
        for field in draft.fields:
            repository.save_operation_field(
                {
                    "operation_id": operation["id"],
                    "scope": field.scope,
                    "raw_name": field.raw_name,
                    "display_name": field.raw_name,
                    "field_path": field.field_path,
                    "data_type": field.data_type,
                    "is_required": field.is_required,
                    "description": field.description,
                    "lifecycle": "review",
                    "metadata": {"worker_stage": "structure_review", "ingestion_strategy": strategy},
                    "evidence": field.evidence,
                    "confidence": 0.7 if strategy != "structured" else 0.9,
                }
            )
            total_fields += 1
            created_for_operation += 1
        operation_summaries.append(
            {
                "operation_id": operation["id"],
                "operation_key": operation["operation_key"],
                "field_count": created_for_operation,
            }
        )

    draft = build_task_draft(stage_task)
    draft["draft_payload"] = {
        **draft["draft_payload"],
        "ingestion_strategy": strategy,
        "field_count": total_fields,
        "operations": operation_summaries,
        "structure_status": "fields_extracted" if total_fields else "no_fields_extracted",
    }
    repository.update_work_queue_task(
        stage_task["id"],
        status="open",
        draft_status="draft_ready",
        draft_payload=draft["draft_payload"],
        draft_rationale=draft["draft_rationale"],
        draft_confidence=0.83 if total_fields and strategy == "structured" else 0.62,
        recommended_action="Review extracted structures and field classifications before semantic mapping review.",
        payload={
            **(stage_task.get("payload") or {}),
            "source_id": source["id"],
            "field_count": total_fields,
        },
    )
    _open_stage_tasks(repository, run_id, "semantic_model_drafting")
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="semantic_model_drafting",
        stage_status="in_progress",
        next_action="Generate semantic model drafts from extracted structures and fields.",
        status="started",
    )
    repository.update_onboarding_run_metadata(
        run_id,
        {
            "worker_current_step": "structure_ready",
            "worker_completed_stages": ["source_review", "asset_discovery", "structure_review"],
        },
    )
    return StageResult(
        stage="structure_review",
        current_stage="semantic_model_drafting",
        stage_status="in_progress",
        next_action="Generate semantic model drafts from extracted structures and fields.",
        run_status="started",
    )


def run_stage_semantic_model_drafting(repository: SemanticLayerRepository, run_id: str) -> StageResult:
    run, source, stage_task = _load_stage_context(repository, run_id, "semantic_model_drafting")
    metadata = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
    operations = repository.list_execution_operations(source_id=source["id"])
    operation_ids = {str(item.get("id") or "") for item in operations}
    fields = [item for item in repository.list_operation_fields() if str(item.get("operation_id") or "") in operation_ids]
    existing_mappings = [item for item in repository.list_field_mappings() if str(item.get("source_id") or "") == source["id"]]
    semantic_types = repository.list_semantic_types()
    canonical_entities = repository.list_canonical_entities()
    canonical_attributes = repository.list_canonical_attributes()
    bundle = next((item for item in repository.list_proposal_bundles() if item.get("run_id") == run_id), None)
    manual_llm_responses = metadata.get("manual_llm_responses") if isinstance(metadata.get("manual_llm_responses"), dict) else {}
    manual_response = manual_llm_responses.get("semantic_model_drafting")
    draft_payload = generate_semantic_model_drafts(
        source=source,
        operations=operations,
        fields=fields,
        semantic_types=semantic_types,
        canonical_attributes=canonical_attributes,
        manual_llm_response=manual_response if isinstance(manual_response, dict) else None,
    )

    if draft_payload.get("status") == "waiting_manual_llm":
        manual_request = build_manual_semantic_model_request(
            run_id=run_id,
            source=source,
            operations=operations,
            fields=fields,
            semantic_types=semantic_types,
            canonical_attributes=canonical_attributes,
            retrieved_candidates=draft_payload.get("suggestions") if isinstance(draft_payload.get("suggestions"), list) else [],
        )
        repository.update_work_queue_task(
            stage_task["id"],
            status="blocked",
            draft_status="waiting_manual_llm",
            draft_payload={"manual_llm_request": manual_request},
            draft_rationale="Waiting for codex_manual semantic model drafting response.",
            draft_confidence=0.0,
            recommended_action="Submit manual_llm_response for semantic_model_drafting, then resume workspace.",
            payload={**(stage_task.get("payload") or {}), "manual_llm_required": True},
        )
        repository.update_onboarding_run_stage(
            run_id,
            current_stage="semantic_model_drafting",
            stage_status="paused",
            next_action="Waiting for manual semantic model drafting response in codex_manual mode.",
            status="paused",
        )
        repository.update_onboarding_run_metadata(
            run_id,
            {
                "worker_current_step": "waiting_manual_llm",
                "manual_llm_request": {"stage": "semantic_model_drafting", "payload": manual_request},
            },
        )
        return StageResult(
            stage="semantic_model_drafting",
            current_stage="semantic_model_drafting",
            stage_status="paused",
            next_action="Waiting for manual semantic model drafting response in codex_manual mode.",
            run_status="paused",
        )

    generated_semantic_types: list[dict[str, Any]] = []
    generated_canonical_entities: list[dict[str, Any]] = []
    generated_canonical_attributes: list[dict[str, Any]] = []
    generated_proposal_ids: list[str] = []
    bundle_order = 40

    semantic_type_by_name = {_normalize_registry_name(str(item.get("name") or "")): item for item in semantic_types}
    canonical_entity_by_name = {_normalize_registry_name(str(item.get("name") or "")): item for item in canonical_entities}
    canonical_attribute_by_semantic = {
        str(item.get("semantic_type_id") or ""): item
        for item in canonical_attributes
        if str(item.get("semantic_type_id") or "")
    }

    for suggestion in draft_payload.get("suggestions", []):
        if not isinstance(suggestion, dict):
            continue
        semantic_type_name = str(suggestion.get("semantic_type_name") or "").strip()
        if not semantic_type_name:
            continue
        semantic_key = _normalize_registry_name(semantic_type_name)
        semantic_record = semantic_type_by_name.get(semantic_key)
        should_create_semantic = semantic_record is None and str(suggestion.get("status") or "") != "matched_existing"
        if should_create_semantic:
            created = repository.create_semantic_type(
                {
                    "name": semantic_type_name,
                    "description": _derive_semantic_type_description(suggestion, source),
                    "datatype": _normalized_datatype(_field_data_type(fields, str(suggestion.get("field_id") or ""))),
                    "entity_kind": "attribute",
                    "status": "draft",
                    "aliases": [str(suggestion.get("raw_name") or "")] if str(suggestion.get("raw_name") or "") else [],
                    "owners": ["semantic-platform-worker"],
                    "tags": ["auto_draft", "semantic_model"],
                    "documentation": _derive_semantic_type_description(suggestion, source),
                }
            )
            semantic_record = created["semantic_type"]
            semantic_type_by_name[semantic_key] = semantic_record
            generated_semantic_types.append(semantic_record)
            proposal = created.get("proposal")
            if proposal and bundle is not None:
                generated_proposal_ids.append(str(proposal["id"]))
                repository.add_proposal_to_bundle(str(bundle["id"]), str(proposal["id"]), item_order=bundle_order)
                bundle_order += 1

        semantic_type_id = str((semantic_record or {}).get("id") or "")
        if not semantic_type_id:
            continue

        if canonical_attribute_by_semantic.get(semantic_type_id) is not None:
            continue

        entity_name = str(suggestion.get("proposed_canonical_entity_name") or "").strip()
        attribute_name = str(suggestion.get("proposed_canonical_attribute_name") or "").strip()
        if not entity_name or not attribute_name:
            entity_name, attribute_name = _derive_canonical_names(semantic_type_name)
        entity_key = _normalize_registry_name(entity_name)
        entity_record = canonical_entity_by_name.get(entity_key)
        if entity_record is None:
            created_entity = repository.create_canonical_entity(
                {
                    "name": entity_name,
                    "description": f"Auto-drafted canonical entity for {semantic_type_name}.",
                    "status": "draft",
                    "metadata": {"generated_by": "semantic_model_drafting"},
                }
            )
            entity_record = created_entity["canonical_entity"]
            canonical_entity_by_name[entity_key] = entity_record
            generated_canonical_entities.append(entity_record)
            proposal = created_entity.get("proposal")
            if proposal and bundle is not None:
                generated_proposal_ids.append(str(proposal["id"]))
                repository.add_proposal_to_bundle(str(bundle["id"]), str(proposal["id"]), item_order=bundle_order)
                bundle_order += 1

        if entity_record is None:
            continue

        created_attribute = repository.create_canonical_attribute(
            {
                "entity_id": str(entity_record["id"]),
                "semantic_type_id": semantic_type_id,
                "name": attribute_name,
                "description": f"Auto-drafted canonical attribute for {semantic_type_name}.",
                "datatype": _normalized_datatype(_field_data_type(fields, str(suggestion.get("field_id") or ""))),
                "identity_role": "",
                "status": "draft",
                "metadata": {"generated_by": "semantic_model_drafting"},
            }
        )
        attribute_record = created_attribute["canonical_attribute"]
        canonical_attribute_by_semantic[semantic_type_id] = attribute_record
        generated_canonical_attributes.append(attribute_record)
        proposal = created_attribute.get("proposal")
        if proposal and bundle is not None:
            generated_proposal_ids.append(str(proposal["id"]))
            repository.add_proposal_to_bundle(str(bundle["id"]), str(proposal["id"]), item_order=bundle_order)
            bundle_order += 1

    repository.update_work_queue_task(
        stage_task["id"],
        status="open",
        draft_status="draft_ready",
        draft_payload={
            **draft_payload,
            "semantic_evidence_clusters": [
                {
                    "field_id": item.get("field_id"),
                    "raw_name": item.get("raw_name"),
                    "field_path": item.get("field_path"),
                    "cluster_summary": item.get("cluster_summary"),
                    "status": item.get("status"),
                    "top_registry_candidate": item.get("top_registry_candidate"),
                }
                for item in draft_payload.get("suggestions", [])
                if isinstance(item, dict)
            ],
            "generated_semantic_types": [
                {"id": item.get("id"), "name": item.get("name"), "status": item.get("status")}
                for item in generated_semantic_types
            ],
            "generated_canonical_entities": [
                {"id": item.get("id"), "name": item.get("name"), "status": item.get("status")}
                for item in generated_canonical_entities
            ],
            "generated_canonical_attributes": [
                {"id": item.get("id"), "name": item.get("name"), "entity_id": item.get("entity_id"), "status": item.get("status")}
                for item in generated_canonical_attributes
            ],
        },
        draft_rationale="Semantic model drafts generated from extracted field evidence and semantic registry retrieval candidates.",
        draft_confidence=0.78 if draft_payload["matched_existing_count"] else 0.56,
        recommended_action="Review semantic meaning and canonical model candidates, approve semantic model, then continue to binding drafting.",
        payload={
            **(stage_task.get("payload") or {}),
            "source_id": source["id"],
            "matched_existing_count": draft_payload["matched_existing_count"],
            "proposed_new_count": draft_payload["proposed_new_count"],
            "generated_semantic_type_count": len(generated_semantic_types),
            "generated_canonical_entity_count": len(generated_canonical_entities),
            "generated_canonical_attribute_count": len(generated_canonical_attributes),
            "generated_proposal_ids": generated_proposal_ids,
        },
    )
    if bundle is not None:
        repository.update_proposal_bundle_summary(
            str(bundle["id"]),
            {
                "semantic_model_suggestions": draft_payload["matched_existing_count"] + draft_payload["proposed_new_count"],
                "semantic_model_reused_registry_candidates": draft_payload["matched_existing_count"],
                "semantic_model_new_concepts": draft_payload["proposed_new_count"],
                "generated_semantic_types": len(generated_semantic_types),
                "generated_canonical_entities": len(generated_canonical_entities),
                "generated_canonical_attributes": len(generated_canonical_attributes),
            },
            status="pending_review"
            if generated_semantic_types or generated_canonical_entities or generated_canonical_attributes
            else None,
        )
    _open_stage_tasks(repository, run_id, "semantic_model_approval")
    approval_task = _find_stage_task(repository.list_work_queue_tasks(run_id=run_id), "semantic_model_approval")
    if approval_task is not None:
        repository.update_work_queue_task(
            str(approval_task["id"]),
            payload={
                **(approval_task.get("payload") or {}),
                "source_id": source["id"],
                "semantic_model_proposal_ids": generated_proposal_ids,
            },
            recommended_action="Approve semantic model proposals to apply them to the authoring registry and unlock binding drafts.",
        )
    repository.update_onboarding_run_metadata(
        run_id,
        {
            "worker_current_step": "semantic_model_drafts_ready",
            "worker_completed_stages": [
                "source_review",
                "asset_discovery",
                "structure_review",
                "semantic_model_drafting",
            ],
        },
    )
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="semantic_model_approval",
        stage_status="pending",
        next_action="Approve semantic model before binding drafting can begin.",
        status="in_review",
    )
    return StageResult(
        stage="semantic_model_drafting",
        current_stage="semantic_model_approval",
        stage_status="pending",
        next_action="Approve semantic model before binding drafting can begin.",
        run_status="in_review",
    )


def run_stage_binding_drafting(repository: SemanticLayerRepository, run_id: str) -> StageResult:
    run, source, stage_task = _load_stage_context(repository, run_id, "binding_drafting")
    metadata = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
    operations = repository.list_execution_operations(source_id=source["id"])
    operation_ids = {str(item.get("id") or "") for item in operations}
    fields = [item for item in repository.list_operation_fields() if str(item.get("operation_id") or "") in operation_ids]
    existing_mappings = [item for item in repository.list_field_mappings() if str(item.get("source_id") or "") == source["id"]]
    semantic_types = repository.list_semantic_types(status="approved")
    canonical_attributes = repository.list_canonical_attributes(status="approved")
    bundle = next((item for item in repository.list_proposal_bundles() if item.get("run_id") == run_id), None)
    manual_llm_responses = metadata.get("manual_llm_responses") if isinstance(metadata.get("manual_llm_responses"), dict) else {}
    manual_response = manual_llm_responses.get("binding_drafting") or manual_llm_responses.get("semantic_mapping")

    draft_payload = generate_semantic_mapping_drafts(
        fields=fields,
        semantic_types=semantic_types,
        canonical_attributes=canonical_attributes,
        existing_mappings=existing_mappings,
        manual_llm_response=manual_response if isinstance(manual_response, dict) else None,
    )

    if draft_payload.get("status") == "waiting_manual_llm":
        manual_request = build_manual_semantic_mapping_request(
            run_id=run_id,
            source=source,
            fields=fields,
            semantic_types=semantic_types,
            canonical_attributes=canonical_attributes,
        )
        repository.update_work_queue_task(
            stage_task["id"],
            status="blocked",
            draft_status="waiting_manual_llm",
            draft_payload={"manual_llm_request": manual_request},
            draft_rationale="Waiting for codex_manual binding drafting response.",
            draft_confidence=0.0,
            recommended_action="Submit manual_llm_response for binding_drafting, then resume workspace.",
            payload={**(stage_task.get("payload") or {}), "manual_llm_required": True},
        )
        repository.update_onboarding_run_stage(
            run_id,
            current_stage="binding_drafting",
            stage_status="paused",
            next_action="Waiting for manual binding drafting response in codex_manual mode.",
            status="paused",
        )
        repository.update_onboarding_run_metadata(
            run_id,
            {
                "worker_current_step": "waiting_manual_llm",
                "manual_llm_request": {"stage": "binding_drafting", "payload": manual_request},
            },
        )
        return StageResult(
            stage="binding_drafting",
            current_stage="binding_drafting",
            stage_status="paused",
            next_action="Waiting for manual binding drafting response in codex_manual mode.",
            run_status="paused",
        )

    created_proposals = 0
    generated_proposal_ids: list[str] = []
    created_mappings = 0
    for suggestion in draft_payload["suggestions"]:
        semantic_type_id = suggestion.get("semantic_type_id")
        if not semantic_type_id:
            continue
        try:
            created = repository.create_field_mapping(
                {
                    "source_id": source["id"],
                    "operation_id": suggestion["operation_id"],
                    "field_id": suggestion["field_id"],
                    "field_path": suggestion["field_path"],
                    "semantic_type_id": semantic_type_id,
                    "canonical_attribute_id": suggestion.get("canonical_attribute_id"),
                    "mapping_kind": suggestion.get("mapping_kind") or "field_semantic",
                    "mapping_type": suggestion.get("mapping_type") or "exact",
                    "status": "draft",
                    "lifecycle": "review",
                    "namespace": "public",
                    "transform_spec": {},
                    "enum_mapping": {},
                    "notes": suggestion.get("rationale") or "",
                    "created_by": "semantic-platform-worker",
                    "evidence": suggestion.get("evidence_refs") or [],
                    "confidence": suggestion.get("confidence"),
                    "proposal_context": {
                        "depends_on_proposal_ids": suggestion.get("depends_on_proposal_ids") or [],
                        "resolution_basis": suggestion.get("resolution_basis") or "missing",
                        "dependency_status": suggestion.get("dependency_status") or "blocked",
                        "review_impact": suggestion.get("review_impact") or ["blocks_mapping"],
                    },
                }
            )
        except ValueError:
            continue
        created_mappings += 1
        proposal = created.get("proposal")
        if proposal and bundle is not None:
            created_proposals += 1
            generated_proposal_ids.append(str(proposal["id"]))
            repository.add_proposal_to_bundle(str(bundle["id"]), str(proposal["id"]), item_order=80 + created_proposals)

    if bundle is not None:
        repository.update_proposal_bundle_summary(
            str(bundle["id"]),
            {
                "binding_suggestions": draft_payload["matched_count"] + draft_payload["unresolved_count"],
                "binding_matched": draft_payload["matched_count"],
                "binding_unresolved": draft_payload["unresolved_count"],
                "generated_mapping_proposals": created_proposals,
            },
            status="pending_review" if created_proposals else None,
        )

    repository.update_work_queue_task(
        stage_task["id"],
        status="open",
        draft_status="draft_ready",
        draft_payload=draft_payload,
        draft_rationale="Binding drafts generated from approved semantic model.",
        draft_confidence=0.74 if draft_payload["matched_count"] else 0.51,
        recommended_action="Review source binding drafts, approve source binding, then continue to bundle review.",
        payload={
            **(stage_task.get("payload") or {}),
            "source_id": source["id"],
            "generated_mapping_count": created_mappings,
            "generated_proposal_count": created_proposals,
            "generated_proposal_ids": generated_proposal_ids,
        },
    )
    _open_stage_tasks(repository, run_id, "binding_approval")
    approval_task = _find_stage_task(repository.list_work_queue_tasks(run_id=run_id), "binding_approval")
    if approval_task is not None:
        repository.update_work_queue_task(
            str(approval_task["id"]),
            payload={
                **(approval_task.get("payload") or {}),
                "source_id": source["id"],
                "binding_proposal_ids": generated_proposal_ids,
            },
            recommended_action="Approve source binding proposals to apply them to the authoring registry before bundle review.",
        )
    _prepare_scaffold_stage(repository, run_id, "proposal_review", "Review proposal bundle drafts and rationale.")
    _prepare_scaffold_stage(repository, run_id, "publish_readiness", "Validate publish readiness after review is complete.")
    repository.update_onboarding_run_metadata(
        run_id,
        {
            "worker_current_step": "binding_drafts_ready",
            "worker_completed_stages": [
                "source_review",
                "asset_discovery",
                "structure_review",
                "semantic_model_drafting",
                "binding_drafting",
            ],
            "manual_llm_request": {},
        },
    )
    repository.update_onboarding_run_stage(
        run_id,
        current_stage="binding_approval",
        stage_status="pending",
        next_action="Approve source bindings before bundle review.",
        status="in_review",
    )
    return StageResult(
        stage="binding_drafting",
        current_stage="binding_approval",
        stage_status="pending",
        next_action="Approve source bindings before bundle review.",
        run_status="in_review",
    )


def _prepare_scaffold_stage(repository: SemanticLayerRepository, run_id: str, stage: str, recommended_action: str) -> None:
    task = _find_stage_task(repository.list_work_queue_tasks(run_id=run_id), stage)
    if task is None:
        return
    draft = build_task_draft(task)
    repository.update_work_queue_task(
        str(task["id"]),
        status="open",
        draft_status="draft_ready",
        draft_payload=draft["draft_payload"],
        draft_rationale=draft["draft_rationale"],
        draft_confidence=draft["draft_confidence"],
        recommended_action=recommended_action,
        payload={**(task.get("payload") or {}), "generated_by": "scaffold_worker"},
    )


def _open_stage_tasks(repository: SemanticLayerRepository, run_id: str, stage: str) -> None:
    tasks = repository.list_work_queue_tasks(run_id=run_id)
    for task in tasks:
        if str(task.get("stage") or "") != stage:
            continue
        repository.update_work_queue_task(str(task["id"]), status="open")


def _load_stage_context(
    repository: SemanticLayerRepository,
    run_id: str,
    stage: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    run = repository.get_onboarding_run(run_id)
    if run is None:
        raise KeyError(f"onboarding run not found: {run_id}")
    source_id = str(run.get("source_id") or "")
    source = repository.get_execution_source(source_id)
    if source is None:
        raise KeyError(f"execution source not found for run: {run_id}")
    task = _find_stage_task(repository.list_work_queue_tasks(run_id=run_id), stage)
    if task is None:
        raise KeyError(f"stage task not found: {stage}")
    return run, source, task


def _find_stage_task(tasks: list[dict[str, Any]], stage: str) -> dict[str, Any] | None:
    return next((task for task in tasks if str(task.get("stage") or "") == stage), None)


def _namespaced_operation_key(source_id: str, operation_key: str) -> str:
    normalized = operation_key.strip().replace(" ", "_").replace("/", "_")
    return f"{source_id}__{normalized}"


def _normalize_registry_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _derive_semantic_type_name(suggestion: dict[str, Any]) -> str:
    raw_name = str(suggestion.get("raw_name") or suggestion.get("field_path") or "").strip()
    if not raw_name:
        return ""
    normalized = raw_name.replace("[", "_").replace("]", "_").replace(".", "_").replace("-", "_").replace("/", "_")
    parts = [part for part in normalized.split("_") if part]
    return "_".join(part.upper() for part in parts[:6])


def _derive_semantic_type_description(suggestion: dict[str, Any], source: dict[str, Any]) -> str:
    field_path = str(suggestion.get("field_path") or suggestion.get("raw_name") or "")
    source_name = str(source.get("name") or source.get("id") or "source")
    return f"Auto-drafted semantic type candidate from `{field_path}` in `{source_name}`."


def _derive_canonical_names(semantic_type_name: str) -> tuple[str, str]:
    parts = [part for part in semantic_type_name.split("_") if part]
    if len(parts) >= 2:
      entity_parts = parts[:-1]
      attribute_part = parts[-1]
    else:
      entity_parts = ["Record"]
      attribute_part = parts[0] if parts else "value"
    entity_name = "".join(part.title() for part in entity_parts) or "Record"
    attribute_name = attribute_part.lower()
    return entity_name, attribute_name


def _field_data_type(fields: list[dict[str, Any]], field_id: str) -> str:
    match = next((item for item in fields if str(item.get("id") or "") == field_id), None)
    return str((match or {}).get("data_type") or "string")


def _normalized_datatype(value: str) -> str:
    lowered = value.strip().lower()
    if lowered in {"string", "integer", "number", "boolean", "date", "datetime", "array", "object"}:
        return lowered
    if lowered in {"int", "long", "short"}:
        return "integer"
    if lowered in {"float", "double", "decimal"}:
        return "number"
    if lowered in {"bool"}:
        return "boolean"
    return "string"
