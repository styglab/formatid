from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prefect.deployments import run_deployment

from services.semantic_platform.adapters.worker.deployments import ONBOARDING_DEPLOYMENT_NAME, ONBOARDING_FLOW_NAME
from services.semantic_platform.internal.context import build_runtime_context
from services.semantic_platform.internal.storage import SemanticLayerRepository


app = FastAPI(title="Semantic Platform Admin API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8018",
        "http://localhost:8018",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_ONBOARDING_STAGE_ORDER = [
    "source_review",
    "asset_discovery",
    "structure_review",
    "semantic_mapping",
    "controls_and_variants",
    "operation_and_binding_modeling",
    "proposal_review",
    "publish_readiness",
]


class SemanticTypePayload(BaseModel):
    name: str | None = Field(default=None)
    description: str = ""
    datatype: str = "string"
    entity_kind: str = "entity"
    parent_entity_id: str = ""
    semantic_role: str = ""
    aliases: list[str] = Field(default_factory=list)
    owners: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    documentation: str = ""
    status: str = "draft"


class RelationshipPayload(BaseModel):
    source_id: str | None = None
    target_id: str
    relation_type: str


class ExecutionSourcePayload(BaseModel):
    name: str | None = Field(default=None)
    provider: str = ""
    source_type: str = "api"
    description: str = ""
    status: str = "draft"
    config: dict[str, Any] = Field(default_factory=dict)


class CapabilityPayload(BaseModel):
    capability_key: str | None = Field(default=None)
    namespace: str = "public"
    name: str | None = Field(default=None)
    description: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    intent_spec: dict[str, Any] = Field(default_factory=dict)
    input_semantic_types: list[str] = Field(default_factory=list)
    output_semantic_types: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class FieldMappingPayload(BaseModel):
    field_id: str | None = None
    source_id: str | None = None
    operation_id: str
    variant_id: str | None = None
    access_path_id: str | None = None
    field_path: str
    semantic_type_id: str
    canonical_attribute_id: str | None = None
    mapping_kind: str = "direct"
    mapping_type: str = "exact"
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    namespace: str = "public"
    transform_spec: dict[str, Any] = Field(default_factory=dict)
    enum_mapping: dict[str, Any] = Field(default_factory=dict)
    notes: str = ""
    confidence: float | None = None



class FieldMappingUpdatePayload(BaseModel):
    field_id: str | None = None
    source_id: str | None = None
    operation_id: str | None = None
    variant_id: str | None = None
    access_path_id: str | None = None
    field_path: str | None = None
    semantic_type_id: str | None = None
    canonical_attribute_id: str | None = None
    mapping_kind: str | None = None
    mapping_type: str | None = None
    version: str | None = None
    lifecycle: str | None = None
    status: str | None = None
    namespace: str | None = None
    transform_spec: dict[str, Any] | None = None
    enum_mapping: dict[str, Any] | None = None
    notes: str | None = None
    confidence: float | None = None


class MappingSuggestionPayload(BaseModel):
    semantic_type_id: str | None = None


class OnboardingTaskDraftPayload(BaseModel):
    reviewer: str = "dashboard"


class OnboardingTaskCompletePayload(BaseModel):
    reviewer: str = "dashboard"


class OnboardingRunResumePayload(BaseModel):
    reviewer: str = "dashboard"

class ReviewPayload(BaseModel):
    reviewer: str = "admin"


class CanonicalEntityPayload(BaseModel):
    semantic_type_id: str | None = None
    name: str | None = Field(default=None)
    namespace: str = "public"
    description: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalAttributePayload(BaseModel):
    entity_id: str | None = None
    semantic_type_id: str | None = None
    name: str | None = Field(default=None)
    namespace: str = "public"
    description: str = ""
    datatype: str = "string"
    identity_role: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalRelationPayload(BaseModel):
    source_entity_id: str | None = None
    target_entity_id: str | None = None
    relation_type: str | None = None
    forward_label: str = ""
    reverse_label: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    metadata: dict[str, Any] = Field(default_factory=dict)


class OperationVariantPayload(BaseModel):
    operation_id: str | None = None
    variant_key: str | None = None
    name: str | None = None
    description: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    fixed_semantic_arguments: dict[str, Any] = Field(default_factory=dict)
    fixed_raw_arguments: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


UPLOAD_ROOT = Path("/tmp/semantic_platform_uploads")


@app.on_event("startup")
def ensure_control_plane_schema() -> None:
    SemanticLayerRepository().ensure_control_plane_schema()


def _paged(items: list[dict[str, Any]], page: int, page_size: int) -> dict[str, Any]:
    safe_page = max(page, 1)
    safe_page_size = max(min(page_size, 100), 1)
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    return {
        "items": items[start:end],
        "total": len(items),
        "page": safe_page,
        "page_size": safe_page_size,
    }


@app.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/context")
def context() -> dict[str, object]:
    return build_runtime_context()


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    return SemanticLayerRepository().overview()


@app.get("/semantic/catalog")
def semantic_catalog() -> dict[str, object]:
    return SemanticLayerRepository().semantic_catalog()


@app.post("/semantic-types/seed")
def seed_semantic_types() -> dict[str, object]:
    return SemanticLayerRepository().seed_semantic_type_registry()


@app.get("/api/semantic-types")
def list_semantic_types(
    query: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_semantic_types(query=query, status=status)


@app.get("/api/semantic-types/page")
def list_semantic_types_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _paged(SemanticLayerRepository().list_semantic_types(query=query, status=status), page, page_size)


@app.get("/api/execution-sources")
def list_execution_sources(
    query: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_execution_sources(query=query, status=status)


@app.get("/api/execution-sources/page")
def list_execution_sources_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _paged(SemanticLayerRepository().list_execution_sources(query=query, status=status), page, page_size)


@app.get("/api/execution-assets")
def list_execution_assets(
    query: str = Query(default=""),
    status: str = Query(default=""),
    source_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_execution_assets(query=query, status=status, source_id=source_id)


@app.get("/api/execution-assets/{asset_id}")
def get_execution_asset(asset_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_execution_asset(asset_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution asset not found")
    return record


@app.get("/api/execution-operations")
def list_execution_operations(
    query: str = Query(default=""),
    status: str = Query(default=""),
    source_id: str = Query(default=""),
    asset_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_execution_operations(
        query=query,
        status=status,
        source_id=source_id,
        asset_id=asset_id,
    )


@app.get("/api/execution-operations/page")
def list_execution_operations_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    source_id: str = Query(default=""),
    asset_id: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _paged(
        SemanticLayerRepository().list_execution_operations(
            query=query,
            status=status,
            source_id=source_id,
            asset_id=asset_id,
        ),
        page,
        page_size,
    )


@app.get("/api/execution-operations/{operation_id}")
def get_execution_operation(operation_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_execution_operation(operation_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution operation not found")
    return record


@app.get("/api/operation-fields")
def list_operation_fields(
    operation_id: str = Query(default=""),
    variant_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_operation_fields(operation_id=operation_id, variant_id=variant_id)


@app.get("/api/onboarding-runs")
def list_onboarding_runs() -> list[dict[str, Any]]:
    repository = SemanticLayerRepository()
    records = repository.list_onboarding_runs()
    sources = repository.list_execution_sources()
    operations = repository.list_execution_operations()
    fields = repository.list_operation_fields()
    mappings = repository.list_field_mappings()
    proposals = repository.list_proposals()
    derived = _build_onboarding_runs(sources, operations, fields, mappings, proposals)
    real_source_ids = {str(item.get("source_id") or "") for item in records}
    merged = records + [item for item in derived if str(item.get("source_id") or "") not in real_source_ids]
    return sorted(merged, key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)


@app.get("/api/onboarding-runs/{run_id}")
def get_onboarding_run(run_id: str) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    records = repository.list_onboarding_runs()
    selected = next((item for item in records if item.get("id") == run_id), None)
    if selected is not None:
        return _build_onboarding_run_detail_from_record(repository, selected)

    sources = repository.list_execution_sources()
    operations = repository.list_execution_operations()
    fields = repository.list_operation_fields()
    mappings = repository.list_field_mappings()
    proposals = repository.list_proposals()
    derived = _build_onboarding_runs(sources, operations, fields, mappings, proposals)
    selected = next((item for item in derived if item.get("id") == run_id), None)
    if selected is None:
        raise HTTPException(status_code=404, detail="onboarding run not found")
    return _build_onboarding_run_detail_from_record(repository, selected)


@app.post("/api/onboarding-runs/{run_id}/resume")
def resume_onboarding_run(run_id: str, payload: OnboardingRunResumePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    run = repository.get_onboarding_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="onboarding run not found")
    updated = repository.update_onboarding_run_stage(
        run_id,
        stage_status="in_progress",
        next_action=f"Run resumed by {payload.reviewer}. Waiting for worker orchestration.",
        status="started",
    )
    trigger = _trigger_onboarding_prefect_run(run_id)
    return {
        "run": updated,
        "trigger": trigger,
    }


@app.post("/api/onboarding-tasks/{task_id}/generate-draft")
def generate_onboarding_task_draft(task_id: str, payload: OnboardingTaskDraftPayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    task = repository.get_work_queue_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="onboarding task not found")
    draft = _build_task_draft(task)
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


@app.post("/api/onboarding-tasks/{task_id}/complete")
def complete_onboarding_task(task_id: str, payload: OnboardingTaskCompletePayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    task = repository.get_work_queue_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="onboarding task not found")
    updated_task = repository.update_work_queue_task(
        task_id,
        status="completed",
        assigned_to=payload.reviewer,
        recommended_action="Task completed. Review next stage task or resume worker.",
    )
    if updated_task is None:
        raise HTTPException(status_code=404, detail="onboarding task not found")
    updated_run = _advance_run_after_task_completion(repository, updated_task["run_id"])
    return {"task": updated_task, "run": updated_run}


@app.get("/api/proposal-bundles")
def list_proposal_bundles() -> list[dict[str, Any]]:
    repository = SemanticLayerRepository()
    records = repository.list_proposal_bundles()
    sources = repository.list_execution_sources()
    operations = repository.list_execution_operations()
    fields = repository.list_operation_fields()
    mappings = repository.list_field_mappings()
    proposals = repository.list_proposals()
    runs = _build_onboarding_runs(sources, operations, fields, mappings, proposals)
    derived = [_build_proposal_bundle(run, proposals) for run in runs]
    real_source_ids = {str(item.get("source_id") or "") for item in records}
    merged = records + [item for item in derived if str(item.get("source_id") or "") not in real_source_ids]
    return sorted(merged, key=lambda item: str(item.get("updated_at") or ""), reverse=True)


@app.get("/api/capability-bindings")
def list_capability_bindings() -> list[dict[str, Any]]:
    repository = SemanticLayerRepository()
    capabilities = repository.list_capabilities()
    operations = repository.list_execution_operations()
    variants = repository.list_operation_variants()
    mappings = repository.list_field_mappings()
    return _build_capability_bindings(capabilities, operations, variants, mappings)


@app.post("/api/operation-fields/{field_id}/mapping-suggestion")
def suggest_operation_field_mapping(field_id: str, payload: MappingSuggestionPayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    operation_field = next((item for item in repository.list_operation_fields() if item.get("id") == field_id), None)
    if operation_field is None:
        raise HTTPException(status_code=404, detail="operation field not found")
    semantic_types = repository.list_semantic_types()
    semantic_suggestions = _suggest_semantic_types(operation_field, semantic_types)
    selected_semantic_type = next(
        (item for item in semantic_types if item.get("id") == payload.semantic_type_id),
        None,
    )
    if selected_semantic_type is None and semantic_suggestions:
        selected_semantic_type = next(
            (item for item in semantic_types if item.get("id") == semantic_suggestions[0]["semantic_type_id"]),
            None,
        )
    field_path = str(operation_field.get("field_path") or operation_field.get("raw_name") or "")
    draft_mapping = {
        "id": "",
        "field_id": operation_field["id"],
        "source_id": None,
        "operation_id": operation_field["operation_id"],
        "variant_id": operation_field.get("variant_id"),
        "field_path": field_path,
        "semantic_type_id": (selected_semantic_type or {}).get("id") or "",
        "mapping_type": "exact",
        "mapping_kind": "direct",
        "transform_spec": {},
        "enum_mapping": {},
        "evidence": [],
    }
    transform_suggestion = (
        _build_transform_suggestion(draft_mapping, selected_semantic_type, operation_field)
        if selected_semantic_type is not None
        else None
    )
    return {
        "mode": "deterministic_assist",
        "field_id": field_id,
        "semantic_type_suggestions": semantic_suggestions,
        "transform_suggestion": transform_suggestion,
    }


@app.get("/api/operation-variants")
def list_operation_variants(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_operation_variants(query=query, status=status, operation_id=operation_id)


@app.get("/api/operation-variants/page")
def list_operation_variants_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _paged(
        SemanticLayerRepository().list_operation_variants(query=query, status=status, operation_id=operation_id),
        page,
        page_size,
    )


@app.post("/api/operation-variants")
def create_operation_variant(payload: OperationVariantPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_operation_variant(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/operation-variants/{variant_id}")
def update_operation_variant(variant_id: str, payload: OperationVariantPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_operation_variant(variant_id, _payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="operation variant not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/operation-variants/{variant_id}")
def delete_operation_variant(variant_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_operation_variant(variant_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="operation variant not found") from exc


@app.get("/api/capabilities")
def list_capabilities(
    query: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_capabilities(query=query, status=status)


@app.get("/api/capabilities/page")
def list_capabilities_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _paged(SemanticLayerRepository().list_capabilities(query=query, status=status), page, page_size)


@app.post("/api/capabilities")
def create_capability(payload: CapabilityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_capability(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/capabilities/{capability_id}")
def get_capability(capability_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_capability(capability_id)
    if record is None:
        raise HTTPException(status_code=404, detail="capability not found")
    return record


@app.patch("/api/capabilities/{capability_id}")
def update_capability(capability_id: str, payload: CapabilityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_capability(capability_id, _payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="capability not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/capabilities/{capability_id}")
def delete_capability(capability_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_capability(capability_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="capability not found") from exc


@app.get("/api/mappings")
def list_mappings(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_field_mappings(query=query, status=status, operation_id=operation_id)


@app.get("/api/mappings/page")
def list_mappings_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return _paged(
        SemanticLayerRepository().list_field_mappings(query=query, status=status, operation_id=operation_id),
        page,
        page_size,
    )


@app.get("/api/mappings/exists")
def mapping_exists(
    operation_id: str = Query(default=""),
    field_path: str = Query(default=""),
    exclude_mapping_id: str = Query(default=""),
) -> dict[str, Any]:
    return SemanticLayerRepository().field_mapping_exists(
        operation_id=operation_id,
        field_path=field_path,
        exclude_mapping_id=exclude_mapping_id,
    )


@app.post("/api/mappings")
def create_mapping(payload: FieldMappingPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_field_mapping(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/mappings/{mapping_id}")
def get_mapping(mapping_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_field_mapping(mapping_id)
    if record is None:
        raise HTTPException(status_code=404, detail="mapping not found")
    return record


@app.post("/api/mappings/{mapping_id}/transform-suggestion")
def suggest_mapping_transform(mapping_id: str) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    mapping = repository.get_field_mapping(mapping_id)
    if mapping is None:
        raise HTTPException(status_code=404, detail="mapping not found")
    semantic_type = next(
        (item for item in repository.list_semantic_types() if item.get("id") == mapping.get("semantic_type_id")),
        None,
    )
    operation_field = next(
        (
            item
            for item in repository.list_operation_fields(operation_id=mapping.get("operation_id", ""))
            if item.get("id") == mapping.get("field_id")
            or str(item.get("field_path") or item.get("raw_name") or "") == str(mapping.get("field_path") or "")
        ),
        None,
    )
    return _build_transform_suggestion(mapping, semantic_type, operation_field)


@app.patch("/api/mappings/{mapping_id}")
def update_mapping(mapping_id: str, payload: FieldMappingUpdatePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_field_mapping(mapping_id, _payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="mapping not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/mappings/{mapping_id}")
def delete_mapping(mapping_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_field_mapping(mapping_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="mapping not found") from exc


@app.post("/api/execution-sources")
def create_execution_source(payload: ExecutionSourcePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_execution_source(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/execution-sources/upload")
async def upload_execution_source(
    file: UploadFile = File(...),
    name: str = Form(...),
    provider: str = Form(default=""),
    source_type: str = Form(default="api"),
    description: str = Form(default=""),
    status: str = Form(default="draft"),
    reference_uri: str = Form(default=""),
    manual_notes: str = Form(default=""),
) -> dict[str, Any]:
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="uploaded file is empty")

    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    safe_name = Path(file.filename or f"upload-{uuid4().hex}").name
    stored_name = f"{uuid4().hex}-{safe_name}"
    stored_path = UPLOAD_ROOT / stored_name
    stored_path.write_bytes(content)

    preview = _text_preview(content)
    content_hash = hashlib.sha256(content).hexdigest()
    upload_metadata = {
        "filename": safe_name,
        "media_type": file.content_type or "application/octet-stream",
        "stored_path": str(stored_path),
        "size_bytes": len(content),
        "sha256": content_hash,
        "preview": preview,
        "suggestion_generation": {
            "mode": "deterministic_assist",
            "status": "ready_for_field_extraction",
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "note": "Field-level semantic and transform suggestions are generated after operation fields are extracted.",
        },
    }
    config = {
        "input_mode": "uploaded_file",
        "reference_uri": reference_uri or str(stored_path),
        "manual_notes": manual_notes,
        "upload": upload_metadata,
    }
    payload = {
        "name": name,
        "provider": provider,
        "source_type": source_type,
        "description": description or f"Uploaded source document: {safe_name}",
        "status": status,
        "config": config,
    }
    try:
        repository = SemanticLayerRepository()
        created = repository.create_execution_source(payload)
        onboarding = repository.create_onboarding_run_for_source(
            source=created.get("execution_source"),
            proposal=created.get("proposal"),
            upload_metadata=upload_metadata,
        )
        trigger = _trigger_onboarding_prefect_run(str((onboarding.get("onboarding_run") or {}).get("id") or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "source": created.get("execution_source"),
        "proposal": created.get("proposal"),
        "onboarding_run": onboarding.get("onboarding_run"),
        "evidence_snapshot": onboarding.get("evidence_snapshot"),
        "proposal_bundle": onboarding.get("proposal_bundle"),
        "work_queue_tasks": onboarding.get("work_queue_tasks") or [],
        "prefect_trigger": trigger,
        "upload": upload_metadata,
    }


@app.get("/api/execution-sources/{source_id}")
def get_execution_source(source_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_execution_source(source_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution source not found")
    return record


@app.patch("/api/execution-sources/{source_id}")
def update_execution_source(source_id: str, payload: ExecutionSourcePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_execution_source(
            source_id,
            _payload_dict(payload, exclude_unset=True),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="execution source not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/execution-sources/{source_id}")
def delete_execution_source(source_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_execution_source(source_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="execution source not found") from exc


@app.post("/api/semantic-types")
def create_semantic_type(payload: SemanticTypePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_semantic_type(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/semantic-types/{semantic_type_id}")
def get_semantic_type(semantic_type_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_semantic_type(semantic_type_id)
    if record is None:
        raise HTTPException(status_code=404, detail="semantic type not found")
    return record


@app.patch("/api/semantic-types/{semantic_type_id}")
def update_semantic_type(semantic_type_id: str, payload: SemanticTypePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_semantic_type(
            semantic_type_id,
            _payload_dict(payload, exclude_unset=True),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="semantic type not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/semantic-types/{semantic_type_id}")
def delete_semantic_type(semantic_type_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_semantic_type(semantic_type_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="semantic type not found") from exc


@app.post("/api/semantic-types/{semantic_type_id}/relationships")
def add_relationship(semantic_type_id: str, payload: RelationshipPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().add_semantic_relationship(
            semantic_type_id,
            _payload_dict(payload, exclude_unset=False),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"semantic type not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/semantic-relationships/{relationship_id}")
def update_relationship(relationship_id: str, payload: RelationshipPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_semantic_relationship(
            relationship_id,
            _payload_dict(payload, exclude_unset=False),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"relationship or semantic type not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/semantic-relationships/{relationship_id}")
def delete_relationship(relationship_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_semantic_relationship(relationship_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="relationship not found") from exc


@app.get("/api/canonical-entities")
def list_canonical_entities(
    query: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_canonical_entities(query=query, status=status)


@app.post("/api/canonical-entities")
def create_canonical_entity(payload: CanonicalEntityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_canonical_entity(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/canonical-entities/{entity_id}")
def update_canonical_entity(entity_id: str, payload: CanonicalEntityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_canonical_entity(entity_id, _payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical entity not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/canonical-entities/{entity_id}")
def delete_canonical_entity(entity_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_canonical_entity(entity_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical entity not found") from exc


@app.get("/api/canonical-attributes")
def list_canonical_attributes(
    entity_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_canonical_attributes(entity_id=entity_id, status=status)


@app.post("/api/canonical-attributes")
def create_canonical_attribute(payload: CanonicalAttributePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_canonical_attribute(_payload_dict(payload, exclude_unset=False))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/canonical-attributes/{attribute_id}")
def update_canonical_attribute(attribute_id: str, payload: CanonicalAttributePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_canonical_attribute(attribute_id, _payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical attribute or entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/canonical-attributes/{attribute_id}")
def delete_canonical_attribute(attribute_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_canonical_attribute(attribute_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical attribute not found") from exc


@app.get("/api/canonical-relations")
def list_canonical_relations(
    entity_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_canonical_relations(entity_id=entity_id, status=status)


@app.post("/api/canonical-relations")
def create_canonical_relation(payload: CanonicalRelationPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_canonical_relation(_payload_dict(payload, exclude_unset=False))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/canonical-relations/{relation_id}")
def update_canonical_relation(relation_id: str, payload: CanonicalRelationPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_canonical_relation(relation_id, _payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical relation or entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/canonical-relations/{relation_id}")
def delete_canonical_relation(relation_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_canonical_relation(relation_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical relation not found") from exc


@app.get("/api/proposals")
def list_proposals(
    status: str = Query(default=""),
    entity_type: str = Query(default=""),
) -> list[dict[str, Any]]:
    proposals = SemanticLayerRepository().list_proposals(status=status)
    if entity_type:
        proposals = [item for item in proposals if item.get("entity_type") == entity_type]
    return proposals


@app.get("/api/proposals/page")
def list_proposals_page(
    status: str = Query(default=""),
    entity_type: str = Query(default=""),
    query: str = Query(default=""),
    ids: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    proposals = SemanticLayerRepository().list_proposals(status=status)
    if entity_type:
        proposals = [item for item in proposals if item.get("entity_type") == entity_type]
    proposal_ids = {item.strip() for item in ids.split(",") if item.strip()}
    if proposal_ids:
        proposals = [item for item in proposals if item.get("id") in proposal_ids]
    if query:
        lowered = query.lower()
        proposals = [
            item
            for item in proposals
            if lowered in str(item.get("id", "")).lower()
            or lowered in str(item.get("title", "")).lower()
            or lowered in str(item.get("entity_type", "")).lower()
            or lowered in str(item.get("entity_id", "")).lower()
        ]
    return _paged(proposals, page, page_size)


@app.get("/api/semantic-relationships")
def list_semantic_relationships(
    semantic_type_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_relationships(
        semantic_type_id=semantic_type_id,
        status=status,
    )


@app.post("/api/proposals/{proposal_id}/approve")
def approve_proposal(proposal_id: str, payload: ReviewPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().review_proposal(
            proposal_id,
            "approved",
            reviewer=payload.reviewer,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc


def _payload_dict(model: BaseModel, *, exclude_unset: bool) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_none=True, exclude_unset=exclude_unset)
    return model.dict(exclude_none=True, exclude_unset=exclude_unset)


def _build_onboarding_runs(
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

    proposals_by_source: dict[str, list[dict[str, Any]]] = {}
    for proposal in proposals:
        payload = proposal.get("payload") or {}
        source_id = str(payload.get("source_id") or proposal.get("entity_id") or "")
        proposals_by_source.setdefault(source_id, []).append(proposal)

    runs: list[dict[str, Any]] = []
    for source in sources:
        source_id = source["id"]
        source_operations = operations_by_source.get(source_id, [])
        operation_ids = {item["id"] for item in source_operations}
        source_fields = [field for operation_id in operation_ids for field in fields_by_operation.get(operation_id, [])]
        source_mappings = mappings_by_source.get(source_id, [])
        source_proposals = [
            proposal
            for proposal in proposals
            if (proposal.get("entity_id") == source_id)
            or str((proposal.get("payload") or {}).get("source_id") or "") == source_id
            or str((proposal.get("payload") or {}).get("operation_id") or "") in operation_ids
        ]
        config = source.get("config") or {}
        upload = config.get("upload") if isinstance(config, dict) else {}
        suggestion_generation = upload.get("suggestion_generation") if isinstance(upload, dict) else None
        run_id = f"run_{source_id}"
        runs.append(
            {
                "id": run_id,
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


def _build_proposal_bundle(run: dict[str, Any], proposals: list[dict[str, Any]]) -> dict[str, Any]:
    source_id = str(run.get("source_id") or "")
    source_proposals = [
        proposal
        for proposal in proposals
        if proposal.get("entity_id") == source_id
        or str((proposal.get("payload") or {}).get("source_id") or "") == source_id
    ]
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


def _build_onboarding_run_detail_from_record(
    repository: SemanticLayerRepository,
    run: dict[str, Any],
) -> dict[str, Any]:
    source_id = str(run.get("source_id") or "")
    source = next((item for item in repository.list_execution_sources() if item.get("id") == source_id), None)
    if source is None:
        raise HTTPException(status_code=404, detail="source not found for onboarding run")

    operations = repository.list_execution_operations(source_id=source_id)
    operation_ids = {str(item.get("id") or "") for item in operations}
    all_fields = repository.list_operation_fields()
    fields = [item for item in all_fields if str(item.get("operation_id") or "") in operation_ids]
    mappings = [item for item in repository.list_field_mappings() if str(item.get("source_id") or "") == source_id]
    proposals = [
        item
        for item in repository.list_proposals()
        if (item.get("entity_id") == source_id)
        or str((item.get("payload") or {}).get("source_id") or "") == source_id
        or str((item.get("payload") or {}).get("operation_id") or "") in operation_ids
    ]

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
        bundle = _build_proposal_bundle(run, proposals)

    return {
        "run": run,
        "source": source,
        "evidence_snapshots": evidence,
        "operations": operations,
        "fields": fields,
        "mappings": mappings,
        "work_queue_tasks": work_queue,
        "proposal_bundle": bundle,
        "proposals": proposals,
    }


def _build_task_draft(task: dict[str, Any]) -> dict[str, Any]:
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


def _advance_run_after_task_completion(repository: SemanticLayerRepository, run_id: str) -> dict[str, Any] | None:
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

    try:
        current_index = _ONBOARDING_STAGE_ORDER.index(current_stage)
    except ValueError:
        current_index = 0
    next_stage = _ONBOARDING_STAGE_ORDER[current_index + 1] if current_index + 1 < len(_ONBOARDING_STAGE_ORDER) else "publish_readiness"
    next_stage_tasks = [task for task in tasks if str(task.get("stage") or "") == next_stage]
    for next_task in next_stage_tasks:
        if next_task.get("status") == "blocked":
            repository.update_work_queue_task(
                str(next_task.get("id") or ""),
                status="open",
                recommended_action="Generate AI draft or complete this task to continue onboarding.",
            )
    next_action = "All onboarding stages completed. Ready for publish review." if next_stage == "publish_readiness" and not next_stage_tasks else f"Continue with {next_stage.replace('_', ' ')} tasks."
    stage_status = "completed" if next_stage == "publish_readiness" and not next_stage_tasks else "pending"
    return repository.update_onboarding_run_stage(
        run_id,
        current_stage=next_stage,
        stage_status=stage_status,
        next_action=next_action,
        status="in_review" if next_stage != "publish_readiness" else "ready_to_publish",
    )


def _trigger_onboarding_prefect_run(run_id: str) -> dict[str, Any]:
    if not run_id:
        return {"status": "skipped", "reason": "missing_run_id"}
    deployment_name = f"{ONBOARDING_FLOW_NAME}/{ONBOARDING_DEPLOYMENT_NAME}"
    try:
        flow_run = run_deployment(name=deployment_name, parameters={"run_id": run_id}, timeout=0)
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


def _build_capability_bindings(
    capabilities: list[dict[str, Any]],
    operations: list[dict[str, Any]],
    variants: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    variants_by_operation: dict[str, list[dict[str, Any]]] = {}
    for variant in variants:
        variants_by_operation.setdefault(str(variant.get("operation_id") or ""), []).append(variant)
    mappings_by_operation: dict[str, list[dict[str, Any]]] = {}
    for mapping in mappings:
        mappings_by_operation.setdefault(str(mapping.get("operation_id") or ""), []).append(mapping)

    bindings: list[dict[str, Any]] = []
    for capability in capabilities:
        capability_outputs = set(capability.get("output_semantic_types") or [])
        best_operation: dict[str, Any] | None = None
        best_overlap = -1
        for operation in operations:
            operation_mappings = mappings_by_operation.get(operation["id"], [])
            operation_semantics = {item.get("semantic_type_id") for item in operation_mappings}
            overlap = len(capability_outputs.intersection(operation_semantics))
            if overlap > best_overlap:
                best_overlap = overlap
                best_operation = operation
        if best_operation is None:
            continue
        binding_variants = variants_by_operation.get(best_operation["id"], [])
        coverage = 0 if not capability_outputs else round(best_overlap / max(len(capability_outputs), 1), 2)
        bindings.append(
            {
                "id": f"binding_{capability['id']}_{best_operation['id']}",
                "capability_id": capability["id"],
                "capability_key": capability.get("capability_key") or capability["id"],
                "capability_name": capability.get("name") or capability.get("capability_key") or capability["id"],
                "operation_id": best_operation["id"],
                "operation_name": best_operation.get("name") or best_operation["id"],
                "variant_ids": [variant["id"] for variant in binding_variants],
                "variant_count": len(binding_variants),
                "semantic_coverage": coverage,
                "status": "candidate" if coverage < 1 else "ready",
                "evidence": "derived from output semantic overlap",
            }
        )
    return sorted(bindings, key=lambda item: (item["status"], item["capability_name"]))


def _text_preview(content: bytes) -> str:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = content.decode("cp949")
        except UnicodeDecodeError:
            return ""
    normalized = " ".join(text.split())
    return normalized[:500]


def _build_transform_suggestion(
    mapping: dict[str, Any],
    semantic_type: dict[str, Any] | None,
    operation_field: dict[str, Any] | None,
) -> dict[str, Any]:
    samples = _extract_transform_samples(mapping, operation_field)
    field_text = " ".join(
        str(value or "")
        for value in [
            mapping.get("field_path"),
            operation_field.get("raw_name") if operation_field else "",
            operation_field.get("display_name") if operation_field else "",
            operation_field.get("description") if operation_field else "",
            semantic_type.get("name") if semantic_type else "",
            semantic_type.get("description") if semantic_type else "",
            semantic_type.get("datatype") if semantic_type else "",
        ]
    ).lower()
    datatype = str((semantic_type or {}).get("datatype") or "").lower()
    suggestion = _suggest_date_transform(samples, datatype, field_text)
    if suggestion is None:
        suggestion = _suggest_number_transform(samples, datatype, field_text)
    if suggestion is None:
        suggestion = _suggest_boolean_transform(samples, datatype, field_text)
    if suggestion is None:
        suggestion = {
            "transform_spec": {
                "kind": "identity",
                "empty_policy": "null",
                "invalid_policy": "keep",
            },
            "mapping_type": "exact",
            "mapping_kind": "direct",
            "confidence": 0.55 if samples else 0.35,
            "rationale": "No strong format mismatch was detected from the current semantic type and sample evidence.",
        }
    transform_spec = suggestion["transform_spec"]
    return {
        "mode": "deterministic_assist",
        "transform_spec": transform_spec,
        "mapping_type": suggestion["mapping_type"],
        "mapping_kind": suggestion["mapping_kind"],
        "enum_mapping": suggestion.get("enum_mapping", {}),
        "confidence": suggestion["confidence"],
        "rationale": suggestion["rationale"],
        "samples": samples,
        "preview": [_preview_transform(value, transform_spec, suggestion.get("enum_mapping", {})) for value in samples[:5]],
    }


def _suggest_semantic_types(operation_field: dict[str, Any], semantic_types: list[dict[str, Any]]) -> list[dict[str, Any]]:
    field_terms = _tokenize_suggestion_text(
        " ".join(
            str(value or "")
            for value in [
                operation_field.get("raw_name"),
                operation_field.get("display_name"),
                operation_field.get("field_path"),
                operation_field.get("description"),
                operation_field.get("data_type"),
            ]
        )
    )
    scored: list[dict[str, Any]] = []
    for semantic_type in semantic_types:
        display = semantic_type.get("draft_snapshot") or semantic_type
        semantic_terms = _tokenize_suggestion_text(
            " ".join(
                str(value or "")
                for value in [
                    display.get("name"),
                    display.get("urn"),
                    display.get("description"),
                    display.get("datatype"),
                    " ".join(display.get("aliases") or []),
                    " ".join(display.get("tags") or []),
                ]
            )
        )
        overlap = field_terms.intersection(semantic_terms)
        raw_name = str(operation_field.get("raw_name") or "").lower()
        semantic_name = str(display.get("name") or "").lower()
        boost = 0.0
        if raw_name and raw_name in semantic_name:
            boost += 0.25
        if semantic_name and semantic_name in raw_name:
            boost += 0.25
        score = min(0.98, 0.35 + (len(overlap) * 0.12) + boost)
        if overlap or boost:
            scored.append(
                {
                    "semantic_type_id": semantic_type["id"],
                    "name": display.get("name") or semantic_type["id"],
                    "datatype": display.get("datatype") or "string",
                    "description": display.get("description") or "",
                    "confidence": round(score, 2),
                    "rationale": f"Matched field evidence: {', '.join(sorted(overlap)[:5])}" if overlap else "Name similarity matched the source field.",
                }
            )
    if not scored:
        fallback = semantic_types[:3]
        return [
            {
                "semantic_type_id": item["id"],
                "name": (item.get("draft_snapshot") or item).get("name") or item["id"],
                "datatype": (item.get("draft_snapshot") or item).get("datatype") or "string",
                "description": (item.get("draft_snapshot") or item).get("description") or "",
                "confidence": 0.25,
                "rationale": "No strong semantic match was detected; review manually.",
            }
            for item in fallback
        ]
    return sorted(scored, key=lambda item: item["confidence"], reverse=True)[:5]


def _tokenize_suggestion_text(value: str) -> set[str]:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return {
        token.lower()
        for token in re.split(r"[^0-9A-Za-z가-힣]+", spaced)
        if len(token.strip()) >= 2
    }


def _extract_transform_samples(mapping: dict[str, Any], operation_field: dict[str, Any] | None) -> list[str]:
    samples: list[str] = []
    for source in [mapping.get("evidence") or [], (operation_field or {}).get("evidence") or []]:
        if not isinstance(source, list):
            continue
        for item in source:
            if isinstance(item, dict):
                value = item.get("sample") or item.get("value") or item.get("example")
                if value is not None:
                    samples.append(str(value))
            elif item is not None:
                samples.append(str(item))
    metadata = (operation_field or {}).get("metadata") or {}
    for key in ("samples", "sample_values", "examples"):
        values = metadata.get(key)
        if isinstance(values, list):
            samples.extend(str(value) for value in values if value is not None)
    deduped: list[str] = []
    for sample in samples:
        normalized = sample.strip()
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped[:10]


def _suggest_date_transform(samples: list[str], datatype: str, field_text: str) -> dict[str, Any] | None:
    if "date" not in datatype and "time" not in datatype and not re.search(r"(date|datetime|dt|일자|날짜|시간)", field_text):
        return None
    sample = samples[0] if samples else ""
    compact_digits = re.sub(r"\D", "", sample)
    if len(compact_digits) == 14:
        input_format = "yyyyMMddHHmmss"
        output_format = "ISO_DATETIME"
        confidence = 0.9
    elif len(compact_digits) == 8:
        input_format = "yyyyMMdd"
        output_format = "ISO_DATE"
        confidence = 0.88
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", sample):
        input_format = "yyyy-MM-dd"
        output_format = "ISO_DATE"
        confidence = 0.78
    else:
        input_format = "auto"
        output_format = "ISO_DATETIME" if "time" in datatype or "datetime" in field_text else "ISO_DATE"
        confidence = 0.62 if samples else 0.45
    return {
        "transform_spec": {
            "kind": "date_parse",
            "input_format": input_format,
            "output_format": output_format,
            "empty_policy": "null",
            "invalid_policy": "reject",
        },
        "mapping_type": "transform",
        "mapping_kind": "transform",
        "confidence": confidence,
        "rationale": "The target semantic type or field evidence indicates a date/time value.",
    }


def _suggest_number_transform(samples: list[str], datatype: str, field_text: str) -> dict[str, Any] | None:
    numeric_target = any(token in datatype for token in ("number", "numeric", "integer", "float", "decimal", "amount"))
    numeric_target = numeric_target or bool(re.search(r"(amount|price|cost|amt|prce|금액|가격)", field_text))
    if not numeric_target:
        return None
    sample = samples[0] if samples else ""
    return {
        "transform_spec": {
            "kind": "number_parse",
            "thousands_separator": "," if "," in sample else "",
            "decimal_separator": ".",
            "empty_policy": "null",
            "invalid_policy": "reject",
        },
        "mapping_type": "transform",
        "mapping_kind": "transform",
        "confidence": 0.86 if samples else 0.5,
        "rationale": "The target semantic type or field evidence indicates a numeric value.",
    }


def _suggest_boolean_transform(samples: list[str], datatype: str, field_text: str) -> dict[str, Any] | None:
    if "bool" not in datatype and not re.search(r"(flag|yn| 여부|여부)", field_text):
        return None
    values = {sample.strip() for sample in samples if sample.strip()}
    enum_mapping: dict[str, bool] = {}
    for value in values:
        lowered = value.lower()
        if lowered in {"y", "yes", "true", "1"}:
            enum_mapping[value] = True
        elif lowered in {"n", "no", "false", "0"}:
            enum_mapping[value] = False
    return {
        "transform_spec": {
            "kind": "enum_map",
            "empty_policy": "null",
            "invalid_policy": "reject",
        },
        "enum_mapping": enum_mapping,
        "mapping_type": "enum",
        "mapping_kind": "enum",
        "confidence": 0.82 if enum_mapping else 0.55,
        "rationale": "The target semantic type or field evidence indicates a boolean/flag value.",
    }


def _preview_transform(value: str, transform_spec: dict[str, Any], enum_mapping: dict[str, Any]) -> dict[str, Any]:
    try:
        output: Any = _apply_preview_transform(value, transform_spec, enum_mapping)
        return {"input": value, "output": output, "ok": True}
    except ValueError as exc:
        return {"input": value, "output": None, "ok": False, "error": str(exc)}


def _apply_preview_transform(value: str, transform_spec: dict[str, Any], enum_mapping: dict[str, Any]) -> Any:
    kind = transform_spec.get("kind")
    if kind == "identity":
        return value
    if kind == "number_parse":
        normalized = value.strip()
        thousands_separator = str(transform_spec.get("thousands_separator") or "")
        if thousands_separator:
            normalized = normalized.replace(thousands_separator, "")
        try:
            return float(normalized) if "." in normalized else int(normalized)
        except ValueError as exc:
            raise ValueError("number_parse failed") from exc
    if kind == "enum_map":
        if value in enum_mapping:
            return enum_mapping[value]
        raise ValueError("enum value is not mapped")
    if kind == "date_parse":
        input_format = str(transform_spec.get("input_format") or "auto")
        output_format = str(transform_spec.get("output_format") or "ISO_DATE")
        parsed = _parse_preview_date(value, input_format)
        return parsed.isoformat() if output_format == "ISO_DATETIME" else parsed.date().isoformat()
    return value


def _parse_preview_date(value: str, input_format: str) -> datetime:
    formats = {
        "yyyyMMddHHmmss": "%Y%m%d%H%M%S",
        "yyyyMMdd": "%Y%m%d",
        "yyyy-MM-dd": "%Y-%m-%d",
    }
    candidates = list(formats.values()) if input_format == "auto" else [formats.get(input_format, input_format)]
    for candidate in candidates:
        try:
            return datetime.strptime(value.strip(), candidate)
        except ValueError:
            continue
    raise ValueError("date_parse failed")


@app.post("/api/proposals/{proposal_id}/reject")
def reject_proposal(proposal_id: str, payload: ReviewPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().review_proposal(
            proposal_id,
            "rejected",
            reviewer=payload.reviewer,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc
