from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from services.context_platform.adapters.admin_api.app.common import (
    paged,
    payload_dict,
)
from services.context_platform.internal.storage import ContextPlatformRepository
from services.context_platform.internal.storage.object_store import ObjectStore


router = APIRouter()


class SourcePayload(BaseModel):
    name: str | None = None
    namespace: str = "public"
    provider: str = ""
    source_type: str = "api"
    description: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    config: dict[str, Any] = Field(default_factory=dict)


class SourceDocumentPayload(BaseModel):
    source_id: str | None = None
    document_type: str = "api_document"
    name: str | None = None
    uri: str = ""
    content_hash: str = ""
    content_type: str = ""
    status: str = "draft"
    metadata: dict[str, Any] = Field(default_factory=dict)
    evidence: list[Any] = Field(default_factory=list)


class SourceOperationPayload(BaseModel):
    source_id: str | None = None
    source_document_id: str | None = None
    operation_key: str | None = None
    method: str = "GET"
    path: str | None = None
    name: str | None = None
    description: str = ""
    auth_spec: dict[str, Any] = Field(default_factory=dict)
    request_spec: dict[str, Any] = Field(default_factory=dict)
    response_spec: dict[str, Any] = Field(default_factory=dict)
    endpoint_metadata: dict[str, Any] = Field(default_factory=dict)
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"


class CanonicalTypePayload(BaseModel):
    namespace: str = "public"
    name: str | None = None
    description: str = ""
    base_type: str = "string"
    uri: str = ""
    typeof: str = ""
    pattern: str = ""
    minimum: float | None = None
    maximum: float | None = None
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    annotations: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalEnumPayload(BaseModel):
    namespace: str = "public"
    name: str | None = None
    description: str = ""
    permissible_values: dict[str, Any] = Field(default_factory=dict)
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    annotations: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalEnumValuePayload(BaseModel):
    enum_id: str | None = None
    code: str | None = None
    meaning: str = ""
    description: str = ""
    aliases: list[str] = Field(default_factory=list)
    annotations: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    status: str = "draft"


class CanonicalSlotPayload(BaseModel):
    namespace: str = "public"
    name: str | None = None
    description: str = ""
    range_kind: str = "type"
    range_ref: str = "string"
    datatype: str = "string"
    aliases: list[str] = Field(default_factory=list)
    examples: list[Any] = Field(default_factory=list)
    mappings: list[Any] = Field(default_factory=list)
    annotations: dict[str, Any] = Field(default_factory=dict)
    constraints: dict[str, Any] = Field(default_factory=dict)
    identity_role: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalClassPayload(BaseModel):
    namespace: str = "public"
    name: str | None = None
    description: str = ""
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalClassSlotPayload(BaseModel):
    class_id: str | None = None
    slot_id: str | None = None
    usage_name: str = ""
    required: bool = False
    multivalued: bool = False
    slot_order: int = 100
    range_override: str = ""
    constraints: dict[str, Any] = Field(default_factory=dict)
    annotations: dict[str, Any] = Field(default_factory=dict)
    status: str = "draft"


class BindingPayload(BaseModel):
    source_id: str | None = None
    source_document_id: str | None = None
    source_operation_id: str | None = None
    source_parameter_id: str | None = None
    source_field_id: str | None = None
    canonical_class_slot_id: str | None = None
    direction: str | None = None
    binding_type: str = "exact"
    transform_spec: dict[str, Any] = Field(default_factory=dict)
    normalization_rule: dict[str, Any] = Field(default_factory=dict)
    enum_mapping: dict[str, Any] = Field(default_factory=dict)
    status: str = "draft"
    confidence: float | None = None
    evidence: list[Any] = Field(default_factory=list)


class CapabilityPayload(BaseModel):
    capability_key: str | None = None
    namespace: str = "public"
    name: str | None = None
    description: str = ""
    intent_spec: dict[str, Any] = Field(default_factory=dict)
    version: str = "1.0.0"
    lifecycle: str = "draft"
    status: str = "draft"
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkbenchActionPayload(BaseModel):
    source_document_id: str = ""
    run_id: str = ""
    proposal_bundle_id: str = ""
    reviewer: str = "system"
    rationale: str = ""
    agent_response: dict[str, Any] = Field(default_factory=dict)
    manual_llm_response: dict[str, Any] = Field(default_factory=dict)


@router.get("/api/sources")
def list_sources(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_sources(query=query, status=status)


@router.get("/api/sources/page")
def list_sources_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return paged(ContextPlatformRepository().list_sources(query=query, status=status), page, page_size)


@router.post("/api/sources")
def create_source(payload: SourcePayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_source(payload_dict(payload, exclude_unset=False)))


@router.post("/api/sources/upload")
async def upload_source(
    file: UploadFile = File(...),
    name: str = Form(...),
    provider: str = Form(default=""),
    source_type: str = Form(default="api"),
    document_type: str = Form(default="auto"),
    description: str = Form(default=""),
    status: str = Form(default="draft"),
    reference_uri: str = Form(default=""),
) -> dict[str, Any]:
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="uploaded file is empty")

    safe_name = Path(file.filename or "api-document.json").name
    media_type = file.content_type or "application/octet-stream"
    is_text_upload = (
        media_type.startswith("text/")
        or media_type in {"application/json", "application/yaml", "application/x-yaml"}
        or safe_name.lower().endswith((".json", ".yaml", ".yml", ".txt", ".md", ".csv"))
    )
    text = raw.decode("utf-8", errors="replace") if is_text_upload else ""
    preview = "".join(ch if ch >= " " or ch in "\n\r\t" else " " for ch in text)
    content_hash = hashlib.sha256(raw).hexdigest()
    stored_object = ObjectStore().put_document(
        filename=safe_name,
        content_type=media_type,
        data=raw,
    )
    repo = ContextPlatformRepository()
    try:
        source = repo.create_source(
            {
                "name": name,
                "provider": provider,
                "source_type": source_type,
                "description": description or f"Uploaded source document: {safe_name}",
                "status": status,
                "config": {
                    "input_mode": "minio_upload",
                    "reference_uri": reference_uri or stored_object["uri"],
                    "upload": {
                        "document_type": document_type,
                        "filename": safe_name,
                        "object_uri": stored_object["uri"],
                        "bucket": stored_object["bucket"],
                        "key": stored_object["key"],
                        "media_type": media_type,
                        "size_bytes": len(raw),
                        "sha256": content_hash,
                    },
                },
            }
        )
        document = repo.create_source_document(
            {
                "source_id": source["id"],
                "document_type": document_type,
                "name": safe_name,
                "uri": stored_object["uri"],
                "content_hash": content_hash,
                "content_type": media_type,
                "status": "queued_for_agent",
                "metadata": {
                    "preview": " ".join(preview.split())[:500],
                    "reference_uri": reference_uri,
                    "object": stored_object,
                    "ingestion_owner": "operator_agent",
                },
            }
        )
        run = repo.create_onboarding_run(
            {
                "source_id": source["id"],
                "source_document_id": document["id"],
                "status": "queued_for_agent",
                "stage": "agent_intake",
                "metadata": {
                    "object_uri": stored_object["uri"],
                    "content_hash": content_hash,
                    "filename": safe_name,
                    "ingestion_mode": "agent_manual",
                    "agent_ingestion_required": True,
                },
            }
        )
        submission = {
            "status": "queued_for_agent",
            "reason": "source document was stored for operator-agent ingestion; dashboard review starts after a proposal bundle is generated",
            "run_id": run["id"],
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "source": source,
        "source_document": document,
        "onboarding_run": run,
        "submission": submission,
        "source_operations": [],
        "proposals": [],
        "upload": {
            "filename": safe_name,
            "object_uri": stored_object["uri"],
            "bucket": stored_object["bucket"],
            "key": stored_object["key"],
            "sha256": content_hash,
        },
    }


@router.get("/api/source-documents")
def list_source_documents(source_id: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_source_documents(source_id=source_id)


@router.get("/api/workbench/workflow")
def get_workbench_workflow(
    source_document_id: str = Query(default=""),
    run_id: str = Query(default=""),
) -> dict[str, Any]:
    return ContextPlatformRepository().workbench_workflow(source_document_id=source_document_id, run_id=run_id)


@router.post("/api/workbench/{action_name}")
def run_workbench_action(action_name: str, payload: WorkbenchActionPayload) -> dict[str, Any]:
    action = action_name.replace("-", "_")
    allowed_actions = {
        "validate",
        "submit_proposal",
        "approve_bundle",
        "reject_bundle",
    }
    agent_owned_actions = {
        "discover",
        "draft_canonical",
        "draft_bindings",
        "draft_capabilities",
    }
    if action in agent_owned_actions:
        raise HTTPException(
            status_code=410,
            detail=(
                "source ingestion and draft generation are operator-agent owned. "
                "Use agent_manual ingestion, then review the generated proposal bundle in the dashboard."
            ),
        )
    if action not in allowed_actions:
        raise HTTPException(status_code=404, detail=f"unknown workbench action: {action_name}")

    repo = ContextPlatformRepository()
    workflow = repo.workbench_workflow(source_document_id=payload.source_document_id, run_id=payload.run_id)
    active_run = workflow.get("active_run") or {}
    active_bundle = workflow.get("active_bundle")

    if action == "validate":
        return {
            "action": action,
            "status": "validated",
            "validation": _validate_workbench_workflow(workflow),
            "workflow": workflow,
        }

    if action == "submit_proposal":
        if not active_bundle:
            raise HTTPException(status_code=409, detail="final proposal bundle is not available")
        return {
            "action": action,
            "status": "ready_for_review",
            "proposal_bundle": active_bundle,
            "workflow": workflow,
        }

    if action == "approve_bundle":
        bundle_id = payload.proposal_bundle_id or str((active_bundle or {}).get("id") or "")
        if not bundle_id:
            raise HTTPException(status_code=409, detail="final proposal bundle is not available")
        try:
            result = repo.approve_proposal_bundle(bundle_id, reviewer=payload.reviewer or "system")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "action": action,
            **result,
            "workflow": repo.workbench_workflow(source_document_id=payload.source_document_id, run_id=payload.run_id),
        }

    if action == "reject_bundle":
        bundle_id = payload.proposal_bundle_id or str((active_bundle or {}).get("id") or "")
        if not bundle_id:
            raise HTTPException(status_code=409, detail="final proposal bundle is not available")
        try:
            result = repo.reject_proposal_bundle(
                bundle_id,
                reviewer=payload.reviewer or "system",
                rationale=payload.rationale or "Rejected from Context Platform workbench.",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "action": action,
            **result,
            "workflow": repo.workbench_workflow(source_document_id=payload.source_document_id, run_id=payload.run_id),
        }

    raise HTTPException(status_code=404, detail=f"unknown workbench action: {action_name}")


@router.post("/api/source-documents")
def create_source_document(payload: SourceDocumentPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_source_document(payload_dict(payload, exclude_unset=False)))


@router.get("/api/source-operations")
def list_source_operations(
    source_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_source_operations(source_id=source_id, status=status)


@router.post("/api/source-operations")
def create_source_operation(payload: SourceOperationPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_source_operation(payload_dict(payload, exclude_unset=False)))


@router.get("/api/source-parameters")
def list_source_parameters(source_operation_id: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_source_parameters(source_operation_id=source_operation_id)


@router.get("/api/source-fields")
def list_source_fields(
    source_operation_id: str = Query(default=""),
    source_document_id: str = Query(default=""),
    direction: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_source_fields(
        source_operation_id=source_operation_id,
        source_document_id=source_document_id,
        direction=direction,
    )


@router.get("/api/endpoint-checks")
def list_endpoint_checks(
    run_id: str = Query(default=""),
    source_operation_id: str = Query(default=""),
    capability_key: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_endpoint_checks(
        run_id=run_id,
        source_operation_id=source_operation_id,
        capability_key=capability_key,
        status=status,
    )


@router.get("/api/meaning-scopes")
def list_meaning_scopes(status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_meaning_scopes(status=status)


@router.get("/api/concept-schemes")
def list_concept_schemes(status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_concept_schemes(status=status)


@router.get("/api/concepts")
def list_concepts(
    query: str = Query(default=""),
    kind: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_concepts(query=query, kind=kind, status=status)


@router.get("/api/value-domains")
def list_value_domains(status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_value_domains(status=status)


@router.get("/api/value-domain-values")
def list_value_domain_values(
    value_domain_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_value_domain_values(value_domain_id=value_domain_id, status=status)


@router.get("/api/object-types")
def list_object_types(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_object_types(query=query, status=status)


@router.get("/api/property-types")
def list_property_types(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_property_types(query=query, status=status)


@router.get("/api/link-types")
def list_link_types(status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_link_types(status=status)


@router.get("/api/canonical-representations")
def list_canonical_representations(
    concept_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_representations(concept_id=concept_id, status=status)


@router.get("/api/representation-schemas")
def list_representation_schemas(
    representation_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_representation_schemas(representation_id=representation_id, status=status)


@router.get("/api/canonical-types")
def list_canonical_types(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_types(query=query, status=status)


@router.post("/api/canonical-types")
def create_canonical_type(payload: CanonicalTypePayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_canonical_type(payload_dict(payload, exclude_unset=False)))


@router.get("/api/canonical-enums")
def list_canonical_enums(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_enums(query=query, status=status)


@router.post("/api/canonical-enums")
def create_canonical_enum(payload: CanonicalEnumPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_canonical_enum(payload_dict(payload, exclude_unset=False)))


@router.get("/api/canonical-enum-values")
def list_canonical_enum_values(enum_id: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_enum_values(enum_id=enum_id)


@router.post("/api/canonical-enum-values")
def create_canonical_enum_value(payload: CanonicalEnumValuePayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_canonical_enum_value(payload_dict(payload, exclude_unset=False)))


@router.get("/api/canonical-slots")
def list_canonical_slots(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_slots(query=query, status=status)


@router.post("/api/canonical-slots")
def create_canonical_slot(payload: CanonicalSlotPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_canonical_slot(payload_dict(payload, exclude_unset=False)))


@router.get("/api/canonical-classes")
def list_canonical_classes(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_classes(query=query, status=status)


@router.post("/api/canonical-classes")
def create_canonical_class(payload: CanonicalClassPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_canonical_class(payload_dict(payload, exclude_unset=False)))


@router.get("/api/canonical-class-slots")
def list_canonical_class_slots(
    class_id: str = Query(default=""),
    slot_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_class_slots(class_id=class_id, slot_id=slot_id, status=status)


@router.post("/api/canonical-class-slots")
def create_canonical_class_slot(payload: CanonicalClassSlotPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_canonical_class_slot(payload_dict(payload, exclude_unset=False)))


@router.get("/api/canonical-class-slot-usages")
def list_canonical_class_slot_usages(class_id: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_class_slot_usages(class_id=class_id, status=status)


@router.get("/api/canonical-relations")
def list_canonical_relations(status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_canonical_relations(status=status)


@router.get("/api/canonical-model/linkml")
def export_linkml_schema(namespace: str = Query(default="public"), status: str = Query(default="")) -> dict[str, Any]:
    return ContextPlatformRepository().export_linkml_schema(namespace=namespace, status=status)


@router.get("/api/bindings")
def list_bindings(
    source_operation_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_bindings(source_operation_id=source_operation_id, status=status)


@router.get("/api/field-bindings")
def list_field_bindings(
    source_operation_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_field_bindings(source_operation_id=source_operation_id, status=status)


@router.get("/api/context-bindings")
def list_context_bindings(
    source_operation_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_context_bindings(source_operation_id=source_operation_id, status=status)


@router.get("/api/parameter-bindings")
def list_parameter_bindings(
    source_operation_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_parameter_bindings(source_operation_id=source_operation_id, status=status)


@router.post("/api/bindings")
def create_binding(payload: BindingPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_binding(payload_dict(payload, exclude_unset=False)))


@router.get("/api/capabilities")
def list_capabilities(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_capabilities(query=query, status=status)


@router.post("/api/capabilities")
def create_capability(payload: CapabilityPayload) -> dict[str, Any]:
    return _create(lambda repo: repo.create_capability(payload_dict(payload, exclude_unset=False)))


@router.get("/api/capability-operations")
def list_capability_operations(capability_id: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_capability_operations(capability_id=capability_id)


@router.get("/api/capability-steps")
def list_capability_steps(capability_id: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_capability_operations(capability_id=capability_id)


@router.get("/api/plans")
def list_plans(status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_plans(status=status)


@router.get("/api/executions")
def list_executions(status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_executions(status=status)


@router.get("/api/proposals")
def list_proposals(
    status: str = Query(default=""),
    entity_type: str = Query(default=""),
    query: str = Query(default=""),
) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_proposals(status=status, entity_type=entity_type, query=query)


@router.get("/api/proposals/page")
def list_proposals_page(
    status: str = Query(default=""),
    entity_type: str = Query(default=""),
    query: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    proposals = ContextPlatformRepository().list_proposals(status=status, entity_type=entity_type, query=query)
    return paged(proposals, page, page_size)


@router.get("/api/onboarding-runs")
def list_onboarding_runs(source_document_id: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_onboarding_runs(source_document_id=source_document_id, status=status)


@router.get("/api/proposal-bundles")
def list_proposal_bundles(status: str = Query(default=""), source_id: str = Query(default="")) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_proposal_bundles(status=status, source_id=source_id)


@router.get("/api/proposal-bundles/{bundle_id}/items")
def list_proposal_bundle_items(bundle_id: str) -> list[dict[str, Any]]:
    return ContextPlatformRepository().list_proposal_bundle_items(bundle_id)


@router.post("/api/proposal-bundles/{bundle_id}/approve")
def approve_proposal_bundle(bundle_id: str, payload: WorkbenchActionPayload | None = None) -> dict[str, Any]:
    try:
        reviewer = payload.reviewer if payload else "system"
        return ContextPlatformRepository().approve_proposal_bundle(bundle_id, reviewer=reviewer or "system")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/proposal-bundles/{bundle_id}/reject")
def reject_proposal_bundle(bundle_id: str, payload: WorkbenchActionPayload | None = None) -> dict[str, Any]:
    try:
        reviewer = payload.reviewer if payload else "system"
        rationale = payload.rationale if payload else ""
        return ContextPlatformRepository().reject_proposal_bundle(
            bundle_id,
            reviewer=reviewer or "system",
            rationale=rationale or "Rejected from Context Platform review.",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _create(action: Any) -> dict[str, Any]:
    try:
        return action(ContextPlatformRepository())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _validate_workbench_workflow(workflow: dict[str, Any]) -> dict[str, Any]:
    blocked_steps = [step for step in workflow.get("steps", []) if step.get("state") == "blocked"]
    warning_steps = [step for step in workflow.get("steps", []) if step.get("state") == "warning"]
    return {
        "valid": not blocked_steps,
        "execution_ready": bool(workflow.get("execution_ready")),
        "mode": workflow.get("mode"),
        "blocked_steps": [{"key": step.get("key"), "title": step.get("title")} for step in blocked_steps],
        "warning_steps": [{"key": step.get("key"), "title": step.get("title")} for step in warning_steps],
    }
