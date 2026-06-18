from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from services.semantic_platform.adapters.admin_api.app.common import (
    paged,
    payload_dict,
)
from services.semantic_platform.internal.authoring import (
    build_transform_suggestion,
    suggest_semantic_types,
)
from services.semantic_platform.internal.planner.contracts import (
    build_capability_bindings,
)
from services.semantic_platform.internal.storage import SemanticLayerRepository


router = APIRouter()


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


@router.get("/api/capability-bindings")
def list_capability_bindings() -> list[dict[str, Any]]:
    repository = SemanticLayerRepository()
    capabilities = repository.list_capabilities()
    operations = repository.list_execution_operations()
    variants = repository.list_operation_variants()
    mappings = repository.list_field_mappings()
    return build_capability_bindings(capabilities, operations, variants, mappings)


@router.post("/api/operation-fields/{field_id}/mapping-suggestion")
def suggest_operation_field_mapping(field_id: str, payload: MappingSuggestionPayload) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    operation_field = next((item for item in repository.list_operation_fields() if item.get("id") == field_id), None)
    if operation_field is None:
        raise HTTPException(status_code=404, detail="operation field not found")
    semantic_types = repository.list_semantic_types()
    semantic_suggestions = suggest_semantic_types(operation_field, semantic_types)
    selected_semantic_type = next((item for item in semantic_types if item.get("id") == payload.semantic_type_id), None)
    if selected_semantic_type is None and semantic_suggestions:
        selected_semantic_type = next(
            (item for item in semantic_types if item.get("id") == semantic_suggestions[0]["semantic_type_id"]),
            None,
        )
    draft_mapping = {
        "id": "",
        "field_id": operation_field["id"],
        "source_id": None,
        "operation_id": operation_field["operation_id"],
        "variant_id": operation_field.get("variant_id"),
        "field_path": str(operation_field.get("field_path") or operation_field.get("raw_name") or ""),
        "semantic_type_id": (selected_semantic_type or {}).get("id") or "",
        "mapping_type": "exact",
        "mapping_kind": "direct",
        "transform_spec": {},
        "enum_mapping": {},
        "evidence": [],
    }
    transform_suggestion = (
        build_transform_suggestion(draft_mapping, selected_semantic_type, operation_field)
        if selected_semantic_type is not None
        else None
    )
    return {
        "mode": "deterministic_assist",
        "field_id": field_id,
        "semantic_type_suggestions": semantic_suggestions,
        "transform_suggestion": transform_suggestion,
    }


@router.get("/api/operation-variants")
def list_operation_variants(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_operation_variants(query=query, status=status, operation_id=operation_id)


@router.get("/api/operation-variants/page")
def list_operation_variants_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return paged(
        SemanticLayerRepository().list_operation_variants(query=query, status=status, operation_id=operation_id),
        page,
        page_size,
    )


@router.post("/api/operation-variants")
def create_operation_variant(payload: OperationVariantPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_operation_variant(payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/api/operation-variants/{variant_id}")
def update_operation_variant(variant_id: str, payload: OperationVariantPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_operation_variant(variant_id, payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="operation variant not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/operation-variants/{variant_id}")
def delete_operation_variant(variant_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_operation_variant(variant_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="operation variant not found") from exc


@router.get("/api/capabilities")
def list_capabilities(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_capabilities(query=query, status=status)


@router.get("/api/capabilities/page")
def list_capabilities_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return paged(SemanticLayerRepository().list_capabilities(query=query, status=status), page, page_size)


@router.post("/api/capabilities")
def create_capability(payload: CapabilityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_capability(payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/capabilities/{capability_id}")
def get_capability(capability_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_capability(capability_id)
    if record is None:
        raise HTTPException(status_code=404, detail="capability not found")
    return record


@router.patch("/api/capabilities/{capability_id}")
def update_capability(capability_id: str, payload: CapabilityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_capability(capability_id, payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="capability not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/capabilities/{capability_id}")
def delete_capability(capability_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_capability(capability_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="capability not found") from exc


@router.get("/api/mappings")
def list_mappings(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_field_mappings(query=query, status=status, operation_id=operation_id)


@router.get("/api/mappings/page")
def list_mappings_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    operation_id: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return paged(
        SemanticLayerRepository().list_field_mappings(query=query, status=status, operation_id=operation_id),
        page,
        page_size,
    )


@router.get("/api/mappings/exists")
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


@router.post("/api/mappings")
def create_mapping(payload: FieldMappingPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_field_mapping(payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/mappings/{mapping_id}")
def get_mapping(mapping_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_field_mapping(mapping_id)
    if record is None:
        raise HTTPException(status_code=404, detail="mapping not found")
    return record


@router.post("/api/mappings/{mapping_id}/transform-suggestion")
def suggest_mapping_transform(mapping_id: str) -> dict[str, Any]:
    repository = SemanticLayerRepository()
    mapping = repository.get_field_mapping(mapping_id)
    if mapping is None:
        raise HTTPException(status_code=404, detail="mapping not found")
    semantic_type = next((item for item in repository.list_semantic_types() if item.get("id") == mapping.get("semantic_type_id")), None)
    operation_field = next(
        (
            item
            for item in repository.list_operation_fields(operation_id=mapping.get("operation_id", ""))
            if item.get("id") == mapping.get("field_id")
            or str(item.get("field_path") or item.get("raw_name") or "") == str(mapping.get("field_path") or "")
        ),
        None,
    )
    return build_transform_suggestion(mapping, semantic_type, operation_field)


@router.patch("/api/mappings/{mapping_id}")
def update_mapping(mapping_id: str, payload: FieldMappingUpdatePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_field_mapping(mapping_id, payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="mapping not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/mappings/{mapping_id}")
def delete_mapping(mapping_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_field_mapping(mapping_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="mapping not found") from exc
