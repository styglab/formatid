from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from services.semantic_platform.adapters.admin_api.app.common import (
    paged,
    payload_dict,
)
from services.semantic_platform.internal.storage import SemanticLayerRepository


router = APIRouter()


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


@router.get("/api/semantic-types")
def list_semantic_types(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_semantic_types(query=query, status=status)


@router.get("/api/semantic-types/page")
def list_semantic_types_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return paged(SemanticLayerRepository().list_semantic_types(query=query, status=status), page, page_size)


@router.post("/api/semantic-types")
def create_semantic_type(payload: SemanticTypePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_semantic_type(payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/semantic-types/{semantic_type_id}")
def get_semantic_type(semantic_type_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_semantic_type(semantic_type_id)
    if record is None:
        raise HTTPException(status_code=404, detail="semantic type not found")
    return record


@router.patch("/api/semantic-types/{semantic_type_id}")
def update_semantic_type(semantic_type_id: str, payload: SemanticTypePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_semantic_type(semantic_type_id, payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="semantic type not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/semantic-types/{semantic_type_id}")
def delete_semantic_type(semantic_type_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_semantic_type(semantic_type_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="semantic type not found") from exc


@router.post("/api/semantic-types/{semantic_type_id}/relationships")
def add_relationship(semantic_type_id: str, payload: RelationshipPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().add_semantic_relationship(
            semantic_type_id,
            payload_dict(payload, exclude_unset=False),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"semantic type not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/api/semantic-relationships/{relationship_id}")
def update_relationship(relationship_id: str, payload: RelationshipPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_semantic_relationship(
            relationship_id,
            payload_dict(payload, exclude_unset=False),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"relationship or semantic type not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/semantic-relationships/{relationship_id}")
def delete_relationship(relationship_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_semantic_relationship(relationship_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="relationship not found") from exc


@router.get("/api/semantic-relationships")
def list_semantic_relationships(
    semantic_type_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_relationships(semantic_type_id=semantic_type_id, status=status)


@router.get("/api/canonical-entities")
def list_canonical_entities(query: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_canonical_entities(query=query, status=status)


@router.post("/api/canonical-entities")
def create_canonical_entity(payload: CanonicalEntityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_canonical_entity(payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/api/canonical-entities/{entity_id}")
def update_canonical_entity(entity_id: str, payload: CanonicalEntityPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_canonical_entity(entity_id, payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical entity not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/canonical-entities/{entity_id}")
def delete_canonical_entity(entity_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_canonical_entity(entity_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical entity not found") from exc


@router.get("/api/canonical-attributes")
def list_canonical_attributes(entity_id: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_canonical_attributes(entity_id=entity_id, status=status)


@router.post("/api/canonical-attributes")
def create_canonical_attribute(payload: CanonicalAttributePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_canonical_attribute(payload_dict(payload, exclude_unset=False))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/api/canonical-attributes/{attribute_id}")
def update_canonical_attribute(attribute_id: str, payload: CanonicalAttributePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_canonical_attribute(attribute_id, payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical attribute or entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/canonical-attributes/{attribute_id}")
def delete_canonical_attribute(attribute_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_canonical_attribute(attribute_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical attribute not found") from exc


@router.get("/api/canonical-relations")
def list_canonical_relations(entity_id: str = Query(default=""), status: str = Query(default="")) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_canonical_relations(entity_id=entity_id, status=status)


@router.post("/api/canonical-relations")
def create_canonical_relation(payload: CanonicalRelationPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_canonical_relation(payload_dict(payload, exclude_unset=False))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/api/canonical-relations/{relation_id}")
def update_canonical_relation(relation_id: str, payload: CanonicalRelationPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_canonical_relation(relation_id, payload_dict(payload, exclude_unset=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"canonical relation or entity not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/canonical-relations/{relation_id}")
def delete_canonical_relation(relation_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_canonical_relation(relation_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="canonical relation not found") from exc
