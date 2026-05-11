from __future__ import annotations

from typing import Any, NotRequired, TypedDict


class EntityRef(TypedDict):
    entity_type: str
    entity_id: str
    label: NotRequired[str | None]
    attributes: NotRequired[dict[str, Any]]


class SemanticRelationship(TypedDict):
    predicate: str
    target: EntityRef
    attributes: NotRequired[dict[str, Any]]


class SemanticObject(TypedDict):
    entity_type: str
    entity_id: str
    label: str
    attributes: dict[str, Any]
    relationships: list[SemanticRelationship]
    semantic_tags: list[str]


class SemanticDocument(TypedDict):
    document_id: str
    entity: EntityRef
    title: str
    text: str
    metadata: dict[str, Any]
    relationships: list[SemanticRelationship]
    semantic_tags: list[str]
