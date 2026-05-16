from __future__ import annotations

from core.semantic.builder import (
    build_semantic_document,
    build_semantic_object,
    relationship,
    relationship_labels,
)
from core.semantic.contracts import EntityRef, SemanticDocument, SemanticObject, SemanticRelationship

__all__ = [
    "EntityRef",
    "SemanticDocument",
    "SemanticObject",
    "SemanticRelationship",
    "build_semantic_document",
    "build_semantic_object",
    "relationship",
    "relationship_labels",
]
