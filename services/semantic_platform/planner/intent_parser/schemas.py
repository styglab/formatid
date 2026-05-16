from __future__ import annotations

from typing import Any, TypedDict


class SemanticFilter(TypedDict, total=False):
    semantic_type: str
    operator: str
    value: Any
    unit: str
    source_text: str


class SemanticIntent(TypedDict):
    query: str
    language: str
    entities: list[str]
    semantic_types: list[str]
    capabilities: list[str]
    semantic_arguments: dict[str, Any]
    filters: list[SemanticFilter]
    metrics: list[str]
    constraints: list[dict[str, Any]]
    confidence: float
    parser: dict[str, Any]
