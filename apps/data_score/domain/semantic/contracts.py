from __future__ import annotations

from typing import Any, TypedDict


class SemanticRubricDimension(TypedDict):
    name: str
    weight: float
    description: str


class SemanticRubric(TypedDict):
    dimensions: list[SemanticRubricDimension]


class SemanticJudgeResult(TypedDict, total=False):
    status: str
    coverage: float
    specificity: float
    consistency: float
    business_fitness: float
    reason: str
    suggestions: list[str]
    confidence: float
    source: str
    details: dict[str, Any]
