from __future__ import annotations

from typing import Any

from apps.data_score.domain.semantic.contracts import SemanticRubric


def generate_rubric(profile: dict[str, Any], business_context: str | None = None) -> SemanticRubric:
    has_textual_column = any(
        bool(column.get("looks_textual"))
        for column in profile.get("columns", [])
        if isinstance(column, dict)
    )
    dimensions = [
        {
            "name": "coverage",
            "weight": 0.3,
            "description": "Whether records contain enough relevant information for use.",
        },
        {
            "name": "specificity",
            "weight": 0.3,
            "description": "Whether values are concrete and detailed rather than generic.",
        },
        {
            "name": "consistency",
            "weight": 0.2,
            "description": "Whether the semantic content aligns across records and fields.",
        },
        {
            "name": "business_fitness",
            "weight": 0.2,
            "description": "Whether the dataset is fit for the declared business context.",
        },
    ]
    if has_textual_column and business_context:
        dimensions.append(
            {
                "name": "documentation_quality",
                "weight": 0.1,
                "description": "Whether descriptive text is understandable and reusable.",
            }
        )
        _normalize_weights(dimensions)
    return {"dimensions": dimensions}


def _normalize_weights(dimensions: list[dict[str, Any]]) -> None:
    total = sum(float(item["weight"]) for item in dimensions)
    if total <= 0:
        return
    for item in dimensions:
        item["weight"] = round(float(item["weight"]) / total, 4)
