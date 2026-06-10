from __future__ import annotations

from typing import Any


def calculate_scores(
    traditional_scores: dict[str, float],
    semantic_result: dict[str, Any],
    rubric: dict[str, Any],
) -> dict[str, float | None]:
    traditional_score = round(_average(list(traditional_scores.values()), fallback=0.0), 2)
    semantic_score = _semantic_score(semantic_result, rubric)
    if semantic_score is None:
        overall_score = traditional_score
    else:
        overall_score = round((traditional_score * 0.6) + (semantic_score * 0.4), 2)
    return {
        "traditional_score": traditional_score,
        "semantic_score": semantic_score,
        "overall_score": overall_score,
    }


def _semantic_score(semantic_result: dict[str, Any], rubric: dict[str, Any]) -> float | None:
    if semantic_result.get("status") != "completed":
        return None
    dimensions = rubric.get("dimensions", [])
    if not isinstance(dimensions, list):
        return None
    weighted_total = 0.0
    total_weight = 0.0
    for item in dimensions:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        if name not in semantic_result:
            continue
        weight = float(item.get("weight", 0.0))
        weighted_total += float(semantic_result[name]) * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return round(weighted_total / total_weight, 2)


def _average(values: list[float], *, fallback: float) -> float:
    return sum(values) / len(values) if values else fallback
