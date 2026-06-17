from __future__ import annotations

from typing import Any

from apps.data_score.domain.semantic.contracts import SemanticJudgeResult, SemanticRubric


def judge_semantic_quality(
    *,
    profile: dict[str, Any],
    rubric: SemanticRubric,
    sample_records: list[dict[str, str]],
    business_context: str | None,
    llm_mode: str,
    manual_judge_result: dict[str, Any] | None,
) -> SemanticJudgeResult:
    if llm_mode == "codex_manual":
        if not isinstance(manual_judge_result, dict):
            raise ValueError("manual_judge_result is required when llm_mode=codex_manual")
        return _manual_result(manual_judge_result)
    if llm_mode == "openai":
        return {
            "status": "not_implemented",
            "reason": "openai judge path is not implemented in this MVP scaffold",
            "suggestions": ["Use LLM_MODE=disabled or codex_manual for the current MVP."],
            "confidence": 0.0,
            "source": "placeholder",
        }
    return _heuristic_result(profile=profile, rubric=rubric, sample_records=sample_records, business_context=business_context)


def _manual_result(payload: dict[str, Any]) -> SemanticJudgeResult:
    result: SemanticJudgeResult = {
        "status": "completed",
        "coverage": float(payload.get("coverage", 0.0)),
        "specificity": float(payload.get("specificity", 0.0)),
        "consistency": float(payload.get("consistency", 0.0)),
        "business_fitness": float(payload.get("business_fitness", 0.0)),
        "reason": str(payload.get("reason") or "manual semantic evaluation"),
        "suggestions": [str(item) for item in payload.get("suggestions", [])] if isinstance(payload.get("suggestions"), list) else [],
        "confidence": float(payload.get("confidence", 1.0)),
        "source": "manual",
    }
    return result


def _heuristic_result(
    *,
    profile: dict[str, Any],
    rubric: SemanticRubric,
    sample_records: list[dict[str, str]],
    business_context: str | None,
) -> SemanticJudgeResult:
    columns = [column for column in profile.get("columns", []) if isinstance(column, dict)]
    textual_columns = [column for column in columns if column.get("looks_textual")]
    avg_text_length = _average([float(column.get("avg_length", 0.0)) for column in textual_columns], fallback=0.0)
    completeness_proxy = _average([100.0 - float(column.get("null_rate", 0.0)) * 100.0 for column in columns], fallback=100.0)

    coverage = min(100.0, round(completeness_proxy, 2))
    specificity = min(100.0, round((avg_text_length / 120.0) * 100.0, 2)) if textual_columns else 55.0
    consistency = min(100.0, round(70.0 + (completeness_proxy * 0.3), 2))
    business_fitness = 75.0 if business_context else 60.0

    dimension_names = {item["name"] for item in rubric["dimensions"]}
    result: SemanticJudgeResult = {
        "status": "completed",
        "coverage": coverage,
        "specificity": specificity,
        "consistency": consistency,
        "business_fitness": business_fitness,
        "reason": _reason(textual_columns=textual_columns, business_context=business_context, sample_records=sample_records),
        "suggestions": _suggestions(coverage=coverage, specificity=specificity, business_context=business_context, dimension_names=dimension_names),
        "confidence": 0.55,
        "source": "heuristic_disabled_mode",
        "details": {
            "textual_column_count": len(textual_columns),
            "sample_record_count": len(sample_records),
        },
    }
    if "documentation_quality" in dimension_names:
        result["details"]["documentation_quality_proxy"] = round(min(100.0, specificity * 0.9), 2)
    return result


def _reason(*, textual_columns: list[dict[str, Any]], business_context: str | None, sample_records: list[dict[str, str]]) -> str:
    if not sample_records:
        return "샘플 레코드가 없어 semantic quality를 충분히 평가할 수 없다."
    if not textual_columns:
        return "설명형 컬럼이 적어 semantic richness보다 구조적 품질 중심으로 평가했다."
    if business_context:
        return "텍스트 컬럼의 밀도와 business context 존재 여부를 기준으로 coverage와 business fitness를 추정했다."
    return "텍스트 길이와 결측 수준을 바탕으로 semantic quality를 휴리스틱하게 평가했다."


def _suggestions(
    *,
    coverage: float,
    specificity: float,
    business_context: str | None,
    dimension_names: set[str],
) -> list[str]:
    suggestions: list[str] = []
    if coverage < 85.0:
        suggestions.append("결측이 많은 핵심 컬럼을 보강하고 필수값 규칙을 추가한다.")
    if specificity < 70.0:
        suggestions.append("설명형 컬럼에 더 구체적인 사업 설명, 분류, 근거 정보를 포함한다.")
    if business_context is None:
        suggestions.append("평가 정확도를 높이기 위해 dataset의 business context를 함께 제공한다.")
    if "documentation_quality" in dimension_names:
        suggestions.append("설명 텍스트의 용어 정의와 사용 목적을 명시해 재사용성을 높인다.")
    return suggestions


def _average(values: list[float], *, fallback: float) -> float:
    return sum(values) / len(values) if values else fallback
