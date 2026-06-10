from __future__ import annotations

from typing import Any


def build_quality_report(
    *,
    dataset_name: str,
    business_context: str | None,
    profile: dict[str, Any],
    traditional_scores: dict[str, float],
    rubric: dict[str, Any],
    semantic_result: dict[str, Any],
    scores: dict[str, float | None],
) -> dict[str, Any]:
    issues = _issues(profile=profile, traditional_scores=traditional_scores, semantic_result=semantic_result)
    suggestions = list(dict.fromkeys([*semantic_result.get("suggestions", []), *_improvement_suggestions(traditional_scores)]))
    return {
        "dataset_id": dataset_name,
        "business_context": business_context,
        "profile": profile,
        "traditional_scores": traditional_scores,
        "rubric": rubric,
        "semantic_scores": semantic_result,
        "scores": scores,
        "issues": issues,
        "suggestions": suggestions,
        "summary": {
            "status": "completed",
            "issue_count": len(issues),
            "traditional_score": scores["traditional_score"],
            "semantic_score": scores["semantic_score"],
            "overall_score": scores["overall_score"],
        },
    }


def _issues(
    *,
    profile: dict[str, Any],
    traditional_scores: dict[str, float],
    semantic_result: dict[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if traditional_scores["completeness"] < 90:
        issues.append(
            {
                "severity": "high",
                "dimension": "completeness",
                "message": "결측 비율이 높아 핵심 데이터 활용성이 떨어진다.",
            }
        )
    if profile.get("duplicate_row_count", 0) > 0:
        issues.append(
            {
                "severity": "medium",
                "dimension": "uniqueness",
                "message": "중복 레코드가 존재한다.",
            }
        )
    if semantic_result.get("status") == "completed" and float(semantic_result.get("specificity", 0.0)) < 70.0:
        issues.append(
            {
                "severity": "medium",
                "dimension": "specificity",
                "message": "설명형 값이 충분히 구체적이지 않다.",
            }
        )
    if semantic_result.get("status") != "completed":
        issues.append(
            {
                "severity": "low",
                "dimension": "semantic_evaluation",
                "message": str(semantic_result.get("reason") or "semantic evaluation was not completed"),
            }
        )
    return issues


def _improvement_suggestions(traditional_scores: dict[str, float]) -> list[str]:
    suggestions: list[str] = []
    if traditional_scores["completeness"] < 90:
        suggestions.append("핵심 컬럼에 대한 null 허용 정책과 입력 검증을 재정의한다.")
    if traditional_scores["uniqueness"] < 100:
        suggestions.append("중복 식별 키를 정의하고 ingestion 단계에서 deduplication을 적용한다.")
    if traditional_scores["timeliness"] < 70:
        suggestions.append("수집 시점 컬럼을 명시하고 freshness 기준을 정의한다.")
    return suggestions
