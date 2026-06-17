from __future__ import annotations

from typing import Any


def run_traditional_dq(profile: dict[str, Any], dataset: dict[str, Any]) -> dict[str, float]:
    row_count = max(int(profile.get("row_count", 0)), 0)
    columns = profile.get("columns", [])
    raw_row_count = max(int(dataset.get("raw_row_count", row_count)), 0)

    completeness = _average(
        [
            max(0.0, 100.0 - float(column.get("null_rate", 0.0)) * 100.0)
            for column in columns
            if isinstance(column, dict)
        ],
        fallback=100.0,
    )

    uniqueness = max(0.0, 100.0 - float(profile.get("duplicate_rate", 0.0)) * 100.0)
    consistency = 100.0 if raw_row_count == row_count else max(0.0, 100.0 - ((raw_row_count - row_count) * 100.0 / max(raw_row_count, 1)))
    validity = _validity_score(dataset)
    timeliness = _timeliness_score(profile)

    return {
        "completeness": round(completeness, 2),
        "validity": round(validity, 2),
        "consistency": round(consistency, 2),
        "uniqueness": round(uniqueness, 2),
        "timeliness": round(timeliness, 2),
    }


def _average(values: list[float], *, fallback: float) -> float:
    return sum(values) / len(values) if values else fallback


def _validity_score(dataset: dict[str, Any]) -> float:
    headers = dataset.get("headers", [])
    rows = dataset.get("rows", [])
    if not headers:
        return 0.0
    valid_header_names = all(isinstance(header, str) and header.strip() for header in headers)
    if not valid_header_names:
        return 0.0

    issues = 0
    checks = 0
    for row in rows:
        for header in headers:
            checks += 1
            value = row.get(header, "")
            if len(str(value)) > 5000:
                issues += 1
    if checks == 0:
        return 100.0
    return max(0.0, 100.0 - (issues * 100.0 / checks))


def _timeliness_score(profile: dict[str, Any]) -> float:
    freshness = profile.get("freshness", {})
    if not isinstance(freshness, dict):
        return 50.0
    return 80.0 if freshness.get("status") == "available" else 50.0
