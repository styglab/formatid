from __future__ import annotations

from collections import Counter
from typing import Any


def build_profile(dataset: dict[str, Any]) -> dict[str, Any]:
    headers = dataset["headers"]
    rows = dataset["rows"]
    row_count = len(rows)

    columns = []
    duplicate_counter = Counter(tuple(row.get(header, "") for header in headers) for row in rows)
    duplicate_rows = sum(count - 1 for count in duplicate_counter.values() if count > 1)

    for header in headers:
        values = [row.get(header, "") for row in rows]
        non_empty_values = [value for value in values if value != ""]
        lengths = [len(value) for value in non_empty_values]
        columns.append(
            {
                "name": header,
                "null_count": len(values) - len(non_empty_values),
                "null_rate": _ratio(len(values) - len(non_empty_values), row_count),
                "distinct_count": len(set(non_empty_values)),
                "min_length": min(lengths) if lengths else 0,
                "max_length": max(lengths) if lengths else 0,
                "avg_length": round(sum(lengths) / len(lengths), 2) if lengths else 0.0,
                "looks_textual": _looks_textual(header, non_empty_values),
            }
        )

    return {
        "row_count": row_count,
        "column_count": len(headers),
        "duplicate_row_count": duplicate_rows,
        "duplicate_rate": _ratio(duplicate_rows, row_count),
        "columns": columns,
        "freshness": _freshness_profile(headers, rows),
    }


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _looks_textual(header: str, values: list[str]) -> bool:
    lowered = header.lower()
    if any(token in lowered for token in ("description", "summary", "content", "note", "comment")):
        return True
    if not values:
        return False
    long_values = sum(1 for value in values if len(value.split()) >= 3 or len(value) >= 20)
    return long_values >= max(1, len(values) // 2)


def _freshness_profile(headers: list[str], rows: list[dict[str, str]]) -> dict[str, Any]:
    candidates = [header for header in headers if header.lower() in {"created_at", "updated_at", "date", "dt"}]
    return {
        "status": "not_evaluated" if not candidates else "available",
        "candidate_columns": candidates,
        "row_count": len(rows),
    }
