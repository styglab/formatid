from __future__ import annotations

import csv
import io
from typing import Any


def load_csv_dataset(csv_text: str) -> dict[str, Any]:
    if not csv_text.strip():
        raise ValueError("csv_text must not be empty")

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise ValueError("csv_text must include a header row")

    headers = [str(name).strip() for name in reader.fieldnames]
    if not all(headers):
        raise ValueError("csv header names must not be empty")

    rows: list[dict[str, str]] = []
    raw_rows = 0
    for item in reader:
        raw_rows += 1
        row = {header: _normalize_cell(item.get(header)) for header in headers}
        rows.append(row)

    return {
        "headers": headers,
        "rows": rows,
        "raw_row_count": raw_rows,
    }


def sample_records(rows: list[dict[str, str]], *, limit: int = 5) -> list[dict[str, str]]:
    return rows[: max(limit, 0)]


def _normalize_cell(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()
