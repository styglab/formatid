from __future__ import annotations

from typing import Any

def build_graph_result_details(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "notices": _count(result.get("notices")),
        "attachment_candidates": _count(result.get("attachment_candidates")),
        "participant_candidates": _count(result.get("participant_candidates")),
        "winner_candidates": _count(result.get("winner_candidates")),
        "status": result.get("status"),
        "skip_reason": result.get("skip_reason"),
    }


def _count(value: object) -> int:
    if isinstance(value, list):
        return len(value)
    return 0
