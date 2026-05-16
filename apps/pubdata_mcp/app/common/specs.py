from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


SPECS_DIR = Path("apps/pubdata_mcp/specs")


def get_tool_spec(tool_name: str) -> dict[str, Any]:
    for provider_spec in load_provider_specs():
        for tool_spec in provider_spec.get("tools", []):
            if isinstance(tool_spec, dict) and tool_spec.get("name") == tool_name:
                return tool_spec
    return {}


def get_response_spec(tool_name: str) -> dict[str, Any] | None:
    tool_spec = get_tool_spec(tool_name)
    response_spec = tool_spec.get("response")
    return response_spec if isinstance(response_spec, dict) else None


def get_evidence_spec(tool_name: str) -> dict[str, Any]:
    tool_spec = get_tool_spec(tool_name)
    evidence_spec = tool_spec.get("evidence")
    return evidence_spec if isinstance(evidence_spec, dict) else {}


@lru_cache(maxsize=1)
def load_provider_specs() -> tuple[dict[str, Any], ...]:
    specs = []
    for path in sorted(SPECS_DIR.rglob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(data, dict):
            specs.append(data)
    return tuple(specs)
