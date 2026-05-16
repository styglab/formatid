from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from services.semantic_platform.ingestion.graphs.source_ingestion.config import DEFAULT_CATALOG_DIR
from services.semantic_platform.ingestion.graphs.source_ingestion.state import SourceGraphState


def apply_catalog_changes(state: SourceGraphState) -> SourceGraphState:
    catalog_dir = Path(os.getenv("SEMANTIC_PLATFORM_CATALOG_DIR", str(DEFAULT_CATALOG_DIR)))
    semantic_changes = state.get("semantic_platform_proposal", {}).get("changes", {})
    execution_changes = state.get("execution_contract_proposal", {}).get("changes", {})
    changed: list[dict[str, Any]] = []

    if semantic_changes.get("semantic_types"):
        changed.append(
            _append_yaml_map(
                catalog_dir / "core" / "semantic_types.yaml",
                "semantic_types",
                semantic_changes["semantic_types"],
            )
        )
    if semantic_changes.get("entities"):
        changed.append(
            _append_yaml_map(
                catalog_dir / "core" / "runtime_entities.yaml",
                "entities",
                semantic_changes["entities"],
            )
        )
    if semantic_changes.get("relations"):
        changed.append(
            _append_yaml_map(
                catalog_dir / "core" / "runtime_relations.yaml",
                "relations",
                semantic_changes["relations"],
            )
        )
    if semantic_changes.get("capabilities"):
        changed.append(
            _append_yaml_map(
                catalog_dir / "capabilities.yaml",
                "capabilities",
                semantic_changes["capabilities"],
            )
        )
    if semantic_changes.get("resources"):
        changed.append(
            _append_yaml_map(
                catalog_dir / "resources" / "resources.yaml",
                "resources",
                semantic_changes["resources"],
            )
        )
    if semantic_changes.get("crosswalks"):
        changed.append(
            _append_yaml_map(
                catalog_dir / "mappings" / "crosswalks.yaml",
                "crosswalks",
                semantic_changes["crosswalks"],
            )
        )
    if execution_changes.get("capability_implementations"):
        changed.append(
            _append_yaml_map(
                catalog_dir / "execution" / "capability_implementations.yaml",
                "capability_implementations",
                _normalize_capability_implementations(execution_changes["capability_implementations"]),
            )
        )
    operation_field_mappings = execution_changes.get("operation_field_mappings") or execution_changes.get(
        "provider_field_mappings"
    )
    if operation_field_mappings:
        changed.append(
            _append_yaml_map(
                catalog_dir / "execution" / "operation_field_mappings.yaml",
                "operation_field_mappings",
                operation_field_mappings,
            )
        )

    state["applied_changes"] = [item for item in changed if item.get("applied") or item.get("skipped")]
    state["messages"].append("apply_catalog_changes:direct_apply")
    return state


def _append_yaml_map(path: Path, top_key: str, changes: dict[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(f"{top_key}:\n", encoding="utf-8")

    text = path.read_text(encoding="utf-8")
    if not text.strip():
        text = f"{top_key}:\n"
    text = re.sub(rf"^{re.escape(top_key)}:\s*\{{\}}\s*$", f"{top_key}:", text, flags=re.MULTILINE)
    if not re.search(rf"^{re.escape(top_key)}:\s*$", text, flags=re.MULTILINE):
        text = text.rstrip() + f"\n{top_key}:\n"

    applied = []
    skipped = []
    lines = [text.rstrip(), ""]
    for name, value in changes.items():
        if not isinstance(value, (dict, list)):
            skipped.append({"name": name, "reason": "unsupported_change_shape"})
            continue
        if _has_yaml_map_key(text, name):
            skipped.append({"name": name, "reason": "already_exists"})
            continue
        lines.append(_yaml_entry(name, _canonical_value(value), indent=2).rstrip())
        lines.append("")
        applied.append(name)
    if applied:
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"path": str(path), "key": top_key, "applied": applied, "skipped": skipped}


def _has_yaml_map_key(text: str, key: str) -> bool:
    return bool(re.search(rf"^  {re.escape(key)}:\s*", text, flags=re.MULTILINE))


def _canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _canonical_value(item)
            for key, item in value.items()
            if key not in {"existing", "evidence"}
        }
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    return value


def _normalize_capability_implementations(changes: dict[str, Any]) -> dict[str, Any]:
    normalized = {}
    for capability, value in changes.items():
        normalized[capability] = value if isinstance(value, list) else [value]
    return normalized


def _yaml_entry(key: str, value: Any, indent: int) -> str:
    return " " * indent + f"{key}:" + _yaml_value(value, indent)


def _yaml_value(value: Any, indent: int) -> str:
    if isinstance(value, dict):
        lines = [""]
        for key, item in value.items():
            lines.append(_yaml_entry(str(key), item, indent + 2))
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return " []"
        if all(not isinstance(item, (dict, list)) for item in value):
            return " [" + ", ".join(_quote_scalar(item) for item in value) + "]"
        lines = [""]
        for item in value:
            if isinstance(item, dict):
                lines.append(" " * (indent + 2) + "-")
                for key, nested in item.items():
                    lines.append(_yaml_entry(str(key), nested, indent + 4))
            else:
                lines.append(" " * (indent + 2) + "- " + _quote_scalar(item))
        return "\n".join(lines)
    return " " + _quote_scalar(value)


def _quote_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if not text:
        return '""'
    if re.fullmatch(r"[A-Za-z0-9가-힣_.:/-]+", text):
        return text
    return json.dumps(text, ensure_ascii=False)
