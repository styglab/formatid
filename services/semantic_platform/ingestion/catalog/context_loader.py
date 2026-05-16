from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from services.semantic_platform.ingestion.graphs.source_ingestion.config import DEFAULT_CATALOG_DIR
from services.semantic_platform.ingestion.graphs.source_ingestion.state import SourceGraphState

def load_catalog_context(state: SourceGraphState) -> SourceGraphState:
    catalog_dir = Path(os.getenv("SEMANTIC_PLATFORM_CATALOG_DIR", str(DEFAULT_CATALOG_DIR)))
    context = {
        "semantic_types": _load_catalog_yaml(catalog_dir / "core" / "semantic_types.yaml", "semantic_types"),
        "entities": _load_catalog_yaml(catalog_dir / "core" / "runtime_entities.yaml", "entities"),
        "relations": _load_catalog_yaml(catalog_dir / "core" / "runtime_relations.yaml", "relations"),
        "capabilities": _load_catalog_yaml(catalog_dir / "capabilities.yaml", "capabilities"),
        "capability_implementations": _load_catalog_yaml(
            catalog_dir / "execution" / "capability_implementations.yaml",
            "capability_implementations",
        ),
        "operation_field_mappings": _load_catalog_yaml(
            catalog_dir / "execution" / "operation_field_mappings.yaml",
            "operation_field_mappings",
        ),
    }
    state["catalog_context"] = context
    state["messages"].append(
        "load_catalog_context:"
        f"semantic_types={len(context['semantic_types'])},"
        f"capabilities={len(context['capabilities'])}"
    )
    return state


def _compact_catalog_context(catalog_context: dict[str, Any]) -> dict[str, Any]:
    semantic_types = catalog_context.get("semantic_types", {})
    capabilities = catalog_context.get("capabilities", {})
    implementations = catalog_context.get("capability_implementations", {})
    field_mappings = catalog_context.get("operation_field_mappings", {})
    return {
        "semantic_types": {
            name: {
                "description_ko": value.get("description_ko"),
                "entity": value.get("entity"),
                "aliases": value.get("aliases", [])[:12],
            }
            for name, value in list(semantic_types.items())[:100]
            if isinstance(value, dict)
        },
        "capabilities": {
            name: {
                "consumes": value.get("consumes", []),
                "produces": value.get("produces", []),
                "entities": value.get("entities", []),
                "description_ko": value.get("description_ko"),
            }
            for name, value in list(capabilities.items())[:80]
            if isinstance(value, dict)
        },
        "capability_implementations": {
            name: value[:5]
            for name, value in list(implementations.items())[:80]
            if isinstance(value, list)
        },
        "operation_field_mappings": {
            name: {
                "capability": value.get("capability"),
                "operation_id": value.get("operation_id"),
                "resource_id": value.get("resource_id"),
                "provider": value.get("provider"),
                "field_name": value.get("field_name"),
                "direction": value.get("direction"),
                "semantic_type": value.get("semantic_type"),
            }
            for name, value in list(field_mappings.items())[:120]
            if isinstance(value, dict)
        },
    }


def _load_catalog_yaml(path: Path, key: str) -> dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    try:
        import yaml

        data = yaml.safe_load(text) or {}
        value = data.get(key, {}) if isinstance(data, dict) else {}
        return value if isinstance(value, dict) else {}
    except ImportError:
        return _load_catalog_yaml_fallback(text, key)


def _load_catalog_yaml_fallback(text: str, key: str) -> dict[str, Any]:
    if f"{key}:" not in text:
        return {}
    values: dict[str, Any] = {}
    for match in re.finditer(r"^  ([A-Za-z0-9_]+):\s*$", text, flags=re.MULTILINE):
        name = match.group(1)
        values[name] = {"name": name}
    return values
