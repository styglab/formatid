from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from apps.pubdata_mcp.app.common.catalog import semantic_resolve
from core.mcp_runtime.importers import import_string


def normalize_items(raw_items: list[dict[str, Any]], response_spec: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not response_spec:
        return raw_items
    item_spec = response_spec.get("item") or {}
    if not item_spec:
        return raw_items

    keep_raw = bool(item_spec.get("keep_raw", True))
    normalized_spec = item_spec.get("normalized") or {}
    if not normalized_spec:
        return raw_items

    normalized_items = []
    for raw_item in raw_items:
        normalized = {}
        semantic_fields = {}
        for target_name, field_spec in normalized_spec.items():
            if not isinstance(field_spec, dict):
                continue
            raw_value = resolve_field_value(raw_item, field_spec, target_name)
            normalized[target_name] = cast_value(
                raw_value,
                field_spec.get("type"),
            )
            catalog_field = field_spec.get("catalog_field")
            if catalog_field:
                semantic_fields[target_name] = catalog_field_metadata(str(catalog_field))
        if keep_raw:
            item = {"raw": raw_item, "normalized": normalized}
        else:
            item = normalized
        if semantic_fields and isinstance(item, dict):
            item["semantic"] = {"fields": semantic_fields}
        normalized_items.append(item)
    return normalized_items


def resolve_field_value(raw_item: dict[str, Any], field_spec: dict[str, Any], target_name: str) -> Any:
    parser = field_spec.get("parser")
    if parser:
        return import_string(str(parser))(raw_item)
    return resolve_path(raw_item, field_spec.get("path", target_name))


def catalog_field_metadata(catalog_field: str) -> dict[str, Any]:
    resolved = semantic_resolve(catalog_field, limit=1)
    matches = resolved.get("matches", []) if isinstance(resolved, dict) else []
    field = matches[0] if matches and isinstance(matches[0], dict) else {}
    return {
        "catalog_field": catalog_field,
        "semantic_name": field.get("name"),
        "kind": field.get("kind"),
        "domain": field.get("domain"),
        "entity": field.get("entity"),
        "type": field.get("type"),
        "description_ko": field.get("description_ko"),
        "aliases": field.get("aliases", []),
    }


def resolve_path(value: Any, path: str | None) -> Any:
    if not path:
        return value
    current = value
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def cast_value(value: Any, target_type: str | None) -> Any:
    if value in (None, ""):
        return None
    match str(target_type or "string").lower():
        case "string":
            return str(value)
        case "integer" | "int":
            return _int(value)
        case "number" | "decimal":
            return _decimal_string(value)
        case "boolean" | "bool":
            return _bool(value)
        case "array" | "list":
            return value if isinstance(value, list) else []
        case "object":
            return value if isinstance(value, dict) else {}
        case "date":
            return _date(value)
        case "datetime":
            return _datetime(value)
        case _:
            return value


def _int(value: Any) -> int | None:
    try:
        return int(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _decimal_string(value: Any) -> str | None:
    try:
        return str(Decimal(str(value).replace(",", "")))
    except (InvalidOperation, ValueError):
        return None


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"y", "yes", "true", "1"}:
        return True
    if normalized in {"n", "no", "false", "0"}:
        return False
    return None


def _date(value: Any) -> str | None:
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return None


def _datetime(value: Any) -> str | None:
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M"):
        try:
            return datetime.strptime(text, fmt).isoformat()
        except ValueError:
            continue
    return _date(text)
