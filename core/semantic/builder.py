from __future__ import annotations

from collections import ChainMap
from typing import Any


def build_semantic_object(
    spec: dict[str, Any],
    entity_type: str,
    record: dict[str, Any],
    *,
    relationships: list[dict[str, Any]] | None = None,
    semantic_tags: list[str] | None = None,
) -> dict[str, Any]:
    entity_spec = spec["entities"][entity_type]
    entity_id = str(_resolve(record, entity_spec["id"]) or "unknown")
    label = str(_resolve(record, entity_spec.get("label")) or entity_id)
    return {
        "semantic_model_version": spec["version"],
        "entity_type": entity_type,
        "entity_id": entity_id,
        "label": label,
        "attributes": _attributes(record, entity_spec.get("attributes", {})),
        "relationships": [
            *_relationships(record, entity_spec.get("relationships", [])),
            *(relationships or []),
        ],
        "semantic_tags": [
            *entity_spec.get("semantic_tags", []),
            *(semantic_tags or []),
        ],
    }


def build_semantic_document(spec: dict[str, Any], semantic_object: dict[str, Any]) -> dict[str, Any]:
    document_spec = spec.get("documents", {}).get(semantic_object["entity_type"], {})
    attributes = semantic_object["attributes"]
    text_parts = []

    for field in document_spec.get("attribute_lines", []):
        value = attributes.get(field["attribute"])
        if value:
            text_parts.append(f"{field['label']}: {value}")

    for field in document_spec.get("relationship_lines", []):
        values = _relationship_labels(semantic_object["relationships"], field["predicate"])
        if values:
            text_parts.append(f"{field['label']}: {', '.join(values)}")

    if not text_parts:
        text_parts.append(semantic_object["label"])

    return {
        "semantic_model_version": semantic_object["semantic_model_version"],
        "document_id": f"{semantic_object['entity_type']}:{semantic_object['entity_id']}",
        "entity": {
            "entity_type": semantic_object["entity_type"],
            "entity_id": semantic_object["entity_id"],
            "label": semantic_object["label"],
        },
        "title": semantic_object["label"],
        "text": "\n".join(text_parts),
        "metadata": {
            field: attributes.get(field)
            for field in document_spec.get("metadata_attributes", [])
            if attributes.get(field) is not None
        }
        | {"semantic_tags": semantic_object["semantic_tags"]},
        "relationships": semantic_object["relationships"],
        "semantic_tags": semantic_object["semantic_tags"],
    }


def relationship(
    predicate: str,
    target_type: str,
    label: Any,
    *,
    entity_id: str | None = None,
    target_attributes: dict[str, Any] | None = None,
    relationship_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = {
        "predicate": predicate,
        "target": {
            "entity_type": target_type,
            "entity_id": entity_id or str(label),
            "label": str(label),
        },
    }
    if target_attributes:
        item["target"]["attributes"] = target_attributes
    if relationship_attributes:
        item["attributes"] = relationship_attributes
    return item


def relationship_labels(relationships: list[dict[str, Any]], predicate: str) -> list[str]:
    return _relationship_labels(relationships, predicate)


def _attributes(record: dict[str, Any], mapping: dict[str, Any]) -> dict[str, Any]:
    return {name: _resolve(record, source) for name, source in mapping.items()}


def _relationships(record: dict[str, Any], specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items = []
    for spec in specs:
        label = _resolve(record, spec["target_label"])
        if label in (None, ""):
            continue
        items.append(
            relationship(
                spec["predicate"],
                spec["target_entity"],
                label,
                entity_id=_resolve(record, spec.get("target_id")),
                target_attributes=_attributes(record, spec.get("target_attributes", {})),
                relationship_attributes=_attributes(record, spec.get("attributes", {})),
            )
        )
    return items


def _relationship_labels(relationships: list[dict[str, Any]], predicate: str) -> list[str]:
    return [
        str(relationship["target"].get("label"))
        for relationship in relationships
        if relationship["predicate"] == predicate and relationship["target"].get("label")
    ]


def _resolve(record: dict[str, Any], source: Any) -> Any:
    if source is None:
        return None
    if isinstance(source, str):
        return _get(record, source)
    if not isinstance(source, dict):
        return source
    if "constant" in source:
        return source["constant"]
    if "first_of" in source:
        for candidate in source["first_of"]:
            value = _resolve(record, candidate)
            if value not in (None, ""):
                return value
        return None
    if "field" in source:
        return _get(record, source["field"])
    if "template" in source:
        return source["template"].format_map(ChainMap(record, _DefaultMapping()))
    return None


def _get(record: dict[str, Any], path: str) -> Any:
    value: Any = record
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


class _DefaultMapping(dict[str, str]):
    def __missing__(self, key: str) -> str:
        return "unknown"
