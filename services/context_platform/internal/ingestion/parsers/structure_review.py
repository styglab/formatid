from __future__ import annotations

from pathlib import Path
from typing import Any

from services.context_platform.internal.ingestion.langgraph.document_structure import (
    extract_document_structure_with_graph,
)
from .common import DiscoveredField, LoadedSource, StructureOperationDraft, parse_csv_rows


def discover_structures(loaded: LoadedSource, operations: list[dict[str, Any]], strategy: str) -> list[StructureOperationDraft]:
    operation_map = {str(item.get("operation_key") or ""): item for item in operations}
    if loaded.content_json and isinstance(loaded.content_json, dict) and isinstance(loaded.content_json.get("paths"), dict):
        return _discover_openapi_json_fields(loaded, operation_map)
    if loaded.filename.lower().endswith((".csv", ".tsv")):
        return _discover_csv_fields(loaded, operations)
    if strategy in {"semi_structured", "document_heavy"}:
        return _discover_document_fields(loaded, operations)
    return []


def _discover_openapi_json_fields(
    loaded: LoadedSource,
    operation_map: dict[str, dict[str, Any]],
) -> list[StructureOperationDraft]:
    document = loaded.content_json if isinstance(loaded.content_json, dict) else {}
    drafts: list[StructureOperationDraft] = []
    for path, path_item in (document.get("paths") or {}).items():
        if not isinstance(path_item, dict):
            continue
        for method, operation_item in path_item.items():
            if not isinstance(operation_item, dict):
                continue
            operation_key = str(operation_item.get("operationId") or f"{method}_{str(path).strip('/').replace('/', '_')}")
            if operation_key not in operation_map:
                continue
            fields: list[DiscoveredField] = []
            for parameter in operation_item.get("parameters") or []:
                if not isinstance(parameter, dict):
                    continue
                param_name = str(parameter.get("name") or "")
                location = str(parameter.get("in") or "query")
                schema = parameter.get("schema") if isinstance(parameter.get("schema"), dict) else {}
                fields.append(
                    DiscoveredField(
                        scope="control" if param_name.lower().endswith("div") else "input",
                        raw_name=param_name,
                        field_path=f"{location}.{param_name}",
                        data_type=str(schema.get("type") or "string"),
                        is_required=bool(parameter.get("required")),
                        description=str(parameter.get("description") or ""),
                        evidence=[{"kind": "parameter", "path": path, "method": str(method).upper(), "name": param_name}],
                    )
                )
            responses = operation_item.get("responses") if isinstance(operation_item.get("responses"), dict) else {}
            best_response = responses.get("200") or responses.get("201") or next(iter(responses.values()), {})
            schema = _extract_response_schema(best_response)
            for field in _flatten_schema_properties(schema, prefix="body"):
                fields.append(field)
            drafts.append(StructureOperationDraft(operation_key=operation_key, fields=fields))
    return drafts


def _discover_csv_fields(loaded: LoadedSource, operations: list[dict[str, Any]]) -> list[StructureOperationDraft]:
    if not operations:
        return []
    rows = parse_csv_rows(loaded.content_text)
    if not rows:
        return []
    headers = rows[0]
    operation_key = str(operations[0].get("operation_key") or operations[0].get("name") or "inspect_rows")
    fields = [
        DiscoveredField(
            scope="output",
            raw_name=header.strip() or f"column_{index + 1}",
            field_path=f"row.{header.strip() or f'column_{index + 1}'}",
            data_type="string",
            is_required=False,
            description=f"Column discovered from {Path(loaded.filename).name}",
            evidence=[{"kind": "csv_header", "column_index": index}],
        )
        for index, header in enumerate(headers)
    ]
    return [StructureOperationDraft(operation_key=operation_key, fields=fields)]


def _discover_document_fields(loaded: LoadedSource, operations: list[dict[str, Any]]) -> list[StructureOperationDraft]:
    if not operations:
        return []
    result = extract_document_structure_with_graph(loaded, operations)
    return result.drafts


def _extract_response_schema(response: Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    content = response.get("content") if isinstance(response.get("content"), dict) else {}
    for media_type in ("application/json", "application/*+json", "*/*"):
        media = content.get(media_type)
        if isinstance(media, dict) and isinstance(media.get("schema"), dict):
            return media["schema"]
    first_media = next(iter(content.values()), None)
    if isinstance(first_media, dict) and isinstance(first_media.get("schema"), dict):
        return first_media["schema"]
    return {}


def _flatten_schema_properties(schema: dict[str, Any], prefix: str) -> list[DiscoveredField]:
    properties = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
    required = set(schema.get("required") or [])
    fields: list[DiscoveredField] = []
    for name, definition in properties.items():
        if not isinstance(definition, dict):
            continue
        field_path = f"{prefix}.{name}"
        data_type = str(definition.get("type") or "string")
        fields.append(
            DiscoveredField(
                scope="output",
                raw_name=str(name),
                field_path=field_path,
                data_type=data_type,
                is_required=name in required,
                description=str(definition.get("description") or ""),
                evidence=[{"kind": "response_schema", "field_path": field_path}],
            )
        )
        if data_type == "object":
            fields.extend(_flatten_schema_properties(definition, prefix=field_path))
    return fields
