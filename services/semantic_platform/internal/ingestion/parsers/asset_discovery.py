from __future__ import annotations

import re
from pathlib import Path

from .common import DiscoveredAsset, LoadedSource


_HTTP_METHODS = ("get", "post", "put", "patch", "delete")


def discover_assets(loaded: LoadedSource, strategy: str) -> list[DiscoveredAsset]:
    if loaded.content_json and isinstance(loaded.content_json, dict) and isinstance(loaded.content_json.get("paths"), dict):
        return _discover_openapi_json_assets(loaded)
    if loaded.filename.lower().endswith((".csv", ".tsv")):
        return _discover_tabular_asset(loaded)
    if strategy in {"semi_structured", "document_heavy"} and loaded.content_text:
        yaml_assets = _discover_openapi_yaml_like_assets(loaded)
        if yaml_assets:
            return yaml_assets
    return [_discover_document_asset(loaded)]


def _discover_openapi_json_assets(loaded: LoadedSource) -> list[DiscoveredAsset]:
    document = loaded.content_json if isinstance(loaded.content_json, dict) else {}
    info = document.get("info") if isinstance(document.get("info"), dict) else {}
    asset_name = str(info.get("title") or loaded.source_name or Path(loaded.filename).stem or "Source Asset")
    asset = DiscoveredAsset(
        name=asset_name,
        asset_type="file",
        locator=loaded.reference_uri or loaded.stored_path or loaded.filename,
        description=str(info.get("description") or f"API asset discovered from {loaded.filename}"),
        metadata={"source_format": "openapi_json", "version": info.get("version") or ""},
    )
    for path, path_item in (document.get("paths") or {}).items():
        if not isinstance(path_item, dict):
            continue
        for method, operation_item in path_item.items():
            if str(method).lower() not in _HTTP_METHODS or not isinstance(operation_item, dict):
                continue
            operation_id = str(operation_item.get("operationId") or f"{method}_{str(path).strip('/').replace('/', '_')}" or "operation")
            display_name = str(operation_item.get("summary") or operation_item.get("description") or operation_id)
            asset.access_paths.append(
                {
                    "name": operation_id,
                    "access_type": "http",
                    "locator": path,
                    "http_method": str(method).upper(),
                    "description": display_name,
                    "operation_key": operation_id,
                    "operation_name": display_name,
                    "operation_description": str(operation_item.get("description") or operation_item.get("summary") or ""),
                    "metadata": {"source_format": "openapi_json"},
                }
            )
    if not asset.access_paths:
        asset.access_paths.append(_default_document_access_path())
    return [asset]


def _discover_openapi_yaml_like_assets(loaded: LoadedSource) -> list[DiscoveredAsset]:
    asset = DiscoveredAsset(
        name=Path(loaded.filename).stem or loaded.source_name,
        asset_type="file" if loaded.source_type == "api" else "other",
        locator=loaded.reference_uri or loaded.stored_path or loaded.filename,
        description=f"YAML-like source discovered from {loaded.filename}",
        metadata={"source_format": "yaml_like_text"},
    )
    current_path = ""
    path_pattern = re.compile(r"^\s{0,4}(/[^:#]+):\s*$")
    method_pattern = re.compile(r"^\s{2,}(get|post|put|patch|delete):\s*$", re.IGNORECASE)
    operation_id_pattern = re.compile(r"^\s{4,}operationId:\s*(.+?)\s*$")
    summary_pattern = re.compile(r"^\s{4,}(summary|description):\s*(.+?)\s*$")
    pending_record: dict[str, str] | None = None
    for line in loaded.content_text.splitlines():
        path_match = path_pattern.match(line)
        if path_match:
            current_path = path_match.group(1).strip()
            pending_record = None
            continue
        method_match = method_pattern.match(line)
        if method_match and current_path:
            if pending_record:
                asset.access_paths.append(pending_record)
            pending_record = {
                "name": f"{method_match.group(1).lower()}_{current_path.strip('/').replace('/', '_') or 'root'}",
                "access_type": "http",
                "locator": current_path,
                "http_method": method_match.group(1).upper(),
                "description": current_path,
                "operation_key": f"{method_match.group(1).lower()}_{current_path.strip('/').replace('/', '_') or 'root'}",
                "operation_name": current_path,
                "operation_description": "",
                "metadata": {"source_format": "yaml_like_text"},
            }
            continue
        if pending_record is None:
            continue
        operation_id_match = operation_id_pattern.match(line)
        if operation_id_match:
            op_key = operation_id_match.group(1).strip().strip("'\"")
            pending_record["name"] = op_key
            pending_record["operation_key"] = op_key
            pending_record["operation_name"] = op_key
            continue
        summary_match = summary_pattern.match(line)
        if summary_match:
            pending_record["operation_description"] = summary_match.group(2).strip().strip("'\"")
            pending_record["description"] = pending_record["operation_description"] or pending_record["description"]
    if pending_record:
        asset.access_paths.append(pending_record)
    if not asset.access_paths:
        return []
    return [asset]


def _discover_tabular_asset(loaded: LoadedSource) -> list[DiscoveredAsset]:
    asset_name = Path(loaded.filename).stem or loaded.source_name
    asset = DiscoveredAsset(
        name=asset_name,
        asset_type="table",
        locator=loaded.reference_uri or loaded.stored_path or loaded.filename,
        description=f"Tabular asset discovered from {loaded.filename}",
        metadata={"source_format": "csv"},
    )
    asset.access_paths.append(
        {
            "name": f"rows_{asset_name.lower().replace(' ', '_')}",
            "access_type": "file_read",
            "locator": asset.locator,
            "http_method": "",
            "description": f"Row access for {asset_name}",
            "operation_key": f"inspect_{asset_name.lower().replace(' ', '_')}_rows",
            "operation_name": f"Inspect {asset_name} Rows",
            "operation_description": f"Inspect row-oriented fields in {asset_name}.",
            "metadata": {"source_format": "csv"},
        }
    )
    return [asset]


def _discover_document_asset(loaded: LoadedSource) -> DiscoveredAsset:
    asset = DiscoveredAsset(
        name=Path(loaded.filename).stem or loaded.source_name or "Source Document",
        asset_type="file",
        locator=loaded.reference_uri or loaded.stored_path or loaded.filename,
        description=f"Document asset discovered from {loaded.filename}",
        metadata={"source_format": "document_text"},
    )
    asset.access_paths.append(_default_document_access_path())
    return asset


def _default_document_access_path() -> dict[str, str]:
    return {
        "name": "document_context",
        "access_type": "file_read",
        "locator": "document",
        "http_method": "",
        "description": "Document context access path",
        "operation_key": "inspect_document_context",
        "operation_name": "Inspect Document Context",
        "operation_description": "Review document context and extracted structure candidates.",
        "metadata": {"source_format": "document_text"},
    }
