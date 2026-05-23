from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

from core.runtime.mcp.importers import import_string
from core.runtime.mcp.loader import load_manifest, load_tool_specs, load_tools_dir, load_tools_file


TYPE_MAP = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def register_from_manifest(mcp: Any, manifest_path: str | Path) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    tool_specs = load_tool_specs(manifest)
    for spec in tool_specs:
        _register_tool(mcp, spec)
    return {"manifest": manifest, "tools": tool_specs}


def register_from_yaml(mcp: Any, tools_path: str | Path) -> dict[str, Any]:
    config = load_tools_file(tools_path)
    tool_specs = config["tools"]
    for spec in tool_specs:
        _register_tool(mcp, spec)
    return {"config": config, "tools": tool_specs}


def register_from_yaml_dir(mcp: Any, specs_dir: str | Path) -> dict[str, Any]:
    configs = load_tools_dir(specs_dir)
    registered_specs = []
    for config in configs:
        for spec in config["tools"]:
            _register_tool(mcp, spec)
            registered_specs.append(spec)
    return {"configs": configs, "tools": registered_specs}


def _register_tool(mcp: Any, spec: dict[str, Any]) -> None:
    name = _required_str(spec, "name")
    description = _required_str(spec, "description")
    handler = import_string(_required_str(spec, "handler"))
    input_spec = spec.get("input") or {}
    fields = input_spec.get("fields") or {}
    required = set(input_spec.get("required") or [])

    def tool(**kwargs: Any) -> Any:
        cleaned = {key: value for key, value in kwargs.items() if value is not None}
        try:
            result = handler(**cleaned)
        except ValueError as exc:
            return {"error": {"type": "invalid_request", "message": str(exc)}}
        if isinstance(result, dict) and spec.get("evidence"):
            result.setdefault("evidence", spec["evidence"])
        return result

    tool.__name__ = name
    tool.__qualname__ = name
    tool.__doc__ = description
    tool.__annotations__ = _annotations(fields)
    tool.__signature__ = _signature(fields, required)  # type: ignore[attr-defined]
    mcp.tool(tool)


def _signature(fields: dict[str, Any], required: set[str]) -> inspect.Signature:
    parameters = []
    for field_name, field_spec in fields.items():
        default = inspect.Parameter.empty if field_name in required else field_spec.get("default", None)
        parameters.append(
            inspect.Parameter(
                field_name,
                inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=_annotation(field_spec),
            )
        )
    return inspect.Signature(parameters=parameters)


def _annotations(fields: dict[str, Any]) -> dict[str, Any]:
    return {field_name: _annotation(field_spec) for field_name, field_spec in fields.items()}


def _annotation(field_spec: dict[str, Any]) -> Any:
    return TYPE_MAP.get(str(field_spec.get("type") or "").lower(), Any)


def _required_str(spec: dict[str, Any], key: str) -> str:
    value = spec.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Tool spec missing required string field: {key}")
    return value.strip()
