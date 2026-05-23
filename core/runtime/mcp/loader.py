from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    data = _load_yaml(manifest_path)
    data["__path"] = str(manifest_path)
    data["__base_dir"] = str(manifest_path.parent)
    return data


def load_tools_file(path: str | Path) -> dict[str, Any]:
    tools_path = Path(path)
    data = _load_yaml(tools_path)
    data["__path"] = str(tools_path)
    data["__base_dir"] = str(tools_path.parent)
    tools = data.get("tools")
    if not isinstance(tools, list):
        raise ValueError(f"YAML tools file must contain a tools list: {tools_path}")
    return data


def load_tools_dir(path: str | Path) -> list[dict[str, Any]]:
    specs_dir = Path(path)
    if not specs_dir.exists():
        raise FileNotFoundError(str(specs_dir))
    if not specs_dir.is_dir():
        raise NotADirectoryError(str(specs_dir))
    return [load_tools_file(spec_path) for spec_path in sorted(specs_dir.rglob("*.yaml"))]


def load_tool_specs(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    base_dir = Path(manifest["__base_dir"])
    tools_dir = base_dir / manifest.get("tools_dir", "tools")
    tool_files = manifest.get("tools")
    if tool_files is None:
        paths = sorted(tools_dir.glob("*.yaml"))
    else:
        paths = [tools_dir / item for item in tool_files]
    specs = []
    for path in paths:
        spec = _load_yaml(path)
        spec["__path"] = str(path)
        specs.append(spec)
    return specs


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return data
