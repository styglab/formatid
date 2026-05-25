from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
from pathlib import Path
from typing import Any

from services.semantic_platform.lib.ingestion.state import (
    INGESTION_GRAPH_VERSION,
    INGESTION_PROMPT_VERSION,
    SourceGraphState,
)


def read_source(state: SourceGraphState) -> SourceGraphState:
    source = Path(state["source_path"])
    data = source.read_bytes()
    sha256 = hashlib.sha256(data).hexdigest()
    source_metadata = _source_metadata(source)
    document_id = _source_document_id(source, sha256, source_metadata)
    metadata = {
        "file_name": source.name,
        "ingestion_graph_version": INGESTION_GRAPH_VERSION,
        "ingestion_prompt_version": INGESTION_PROMPT_VERSION,
        "embedding_model": os.getenv("SEMANTIC_PLATFORM_EMBEDDING_MODEL", "BGE-m3-ko"),
        **source_metadata,
    }
    return {
        **state,
        "source_bytes": data,
        "source_document": {
            "id": document_id,
            "path": str(source),
            "file_name": source.name,
            "sha256": sha256,
            "mime_type": mimetypes.guess_type(source.name)[0],
            "size_bytes": len(data),
            "metadata": metadata,
        },
    }


def _source_document_id(source: Path, sha256: str, metadata: dict[str, Any]) -> str:
    provider = _slug_optional(metadata.get("provider"))
    source_key = _slug_optional(metadata.get("source_key") or metadata.get("key"))
    version = _slug_optional(metadata.get("version"))
    if provider and source_key:
        parts = ["source", provider, source_key]
        if version:
            parts.append(version)
        return ".".join(parts)
    return f"source.{sha256[:8]}.{_slug(source.stem)}"


def _source_metadata(source: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    metadata.update(_load_source_manifest_metadata(source))
    metadata.update(_load_source_sidecar_metadata(source))
    return metadata


def _load_source_manifest_metadata(source: Path) -> dict[str, Any]:
    manifest_path = os.getenv("SEMANTIC_PLATFORM_SOURCE_MANIFEST")
    candidates = [Path(manifest_path)] if manifest_path else [source.parent / "manifest.json", source.parent / "sources.json"]
    source_resolved = source.resolve()
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        entries = payload.get("sources") if isinstance(payload, dict) else payload
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict) or not entry.get("path"):
                continue
            entry_path = Path(str(entry["path"]))
            if not entry_path.is_absolute():
                entry_path = candidate.parent / entry_path
            try:
                if entry_path.resolve() == source_resolved:
                    return {key: value for key, value in entry.items() if key != "path"}
            except OSError:
                continue
    return {}


def _load_source_sidecar_metadata(source: Path) -> dict[str, Any]:
    candidates = [
        source.with_suffix(source.suffix + ".source.json"),
        source.with_suffix(".source.json"),
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        return payload if isinstance(payload, dict) else {}
    return {}


def _slug(value: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", value).strip("_").lower()
    return slug[:80] or "document"


def _slug_optional(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return ""
    return _slug(str(value))
