from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


DEFAULT_PATTERNS = ("*.md", "*.txt", "*.docx", "*.html", "*.xml", "*.zip")
SKIP_PARTS = {"proposals", "__pycache__"}


def scan_sources(root: str | Path = "sources", patterns: tuple[str, ...] = DEFAULT_PATTERNS) -> list[dict[str, Any]]:
    root_path = Path(root)
    if not root_path.exists():
        return []
    documents: list[dict[str, Any]] = []
    for pattern in patterns:
        for path in root_path.rglob(pattern):
            if not path.is_file() or SKIP_PARTS.intersection(path.parts):
                continue
            if any(part.startswith(".") for part in path.relative_to(root_path).parts):
                continue
            documents.append(source_document(path))
    documents.sort(key=lambda item: item["path"])
    return documents


def source_document(path: str | Path) -> dict[str, Any]:
    source_path = Path(path)
    raw = source_path.read_bytes()
    return {
        "path": str(source_path),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "suffix": source_path.suffix.lower(),
    }
