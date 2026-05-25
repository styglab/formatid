from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_REGISTRY_PATH = Path("sources/.semantic_ingestion_registry.json")


def load_registry(path: str | Path = DEFAULT_REGISTRY_PATH) -> dict[str, Any]:
    registry_path = Path(path)
    if not registry_path.exists():
        return {"documents": {}, "runs": []}
    data = json.loads(registry_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {"documents": {}, "runs": []}
    data.setdefault("documents", {})
    data.setdefault("runs", [])
    return data


def save_registry(registry: dict[str, Any], path: str | Path = DEFAULT_REGISTRY_PATH) -> None:
    registry_path = Path(path)
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry["updated_at"] = _now()
    registry_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def should_process(document: dict[str, Any], registry: dict[str, Any], force: bool = False) -> bool:
    if force:
        return True
    previous = registry.get("documents", {}).get(document["path"])
    return not previous or previous.get("sha256") != document.get("sha256")


def mark_processed(
    registry: dict[str, Any],
    document: dict[str, Any],
    result: dict[str, Any],
    status: str = "proposal_written",
) -> None:
    registry.setdefault("documents", {})[document["path"]] = {
        "sha256": document["sha256"],
        "size_bytes": document["size_bytes"],
        "last_processed_at": _now(),
        "status": status,
        "proposal_path": result.get("proposal_path"),
        "messages": result.get("messages", []),
    }
    registry.setdefault("runs", []).append(
        {
            "source_path": document["path"],
            "sha256": document["sha256"],
            "status": status,
            "proposal_path": result.get("proposal_path"),
            "at": _now(),
        }
    )


def mark_skipped(registry: dict[str, Any], document: dict[str, Any], reason: str) -> None:
    registry.setdefault("runs", []).append(
        {
            "source_path": document["path"],
            "sha256": document["sha256"],
            "status": "skipped",
            "reason": reason,
            "at": _now(),
        }
    )


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
