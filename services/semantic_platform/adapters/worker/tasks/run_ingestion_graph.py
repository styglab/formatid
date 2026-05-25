from __future__ import annotations

from pathlib import Path
from typing import Any

from services.semantic_platform.lib.ingestion import run_source_ingestion


def run_ingestion_graph(
    source_path: str | Path,
    commit_mode: str = "proposal",
    manual_llm_response: dict[str, Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return run_source_ingestion(
        source_path,
        manual_llm_response=manual_llm_response,
        apply=commit_mode == "direct_apply",
        force=force,
    )
