from __future__ import annotations

from pathlib import Path
from typing import Any

from services.semantic_platform.ingestion.source_graph import run_source_graph


def run_graph_for_source(
    source_path: str | Path,
    output_dir: str | Path,
    chunks_output_dir: str | Path,
    provider: str | None = None,
    commit_mode: str = "proposal",
    manual_llm_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return run_source_graph(
        source_path=source_path,
        output_dir=output_dir,
        chunks_output_dir=chunks_output_dir,
        apply=False,
        commit_mode=commit_mode,
        provider=provider,
        manual_llm_response=manual_llm_response,
    )
