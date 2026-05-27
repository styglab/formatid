from __future__ import annotations

from pathlib import Path
from typing import Any

from services.semantic_platform.lib.ingestion.api_client import upload_and_ingest_source


def run_ingestion_graph(
    source_path: str | Path,
    commit_mode: str = "proposal",
    manual_llm_response: dict[str, Any] | None = None,
    llm_secret_ref: str | None = None,
    llm_mode: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return upload_and_ingest_source(
        source_path,
        commit_mode=commit_mode,
        manual_llm_response=manual_llm_response,
        llm_secret_ref=llm_secret_ref,
        llm_mode=llm_mode,
        force=force,
        requested_by="worker",
        wait=True,
    )
