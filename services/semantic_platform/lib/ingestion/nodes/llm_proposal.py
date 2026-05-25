from __future__ import annotations

from services.semantic_platform.lib.ingestion.llm.proposal import (
    llm_propose_capability_catalog as llm_propose_capability_catalog_state,
    llm_propose_execution_catalog as llm_propose_execution_catalog_state,
)
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def llm_propose_capability_catalog(state: SourceGraphState) -> SourceGraphState:
    return llm_propose_capability_catalog_state(state)


def llm_propose_execution_catalog(state: SourceGraphState) -> SourceGraphState:
    return llm_propose_execution_catalog_state(state)


__all__ = ["llm_propose_capability_catalog", "llm_propose_execution_catalog"]
