from __future__ import annotations

from services.semantic_platform.lib.ingestion.llm.proposal import operation_variant_candidates
from services.semantic_platform.lib.ingestion.proposal import build_review_proposal as build_review_proposal_state
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def build_review_proposal(state: SourceGraphState) -> SourceGraphState:
    from services.semantic_platform.lib.ingestion import graph

    return build_review_proposal_state(
        state,
        graph_node_names=graph.SOURCE_INGESTION_GRAPH.node_names,
        operation_variant_candidates=operation_variant_candidates(state),
    )


__all__ = ["build_review_proposal"]
