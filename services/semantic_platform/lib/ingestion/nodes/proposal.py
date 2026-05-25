from __future__ import annotations

from services.semantic_platform.lib.ingestion.llm.proposal import operation_variant_candidates
from services.semantic_platform.lib.ingestion.proposal import build_review_proposal as build_review_proposal_state
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def keep_passed_verified_capabilities(state: SourceGraphState) -> SourceGraphState:
    variants = state.get("operation_variants", [])
    if not variants:
        return state
    passed_variant_ids = {
        str(result.get("variant_id") or "")
        for result in state.get("verification_results", [])
        if result.get("status") == "passed" and result.get("variant_id")
    }
    if not passed_variant_ids:
        return {
            **state,
            "capabilities": [],
            "capability_entity_links": [],
            "capability_dependencies": [],
            "operation_fields": [],
            "operation_contracts": [],
            "operation_variants": [],
            "field_mappings": [],
            "planning_examples": [],
            "capability_implementations": [],
        }

    kept_variants = [variant for variant in variants if str(variant.get("variant_id") or "") in passed_variant_ids]
    kept_operation_ids = {str(variant.get("operation_id") or "") for variant in kept_variants}
    kept_capability_ids = {str(variant.get("capability_id") or variant.get("capability") or "") for variant in kept_variants}
    return {
        **state,
        "operations": [
            operation
            for operation in state.get("operations", [])
            if str(operation.get("operation_id") or "") in kept_operation_ids
        ],
        "capabilities": [
            capability
            for capability in state.get("capabilities", [])
            if str(capability.get("id") or "") in kept_capability_ids
        ],
        "capability_entity_links": [
            link
            for link in state.get("capability_entity_links", [])
            if str(link.get("capability_id") or "") in kept_capability_ids
        ],
        "capability_dependencies": [
            dependency
            for dependency in state.get("capability_dependencies", [])
            if str(dependency.get("capability_id") or "") in kept_capability_ids
        ],
        "operation_fields": [
            field
            for field in state.get("operation_fields", [])
            if str(field.get("operation_id") or "") in kept_operation_ids
        ],
        "operation_contracts": [
            contract
            for contract in state.get("operation_contracts", [])
            if str(contract.get("operation_id") or "") in kept_operation_ids
        ],
        "operation_variants": kept_variants,
        "field_mappings": [
            mapping
            for mapping in state.get("field_mappings", [])
            if str(mapping.get("operation_id") or "") in kept_operation_ids
        ],
        "planning_examples": [
            example
            for example in state.get("planning_examples", [])
            if set(str(value) for value in _list_values(example.get("expected_capability_ids"))) & kept_capability_ids
        ],
        "capability_implementations": [
            implementation
            for implementation in state.get("capability_implementations", [])
            if str(implementation.get("variant_id") or "") in passed_variant_ids
            and str(implementation.get("capability_id") or "") in kept_capability_ids
        ],
    }


def build_review_proposal(state: SourceGraphState) -> SourceGraphState:
    from services.semantic_platform.lib.ingestion import graph

    return build_review_proposal_state(
        state,
        graph_node_names=graph.SOURCE_INGESTION_GRAPH.node_names,
        operation_variant_candidates=operation_variant_candidates(state),
    )


__all__ = ["build_review_proposal", "keep_passed_verified_capabilities"]


def _list_values(value: object) -> list[object]:
    return value if isinstance(value, list) else []
