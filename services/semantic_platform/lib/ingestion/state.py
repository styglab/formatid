from __future__ import annotations

from typing import Any, TypedDict


INGESTION_GRAPH_VERSION = "2026-05-19.capability-closure-v2"
INGESTION_PROMPT_VERSION = "2026-05-19.contract-interpreter-v2"


class SourceGraphState(TypedDict, total=False):
    source_path: str
    source_bytes: bytes
    source_document: dict[str, Any]
    extracted_text: str
    document_blocks: list[dict[str, Any]]
    api_sections: list[dict[str, Any]]
    structured_evidence: dict[str, Any]
    catalog_context: dict[str, Any]
    endpoint_candidate_checks: list[dict[str, Any]]
    verified_api_sections: list[dict[str, Any]]
    chunks: list[dict[str, Any]]
    manual_llm_response: dict[str, Any] | None
    analysis: dict[str, Any]
    capability_analysis: dict[str, Any]
    execution_analysis: dict[str, Any]
    resources: list[dict[str, Any]]
    operations: list[dict[str, Any]]
    operation_fields: list[dict[str, Any]]
    semantic_types: list[dict[str, Any]]
    entities: list[dict[str, Any]]
    entity_identifiers: list[dict[str, Any]]
    capabilities: list[dict[str, Any]]
    capability_entity_links: list[dict[str, Any]]
    capability_dependencies: list[dict[str, Any]]
    operation_contracts: list[dict[str, Any]]
    operation_variants: list[dict[str, Any]]
    field_mappings: list[dict[str, Any]]
    semantic_join_rules: list[dict[str, Any]]
    planning_examples: list[dict[str, Any]]
    capability_implementations: list[dict[str, Any]]
    verification_results: list[dict[str, Any]]
    proposal: dict[str, Any]
    proposal_items: list[dict[str, Any]]
    proposals: list[dict[str, Any]]
    proposal_item_groups: list[list[dict[str, Any]]]
    stored_proposal: dict[str, Any]
    stored_proposals: list[dict[str, Any]]
    apply_result: dict[str, Any] | None
    apply_results: list[dict[str, Any]]
    capability_document_result: dict[str, Any] | None
    embedding_result: dict[str, Any] | None
