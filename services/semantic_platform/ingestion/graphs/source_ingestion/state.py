from __future__ import annotations

from typing import Any, TypedDict

class SourceGraphState(TypedDict, total=False):
    source_path: str
    output_dir: str
    chunks_output_dir: str
    apply: bool
    commit_mode: str
    provider: str
    provider_hint: str | None
    document_id: str
    sha256: str
    raw_bytes_size: int
    extracted_text: str
    source_chunks: list[dict[str, Any]]
    chunks_path: str
    catalog_context: dict[str, Any]
    manual_llm_response: dict[str, Any]
    structured_spec: dict[str, Any]
    semantic_platform_proposal: dict[str, Any]
    execution_contract_proposal: dict[str, Any]
    proposal_path: str
    apply_audit_path: str
    applied_changes: list[dict[str, Any]]
    messages: list[str]
