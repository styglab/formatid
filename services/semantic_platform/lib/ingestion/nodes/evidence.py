from __future__ import annotations

import os

from services.semantic_platform.lib.ingestion.evidence import (
    detect_api_sections,
    extract_blocks,
    extract_structured_evidence,
    sections_to_chunks,
)
from services.semantic_platform.lib.ingestion.extraction import compact_text, extract_text
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def extract_text_node(state: SourceGraphState) -> SourceGraphState:
    limit = int(os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_MAX_TEXT_CHARS", "40000"))
    return {**state, "extracted_text": compact_text(extract_text(state["source_path"]), limit)}


def extract_blocks_node(state: SourceGraphState) -> SourceGraphState:
    return {**state, "document_blocks": extract_blocks(state["extracted_text"])}


def detect_api_sections_node(state: SourceGraphState) -> SourceGraphState:
    sections = detect_api_sections(state["document_blocks"], state["source_document"]["id"])
    return {
        **state,
        "api_sections": sections,
        "chunks": sections_to_chunks(sections, state["source_document"]["id"]),
    }


def extract_structured_evidence_node(state: SourceGraphState) -> SourceGraphState:
    return {
        **state,
        "structured_evidence": extract_structured_evidence(state["document_blocks"], state["api_sections"]),
    }
