from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class DocumentStructureOperationCandidateModel(BaseModel):
    chunk_id: str
    operation_name: str
    method: str = "GET"
    base_url: str = ""
    path: str = ""
    source_url: str = ""
    description: str
    evidence_refs: list[dict[str, Any]]


class DocumentStructureFieldCandidateModel(BaseModel):
    chunk_id: str
    scope: str
    wire_name: str | None = None
    raw_name: str
    field_path: str
    label_ko: str | None = None
    label_en: str | None = None
    data_type: str
    is_required: bool
    description: str
    sample_value: str | None = None
    source_evidence_tier: str | None = None
    evidence: list[dict[str, Any]]


class DocumentStructureResponseModel(BaseModel):
    operation_candidates: list[DocumentStructureOperationCandidateModel]
    field_candidates: list[DocumentStructureFieldCandidateModel]


def build_manual_document_structure_request(
    *,
    run_id: str,
    source: dict[str, Any],
    operation_key: str,
    chunks: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "type": "source_contract_extraction",
        "legacy_type": "document_structure_extraction",
        "run_id": run_id,
        "stage": "source_contract_extraction",
        "source": {
            "id": source.get("id"),
            "name": source.get("name"),
            "source_type": source.get("source_type"),
            "provider": source.get("provider"),
        },
        "operation_key": operation_key,
        "instructions": [
            "Review structured document chunks from an API guide and extract the executable source contract only.",
            "Prioritize request table, response table, and message example chunks.",
            "Ignore table labels, dates, URLs, index rows, and error-code glossary rows unless they are actual API fields.",
            "For request fields, classify pagination, format, and service key values as scope `control` when appropriate.",
            "For response examples, keep only meaningful XML/JSON payload fields.",
            "Keep operation and field candidates grounded to the same chunk_id/evidence_refs that contain the table or example; do not assign fields from one endpoint section to another.",
            "Classify result code/message, counts, pagination headers, and response envelope wrappers as control/transport evidence instead of business output fields.",
            "Do not canonicalize business meaning in this stage. Do not output Concept, Representation, Binding, or Capability data here.",
            "For executable API sources, `wire_name` and `raw_name` must be the actual request/response key used on the wire, such as `crno`, `bizYear`, or `enpSaleAmt`.",
            "Put Korean display labels such as `법인등록번호` or `기업매출금액` in `label_ko`, not in `wire_name`, `raw_name`, or `field_path`.",
            "Field paths must be executable structural paths based on wire names, such as `request.query.crno` or `response.body.items.item.enpSaleAmt`.",
        ],
        "chunks": [
            {
                "chunk_id": chunk.get("chunk_id"),
                "chunk_type": chunk.get("chunk_type"),
                "structured_type": chunk.get("structured_type"),
                "operation_label": chunk.get("operation_label"),
                "preview": chunk.get("preview"),
                "text": chunk.get("text"),
                "evidence_refs": chunk.get("evidence_refs") or [],
            }
            for chunk in chunks
        ],
        "response_contract": {
            "coverage": {
                "operation_candidate": "Only include operations directly evidenced by the provided chunks.",
                "field_candidate": "Each field candidate must reference the chunk that contains the field table row or payload example.",
            },
            "operation_candidates": [
                {
                    "chunk_id": "string",
                    "operation_name": "actual operation id/name such as getSummFinaStat_V2",
                    "method": "GET|POST|PUT|PATCH|DELETE|HEAD",
                    "base_url": "runtime base URL without operation path",
                    "path": "operation path such as /getBs_V2",
                    "source_url": "original URL from the document as evidence only",
                    "description": "string",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "field_candidates": [
                {
                    "chunk_id": "string",
                    "scope": "input|output|control",
                    "wire_name": "actual API key used on the wire",
                    "raw_name": "same as wire_name unless the source format has a separate original name",
                    "field_path": "request.query.<wire_name>|request.body.<wire_name>|response.body.items.item.<wire_name>",
                    "label_ko": "Korean display label from the document, not executable",
                    "label_en": "English display label when present",
                    "data_type": "string",
                    "is_required": "boolean",
                    "description": "string",
                    "sample_value": "string|null",
                    "source_evidence_tier": "openapi|table|example|narrative|external",
                    "evidence": ["list of evidence ref objects"],
                }
            ],
        },
    }


def normalize_manual_document_structure_response(payload: dict[str, Any]) -> dict[str, Any]:
    source_structure = payload.get("source_structure") if isinstance(payload.get("source_structure"), dict) else {}
    operation_candidates = payload.get("operation_candidates") if isinstance(payload.get("operation_candidates"), list) else []
    field_candidates = payload.get("field_candidates") if isinstance(payload.get("field_candidates"), list) else []
    if not operation_candidates and isinstance(source_structure.get("operations"), list):
        operation_candidates = source_structure.get("operations") or []
    nested_field_candidates = _nested_field_candidates(operation_candidates)
    if not field_candidates and nested_field_candidates:
        field_candidates = nested_field_candidates
    if not field_candidates and isinstance(source_structure.get("field_candidates"), list):
        field_candidates = source_structure.get("field_candidates") or []
    if not field_candidates and isinstance(source_structure.get("fields"), list):
        field_candidates = source_structure.get("fields") or []
    normalized_operations: list[dict[str, Any]] = []
    normalized_fields: list[dict[str, Any]] = []

    for item in operation_candidates:
        if not isinstance(item, dict):
            continue
        normalized_operations.append(
            {
                "chunk_id": str(item.get("chunk_id") or ""),
                "operation_name": str(item.get("operation_name") or item.get("operation_key") or item.get("name") or ""),
                "method": str(item.get("method") or "GET").upper(),
                "base_url": str(item.get("base_url") or ""),
                "path": str(item.get("path") or ""),
                "source_url": str(item.get("source_url") or ""),
                "description": str(item.get("description") or ""),
                "evidence_refs": item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else [],
            }
        )

    for item in field_candidates:
        if not isinstance(item, dict):
            continue
        wire_name = str(item.get("wire_name") or item.get("original_name") or item.get("raw_name") or "")
        raw_name = str(item.get("raw_name") or wire_name)
        normalized_fields.append(
            {
                "chunk_id": str(item.get("chunk_id") or ""),
                "scope": str(item.get("scope") or "output"),
                "wire_name": wire_name,
                "raw_name": raw_name,
                "field_path": str(item.get("field_path") or ""),
                "label_ko": str(item.get("label_ko") or item.get("korean_label") or ""),
                "label_en": str(item.get("label_en") or item.get("english_label") or ""),
                "data_type": str(item.get("data_type") or "string"),
                "is_required": bool(item.get("is_required")),
                "description": str(item.get("description") or ""),
                "sample_value": str(item.get("sample_value") or ""),
                "source_evidence_tier": str(item.get("source_evidence_tier") or item.get("evidence_tier") or ""),
                "evidence": item.get("evidence") if isinstance(item.get("evidence"), list) else [],
            }
        )

    return {
        "llm_mode": "agent_manual",
        "engine": "agent_manual_document_structure_graph",
        "operation_candidates": normalized_operations,
        "field_candidates": normalized_fields,
    }


def _nested_field_candidates(operation_candidates: list[Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for operation in operation_candidates:
        if not isinstance(operation, dict):
            continue
        chunk_id = str(operation.get("chunk_id") or "")
        for key, scope in (("parameters", "input"), ("request_fields", "input"), ("response_fields", "output"), ("fields", "output")):
            nested = operation.get(key)
            if not isinstance(nested, list):
                continue
            for item in nested:
                if not isinstance(item, dict):
                    continue
                fields.append(
                    {
                        **item,
                        "chunk_id": str(item.get("chunk_id") or chunk_id),
                        "scope": str(item.get("scope") or scope),
                    }
                )
    return fields


def generate_openai_document_structure_response(
    *,
    source: dict[str, Any],
    operation_key: str,
    chunks: list[dict[str, Any]],
) -> dict[str, Any]:
    raise RuntimeError(
        "OpenAI-backed Context Platform ingestion is no longer supported; "
        "run agent_manual mode and provide an explicit agent response artifact"
    )
