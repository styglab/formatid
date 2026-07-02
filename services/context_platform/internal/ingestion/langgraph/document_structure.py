from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, TypedDict
from urllib.parse import urlparse

from langgraph.graph import END, StateGraph

from services.context_platform.internal.ingestion.document_extractors import (
    DocumentChunk,
    extract_document_chunks,
    summarize_chunks,
)
from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode
from services.context_platform.internal.ingestion.llm.document_structure import (
    build_manual_document_structure_request,
    normalize_manual_document_structure_response,
)
from services.context_platform.internal.ingestion.parsers.common import DiscoveredField, LoadedSource, StructureOperationDraft


_REQUEST_TABLE_HEADING = "request_table"
_RESPONSE_TABLE_HEADING = "response_table"
_MESSAGE_EXAMPLES_HEADING = "message_examples"
_DETAIL_INFO_HEADING = "detail_info"
_ERROR_CODE_HEADING = "error_code_table"


class DocumentStructureState(TypedDict, total=False):
    loaded: LoadedSource
    operation_key: str
    llm_mode: str
    chunks: list[DocumentChunk]
    chunk_source: str
    chunk_summaries: list[dict[str, Any]]
    classified_chunks: list[dict[str, Any]]
    operation_candidates: list[dict[str, Any]]
    field_candidates: list[dict[str, Any]]
    manual_llm_request: dict[str, Any]
    status: str
    drafts: list[StructureOperationDraft]


@dataclass
class DocumentStructureResult:
    drafts: list[StructureOperationDraft]
    chunk_summaries: list[dict[str, Any]]
    classified_chunks: list[dict[str, Any]]
    operation_candidates: list[dict[str, Any]]
    field_candidates: list[dict[str, Any]]
    engine: str
    llm_mode: str
    status: str
    manual_llm_request: dict[str, Any] | None = None


def extract_document_structure_with_graph(
    loaded: LoadedSource,
    operations: list[dict[str, Any]],
    *,
    source: dict[str, Any] | None = None,
    run_id: str = "",
    llm_mode: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> DocumentStructureResult:
    operation_key = str(operations[0].get("operation_key") or operations[0].get("name") or "inspect_document_context")
    resolved_llm_mode = resolve_llm_mode(llm_mode)
    state: DocumentStructureState = {
        "loaded": loaded,
        "operation_key": operation_key,
        "llm_mode": resolved_llm_mode,
    }
    if isinstance(source, dict):
        state["manual_llm_request"] = {
            "_source": source,
            "_run_id": run_id,
            "_manual_llm_response": manual_llm_response,
        }
    graph = _build_graph()
    result = graph.invoke(state)
    chunk_source = str(result.get("chunk_source") or "fallback")
    llm_status = str(result.get("status") or "ready")
    engine = "docling_chunk_graph" if chunk_source == "docling" else "fallback_chunk_graph"
    if llm_status == "waiting_manual_llm":
        engine = "agent_manual_pending_document_structure_graph"
    elif str(result.get("llm_mode") or "disabled") == "agent_manual":
        engine = "agent_manual_document_structure_graph"
    return DocumentStructureResult(
        drafts=result.get("drafts") or [],
        chunk_summaries=result.get("chunk_summaries") or [],
        classified_chunks=result.get("classified_chunks") or [],
        operation_candidates=result.get("operation_candidates") or [],
        field_candidates=result.get("field_candidates") or [],
        engine=engine,
        llm_mode=str(result.get("llm_mode") or "disabled"),
        status=llm_status,
        manual_llm_request=_drop_internal_request_keys(result.get("manual_llm_request")),
    )


def _build_graph():
    graph = StateGraph(DocumentStructureState)
    graph.add_node("load_document_elements", _load_document_elements)
    graph.add_node("classify_chunks", _classify_chunks)
    graph.add_node("extract_candidates", _extract_candidates)
    graph.add_node("merge_candidates", _merge_candidates)
    graph.set_entry_point("load_document_elements")
    graph.add_edge("load_document_elements", "classify_chunks")
    graph.add_edge("classify_chunks", "extract_candidates")
    graph.add_edge("extract_candidates", "merge_candidates")
    graph.add_edge("merge_candidates", END)
    return graph.compile()


def _load_document_elements(state: DocumentStructureState) -> DocumentStructureState:
    loaded = state["loaded"]
    chunks, chunk_source = extract_document_chunks(loaded)
    state["chunks"] = chunks
    state["chunk_source"] = chunk_source
    state["chunk_summaries"] = summarize_chunks(chunks)
    return state


def _classify_chunks(state: DocumentStructureState) -> DocumentStructureState:
    classified: list[dict[str, Any]] = []
    current_operation_label = ""
    current_subsection = ""
    in_error_code_section = False

    for chunk in state.get("chunks") or []:
        text = chunk.text.strip()
        first_line = text.splitlines()[0].strip() if text else ""
        if not text:
            continue

        operation_match = re.match(r"^\d+\)\s*(.+?)\s*상세기능명세", first_line)
        if operation_match:
            current_operation_label = operation_match.group(1).strip()
            current_subsection = ""
            in_error_code_section = False

        if "OpenAPI 에러 코드정리" in first_line:
            in_error_code_section = True
            current_subsection = _ERROR_CODE_HEADING
        elif re.match(r"^[a-z]\)\s*상세기능정보", first_line, flags=re.IGNORECASE):
            current_subsection = _DETAIL_INFO_HEADING
        elif re.match(r"^[b-z]\)\s*요청 메시지 명세", first_line, flags=re.IGNORECASE):
            current_subsection = _REQUEST_TABLE_HEADING
        elif re.match(r"^[c-z]\)\s*응답 메시지 명세", first_line, flags=re.IGNORECASE):
            current_subsection = _RESPONSE_TABLE_HEADING
        elif re.match(r"^[d-z]\)\s*요청/응답 메시지 예제", first_line, flags=re.IGNORECASE):
            current_subsection = _MESSAGE_EXAMPLES_HEADING

        structured_type = _structured_chunk_type(chunk, current_subsection, in_error_code_section)
        classified.append(
            {
                "chunk_id": chunk.chunk_id,
                "chunk_type": chunk.chunk_type,
                "structured_type": structured_type,
                "operation_label": current_operation_label,
                "heading": chunk.heading,
                "text": text,
                "preview": text[:320],
                "evidence_refs": chunk.evidence_refs,
                "metadata": chunk.metadata,
            }
        )

    state["classified_chunks"] = classified
    state["chunk_summaries"] = [
        {
            **summary,
            "structured_type": next(
                (item.get("structured_type") for item in classified if item.get("chunk_id") == summary.get("chunk_id")),
                "other",
            ),
            "operation_label": next(
                (item.get("operation_label") for item in classified if item.get("chunk_id") == summary.get("chunk_id")),
                "",
            ),
        }
        for summary in state.get("chunk_summaries") or []
    ]
    return state


def _extract_candidates(state: DocumentStructureState) -> DocumentStructureState:
    llm_mode = str(state.get("llm_mode") or "disabled")
    classified_chunks = state.get("classified_chunks") or []
    request_info = state.get("manual_llm_request") if isinstance(state.get("manual_llm_request"), dict) else {}
    manual_llm_response = request_info.get("_manual_llm_response")

    if llm_mode == "agent_manual":
        if not isinstance(manual_llm_response, dict):
            source = request_info.get("_source") if isinstance(request_info.get("_source"), dict) else {}
            run_id = str(request_info.get("_run_id") or "")
            state["manual_llm_request"] = build_manual_document_structure_request(
                run_id=run_id,
                source=source,
                operation_key=state["operation_key"],
                chunks=[
                    item
                    for item in classified_chunks
                    if item.get("structured_type") in {_DETAIL_INFO_HEADING, _REQUEST_TABLE_HEADING, _RESPONSE_TABLE_HEADING, _MESSAGE_EXAMPLES_HEADING}
                ],
            )
            state["status"] = "waiting_manual_llm"
            state["operation_candidates"] = []
            state["field_candidates"] = []
            return state
        normalized = normalize_manual_document_structure_response(manual_llm_response)
        state["operation_candidates"] = normalized.get("operation_candidates") or []
        state["field_candidates"] = _filter_noise_fields(normalized.get("field_candidates") or [])
        state["status"] = "ready"
        state["manual_llm_request"] = {}
        return state

    operation_candidates = _extract_operation_candidates(classified_chunks)
    field_candidates = _extract_field_candidates(classified_chunks)
    state["operation_candidates"] = operation_candidates
    state["field_candidates"] = field_candidates
    state["status"] = "ready"
    state["manual_llm_request"] = {}
    return state


def _merge_candidates(state: DocumentStructureState) -> DocumentStructureState:
    if str(state.get("status") or "") == "waiting_manual_llm":
        state["drafts"] = []
        return state

    operation_key = state["operation_key"]
    deduped_fields = _dedupe_fields(state.get("field_candidates") or [])
    fields = [
        DiscoveredField(
            scope=str(item.get("scope") or "output"),
            raw_name=str(item.get("wire_name") or item.get("raw_name") or ""),
            field_path=str(item.get("field_path") or ""),
            data_type=str(item.get("data_type") or "string"),
            is_required=bool(item.get("is_required")),
            description=str(item.get("description") or ""),
            evidence=item.get("evidence") if isinstance(item.get("evidence"), list) else [],
        )
        for item in deduped_fields
        if str(item.get("wire_name") or item.get("raw_name") or "")
    ]
    state["drafts"] = [StructureOperationDraft(operation_key=operation_key, fields=fields)]
    return state


def _structured_chunk_type(chunk: DocumentChunk, current_subsection: str, in_error_code_section: bool) -> str:
    text = chunk.text.strip()
    if chunk.chunk_type == "heading":
        return "heading"
    if in_error_code_section:
        return _ERROR_CODE_HEADING
    if current_subsection:
        return current_subsection
    if "Call Back URL" in text or "상세기능 설명" in text:
        return _DETAIL_INFO_HEADING
    return "other"


def _extract_operation_candidates(classified_chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for chunk in classified_chunks:
        if chunk.get("structured_type") != _DETAIL_INFO_HEADING:
            continue
        text = str(chunk.get("text") or "")
        operation_names = re.findall(r"get[A-Za-z0-9_]+", text)
        source_urls = re.findall(r"http[s]?://[^\s|)]+", text)
        operation_label = str(chunk.get("operation_label") or "")
        for operation_name in operation_names:
            normalized = operation_name.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            description = operation_label or normalized
            source_url = _source_url_for_operation(normalized, source_urls)
            endpoint = _endpoint_parts(normalized, source_url)
            candidates.append(
                {
                    "chunk_id": str(chunk.get("chunk_id") or ""),
                    "operation_name": normalized,
                    "method": "GET",
                    "base_url": endpoint["base_url"],
                    "path": endpoint["path"],
                    "source_url": source_url,
                    "description": description,
                    "evidence_refs": chunk.get("evidence_refs") if isinstance(chunk.get("evidence_refs"), list) else [],
                }
            )
    return candidates


def _source_url_for_operation(operation_name: str, source_urls: list[str]) -> str:
    for url in source_urls:
        if operation_name in url:
            return url
    return source_urls[0] if source_urls else ""


def _endpoint_parts(operation_name: str, source_url: str) -> dict[str, str]:
    parsed = urlparse(source_url) if source_url else None
    if parsed and parsed.scheme and parsed.netloc and parsed.path:
        path = parsed.path.rstrip("/")
        operation_path = f"/{path.rsplit('/', 1)[-1]}"
        base_path = path.rsplit("/", 1)[0] if "/" in path else ""
        return {
            "base_url": f"{parsed.scheme}://{parsed.netloc}{base_path}".rstrip("/"),
            "path": operation_path,
        }
    return {"base_url": "", "path": f"/{operation_name}"}


def _extract_field_candidates(classified_chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for chunk in classified_chunks:
        structured_type = str(chunk.get("structured_type") or "")
        text = str(chunk.get("text") or "")
        evidence = chunk.get("evidence_refs") if isinstance(chunk.get("evidence_refs"), list) else []
        if structured_type in {_REQUEST_TABLE_HEADING, _RESPONSE_TABLE_HEADING}:
            fields.extend(_parse_table_like_chunk(text, structured_type, str(chunk.get("chunk_id") or ""), evidence))
        elif structured_type == _MESSAGE_EXAMPLES_HEADING:
            fields.extend(_parse_example_chunk(text, str(chunk.get("chunk_id") or ""), evidence))
    return _filter_noise_fields(fields)


def _parse_table_like_chunk(text: str, structured_type: str, chunk_id: str, evidence_refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matches = list(
        re.finditer(
            r"(?P<raw>[A-Za-z][A-Za-z0-9_]*)\, \*\*항목명\(국문\)\*\* = (?P<ko>.+?)\. "
            r"(?P=raw)\, \*\*항목크기\*\* = (?P<size>.+?)\. "
            r"(?P=raw)\, \*\*항목구분\*\* = (?P<required>[01])\. "
            r"(?P=raw)\, \*\*샘플데이터\*\* = (?P<sample>.+?)\. "
            r"(?P=raw)\, \*\*항목설명\*\* = (?P<description>.+?)(?=(?: [A-Za-z][A-Za-z0-9_]*\, \*\*항목명\(국문\)\*\* = )|$)",
            text,
        )
    )
    fields: list[dict[str, Any]] = []
    scope = "input" if structured_type == _REQUEST_TABLE_HEADING else "output"
    for match in matches:
        raw_name = match.group("raw").strip()
        sample_value = match.group("sample").strip()
        description = match.group("description").strip()
        field_scope = _infer_request_scope(raw_name, description, sample_value) if scope == "input" else "output"
        fields.append(
            {
                "chunk_id": chunk_id,
                "scope": field_scope,
                "wire_name": raw_name,
                "raw_name": raw_name,
                "label_ko": match.group("ko").strip(),
                "label_en": "",
                "field_path": f"{'request.query' if field_scope in {'input', 'control'} else 'response.body.items.item'}.{raw_name}",
                "data_type": _infer_data_type(f"{match.group('size')} {sample_value} {description}"),
                "is_required": match.group("required") == "1",
                "description": f"{match.group('ko').strip()} - {description}",
                "sample_value": sample_value,
                "source_evidence_tier": "table",
                "evidence": [
                    *evidence_refs,
                    {"kind": "structured_table_row", "chunk_id": chunk_id, "raw_name": raw_name},
                ],
            }
        )
    return fields


def _parse_example_chunk(text: str, chunk_id: str, evidence_refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    response_xml = text.split("<?xml", 1)[1] if "<?xml" in text else ""
    if not response_xml:
        return []
    tags = re.findall(r"<([A-Za-z][A-Za-z0-9_]*)>[^<]+</\1>", response_xml)
    fields: list[dict[str, Any]] = []
    seen: set[str] = set()
    for tag in tags:
        if tag in {"response", "header", "body", "items", "item"} or tag in seen:
            continue
        seen.add(tag)
        fields.append(
            {
                "chunk_id": chunk_id,
                "scope": "output",
                "wire_name": tag,
                "raw_name": tag,
                "label_ko": "",
                "label_en": "",
                "field_path": f"response.body.items.item.{tag}",
                "data_type": "string",
                "is_required": False,
                "description": "Field observed in response message example.",
                "sample_value": "",
                "source_evidence_tier": "example",
                "evidence": [
                    *evidence_refs,
                    {"kind": "message_example_tag", "chunk_id": chunk_id, "raw_name": tag},
                ],
            }
        )
    return fields


def _infer_request_scope(raw_name: str, description: str, sample_value: str) -> str:
    lowered = raw_name.lower()
    control_names = {"numofrows", "pageno", "resulttype", "servicekey"}
    if lowered in control_names or lowered.endswith("div") or "구분" in description:
        return "control"
    if sample_value.lower() in {"xml", "json"}:
        return "control"
    return "input"


def _filter_noise_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in fields:
        raw_name = str(item.get("wire_name") or item.get("raw_name") or "")
        if not raw_name:
            continue
        lowered = raw_name.lower()
        if lowered == "http":
            continue
        if re.fullmatch(r"\d{4}[_-]\d{2}[_-]\d{2}", raw_name):
            continue
        if raw_name in {"항목구분", "샘플데이터", "항목설명", "항목명", "항목명영문"}:
            continue
        if len(raw_name) > 80:
            continue
        filtered.append(item)
    return filtered


def _dedupe_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for item in fields:
        key = (str(item.get("scope") or "output"), str(item.get("wire_name") or item.get("raw_name") or ""))
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = item
            continue
        existing_evidence = existing.get("evidence") if isinstance(existing.get("evidence"), list) else []
        new_evidence = item.get("evidence") if isinstance(item.get("evidence"), list) else []
        existing["evidence"] = [*existing_evidence, *new_evidence][:8]
        if not existing.get("description") and item.get("description"):
            existing["description"] = item.get("description")
    return list(deduped.values())


def _infer_data_type(value: str) -> str:
    lowered = value.lower()
    if re.search(r"\b(integer|number|decimal|금액|수치|amount|year|ratio|count|code)\b", lowered):
        return "number"
    if re.search(r"\b(boolean|true|false|여부)\b", lowered):
        return "boolean"
    return "string"


def _drop_internal_request_keys(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return payload
    return {key: value for key, value in payload.items() if not key.startswith("_")}
