from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.context_platform.internal.ingestion.document_extractors import DocumentChunk
from services.context_platform.internal.ingestion.document_extractors import extract_document_chunks
from services.context_platform.internal.ingestion.parsers.common import LoadedSource
from services.context_platform.internal.ingestion.parsers.common import extract_text_from_bytes


DEFAULT_LANGEXTRACT_MODEL = "gemini-3.5-flash"
OPENAPI_HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "trace"}
CONTROL_PARAMETER_NAMES = {
    "authorization",
    "apikey",
    "api_key",
    "key",
    "numofrows",
    "page",
    "pageno",
    "pagesize",
    "perpage",
    "returntype",
    "servicekey",
    "type",
}


@dataclass
class GroundedExtraction:
    extraction_class: str
    extraction_text: str
    attributes: dict[str, Any]
    char_start: int | None = None
    char_end: int | None = None


def draft_agent_response_from_source_path(
    source_path: str | Path,
    *,
    source_name: str = "",
    source_type: str = "api",
    model_id: str = "",
) -> dict[str, Any]:
    path = Path(source_path)
    raw = path.read_bytes()
    content_text = extract_text_from_bytes(path.name, raw)
    openapi_document = _load_openapi_document(content_text)
    if openapi_document:
        return draft_agent_response_from_openapi(openapi_document, source_name=source_name or path.stem)

    loaded = LoadedSource(
        source_id="",
        source_name=source_name or path.stem,
        source_type=source_type,
        filename=path.name,
        media_type="application/octet-stream",
        reference_uri=str(path),
        stored_path=str(path),
        content_text=content_text,
        content_json=None,
    )
    chunks, chunk_source = extract_document_chunks(loaded)
    return draft_agent_response_from_chunks(chunks, chunk_source=chunk_source, model_id=model_id)


def draft_agent_response_from_openapi(document: dict[str, Any], *, source_name: str = "") -> dict[str, Any]:
    response = _empty_agent_response()
    response["source_structure"]["operations"] = _openapi_operations(document)
    response["metadata"] = {
        "proposal_builder": "openapi_source_contract_parser",
        "source_contract_extractor": "openapi_parser",
        "openapi_version": str(document.get("openapi") or document.get("swagger") or ""),
        "source_name": source_name,
    }
    return response


def draft_agent_response_from_chunks(
    chunks: list[DocumentChunk],
    *,
    chunk_source: str = "",
    model_id: str = "",
) -> dict[str, Any]:
    document_text, spans = _document_text_with_chunk_markers(chunks)
    extractions = _run_langextract(document_text, model_id=model_id or os.getenv("LANGEXTRACT_MODEL_ID") or DEFAULT_LANGEXTRACT_MODEL)
    response = build_agent_response_from_extractions(extractions, chunk_spans=spans)
    response.setdefault("metadata", {})
    response["metadata"].update(
        {
            "proposal_builder": "langextract_agent_manual",
            "source_contract_extractor": "langextract",
            "chunk_source": chunk_source,
            "model_id": model_id or os.getenv("LANGEXTRACT_MODEL_ID") or DEFAULT_LANGEXTRACT_MODEL,
        }
    )
    return response


def build_agent_response_from_extractions(
    extractions: list[GroundedExtraction | dict[str, Any]],
    *,
    chunk_spans: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    operations: dict[str, dict[str, Any]] = {}
    orphan_fields: list[dict[str, Any]] = []
    spans = chunk_spans or []

    for extraction in extractions:
        item = _coerce_extraction(extraction)
        attrs = item.attributes
        chunk_id = str(attrs.get("chunk_id") or _chunk_id_for_offset(item.char_start, spans) or "")
        evidence_refs = _evidence_refs(item, chunk_id)
        extraction_class = item.extraction_class

        if extraction_class == "source_operation":
            operation_name = str(attrs.get("operation_name") or attrs.get("operation_key") or item.extraction_text).strip()
            if not operation_name:
                continue
            op = operations.setdefault(operation_name, _new_operation(operation_name))
            op.update(
                {
                    "chunk_id": chunk_id or op.get("chunk_id", ""),
                    "operation_key": operation_name,
                    "operation_name": operation_name,
                    "method": str(attrs.get("method") or op.get("method") or "GET").upper(),
                    "base_url": str(attrs.get("base_url") or op.get("base_url") or ""),
                    "path": str(attrs.get("path") or op.get("path") or ""),
                    "source_url": str(attrs.get("source_url") or op.get("source_url") or ""),
                    "description": str(attrs.get("description") or op.get("description") or item.extraction_text),
                    "evidence_refs": _merge_evidence(op.get("evidence_refs"), evidence_refs),
                }
            )
            continue

        if extraction_class not in {"source_parameter", "source_response_field"}:
            continue

        field = _field_from_extraction(item, chunk_id=chunk_id, evidence_refs=evidence_refs)
        operation_name = str(attrs.get("operation_name") or attrs.get("operation_key") or "").strip()
        if operation_name:
            op = operations.setdefault(operation_name, _new_operation(operation_name))
            target = "parameters" if extraction_class == "source_parameter" else "response_fields"
            op[target].append(field)
        else:
            orphan_fields.append(field)

    response = _empty_agent_response()
    response["source_structure"]["operations"] = [_dedupe_operation_fields(operation) for operation in operations.values()]
    response["source_structure"]["field_candidates"] = orphan_fields
    return response


def _empty_agent_response() -> dict[str, Any]:
    return {
        "source_structure": {
            "operations": [],
            "field_candidates": [],
        },
        "meaning_resolution": {
            "concept_decisions": [],
            "representation_decisions": [],
            "representation_schema_decisions": [],
            "value_domain_decisions": [],
            "relation_suggestions": [],
        },
        "resolution_generation": {
            "field_bindings": [],
            "context_bindings": [],
            "parameter_bindings": [],
            "transform_rules": [],
        },
        "capability_generation": {"suggestions": []},
    }


def _load_openapi_document(content_text: str) -> dict[str, Any] | None:
    text = content_text.strip()
    if not text:
        return None
    payload: Any = None
    if text.startswith("{"):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = None
    if payload is None and ("openapi:" in text[:2000] or "swagger:" in text[:2000] or "paths:" in text[:4000]):
        try:
            import yaml

            payload = yaml.safe_load(text)
        except Exception:
            payload = None
    if _is_openapi_document(payload):
        return payload
    return None


def _is_openapi_document(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("paths"), dict) and bool(payload.get("openapi") or payload.get("swagger"))


def _openapi_operations(document: dict[str, Any]) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    paths = document.get("paths") if isinstance(document.get("paths"), dict) else {}
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        path_parameters = path_item.get("parameters") if isinstance(path_item.get("parameters"), list) else []
        for method, operation in path_item.items():
            if method.lower() not in OPENAPI_HTTP_METHODS or not isinstance(operation, dict):
                continue
            operation_name = str(operation.get("operationId") or _operation_name(method, str(path))).strip()
            chunk_id = f"openapi:{method.lower()}:{path}"
            parameters = _openapi_operation_parameters(document, path=str(path), method=method, path_parameters=path_parameters, operation=operation)
            parameters.extend(_openapi_security_parameters(document, operation=operation, path=str(path), method=method))
            response_fields = _openapi_response_fields(document, path=str(path), method=method, operation=operation)
            operations.append(
                _dedupe_operation_fields(
                    {
                        "chunk_id": chunk_id,
                        "operation_key": operation_name,
                        "operation_name": operation_name,
                        "method": method.upper(),
                        "base_url": _openapi_base_url(document),
                        "path": str(path),
                        "source_url": "",
                        "description": str(operation.get("summary") or operation.get("description") or operation_name),
                        "evidence_refs": [{"kind": "openapi_operation", "path": str(path), "method": method.upper()}],
                        "parameters": parameters,
                        "response_fields": response_fields,
                    }
                )
            )
    return operations


def _openapi_operation_parameters(
    document: dict[str, Any],
    *,
    path: str,
    method: str,
    path_parameters: list[Any],
    operation: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_parameters = [*_safe_list(path_parameters), *_safe_list(operation.get("parameters"))]
    parameters: list[dict[str, Any]] = []
    seen_refs: set[str] = set()
    for raw_parameter in raw_parameters:
        parameter = _resolve_ref(document, raw_parameter)
        if not isinstance(parameter, dict):
            continue
        ref_key = json.dumps(parameter, sort_keys=True, ensure_ascii=False)
        if ref_key in seen_refs:
            continue
        seen_refs.add(ref_key)
        location = str(parameter.get("in") or "query")
        schema = parameter.get("schema") if isinstance(parameter.get("schema"), dict) else parameter
        if location == "body":
            body_schema = _resolve_ref(document, schema)
            body_fields = _flatten_schema_fields(
                document,
                body_schema,
                prefix="request.body",
                direction="parameter",
                path=path,
                method=method,
                required_names=_required_names(body_schema),
            )
            parameters.extend(body_fields)
            if not body_fields and parameter.get("name"):
                parameters.append(_openapi_parameter_field(parameter, path=path, method=method, location="body"))
            continue
        parameters.append(_openapi_parameter_field(parameter, path=path, method=method, location=location))
    return parameters


def _openapi_parameter_field(parameter: dict[str, Any], *, path: str, method: str, location: str) -> dict[str, Any]:
    name = str(parameter.get("name") or "")
    field_path = f"request.{location}.{name}" if name else f"request.{location}"
    data_type = str(parameter.get("type") or _schema_type(parameter.get("schema")) or "string")
    description = str(parameter.get("description") or "")
    return {
        "chunk_id": f"openapi:{method.lower()}:{path}",
        "scope": _parameter_scope(name),
        "name": name,
        "wire_name": name,
        "raw_name": name,
        "parameter_path": field_path,
        "field_path": field_path,
        "label_ko": _korean_label(description),
        "label_en": "",
        "data_type": data_type,
        "is_required": bool(parameter.get("required", False)),
        "description": description,
        "sample_value": "",
        "evidence": [{"kind": "openapi_parameter", "path": path, "method": method.upper(), "name": name}],
    }


def _openapi_security_parameters(
    document: dict[str, Any],
    *,
    operation: dict[str, Any],
    path: str,
    method: str,
) -> list[dict[str, Any]]:
    security_definitions = document.get("securityDefinitions")
    if not isinstance(security_definitions, dict):
        components = document.get("components") if isinstance(document.get("components"), dict) else {}
        security_definitions = components.get("securitySchemes") if isinstance(components.get("securitySchemes"), dict) else {}
    if not isinstance(security_definitions, dict):
        return []
    security = operation.get("security")
    if not isinstance(security, list):
        security = document.get("security") if isinstance(document.get("security"), list) else []
    parameters: list[dict[str, Any]] = []
    for item in security:
        if not isinstance(item, dict):
            continue
        for scheme_name in item.keys():
            scheme = _resolve_ref(document, security_definitions.get(scheme_name))
            if not isinstance(scheme, dict) or str(scheme.get("type") or "").lower() != "apikey":
                continue
            location = str(scheme.get("in") or "query")
            name = str(scheme.get("name") or scheme_name)
            parameters.append(
                {
                    "chunk_id": f"openapi:{method.lower()}:{path}",
                    "scope": "control",
                    "name": name,
                    "wire_name": name,
                    "raw_name": name,
                    "parameter_path": f"request.{location}.{name}",
                    "field_path": f"request.{location}.{name}",
                    "label_ko": "",
                    "label_en": "",
                    "data_type": "string",
                    "is_required": True,
                    "description": str(scheme.get("description") or f"OpenAPI security scheme {scheme_name}"),
                    "sample_value": "",
                    "evidence": [{"kind": "openapi_security_parameter", "path": path, "method": method.upper(), "name": name}],
                }
            )
    return parameters


def _openapi_response_fields(document: dict[str, Any], *, path: str, method: str, operation: dict[str, Any]) -> list[dict[str, Any]]:
    responses = operation.get("responses") if isinstance(operation.get("responses"), dict) else {}
    response = responses.get("200") or responses.get("201") or responses.get("default") or {}
    schema: Any = None
    if isinstance(response, dict):
        if isinstance(response.get("schema"), dict):
            schema = response.get("schema")
        else:
            content = response.get("content") if isinstance(response.get("content"), dict) else {}
            media = content.get("application/json") or content.get("*/*") or next(iter(content.values()), {})
            schema = media.get("schema") if isinstance(media, dict) else None
    return _flatten_schema_fields(document, schema, prefix="response.body", direction="response", path=path, method=method)


def _flatten_schema_fields(
    document: dict[str, Any],
    schema: Any,
    *,
    prefix: str,
    direction: str,
    path: str,
    method: str,
    required_names: set[str] | None = None,
) -> list[dict[str, Any]]:
    resolved = _resolve_ref(document, schema)
    if not isinstance(resolved, dict):
        return []
    if "allOf" in resolved and isinstance(resolved.get("allOf"), list):
        merged: list[dict[str, Any]] = []
        for child in resolved["allOf"]:
            merged.extend(
                _flatten_schema_fields(
                    document,
                    child,
                    prefix=prefix,
                    direction=direction,
                    path=path,
                    method=method,
                    required_names=required_names,
                )
            )
        return merged
    schema_type = _schema_type(resolved)
    if schema_type == "array":
        return _flatten_schema_fields(document, resolved.get("items"), prefix=f"{prefix}[]", direction=direction, path=path, method=method)
    properties = resolved.get("properties") if isinstance(resolved.get("properties"), dict) else {}
    if not properties:
        return []
    parent_required = _required_names(resolved)
    fields: list[dict[str, Any]] = []
    for name, child_schema in properties.items():
        child = _resolve_ref(document, child_schema)
        child_type = _schema_type(child)
        field_path = f"{prefix}.{name}"
        is_required = name in (required_names or parent_required)
        fields.append(
            _openapi_schema_field(
                child if isinstance(child, dict) else {},
                name=str(name),
                field_path=field_path,
                direction=direction,
                path=path,
                method=method,
                is_required=is_required,
            )
        )
        if child_type == "array":
            fields.extend(
                _flatten_schema_fields(
                    document,
                    child.get("items") if isinstance(child, dict) else {},
                    prefix=f"{field_path}[]",
                    direction=direction,
                    path=path,
                    method=method,
                )
            )
        elif isinstance(child, dict) and isinstance(child.get("properties"), dict):
            fields.extend(
                _flatten_schema_fields(document, child, prefix=field_path, direction=direction, path=path, method=method)
            )
    return fields


def _openapi_schema_field(
    schema: dict[str, Any],
    *,
    name: str,
    field_path: str,
    direction: str,
    path: str,
    method: str,
    is_required: bool,
) -> dict[str, Any]:
    description = str(schema.get("description") or schema.get("title") or "")
    common = {
        "chunk_id": f"openapi:{method.lower()}:{path}",
        "scope": "output" if direction == "response" else _parameter_scope(name),
        "wire_name": name,
        "raw_name": name,
        "field_path": field_path,
        "label_ko": _korean_label(description),
        "label_en": "",
        "data_type": _schema_type(schema) or "object",
        "is_required": is_required,
        "description": description,
        "sample_value": "",
        "evidence": [
            {
                "kind": "openapi_response_field" if direction == "response" else "openapi_request_body_field",
                "path": path,
                "method": method.upper(),
                "name": name,
            }
        ],
    }
    if direction == "parameter":
        common["name"] = name
        common["parameter_path"] = field_path
    return common


def _resolve_ref(document: dict[str, Any], value: Any) -> Any:
    if not isinstance(value, dict) or not isinstance(value.get("$ref"), str):
        return value
    ref = value["$ref"]
    if not ref.startswith("#/"):
        return value
    current: Any = document
    for token in ref[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or token not in current:
            return value
        current = current[token]
    if isinstance(current, dict):
        merged = {key: item for key, item in value.items() if key != "$ref"}
        return {**current, **merged}
    return current


def _openapi_base_url(document: dict[str, Any]) -> str:
    servers = document.get("servers")
    if isinstance(servers, list) and servers and isinstance(servers[0], dict):
        return str(servers[0].get("url") or "").rstrip("/")
    host = str(document.get("host") or "").strip()
    if not host:
        return ""
    schemes = document.get("schemes") if isinstance(document.get("schemes"), list) else []
    scheme = str(schemes[0]) if schemes else "https"
    base_path = str(document.get("basePath") or "").strip()
    if base_path and not base_path.startswith("/"):
        base_path = f"/{base_path}"
    return f"{scheme}://{host}{base_path}".rstrip("/")


def _operation_name(method: str, path: str) -> str:
    normalized_path = re.sub(r"[^a-zA-Z0-9]+", "_", path).strip("_").lower()
    return f"{method.lower()}_{normalized_path}" if normalized_path else method.lower()


def _required_names(schema: Any) -> set[str]:
    resolved = schema if isinstance(schema, dict) else {}
    required = resolved.get("required")
    return {str(item) for item in required if item} if isinstance(required, list) else set()


def _safe_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _schema_type(schema: Any) -> str:
    if not isinstance(schema, dict):
        return ""
    value = schema.get("type")
    if isinstance(value, str) and value:
        return value
    if "properties" in schema:
        return "object"
    if "enum" in schema:
        return "string"
    return ""


def _parameter_scope(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]", "", name.lower())
    return "control" if normalized in CONTROL_PARAMETER_NAMES else "input"


def _korean_label(description: str) -> str:
    text = description.strip()
    if not re.search(r"[가-힣]", text):
        return ""
    return text.splitlines()[0].strip()[:120]


def _run_langextract(document_text: str, *, model_id: str) -> list[GroundedExtraction]:
    try:
        import langextract as lx
    except Exception as exc:  # pragma: no cover - environment dependent
        raise RuntimeError("langextract is not installed in the worker image") from exc

    examples = [
        lx.data.ExampleData(
            text=(
                "[chunk_id=example_001]\n"
                "상세기능명 getSummFinaStat_V2\n"
                "요청 메시지 명세: 법인등록번호 crno, 사업연도 bizYear\n"
                "응답 메시지 명세: 기업매출금액 enpSaleAmt, 통화코드 curCd"
            ),
            extractions=[
                lx.data.Extraction(
                    extraction_class="source_operation",
                    extraction_text="getSummFinaStat_V2",
                    attributes={
                        "operation_name": "getSummFinaStat_V2",
                        "method": "GET",
                        "path": "/getSummFinaStat_V2",
                        "description": "요약재무제표조회",
                        "chunk_id": "example_001",
                    },
                ),
                lx.data.Extraction(
                    extraction_class="source_parameter",
                    extraction_text="crno",
                    attributes={
                        "operation_name": "getSummFinaStat_V2",
                        "wire_name": "crno",
                        "raw_name": "crno",
                        "label_ko": "법인등록번호",
                        "scope": "input",
                        "field_path": "request.query.crno",
                        "data_type": "string",
                        "is_required": True,
                        "chunk_id": "example_001",
                    },
                ),
                lx.data.Extraction(
                    extraction_class="source_response_field",
                    extraction_text="enpSaleAmt",
                    attributes={
                        "operation_name": "getSummFinaStat_V2",
                        "wire_name": "enpSaleAmt",
                        "raw_name": "enpSaleAmt",
                        "label_ko": "기업매출금액",
                        "scope": "output",
                        "field_path": "response.body.items.item.enpSaleAmt",
                        "data_type": "number",
                        "is_required": False,
                        "chunk_id": "example_001",
                    },
                ),
            ],
        )
    ]
    prompt = (
        "Extract only executable API source contract facts from Korean or English API guide text. "
        "Use extraction classes source_operation, source_parameter, and source_response_field. "
        "wire_name and raw_name must be actual API keys used on the wire, not Korean display labels. "
        "Put Korean labels in label_ko. Include operation_name, method, base_url, path, field_path, "
        "scope, data_type, is_required, description, and chunk_id when available. "
        "Do not infer business concepts, bindings, capabilities, or canonical model terms."
    )
    result = lx.extract(
        text_or_documents=document_text,
        prompt_description=prompt,
        examples=examples,
        model_id=model_id,
        extraction_passes=2,
        max_workers=8,
        max_char_buffer=1800,
    )
    grounded: list[GroundedExtraction] = []
    for extraction in getattr(result, "extractions", []) or []:
        char_interval = getattr(extraction, "char_interval", None)
        if char_interval is None:
            continue
        start = getattr(char_interval, "start_pos", None)
        end = getattr(char_interval, "end_pos", None)
        grounded.append(
            GroundedExtraction(
                extraction_class=str(getattr(extraction, "extraction_class", "")),
                extraction_text=str(getattr(extraction, "extraction_text", "")),
                attributes=dict(getattr(extraction, "attributes", {}) or {}),
                char_start=start if isinstance(start, int) else None,
                char_end=end if isinstance(end, int) else None,
            )
        )
    return grounded


def _document_text_with_chunk_markers(chunks: list[DocumentChunk]) -> tuple[str, list[dict[str, Any]]]:
    parts: list[str] = []
    spans: list[dict[str, Any]] = []
    offset = 0
    for chunk in chunks:
        heading = f" heading={json.dumps(chunk.heading, ensure_ascii=False)}" if chunk.heading else ""
        marker = f"[chunk_id={chunk.chunk_id} type={chunk.chunk_type}{heading}]\n"
        text = marker + chunk.text.strip() + "\n"
        start = offset
        end = start + len(text)
        spans.append({"chunk_id": chunk.chunk_id, "start": start, "end": end, "evidence_refs": chunk.evidence_refs})
        parts.append(text)
        offset = end
    return "\n".join(parts), spans


def _coerce_extraction(extraction: GroundedExtraction | dict[str, Any]) -> GroundedExtraction:
    if isinstance(extraction, GroundedExtraction):
        return extraction
    attrs = extraction.get("attributes") if isinstance(extraction.get("attributes"), dict) else {}
    interval = extraction.get("char_interval") if isinstance(extraction.get("char_interval"), dict) else {}
    return GroundedExtraction(
        extraction_class=str(extraction.get("extraction_class") or ""),
        extraction_text=str(extraction.get("extraction_text") or ""),
        attributes=attrs,
        char_start=interval.get("start") if isinstance(interval.get("start"), int) else extraction.get("char_start"),
        char_end=interval.get("end") if isinstance(interval.get("end"), int) else extraction.get("char_end"),
    )


def _new_operation(operation_name: str) -> dict[str, Any]:
    return {
        "chunk_id": "",
        "operation_key": operation_name,
        "operation_name": operation_name,
        "method": "GET",
        "base_url": "",
        "path": "",
        "source_url": "",
        "description": operation_name,
        "evidence_refs": [],
        "parameters": [],
        "response_fields": [],
    }


def _field_from_extraction(item: GroundedExtraction, *, chunk_id: str, evidence_refs: list[dict[str, Any]]) -> dict[str, Any]:
    attrs = item.attributes
    wire_name = str(attrs.get("wire_name") or attrs.get("raw_name") or item.extraction_text).strip()
    scope = str(attrs.get("scope") or ("output" if item.extraction_class == "source_response_field" else "input"))
    default_path_prefix = "response.body.items.item" if scope == "output" else "request.query"
    field_path = str(attrs.get("field_path") or f"{default_path_prefix}.{wire_name}")
    field = {
        "chunk_id": chunk_id,
        "scope": scope,
        "wire_name": wire_name,
        "raw_name": str(attrs.get("raw_name") or wire_name),
        "field_path": field_path,
        "label_ko": str(attrs.get("label_ko") or ""),
        "label_en": str(attrs.get("label_en") or ""),
        "data_type": str(attrs.get("data_type") or "string"),
        "is_required": _as_bool(attrs.get("is_required")),
        "description": str(attrs.get("description") or item.extraction_text),
        "sample_value": str(attrs.get("sample_value") or ""),
        "evidence": evidence_refs,
    }
    if item.extraction_class == "source_parameter":
        field["name"] = wire_name
        field["parameter_path"] = field_path
    return field


def _chunk_id_for_offset(offset: int | None, spans: list[dict[str, Any]]) -> str:
    if offset is None:
        return ""
    for span in spans:
        if int(span.get("start") or 0) <= offset < int(span.get("end") or 0):
            return str(span.get("chunk_id") or "")
    return ""


def _evidence_refs(item: GroundedExtraction, chunk_id: str) -> list[dict[str, Any]]:
    evidence = {
        "kind": "langextract_grounding",
        "chunk_id": chunk_id,
        "extraction_class": item.extraction_class,
        "extraction_text": item.extraction_text[:240],
    }
    if item.char_start is not None and item.char_end is not None:
        evidence["char_start"] = item.char_start
        evidence["char_end"] = item.char_end
    return [evidence]


def _merge_evidence(left: Any, right: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = list(left) if isinstance(left, list) else []
    for item in right:
        if item not in merged:
            merged.append(item)
    return merged


def _dedupe_operation_fields(operation: dict[str, Any]) -> dict[str, Any]:
    for key in ("parameters", "response_fields"):
        seen: set[tuple[str, str]] = set()
        deduped: list[dict[str, Any]] = []
        for field in operation.get(key) or []:
            identity = (str(field.get("wire_name") or field.get("raw_name") or ""), str(field.get("field_path") or ""))
            if identity in seen:
                continue
            seen.add(identity)
            deduped.append(field)
        operation[key] = deduped
    return operation


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y", "required", "필수"}
    return bool(value)
