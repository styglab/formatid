from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from services.context_platform.internal.ingestion.canonical_reconciliation import build_canonical_model_reconciliation
from services.context_platform.internal.ingestion.binding_generation import build_binding_generation
from services.context_platform.internal.ingestion.capability_generation import build_capability_generation
from services.context_platform.internal.ingestion.langgraph.document_structure import DocumentStructureResult
from services.context_platform.internal.ingestion.langgraph.document_structure import extract_document_structure_with_graph
from services.context_platform.internal.ingestion.llm.document_structure import normalize_manual_document_structure_response
from services.context_platform.internal.ingestion.parsers.common import LoadedSource
from services.context_platform.internal.storage import ContextPlatformRepository
from services.context_platform.internal.storage.object_store import ObjectStore


SECRET_PARAMETER_PATTERN = re.compile(r"(api[-_]?key|servicekey|token|secret|password|authorization|auth)", re.IGNORECASE)


STAGE_STRUCTURE_REVIEW = "source_structure_review"
STAGE_SOURCE_GRAPH = "source_graph_extracted"
STAGE_MEANING_RESOLUTION = "meaning_resolution"
STAGE_RESOLUTION_GENERATION = "resolution_generation"
STAGE_CAPABILITY_GENERATION = "capability_generation"
STAGE_CAPABILITY_CONTRACTING = STAGE_CAPABILITY_GENERATION
STAGE_OPERATION_VERIFICATION = "operation_verification"
STAGE_PROPOSAL_BUNDLE = "proposal_bundle_created"


def _metadata_agent_mode(run_metadata: dict[str, Any]) -> str | None:
    mode = run_metadata.get("agent_mode")
    if isinstance(mode, str):
        return mode
    mode = run_metadata.get("llm_mode")
    return mode if isinstance(mode, str) else None


def ingest_source_document(run_id: str, repository: ContextPlatformRepository | None = None) -> dict[str, Any]:
    from services.context_platform.internal.ingestion.langgraph.pipeline import run_ingestion_pipeline_graph

    return run_ingestion_pipeline_graph(run_id, repository=repository).result


def _manual_stage_response(
    run_metadata: dict[str, Any],
    direct_key: str,
    bundle_key: str,
    *,
    legacy_direct_keys: tuple[str, ...] = (),
    legacy_bundle_keys: tuple[str, ...] = (),
) -> dict[str, Any] | None:
    direct = run_metadata.get(direct_key)
    if isinstance(direct, dict):
        return direct
    for legacy_key in legacy_direct_keys:
        direct = run_metadata.get(legacy_key)
        if isinstance(direct, dict):
            return direct
    combined = run_metadata.get("agent_response")
    if not isinstance(combined, dict):
        combined = run_metadata.get("manual_llm_response")
    if not isinstance(combined, dict):
        return None
    bundled = combined.get(bundle_key)
    if isinstance(bundled, dict):
        return bundled
    for legacy_key in legacy_bundle_keys:
        bundled = combined.get(legacy_key)
        if isinstance(bundled, dict):
            return bundled
    return None


def _is_manual_agent_mode(mode: str | None) -> bool:
    return str(mode or "").strip().lower() in {"manual", "agent_manual", "codex_manual"}


def _manual_coverage_error(payload: dict[str, Any], *, stage: str) -> str:
    items = payload.get("decisions") if stage in {"canonical_reconciliation", STAGE_MEANING_RESOLUTION} else payload.get("suggestions")
    if not isinstance(items, list):
        return ""
    unresolved = [
        item
        for item in items
        if isinstance(item, dict)
        and str(item.get("decision") or "") == "conflict"
        and not bool(item.get("llm_decision"))
    ]
    if not unresolved:
        return ""
    examples = []
    for item in unresolved[:5]:
        source_term = item.get("source_term") if isinstance(item.get("source_term"), dict) else item
        examples.append(
            ":".join(
                part
                for part in [
                    str(source_term.get("source_operation_id") or ""),
                    str(source_term.get("source_parameter_id") or source_term.get("source_field_id") or ""),
                    str(source_term.get("field_path") or source_term.get("raw_name") or ""),
                ]
                if part
            )
        )
    suffix = f" examples={examples}" if examples else ""
    return f"{stage} manual response is incomplete; {len(unresolved)} source terms remain unresolved.{suffix}"


def _source_with_run_verification_config(source: dict[str, Any], run_metadata: dict[str, Any]) -> dict[str, Any]:
    agent_response = run_metadata.get("agent_response")
    if not isinstance(agent_response, dict):
        agent_response = run_metadata.get("manual_llm_response")
    if not isinstance(agent_response, dict):
        return source
    verification = _sanitize_verification_config(agent_response.get("verification"))
    if not verification:
        return source
    merged = dict(source)
    config = dict(source.get("config") if isinstance(source.get("config"), dict) else {})
    current_verification = dict(config.get("verification") if isinstance(config.get("verification"), dict) else {})
    config["verification"] = _merge_dicts(current_verification, verification)
    merged["config"] = config
    return merged


def _sanitize_verification_config(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    sanitized: dict[str, Any] = {}
    secret_env = value.get("secret_env")
    if isinstance(secret_env, dict):
        sanitized["secret_env"] = {
            str(key): str(env_name)
            for key, env_name in secret_env.items()
            if key and isinstance(env_name, str) and re.fullmatch(r"[A-Z0-9_]+", env_name)
        }
    sample_parameters = _sanitize_sample_parameters(value.get("sample_parameters"))
    if sample_parameters:
        sanitized["sample_parameters"] = sample_parameters
    allow_methods = value.get("allow_methods")
    if isinstance(allow_methods, list):
        methods = [str(method).upper() for method in allow_methods if str(method or "").strip()]
        if methods:
            sanitized["allow_methods"] = methods
    if value.get("allow_unsafe_methods") is True:
        sanitized["allow_unsafe_methods"] = True
    return sanitized


def _sanitize_sample_parameters(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    sanitized: dict[str, Any] = {}
    for scope, parameters in value.items():
        if isinstance(parameters, dict):
            clean_parameters = {
                str(name): str(sample)
                for name, sample in parameters.items()
                if not SECRET_PARAMETER_PATTERN.search(str(name))
                and sample is not None
                and str(sample) != ""
            }
            if clean_parameters:
                sanitized[str(scope)] = clean_parameters
        elif not SECRET_PARAMETER_PATTERN.search(str(scope)) and parameters is not None and str(parameters) != "":
            sanitized[str(scope)] = str(parameters)
    return sanitized


def _merge_dicts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def parse_uploaded_source_document(
    repo: ContextPlatformRepository,
    run: dict[str, Any],
    source: dict[str, Any],
    document: dict[str, Any],
) -> dict[str, Any]:
    filename = str(document.get("name") or "")
    content_type = str(document.get("content_type") or "")
    suffix = Path(filename).suffix.lower()
    uri = str(document.get("uri") or "")
    run_metadata = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
    manual_llm_response = run_metadata.get("agent_response")
    if not isinstance(manual_llm_response, dict):
        manual_llm_response = run_metadata.get("manual_llm_response")
    if isinstance(manual_llm_response, dict) and isinstance(manual_llm_response.get("source_structure"), dict):
        normalized = normalize_manual_document_structure_response(manual_llm_response)
        result = DocumentStructureResult(
            drafts=[],
            chunk_summaries=[],
            classified_chunks=[],
            operation_candidates=normalized.get("operation_candidates") or [],
            field_candidates=normalized.get("field_candidates") or [],
            engine=str(normalized.get("engine") or "agent_manual_document_structure_graph"),
            llm_mode=str(normalized.get("llm_mode") or "agent_manual"),
            status="ready",
        )
        return {"status": "ready", "parsed": _parsed_from_document_structure(result)}

    if suffix in {".pdf", ".docx", ".html", ".htm"} or content_type in {
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "text/html",
    }:
        raw = ObjectStore().read_bytes(uri)
        with tempfile.TemporaryDirectory(prefix="context-platform-ingest-") as tmpdir:
            path = Path(tmpdir) / (Path(filename).name or "source-document")
            path.write_bytes(raw)
            loaded = LoadedSource(
                source_id=str(source.get("id") or ""),
                source_name=str(source.get("name") or ""),
                source_type=str(source.get("source_type") or "api"),
                filename=path.name,
                media_type=content_type or "application/octet-stream",
                reference_uri=uri,
                stored_path=str(path),
                content_text="",
                content_json=None,
            )
            result = extract_document_structure_with_graph(
                loaded,
                operations=[{"operation_key": f"{source.get('id')}:document_structure"}],
                source=source,
                run_id=str(run.get("id") or ""),
                llm_mode=_metadata_agent_mode(run_metadata),
                manual_llm_response=manual_llm_response if isinstance(manual_llm_response, dict) else None,
            )
        if result.status == "waiting_manual_llm":
            return {
                "status": "waiting_manual_llm",
                "manual_llm_request": result.manual_llm_request,
                "chunk_count": len(result.chunk_summaries),
                "parser": result.engine,
            }
        return {"status": "ready", "parsed": _parsed_from_document_structure(result)}

    text = ObjectStore().read_text(uri)
    return {"status": "ready", "parsed": parse_source_document(text, str(document.get("document_type") or "auto"))}


def build_canonical_reconciliation_for_run(
    repo: ContextPlatformRepository,
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    llm_mode: str | None,
    manual_llm_response: dict[str, Any] | None,
) -> dict[str, Any]:
    try:
        from services.context_platform.internal.ingestion.langgraph.canonical_reconciliation import run_canonical_reconciliation_graph

        result = run_canonical_reconciliation_graph(
            repo,
            run_id=run_id,
            source=source,
            document=document,
            operations=operations,
            document_fields=document_fields,
            llm_mode=llm_mode,
            manual_llm_response=manual_llm_response,
        )
        return {
            "status": result.status,
            "engine": result.engine,
            "llm_mode": result.llm_mode,
            "payload": result.payload,
            "manual_llm_request": result.manual_llm_request,
            "error": result.error,
        }
    except ImportError:
        payload = build_canonical_model_reconciliation(
            repo,
            source=source,
            document=document,
            operations=operations,
            document_fields=document_fields,
            llm_mode=llm_mode,
            manual_llm_response=manual_llm_response,
        )
        return {
            "status": "ready",
            "engine": str(payload.get("engine") or "heuristic_meaning_resolution"),
            "llm_mode": str(payload.get("llm_mode") or "disabled"),
            "payload": payload,
            "manual_llm_request": None,
            "error": None,
        }


def build_binding_generation_for_run(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    llm_mode: str | None,
    manual_llm_response: dict[str, Any] | None,
) -> dict[str, Any]:
    try:
        from services.context_platform.internal.ingestion.langgraph.binding_generation import run_binding_generation_graph

        result = run_binding_generation_graph(
            run_id=run_id,
            source=source,
            document=document,
            operations=operations,
            document_fields=document_fields,
            canonical_reconciliation=canonical_reconciliation,
            llm_mode=llm_mode,
            manual_llm_response=manual_llm_response,
        )
        return {
            "status": result.status,
            "engine": result.engine,
            "llm_mode": result.llm_mode,
            "payload": result.payload,
            "manual_llm_request": result.manual_llm_request,
            "error": result.error,
        }
    except ImportError:
        payload = build_binding_generation(
            run_id=run_id,
            source=source,
            document=document,
            operations=operations,
            document_fields=document_fields,
            canonical_reconciliation=canonical_reconciliation,
            llm_mode=llm_mode,
            manual_llm_response=manual_llm_response,
        )
        return {
            "status": "ready",
            "engine": str(payload.get("engine") or "heuristic_resolution_generation"),
            "llm_mode": str(payload.get("llm_mode") or "disabled"),
            "payload": payload,
            "manual_llm_request": None,
            "error": None,
        }


def build_capability_generation_for_run(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
    llm_mode: str | None,
    manual_llm_response: dict[str, Any] | None,
) -> dict[str, Any]:
    try:
        from services.context_platform.internal.ingestion.langgraph.capability_generation import run_capability_generation_graph

        result = run_capability_generation_graph(
            run_id=run_id,
            source=source,
            document=document,
            operations=operations,
            canonical_reconciliation=canonical_reconciliation,
            binding_generation=binding_generation,
            llm_mode=llm_mode,
            manual_llm_response=manual_llm_response,
        )
        return {
            "status": result.status,
            "engine": result.engine,
            "llm_mode": result.llm_mode,
            "payload": result.payload,
            "manual_llm_request": result.manual_llm_request,
            "error": result.error,
        }
    except ImportError:
        payload = build_capability_generation(
            run_id=run_id,
            source=source,
            document=document,
            operations=operations,
            canonical_reconciliation=canonical_reconciliation,
            binding_generation=binding_generation,
            llm_mode=llm_mode,
            manual_llm_response=manual_llm_response,
        )
        return {
            "status": "ready",
            "engine": str(payload.get("engine") or "heuristic_capability_generation"),
            "llm_mode": str(payload.get("llm_mode") or "disabled"),
            "payload": payload,
            "manual_llm_request": None,
            "error": None,
        }


def _parsed_from_document_structure(result: DocumentStructureResult) -> dict[str, Any]:
    parameter_candidates = [
        item
        for item in result.field_candidates
        if str(item.get("scope") or "") in {"input", "control"}
    ]
    output_candidates = [
        item
        for item in result.field_candidates
        if str(item.get("scope") or "output") == "output"
    ]
    operations: list[dict[str, Any]] = []
    for candidate in result.operation_candidates:
        operation_name = str(candidate.get("operation_name") or "").strip()
        if not operation_name:
            continue
        description = str(candidate.get("description") or "")
        endpoint = _operation_endpoint(candidate, operation_name, description)
        chunk_id = str(candidate.get("chunk_id") or "")
        operation_parameters = [
            _field_candidate_to_parameter(item)
            for item in _field_candidates_for_operation(
                parameter_candidates,
                chunk_id=chunk_id,
                allow_global_fallback=True,
            )
        ]
        operation_outputs = [
            _field_candidate_to_source_field(item)
            for item in _field_candidates_for_operation(
                output_candidates,
                chunk_id=chunk_id,
                allow_global_fallback=False,
            )
        ]
        operations.append(
            {
                "method": endpoint["method"],
                "path": endpoint["path"],
                "operation_key": operation_name,
                "name": operation_name,
                "description": description,
                "endpoint_metadata": endpoint["endpoint_metadata"],
                "parameters": operation_parameters,
                "response_fields": operation_outputs,
                "request_spec": {"parameters": operation_parameters},
                "response_spec": {"fields": operation_outputs},
            }
        )
    document_fields = [] if operations else [_field_candidate_to_source_field(item) for item in result.field_candidates]
    return {
        "format": result.engine,
        "document_type": "api_document",
        "operations": operations,
        "document_fields": document_fields,
    }


def _field_candidates_for_operation(
    candidates: list[dict[str, Any]],
    *,
    chunk_id: str,
    allow_global_fallback: bool,
) -> list[dict[str, Any]]:
    if chunk_id:
        scoped = [item for item in candidates if str(item.get("chunk_id") or "") == chunk_id]
        if scoped:
            return _dedupe_field_candidates(scoped)
        if not allow_global_fallback:
            return []
    unscoped = [item for item in candidates if not str(item.get("chunk_id") or "")]
    if unscoped:
        return _dedupe_field_candidates(unscoped)
    return _dedupe_field_candidates(candidates) if allow_global_fallback else []


def _dedupe_field_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in candidates:
        key = (
            str(item.get("scope") or ""),
            str(item.get("field_path") or ""),
            str(item.get("raw_name") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _field_candidate_to_parameter(item: dict[str, Any]) -> dict[str, Any]:
    sample_value = str(item.get("sample_value") or "")
    wire_name = str(item.get("wire_name") or item.get("raw_name") or "")
    label_ko = str(item.get("label_ko") or "")
    label_en = str(item.get("label_en") or "")
    source_evidence_tier = str(item.get("source_evidence_tier") or "")
    parameter_path = str(item.get("field_path") or "")
    return {
        "name": wire_name,
        "raw_name": wire_name,
        "location": _parameter_location_from_path(parameter_path),
        "parameter_path": parameter_path,
        "data_type": str(item.get("data_type") or "string"),
        "is_required": bool(item.get("is_required")),
        "description": str(item.get("description") or ""),
        "enum_values": [],
        "default_value": sample_value or None,
        "metadata": {
            "wire_name": wire_name,
            **({"label_ko": label_ko} if label_ko else {}),
            **({"label_en": label_en} if label_en else {}),
            **({"sample_value": sample_value} if sample_value else {}),
            **({"source_evidence_tier": source_evidence_tier} if source_evidence_tier else {}),
        },
    }


def _field_candidate_to_source_field(item: dict[str, Any]) -> dict[str, Any]:
    raw_name = str(item.get("wire_name") or item.get("raw_name") or "")
    label_ko = str(item.get("label_ko") or "")
    label_en = str(item.get("label_en") or "")
    source_evidence_tier = str(item.get("source_evidence_tier") or "")
    return {
        "field_path": str(item.get("field_path") or raw_name),
        "raw_name": raw_name,
        "display_name": label_ko or label_en or raw_name,
        "data_type": str(item.get("data_type") or "string"),
        "is_required": bool(item.get("is_required")),
        "description": str(item.get("description") or ""),
        "metadata": {
            "wire_name": raw_name,
            **({"label_ko": label_ko} if label_ko else {}),
            **({"label_en": label_en} if label_en else {}),
            **({"source_evidence_tier": source_evidence_tier} if source_evidence_tier else {}),
        },
        "evidence": item.get("evidence") if isinstance(item.get("evidence"), list) else [],
    }


def _parameter_location_from_path(parameter_path: str) -> str:
    if parameter_path.startswith("request.body"):
        return "body"
    if parameter_path.startswith("request.header"):
        return "header"
    if parameter_path.startswith("request.path"):
        return "path"
    return "query"


def _operation_endpoint(candidate: dict[str, Any], operation_name: str, description: str) -> dict[str, Any]:
    method = str(candidate.get("method") or "GET").upper()
    source_url = str(candidate.get("source_url") or "").strip()
    if not source_url:
        match = re.search(r"https?://[^\s)]+", description)
        source_url = match.group(0) if match else ""
    base_url = str(candidate.get("base_url") or "").strip()
    path = str(candidate.get("path") or "").strip()
    if source_url and (not base_url or not path):
        inferred = _endpoint_parts_from_source_url(source_url)
        base_url = base_url or inferred["base_url"]
        path = path or inferred["path"]
    path = path if path.startswith("/") else f"/{path}" if path else f"/{operation_name}"
    endpoint_metadata: dict[str, Any] = {}
    if base_url:
        endpoint_metadata["base_url"] = base_url.rstrip("/")
        endpoint_metadata["verification"] = {"base_url": base_url.rstrip("/")}
    if source_url:
        endpoint_metadata["source_url"] = source_url
    return {
        "method": method,
        "path": path,
        "endpoint_metadata": endpoint_metadata,
    }


def _endpoint_parts_from_source_url(source_url: str) -> dict[str, str]:
    parsed = urlparse(source_url)
    if not parsed.scheme or not parsed.netloc:
        return {"base_url": "", "path": ""}
    path = parsed.path.rstrip("/")
    base_path = path.rsplit("/", 1)[0] if "/" in path else ""
    return {
        "base_url": f"{parsed.scheme}://{parsed.netloc}{base_path}".rstrip("/"),
        "path": f"/{path.rsplit('/', 1)[-1]}" if path else "",
    }


def parse_source_document(text: str, document_type: str = "auto") -> dict[str, Any]:
    text = text.replace("\x00", " ")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {"format": "text", "operations": [], "document_fields": _text_fields(text)}
    if not isinstance(payload, dict):
        return {"format": "json", "operations": []}
    paths = payload.get("paths")
    if not isinstance(paths, dict):
        return {"format": "json", "operations": [], "document_fields": _json_document_fields(payload)}
    operations: list[dict[str, Any]] = []
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete"} or not isinstance(operation, dict):
                continue
            parameters = _openapi_parameters(path_item, operation)
            response_fields = _openapi_response_fields(operation)
            operations.append(
                {
                    "method": method.upper(),
                    "path": str(path),
                    "operation_key": str(operation.get("operationId") or f"{method.upper()} {path}"),
                    "name": str(operation.get("summary") or operation.get("operationId") or f"{method.upper()} {path}"),
                    "description": str(operation.get("description") or ""),
                    "parameters": parameters,
                    "response_fields": response_fields,
                    "request_spec": {"parameters": parameters},
                    "response_spec": {"fields": response_fields},
                }
            )
    return {"format": "openapi", "document_type": document_type, "operations": operations, "document_fields": []}


def persist_discovered_operations(
    repo: ContextPlatformRepository,
    source: dict[str, Any],
    document: dict[str, Any],
    parsed: dict[str, Any],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for operation in parsed.get("operations", []):
        operation_key = f"{source['id']}:{operation['operation_key']}"
        source_operation = repo.get_source_operation_by_key(operation_key)
        if source_operation:
            source_operation = {
                **source_operation,
                "description": str(source_operation.get("description") or operation.get("description") or ""),
                "endpoint_metadata": {
                    **(source_operation.get("endpoint_metadata") if isinstance(source_operation.get("endpoint_metadata"), dict) else {}),
                    **(operation.get("endpoint_metadata") if isinstance(operation.get("endpoint_metadata"), dict) else {}),
                },
            }
            parameters = repo.list_source_parameters(source_operation_id=source_operation["id"])
            fields = repo.list_source_fields(source_operation_id=source_operation["id"])
        else:
            source_operation = repo.create_source_operation(
                {
                    "source_id": source["id"],
                    "source_document_id": document["id"],
                    "operation_key": operation_key,
                    "method": operation["method"],
                    "path": operation["path"],
                    "name": operation["name"],
                    "description": operation["description"],
                    "request_spec": operation.get("request_spec") or {},
                    "response_spec": operation.get("response_spec") or {},
                    "endpoint_metadata": operation.get("endpoint_metadata") or {},
                    "status": "draft",
                    "evidence": [{"source_document_id": document["id"], "parser": parsed.get("format")}],
                }
            )
            parameters = [
                repo.create_source_parameter({**parameter, "source_operation_id": source_operation["id"], "status": "draft"})
                for parameter in operation.get("parameters", [])
            ]
            fields = [
                repo.create_source_field(
                    {
                        **field,
                        "source_id": source["id"],
                        "source_document_id": document["id"],
                        "source_operation_id": source_operation["id"],
                        "direction": "output",
                        "status": "draft",
                    }
                )
                for field in operation.get("response_fields", [])
            ]
        records.append({**source_operation, "parameters": parameters, "fields": fields})
    return records


def persist_document_fields(
    repo: ContextPlatformRepository,
    source: dict[str, Any],
    document: dict[str, Any],
    parsed: dict[str, Any],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for field in parsed.get("document_fields", []):
        records.append(
            repo.create_source_field(
                {
                    **field,
                    "source_id": source["id"],
                    "source_document_id": document["id"],
                    "direction": "output",
                    "status": "draft",
                    "evidence": [{"source_document_id": document["id"], "parser": parsed.get("format")}],
                }
            )
        )
    return records


def create_ingestion_proposals(
    repo: ContextPlatformRepository,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
    capability_generation: dict[str, Any],
    verification_result: dict[str, Any],
) -> list[dict[str, Any]]:
    proposals = []
    proposals.append(
        repo.create_proposal(
            {
                "source_type": "source_document_ingestion",
                "title": f"Resolve meaning and representations for {document.get('name') or 'source document'}",
                "entity_type": "meaning_resolution",
                "entity_id": document["id"],
                "change_type": "reconcile",
                "status": "proposed",
                "payload": canonical_reconciliation,
                "rationale": "Resolve extracted source terms into Concepts, CanonicalRepresentations, RepresentationSchemas, or explicit skip/conflict decisions.",
                "evidence": [{"source_document_id": document["id"], "kind": "meaning_resolution"}],
            }
        )
    )
    proposals.append(
        repo.create_proposal(
            {
                "source_type": "source_document_ingestion",
                "title": f"Generate capabilities for {document.get('name') or 'source document'}",
                "entity_type": "capability_generation",
                "entity_id": document["id"],
                "change_type": "suggest",
                "status": "proposed",
                "payload": capability_generation,
                "rationale": "Generate planner-facing capability contracts, capability input/output contracts, and executable capability-step links from reviewed resolution bindings.",
                "evidence": [{"source_document_id": document["id"], "kind": "capability_generation"}],
            }
        )
    )
    proposals.append(
        repo.create_proposal(
            {
                "source_type": "source_document_ingestion",
                "title": f"Verify operation and capability contracts for {document.get('name') or 'source document'}",
                "entity_type": "endpoint_check_summary",
                "entity_id": document["id"],
                "change_type": "verify",
                "status": "proposed",
                "payload": {
                    "source_document_id": document["id"],
                    "verification_summary": verification_result.get("summary") or {},
                    "operation_checks": verification_result.get("operation_checks") or [],
                    "capability_checks": verification_result.get("capability_checks") or [],
                },
                "rationale": "API-derived operations and capabilities must carry endpoint or skipped/needs-input verification evidence before final review.",
                "evidence": [{"source_document_id": document["id"], "kind": "endpoint_checks"}],
            }
        )
    )
    decision_index = _canonical_decision_index(canonical_reconciliation)
    binding_index = _binding_suggestion_index(binding_generation)
    for operation in operations:
        proposals.append(
            repo.create_proposal(
                {
                    "source_type": "api_document_ingestion",
                    "title": f"Review source operation {operation['method']} {operation['path']}",
                    "entity_type": "source_operation",
                    "entity_id": operation["id"],
                    "change_type": "create",
                    "status": "proposed",
                    "payload": {
                        "source_id": source["id"],
                        "source_document_id": document["id"],
                        "source_operation_id": operation["id"],
                    },
                    "evidence": [{"source_document_id": document["id"]}],
                }
            )
        )
        for parameter in operation.get("parameters", []):
            proposals.extend(
                _term_proposals(
                    repo,
                    source,
                    document,
                    parameter,
                    source_operation_id=operation["id"],
                    source_kind="parameter",
                    canonical_decision=decision_index.get(("parameter", parameter.get("id"))) or {},
                    binding_suggestion=binding_index.get(("parameter", parameter.get("id"))) or {},
                )
            )
        for field in operation.get("fields", []):
            proposals.extend(
                _term_proposals(
                    repo,
                    source,
                    document,
                    field,
                    source_operation_id=operation["id"],
                    source_kind="field",
                    canonical_decision=decision_index.get(("field", field.get("id"))) or {},
                    binding_suggestion=binding_index.get(("field", field.get("id"))) or {},
                )
            )
    for field in document_fields:
        proposals.extend(
            _term_proposals(
                repo,
                source,
                document,
                field,
                source_operation_id=None,
            source_kind="field",
            canonical_decision=decision_index.get(("field", field.get("id"))) or {},
            binding_suggestion=binding_index.get(("field", field.get("id"))) or {},
        )
    )
    proposals.extend(_relation_proposals(repo, document, canonical_reconciliation))
    proposals.extend(_capability_proposals(repo, document, capability_generation))
    return proposals


def _relation_proposals(
    repo: ContextPlatformRepository,
    document: dict[str, Any],
    canonical_reconciliation: dict[str, Any],
) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    for suggestion in canonical_reconciliation.get("relation_suggestions", []):
        if not isinstance(suggestion, dict):
            continue
        if suggestion.get("decision") != "propose_relation":
            continue
        source_class_name = str(suggestion.get("source_class_name") or "")
        target_class_name = str(suggestion.get("target_class_name") or "")
        relation_type = str(suggestion.get("relation_type") or "")
        if not source_class_name or not target_class_name or not relation_type:
            continue
        proposal_payload = {
            "source_document_id": document["id"],
            "relation_decision": suggestion.get("decision"),
            "source_class_id": suggestion.get("source_class_id"),
            "source_class_name": source_class_name,
            "target_class_id": suggestion.get("target_class_id"),
            "target_class_name": target_class_name,
            "relation_type": relation_type,
            "forward_label": suggestion.get("forward_label") or relation_type,
            "reverse_label": suggestion.get("reverse_label") or "",
            "description": suggestion.get("description") or "",
            "cardinality": suggestion.get("cardinality") or "",
            "required": bool(suggestion.get("required", False)),
            "metadata": suggestion.get("metadata") if isinstance(suggestion.get("metadata"), dict) else {},
            "confidence": suggestion.get("confidence"),
            "depends_on_canonical_classes": True,
        }
        evidence = suggestion.get("evidence_refs") if isinstance(suggestion.get("evidence_refs"), list) else [{"source_document_id": document["id"]}]
        proposals.append(
            repo.create_proposal(
                {
                    "source_type": "source_document_ingestion",
                    "title": f"Suggest relation {source_class_name}.{relation_type} -> {target_class_name}",
                    "entity_type": "link_type",
                    "entity_id": f"{source_class_name}:{relation_type}:{target_class_name}",
                    "change_type": "suggest",
                    "status": "proposed",
                    "payload": proposal_payload,
                    "rationale": str(suggestion.get("rationale") or ""),
                    "evidence": evidence,
                }
            )
        )
    return proposals


def _capability_proposals(
    repo: ContextPlatformRepository,
    document: dict[str, Any],
    capability_generation: dict[str, Any],
) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    for suggestion in capability_generation.get("suggestions", []):
        if not isinstance(suggestion, dict):
            continue
        if suggestion.get("decision") != "propose_capability":
            continue
        capability = suggestion.get("capability") if isinstance(suggestion.get("capability"), dict) else {}
        capability_key = str(capability.get("capability_key") or suggestion.get("source_operation_id") or "")
        if not capability_key:
            continue
        proposal_payload = {
            "source_document_id": document["id"],
            "source_operation_id": suggestion.get("source_operation_id"),
            "capability": capability,
            "inputs": suggestion.get("inputs") if isinstance(suggestion.get("inputs"), list) else [],
            "outputs": suggestion.get("outputs") if isinstance(suggestion.get("outputs"), list) else [],
            "operation_link": suggestion.get("operation_link") if isinstance(suggestion.get("operation_link"), dict) else {},
            "confidence": suggestion.get("confidence"),
            "depends_on_bindings": True,
        }
        evidence = suggestion.get("evidence_refs") if isinstance(suggestion.get("evidence_refs"), list) else [{"source_document_id": document["id"]}]
        proposals.append(
            repo.create_proposal(
                {
                    "source_type": "source_document_ingestion",
                    "title": f"Suggest capability {capability_key}",
                    "entity_type": "capability",
                    "entity_id": capability_key,
                    "change_type": "suggest",
                    "status": "proposed",
                    "payload": proposal_payload,
                    "rationale": str(suggestion.get("rationale") or ""),
                    "evidence": evidence,
                }
            )
        )
        proposals.append(
            repo.create_proposal(
                {
                    "source_type": "source_document_ingestion",
                    "title": f"Link capability {capability_key} to source operation",
                    "entity_type": "capability_step",
                    "entity_id": f"{capability_key}:{suggestion.get('source_operation_id') or ''}",
                    "change_type": "suggest",
                    "status": "proposed",
                    "payload": {
                        "capability_key": capability_key,
                        "source_operation_id": suggestion.get("source_operation_id"),
                        "operation_link": suggestion.get("operation_link") if isinstance(suggestion.get("operation_link"), dict) else {},
                    },
                    "rationale": "Capability execution must use source_operations as the executable operation reference.",
                    "evidence": evidence,
                }
            )
        )
    return proposals


def _term_proposals(
    repo: ContextPlatformRepository,
    source: dict[str, Any],
    document: dict[str, Any],
    term_record: dict[str, Any],
    *,
    source_operation_id: str | None,
    source_kind: str,
    canonical_decision: dict[str, Any],
    binding_suggestion: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_name = str(term_record.get("raw_name") or term_record.get("name") or "")
    field_path = str(term_record.get("field_path") or (f"request.{raw_name}" if source_kind == "parameter" else raw_name))
    source_term_id = term_record.get("id")
    scope = f"operation {source_kind}" if source_operation_id else f"document {source_kind}"
    decision = str(canonical_decision.get("decision") or "create")
    base_payload = {
        "source_id": source["id"],
        "source_document_id": document["id"],
        "source_operation_id": source_operation_id,
        "source_field_id": source_term_id if source_kind == "field" else None,
        "source_parameter_id": source_term_id if source_kind == "parameter" else None,
        "source_kind": source_kind,
        "field_path": field_path,
        "raw_name": raw_name,
        "direction": canonical_decision.get("source_term", {}).get("direction") or ("input" if source_kind == "parameter" else "output"),
        "scope": scope,
        "canonical_reconciliation": canonical_decision,
        "canonical_decision": decision,
        "linkml_fragment": canonical_decision.get("linkml_fragment") if isinstance(canonical_decision.get("linkml_fragment"), dict) else {},
        "binding_suggestion": binding_suggestion,
    }
    binding_decision = str(binding_suggestion.get("decision") or "bind")
    binding_payload = {
        **base_payload,
        "binding_decision": binding_decision,
        "binding_kind": binding_suggestion.get("binding_kind") or _binding_kind_from_suggestion(binding_suggestion),
        "canonical_class_slot_id": binding_suggestion.get("canonical_class_slot_id"),
        "canonical_ref": binding_suggestion.get("canonical_ref") if isinstance(binding_suggestion.get("canonical_ref"), dict) else {},
        "binding_type": binding_suggestion.get("binding_type") or "exact",
        "transform_spec": binding_suggestion.get("transform_spec") if isinstance(binding_suggestion.get("transform_spec"), dict) else {"type": "none"},
        "normalization_rule": binding_suggestion.get("normalization_rule") if isinstance(binding_suggestion.get("normalization_rule"), dict) else {},
        "enum_mapping": binding_suggestion.get("enum_mapping") if isinstance(binding_suggestion.get("enum_mapping"), dict) else {},
        "depends_on_canonical_decision": bool(binding_suggestion.get("depends_on_canonical_decision")),
        "confidence": binding_suggestion.get("confidence"),
    }
    proposals: list[dict[str, Any]] = []
    if decision != "skip":
        proposals.append(repo.create_proposal(
            {
                "source_type": "source_document_ingestion",
                "title": f"{decision.title()} meaning resolution decision for {field_path}",
                "entity_type": "meaning_resolution_decision",
                "entity_id": str(source_term_id or field_path),
                "change_type": decision,
                "status": "proposed",
                "payload": base_payload,
                "rationale": str(canonical_decision.get("rationale") or ""),
                "evidence": canonical_decision.get("evidence_refs") if isinstance(canonical_decision.get("evidence_refs"), list) else [{"source_document_id": document["id"], "field_path": field_path}],
            }
        ))
    proposals.append(
        repo.create_proposal(
            {
                "source_type": "source_document_ingestion",
                "title": f"{binding_decision.replace('_', ' ').title()} resolution binding for {field_path}",
                "entity_type": "resolution_binding",
                "entity_id": str(source_term_id or field_path),
                "change_type": "suggest",
                "status": "proposed",
                "payload": binding_payload,
                "rationale": str(binding_suggestion.get("rationale") or "Binding must use the reviewed canonical reconciliation decision for this source term."),
                "evidence": binding_suggestion.get("evidence_refs")
                if isinstance(binding_suggestion.get("evidence_refs"), list)
                else canonical_decision.get("evidence_refs")
                if isinstance(canonical_decision.get("evidence_refs"), list)
                else [{"source_document_id": document["id"], "field_path": field_path}],
            }
        ),
    )
    return proposals


def _binding_kind_from_suggestion(suggestion: dict[str, Any]) -> str:
    value = str(suggestion.get("binding_kind") or "")
    if value in {"field", "context", "parameter"}:
        return value
    if suggestion.get("context_key"):
        return "context"
    if str(suggestion.get("source_kind") or "") == "parameter":
        return "parameter"
    return "field"


def _canonical_decision_index(canonical_reconciliation: dict[str, Any]) -> dict[tuple[str, Any], dict[str, Any]]:
    index: dict[tuple[str, Any], dict[str, Any]] = {}
    for decision in canonical_reconciliation.get("decisions", []):
        if not isinstance(decision, dict):
            continue
        source_term = decision.get("source_term") if isinstance(decision.get("source_term"), dict) else {}
        source_kind = str(source_term.get("source_kind") or "")
        if source_kind == "field" and source_term.get("source_field_id"):
            index[(source_kind, source_term.get("source_field_id"))] = decision
        if source_kind == "parameter" and source_term.get("source_parameter_id"):
            index[(source_kind, source_term.get("source_parameter_id"))] = decision
    return index


def _binding_suggestion_index(binding_generation: dict[str, Any]) -> dict[tuple[str, Any], dict[str, Any]]:
    index: dict[tuple[str, Any], dict[str, Any]] = {}
    for suggestion in binding_generation.get("suggestions", []):
        if not isinstance(suggestion, dict):
            continue
        source_kind = str(suggestion.get("source_kind") or "")
        if source_kind == "field" and suggestion.get("source_field_id"):
            index[(source_kind, suggestion.get("source_field_id"))] = suggestion
        if source_kind == "parameter" and suggestion.get("source_parameter_id"):
            index[(source_kind, suggestion.get("source_parameter_id"))] = suggestion
    return index


def create_evidence_snapshot(
    repo: ContextPlatformRepository,
    run: dict[str, Any],
    source: dict[str, Any],
    document: dict[str, Any],
    parsed: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
    capability_generation: dict[str, Any],
    verification_result: dict[str, Any],
) -> dict[str, Any]:
    return repo.create_evidence_snapshot(
        {
            "run_id": run["id"],
            "source_id": source["id"],
            "source_document_id": document["id"],
            "snapshot_type": "source_document_ingestion",
            "content_hash": document.get("content_hash") or "",
            "source_ref": {"document_name": document.get("name"), "document_type": document.get("document_type")},
            "operation_evidence": [
                {
                    "source_operation_id": operation["id"],
                    "method": operation["method"],
                    "path": operation["path"],
                    "endpoint_checks": [
                        item
                        for item in verification_result.get("operation_checks", [])
                        if item.get("source_operation_id") == operation["id"]
                    ],
                }
                for operation in operations
            ],
            "schema_evidence": [
                {"source_field_id": field["id"], "field_path": field["field_path"], "raw_name": field.get("raw_name")}
                for field in document_fields
            ],
            "ai_context": {
                "parser": parsed.get("format"),
                "proposal_builder": "agent_manual",
                "meaning_resolution": {
                    "decision_counts": canonical_reconciliation.get("decision_counts") or {},
                    "term_count": canonical_reconciliation.get("term_count") or 0,
                },
                "canonical_reconciliation": {
                    "decision_counts": canonical_reconciliation.get("decision_counts") or {},
                    "term_count": canonical_reconciliation.get("term_count") or 0,
                },
                "resolution_generation": {
                    "decision_counts": binding_generation.get("decision_counts") or {},
                    "term_count": binding_generation.get("term_count") or 0,
                    "engine": binding_generation.get("engine"),
                },
                "binding_generation": {
                    "decision_counts": binding_generation.get("decision_counts") or {},
                    "term_count": binding_generation.get("term_count") or 0,
                    "engine": binding_generation.get("engine"),
                },
                "capability_contracting": {
                    "decision_counts": capability_generation.get("decision_counts") or {},
                    "operation_count": capability_generation.get("operation_count") or 0,
                    "engine": capability_generation.get("engine"),
                },
                "capability_generation": {
                    "decision_counts": capability_generation.get("decision_counts") or {},
                    "operation_count": capability_generation.get("operation_count") or 0,
                    "engine": capability_generation.get("engine"),
                },
                "operation_verification": verification_result.get("summary") or {},
            },
        }
    )


def create_final_proposal_bundle(
    repo: ContextPlatformRepository,
    run: dict[str, Any],
    source: dict[str, Any],
    document: dict[str, Any],
    proposals: list[dict[str, Any]],
    evidence_snapshot: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
    capability_generation: dict[str, Any],
    verification_result: dict[str, Any],
) -> dict[str, Any]:
    executable = bool(operations)
    return repo.create_proposal_bundle(
        {
            "run_id": run["id"],
            "source_id": source["id"],
            "evidence_snapshot_id": evidence_snapshot["id"],
            "title": f"Review {document.get('name') or 'source document'} ingestion bundle",
            "status": "proposed",
            "summary": {
                "source_document_id": document["id"],
                "document_type": document.get("document_type"),
                "operation_count": len(operations),
                "document_field_count": len(document_fields),
                "proposal_count": len(proposals),
                "meaning_decision_counts": canonical_reconciliation.get("decision_counts") or {},
                "meaning_term_count": canonical_reconciliation.get("term_count") or 0,
                "canonical_decision_counts": canonical_reconciliation.get("decision_counts") or {},
                "canonical_term_count": canonical_reconciliation.get("term_count") or 0,
                "relation_decision_counts": canonical_reconciliation.get("relation_decision_counts") or {},
                "relation_suggestion_count": len(canonical_reconciliation.get("relation_suggestions") or []),
                "resolution_decision_counts": binding_generation.get("decision_counts") or {},
                "resolution_term_count": binding_generation.get("term_count") or 0,
                "binding_decision_counts": binding_generation.get("decision_counts") or {},
                "binding_term_count": binding_generation.get("term_count") or 0,
                "capability_contracting_decision_counts": capability_generation.get("decision_counts") or {},
                "capability_contract_count": capability_generation.get("operation_count") or 0,
                "capability_decision_counts": capability_generation.get("decision_counts") or {},
                "capability_operation_count": capability_generation.get("operation_count") or 0,
                "verification_summary": verification_result.get("summary") or {},
                "executable": executable,
                "execution_note": "Executable operation links can be reviewed." if executable else "No executable source operation was extracted.",
            },
        },
        [proposal["id"] for proposal in proposals],
    )


def _openapi_parameters(path_item: dict[str, Any], operation: dict[str, Any]) -> list[dict[str, Any]]:
    raw_parameters = []
    if isinstance(path_item.get("parameters"), list):
        raw_parameters.extend(path_item["parameters"])
    if isinstance(operation.get("parameters"), list):
        raw_parameters.extend(operation["parameters"])
    parameters: list[dict[str, Any]] = []
    for item in raw_parameters:
        if not isinstance(item, dict):
            continue
        schema = item.get("schema") if isinstance(item.get("schema"), dict) else {}
        parameters.append(
            {
                "name": str(item.get("name") or ""),
                "location": str(item.get("in") or "query"),
                "data_type": str(schema.get("type") or "string"),
                "is_required": bool(item.get("required", False)),
                "description": str(item.get("description") or ""),
                "enum_values": schema.get("enum") if isinstance(schema.get("enum"), list) else [],
            }
        )
    return [item for item in parameters if item["name"]]


def _openapi_response_fields(operation: dict[str, Any]) -> list[dict[str, Any]]:
    responses = operation.get("responses") if isinstance(operation.get("responses"), dict) else {}
    response = responses.get("200") or responses.get("201") or responses.get("default") or {}
    content = response.get("content") if isinstance(response, dict) and isinstance(response.get("content"), dict) else {}
    media = content.get("application/json") or next(iter(content.values()), {})
    schema = media.get("schema") if isinstance(media, dict) and isinstance(media.get("schema"), dict) else {}
    fields: list[dict[str, Any]] = []
    _collect_schema_fields(schema, "$", fields)
    return fields


def _collect_schema_fields(schema: dict[str, Any], path: str, fields: list[dict[str, Any]]) -> None:
    schema_type = schema.get("type")
    if schema_type == "array" and isinstance(schema.get("items"), dict):
        _collect_schema_fields(schema["items"], f"{path}[]", fields)
        return
    properties = schema.get("properties")
    if isinstance(properties, dict):
        required = set(schema.get("required") if isinstance(schema.get("required"), list) else [])
        for name, child in properties.items():
            child_path = f"{path}.{name}" if path != "$" else f"$.{name}"
            if isinstance(child, dict) and child.get("properties"):
                _collect_schema_fields(child, child_path, fields)
            else:
                fields.append(
                    {
                        "field_path": child_path,
                        "raw_name": str(name),
                        "data_type": str(child.get("type") or "object") if isinstance(child, dict) else "object",
                        "is_required": name in required,
                        "description": str(child.get("description") or "") if isinstance(child, dict) else "",
                    }
                )


def _json_document_fields(payload: dict[str, Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    _collect_payload_fields(payload, "$", fields)
    return fields[:200]


def _collect_payload_fields(value: Any, path: str, fields: list[dict[str, Any]]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path != "$" else f"$.{key}"
            if isinstance(child, dict):
                _collect_payload_fields(child, child_path, fields)
            elif isinstance(child, list) and child and isinstance(child[0], dict):
                _collect_payload_fields(child[0], f"{child_path}[]", fields)
            else:
                fields.append(
                    {
                        "field_path": child_path,
                        "raw_name": str(key),
                        "display_name": str(key),
                        "data_type": _infer_data_type(child),
                        "description": "",
                    }
                )


def _text_fields(text: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        delimiter = ":" if ":" in stripped else "," if "," in stripped else "\t" if "\t" in stripped else ""
        if delimiter:
            name, _, description = stripped.partition(delimiter)
        else:
            parts = stripped.split()
            name = parts[0] if parts else ""
            description = " ".join(parts[1:])
        name = name.strip()
        if not name or len(name) > 120:
            continue
        fields.append(
            {
                "field_path": name,
                "raw_name": name,
                "display_name": name,
                "data_type": "string",
                "description": description.strip(),
            }
        )
        if len(fields) >= 200:
            break
    return fields


def _infer_data_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if value is None:
        return "unknown"
    return "string"
