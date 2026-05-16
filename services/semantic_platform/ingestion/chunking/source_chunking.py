from __future__ import annotations

import hashlib
import json
import re
from typing import Any

def _split_source_chunks(text: str, document_id: str, provider: str) -> list[dict[str, Any]]:
    normalized_text = text.strip()
    if not normalized_text:
        return []

    chunks: list[dict[str, Any]] = []
    overview_end = min(len(text), 4000)
    chunks.append(
        _make_source_chunk(
            document_id=document_id,
            provider=provider,
            chunk_order=0,
            chunk_type="service_overview",
            start=0,
            end=overview_end,
            text=text[:overview_end],
            title_ko="문서 개요",
            operation_id=None,
        )
    )

    openapi_chunks = _openapi_operation_chunks(text, document_id, provider, start_order=1)
    if openapi_chunks:
        chunks.extend(openapi_chunks)
        return _dedupe_source_chunks(chunks)

    operation_chunks = _operation_section_chunks(text, document_id, provider, start_order=1)
    if operation_chunks:
        chunks.extend(operation_chunks)
        return _dedupe_source_chunks(chunks)

    parameter_operation_chunks = _query_parameter_operation_chunks(text, document_id, provider, start_order=1)
    if parameter_operation_chunks:
        chunks.extend(parameter_operation_chunks)
        return _dedupe_source_chunks(chunks)

    endpoint_chunks = _endpoint_section_chunks(text, document_id, provider, start_order=1)
    if endpoint_chunks:
        chunks.extend(endpoint_chunks)
        return _dedupe_source_chunks(chunks)

    operation_id_chunks = _operation_id_window_chunks(text, document_id, provider, start_order=1)
    if operation_id_chunks:
        chunks.extend(operation_id_chunks)
        return _dedupe_source_chunks(chunks)

    schema_chunks = _schema_signal_window_chunks(text, document_id, provider, start_order=1)
    chunks.extend(schema_chunks)
    return _dedupe_source_chunks(chunks)


def _openapi_operation_chunks(
    text: str,
    document_id: str,
    provider: str,
    start_order: int,
) -> list[dict[str, Any]]:
    document = _loads_json_lenient(text)
    if not isinstance(document, dict):
        return []
    if not isinstance(document, dict) or not isinstance(document.get("paths"), dict):
        return []

    definitions = document.get("definitions", {})
    if not isinstance(definitions, dict):
        definitions = {}
    base = {
        "swagger": document.get("swagger"),
        "openapi": document.get("openapi"),
        "info": document.get("info", {}),
        "host": document.get("host"),
        "basePath": document.get("basePath"),
        "schemes": document.get("schemes", []),
        "securityDefinitions": document.get("securityDefinitions", {}),
    }
    chunks = []
    for path, methods in document["paths"].items():
        if not isinstance(methods, dict):
            continue
        for method, operation in methods.items():
            if method.lower() not in {"get", "post", "put", "delete", "patch"}:
                continue
            if not isinstance(operation, dict):
                continue
            operation_id = operation.get("operationId") or f"{method}_{path.strip('/').replace('/', '_')}"
            operation_document = {
                **base,
                "path": path,
                "method": method.upper(),
                "operation": operation,
                "referenced_definitions": _collect_openapi_references(operation, definitions),
            }
            chunk_text = json.dumps(operation_document, ensure_ascii=False, indent=2, sort_keys=True)
            start = _find_openapi_path_offset(text, path)
            end = _find_next_openapi_path_offset(text, start)
            title = _openapi_operation_title(operation, path)
            chunks.append(
                _make_source_chunk(
                    document_id=document_id,
                    provider=provider,
                    chunk_order=start_order + len(chunks),
                    chunk_type="openapi_operation",
                    start=start,
                    end=end,
                    text=chunk_text,
                    title_ko=title,
                    operation_id=str(operation_id),
                    max_chars=50000,
                )
            )
    return chunks


def _loads_json_lenient(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        stripped = text.strip()
        open_braces = stripped.count("{")
        close_braces = stripped.count("}")
        open_brackets = stripped.count("[")
        close_brackets = stripped.count("]")
        if open_braces < close_braces or open_brackets < close_brackets:
            return None
        repaired = stripped + ("]" * (open_brackets - close_brackets)) + ("}" * (open_braces - close_braces))
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            return None


def _collect_openapi_references(value: Any, definitions: dict[str, Any], limit: int = 30) -> dict[str, Any]:
    collected: dict[str, Any] = {}

    def visit(node: Any) -> None:
        if len(collected) >= limit:
            return
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/definitions/"):
                name = ref.rsplit("/", 1)[-1]
                if name not in collected and name in definitions:
                    collected[name] = definitions[name]
                    visit(definitions[name])
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    return collected


def _find_openapi_path_offset(text: str, path: str) -> int:
    match = re.search(rf'"{re.escape(path)}"\s*:', text)
    return match.start() if match else 0


def _find_next_openapi_path_offset(text: str, start: int) -> int:
    match = re.search(r'\n\s*"/[^"]+"\s*:\s*\{', text[start + 1 :])
    return start + 1 + match.start() if match else len(text)


def _openapi_operation_title(operation: dict[str, Any], path: str) -> str | None:
    tags = operation.get("tags")
    if isinstance(tags, list) and tags:
        return str(tags[0])
    summary = operation.get("summary")
    if summary:
        return str(summary)
    return _title_for_endpoint_path(path, json.dumps(operation, ensure_ascii=False))


def _operation_section_chunks(
    text: str,
    document_id: str,
    provider: str,
    start_order: int,
) -> list[dict[str, Any]]:
    lines = _line_spans(text)
    marker_indexes = [
        index
        for index, (_, _, line) in enumerate(lines)
        if "오퍼레이션" in line and "명세" in line
    ]
    if not marker_indexes:
        return []

    section_starts = [_section_start_from_marker(lines, line_index) for line_index in marker_indexes]
    chunks = []
    for offset, line_index in enumerate(marker_indexes):
        start = section_starts[offset]
        end = section_starts[offset + 1] if offset + 1 < len(section_starts) else len(text)
        section = text[start:end].strip()
        if len(section) < 120:
            continue
        title_ko = _extract_korean_operation_title(section) or _extract_nearby_title(lines, line_index)
        operation_id = _extract_operation_id(section) or _extract_endpoint_operation_id(section)
        chunks.append(
            _make_source_chunk(
                document_id=document_id,
                provider=provider,
                chunk_order=start_order + len(chunks),
                chunk_type="operation_section",
                start=start,
                end=end,
                text=section,
                title_ko=title_ko,
                operation_id=operation_id,
            )
        )
    return chunks


def _endpoint_section_chunks(
    text: str,
    document_id: str,
    provider: str,
    start_order: int,
) -> list[dict[str, Any]]:
    lines = _line_spans(text)
    explicit_path_chunks = _explicit_path_chunks(text, document_id, provider, start_order)
    if explicit_path_chunks:
        return explicit_path_chunks

    endpoint_indexes = [
        index
        for index, (_, _, line) in enumerate(lines)
        if _looks_like_endpoint_line(line)
    ]
    if not endpoint_indexes:
        return []

    chunks = []
    for offset, line_index in enumerate(endpoint_indexes):
        start = max(0, lines[line_index][0] - 1200)
        next_line_index = endpoint_indexes[offset + 1] if offset + 1 < len(endpoint_indexes) else len(lines)
        end = lines[next_line_index][0] if next_line_index < len(lines) else min(len(text), lines[line_index][1] + 7000)
        section = text[start:end].strip()
        if len(section) < 80:
            continue
        operation_id = _extract_operation_id(section) or _extract_endpoint_operation_id(section)
        title_ko = _extract_nearby_title(lines, line_index)
        chunks.append(
            _make_source_chunk(
                document_id=document_id,
                provider=provider,
                chunk_order=start_order + len(chunks),
                chunk_type="endpoint_section",
                start=start,
                end=end,
                text=section,
                title_ko=title_ko,
                operation_id=operation_id,
            )
        )
    return chunks


def _query_parameter_operation_chunks(
    text: str,
    document_id: str,
    provider: str,
    start_order: int,
) -> list[dict[str, Any]]:
    operations = _extract_query_parameter_operations(text)
    if not operations:
        return []

    chunks = []
    for operation_id, title in operations.items():
        section = _section_for_parameter_operation(text, operation_id)
        if len(section.strip()) < 120:
            continue
        start = text.find(section[:80].strip()) if section[:80].strip() else 0
        if start < 0:
            start = 0
        chunks.append(
            _make_source_chunk(
                document_id=document_id,
                provider=provider,
                chunk_order=start_order + len(chunks),
                chunk_type="query_parameter_operation",
                start=start,
                end=min(len(text), start + len(section)),
                text=section,
                title_ko=title,
                operation_id=operation_id,
            )
        )
    return chunks


def _extract_query_parameter_operations(text: str) -> dict[str, str]:
    operations: dict[str, str] = {}
    lines = [line.strip() for line in text.splitlines()]

    in_operation_table = False
    for line in lines:
        if line == "오퍼레이션":
            in_operation_table = True
            continue
        if in_operation_table and line.startswith("요청파라미터"):
            in_operation_table = False
        if not in_operation_table:
            continue
        match = re.match(r"^([A-Z][A-Za-z0-9_]{2,})\s+(.+)$", line)
        if match and not match.group(1).endswith("Service"):
            operations[match.group(1)] = match.group(2).strip()

    for match in re.finditer(r"[?&]request=([A-Za-z][A-Za-z0-9_]+)", text):
        operation_id = match.group(1)
        if not operation_id.endswith("Service"):
            operations.setdefault(operation_id, _title_from_operation_id(operation_id))

    for line in lines:
        if not line.startswith("request"):
            continue
        for value in re.findall(r"\b([A-Z][A-Za-z0-9_]{2,})\b", line):
            if value not in {"Request", "REQUEST"} and not value.endswith("Service"):
                operations.setdefault(value, _title_from_operation_id(value))

    return operations


def _section_for_parameter_operation(text: str, operation_id: str) -> str:
    operation_pos = text.find(operation_id)
    request_params_pos = text.find("요청파라미터")
    response_pos = text.find("응답결과")
    error_pos = text.find("오류 응답결과")
    start = 0 if operation_pos < 0 else max(0, operation_pos - 1200)
    end_candidates = [pos for pos in (error_pos,) if pos > 0]
    end = min(end_candidates) if end_candidates else len(text)
    if request_params_pos > 0:
        start = min(start, request_params_pos)
    if response_pos > 0:
        end = max(end, min(len(text), response_pos + 3000))
    return text[start:end]


def _title_from_operation_id(operation_id: str) -> str:
    words = re.sub(r"(?<!^)(?=[A-Z])", " ", operation_id).strip()
    return words or operation_id


def _operation_id_window_chunks(
    text: str,
    document_id: str,
    provider: str,
    start_order: int,
) -> list[dict[str, Any]]:
    operation_ids = []
    for match in re.finditer(r"\b(get|list|search|fetch|validate|check)[A-Z][A-Za-z0-9]{2,}\b", text):
        operation_id = match.group(0)
        if operation_id not in operation_ids:
            operation_ids.append(operation_id)
    chunks = []
    used_ranges: list[tuple[int, int]] = []
    for operation_id in operation_ids[:80]:
        matches = list(re.finditer(rf"\b{re.escape(operation_id)}\b", text))
        if not matches:
            continue
        match = matches[1] if len(matches) > 1 else matches[0]
        start = max(0, match.start() - 1800)
        end = min(len(text), match.end() + 7000)
        if _range_overlaps(start, end, used_ranges):
            continue
        used_ranges.append((start, end))
        section = text[start:end].strip()
        if len(section) < 120:
            continue
        chunks.append(
            _make_source_chunk(
                document_id=document_id,
                provider=provider,
                chunk_order=start_order + len(chunks),
                chunk_type="operation_id_window",
                start=start,
                end=end,
                text=section,
                title_ko=_extract_title_from_text(section),
                operation_id=operation_id,
            )
        )
    return chunks


def _explicit_path_chunks(
    text: str,
    document_id: str,
    provider: str,
    start_order: int,
) -> list[dict[str, Any]]:
    path_matches = []
    for match in re.finditer(r"https?://[^\s\"']+/(?P<path>[A-Za-z0-9_-]+)\?[^ \n\"']*", text):
        path = "/" + match.group("path")
        if path.lower() in {"/api-docs"}:
            continue
        path_matches.append((path, match.start()))
    if len({path for path, _ in path_matches}) < 2:
        return []

    chunks = []
    seen_paths = set()
    for path, position in path_matches:
        if path in seen_paths:
            continue
        seen_paths.add(path)
        start = _semantic_section_start_for_endpoint(text, path, position)
        end = _semantic_section_end_for_endpoint(text, path, position)
        section = text[start:end].strip()
        if len(section) < 120:
            continue
        chunks.append(
            _make_source_chunk(
                document_id=document_id,
                provider=provider,
                chunk_order=start_order + len(chunks),
                chunk_type="endpoint_path_section",
                start=start,
                end=end,
                text=section,
                title_ko=_title_for_endpoint_path(path, section),
                operation_id=path.strip("/"),
            )
        )
    return chunks


def _schema_signal_window_chunks(
    text: str,
    document_id: str,
    provider: str,
    start_order: int,
) -> list[dict[str, Any]]:
    signals = ("요청 메시지", "응답 메시지", "요청항목", "응답항목", "REST", "URI", "request", "response")
    chunks = []
    used_ranges: list[tuple[int, int]] = []
    for signal in signals:
        for match in re.finditer(re.escape(signal), text, flags=re.IGNORECASE):
            start = max(0, match.start() - 1800)
            end = min(len(text), match.end() + 7000)
            if _range_overlaps(start, end, used_ranges):
                continue
            used_ranges.append((start, end))
            section = text[start:end].strip()
            if len(section) < 120:
                continue
            chunks.append(
                _make_source_chunk(
                    document_id=document_id,
                    provider=provider,
                    chunk_order=start_order + len(chunks),
                    chunk_type="schema_window",
                    start=start,
                    end=end,
                    text=section,
                    title_ko=_extract_title_from_text(section),
                    operation_id=_extract_operation_id(section),
                )
            )
            if len(chunks) >= 40:
                return chunks
    return chunks


def _make_source_chunk(
    document_id: str,
    provider: str,
    chunk_order: int,
    chunk_type: str,
    start: int,
    end: int,
    text: str,
    title_ko: str | None,
    operation_id: str | None,
    max_chars: int = 12000,
) -> dict[str, Any]:
    chunk_text = _clip_chunk_text(text, max_chars=max_chars)
    chunk_id_basis = f"{document_id}:{chunk_type}:{operation_id or ''}:{start}:{end}"
    return {
        "document_id": document_id,
        "provider_hint": provider if provider != "unknown" else None,
        "chunk_id": hashlib.sha256(chunk_id_basis.encode("utf-8")).hexdigest()[:16],
        "chunk_order": chunk_order,
        "chunk_type": chunk_type,
        "operation_id": operation_id,
        "title_ko": title_ko,
        "service_id": _extract_service_id(text),
        "start_char": start,
        "end_char": end,
        "text_hash": hashlib.sha256(chunk_text.encode("utf-8")).hexdigest(),
        "text": chunk_text,
        "signals": _chunk_signals(chunk_text),
    }


def _line_spans(text: str) -> list[tuple[int, int, str]]:
    spans = []
    position = 0
    for line in text.splitlines(keepends=True):
        start = position
        end = position + len(line)
        spans.append((start, end, line.strip()))
        position = end
    if not spans:
        spans.append((0, len(text), text.strip()))
    return spans


def _section_start_from_marker(lines: list[tuple[int, int, str]], marker_index: int) -> int:
    for index in range(marker_index, max(-1, marker_index - 8), -1):
        line = lines[index][2]
        if re.search(r"^\s*(\d+(\.\d+){0,3}|[가-힣A-Za-z0-9_]{3,})", line):
            return lines[index][0]
    return lines[marker_index][0]


def _extract_nearby_title(lines: list[tuple[int, int, str]], marker_index: int) -> str | None:
    for index in range(marker_index, max(-1, marker_index - 8), -1):
        line = lines[index][2].strip("[] ")
        if not line or len(line) > 100:
            continue
        if "명세" in line and len(line) <= 20:
            continue
        if re.search(r"[가-힣]", line):
            return line
    return None


def _extract_korean_operation_title(section: str) -> str | None:
    lines = [line.strip() for line in section.splitlines()]
    for index, line in enumerate(lines):
        if "오퍼레이션명" not in line or "국문" not in line:
            continue
        values = []
        for candidate in lines[index + 1 : index + 8]:
            if not candidate:
                continue
            if "오퍼레이션" in candidate or "유형" in candidate or "영문" in candidate:
                break
            if re.fullmatch(r"[\[\]()./ -]+", candidate):
                continue
            values.append(candidate)
        title = "".join(values).strip("[] ")
        if title and re.search(r"[가-힣]", title):
            return title[:120]
    return None


def _extract_title_from_text(text: str) -> str | None:
    for line in text.splitlines()[:16]:
        value = line.strip("[] ")
        if 2 <= len(value) <= 100 and re.search(r"[가-힣]", value):
            return value
    return None


def _extract_operation_id(text: str) -> str | None:
    compact_text = re.sub(r"(?<=[A-Za-z])\s+(?=[A-Za-z])", "", text)
    direct_patterns = [
        r"오퍼레이션\s*명\s*\(?영문\)?\s*[:：]?\s*([A-Za-z][A-Za-z0-9_]{3,})",
        r"operation\s*id\s*[:：]?\s*([A-Za-z][A-Za-z0-9_]{3,})",
    ]
    for pattern in direct_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        compact_match = re.search(pattern, compact_text, flags=re.IGNORECASE)
        candidates = [candidate.group(1) for candidate in (match, compact_match) if candidate]
        if candidates:
            return max(candidates, key=len)
    match = re.search(r"\b(?:get|list|search|fetch|validate|check)[A-Z][A-Za-z0-9]{2,}\b", text)
    compact_match = re.search(r"\b(?:get|list|search|fetch|validate|check)[A-Z][A-Za-z0-9]{2,}\b", compact_text)
    if compact_match and (not match or len(compact_match.group(0)) > len(match.group(0))):
        return compact_match.group(0)
    if match:
        return match.group(0)
    return None


def _extract_endpoint_operation_id(text: str) -> str | None:
    request_match = re.search(r"[?&]request=([A-Za-z][A-Za-z0-9_]+)", text)
    if request_match:
        return request_match.group(1)
    operation_match = re.search(r"[?&]operation=([A-Za-z][A-Za-z0-9_]+)", text)
    if operation_match:
        return operation_match.group(1)
    url_match = re.search(r"https?://[^\s\"']+/([A-Za-z][A-Za-z0-9_-]+)(?:\?|[\s\"']|$)", text)
    if url_match:
        candidate = url_match.group(1)
        if candidate.lower() not in {"api", "data", "api-docs"} and not candidate.endswith("Service"):
            return candidate
    return None


def _extract_service_id(text: str) -> str | None:
    patterns = [
        r"서비스\s*ID\s*[:：]?\s*([A-Za-z][A-Za-z0-9_]{3,})",
        r"\b([A-Za-z][A-Za-z0-9_]*Service)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def _looks_like_endpoint_line(line: str) -> bool:
    stripped = line.strip()
    if re.search(r"(?i)\b(GET|POST|PUT|DELETE|PATCH)\s+/[A-Za-z0-9_/{}/.-]+", stripped):
        return True
    if re.search(r"https?://[^\s\"']+/[A-Za-z0-9_./{}-]+", stripped):
        return True
    if re.search(r"/[A-Za-z0-9_./{}-]+\?(?:[^ \t]+)", stripped):
        return True
    return bool(re.fullmatch(r"/[A-Za-z0-9][A-Za-z0-9_/{}/.-]*", stripped))


def _semantic_section_start_for_endpoint(text: str, path: str, position: int) -> int:
    if path == "/validate":
        markers = [match.start() for match in re.finditer("진위확인", text)]
        return min(markers) if markers else max(0, position - 1200)
    if path == "/status":
        markers = [match.start() for match in re.finditer("상태조회", text) if match.start() <= position]
        return max(markers) if markers else max(0, position - 1200)
    return max(0, position - 1200)


def _semantic_section_end_for_endpoint(text: str, path: str, position: int) -> int:
    if path == "/validate":
        next_status = text.find("상태조회", position)
        return next_status if next_status > 0 else min(len(text), position + 5000)
    return min(len(text), position + 5000)


def _title_for_endpoint_path(path: str, section: str) -> str | None:
    known_titles = {
        "/validate": "사업자등록정보 진위확인",
        "/status": "사업자등록 상태조회",
    }
    if path in known_titles:
        return known_titles[path]
    return _extract_title_from_text(section)


def _chunk_signals(text: str) -> list[str]:
    signals = []
    checks = {
        "request_schema": ("요청", "request"),
        "response_schema": ("응답", "response"),
        "endpoint": ("REST", "URI", "GET ", "POST ", "/"),
        "auth": ("인증", "serviceKey", "Authorization"),
        "pagination": ("pageNo", "numOfRows", "페이지"),
        "example": ("예제", "샘플", "example"),
    }
    lower_text = text.lower()
    for name, candidates in checks.items():
        if any(candidate.lower() in lower_text for candidate in candidates):
            signals.append(name)
    return signals


def _clip_chunk_text(text: str, max_chars: int = 12000) -> str:
    compacted = re.sub(r"\n{3,}", "\n\n", text.strip())
    return compacted[:max_chars]


def _range_overlaps(start: int, end: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start < existing_end and end > existing_start for existing_start, existing_end in ranges)


def _dedupe_source_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_operation: dict[str, dict[str, Any]] = {}
    for chunk in chunks:
        operation_id = chunk.get("operation_id")
        if not operation_id or chunk.get("chunk_type") == "service_overview":
            continue
        current = best_by_operation.get(operation_id)
        if current is None or _chunk_quality_score(chunk) > _chunk_quality_score(current):
            best_by_operation[operation_id] = chunk

    operation_ids_seen = set()
    operation_filtered = []
    for chunk in chunks:
        operation_id = chunk.get("operation_id")
        if not operation_id or chunk.get("chunk_type") == "service_overview":
            operation_filtered.append(chunk)
            continue
        if operation_id in operation_ids_seen:
            continue
        operation_ids_seen.add(operation_id)
        operation_filtered.append(best_by_operation.get(operation_id, chunk))

    deduped = []
    seen_hashes = set()
    for chunk in operation_filtered:
        text_hash = (chunk["text_hash"], chunk.get("operation_id"))
        if text_hash in seen_hashes:
            continue
        seen_hashes.add(text_hash)
        chunk["chunk_order"] = len(deduped)
        deduped.append(chunk)
    return deduped


def _chunk_quality_score(chunk: dict[str, Any]) -> tuple[int, int, int]:
    signals = set(chunk.get("signals", []))
    schema_score = int("request_schema" in signals) + int("response_schema" in signals)
    text_len = len(chunk.get("text") or "")
    starts_near_heading = 1 if str(chunk.get("title_ko") or "").strip() else 0
    return (schema_score, starts_near_heading, text_len)


def _summarize_source_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "chunk_id": chunk.get("chunk_id"),
            "chunk_order": chunk.get("chunk_order"),
            "chunk_type": chunk.get("chunk_type"),
            "operation_id": chunk.get("operation_id"),
            "title_ko": chunk.get("title_ko"),
            "signals": chunk.get("signals", []),
        }
        for chunk in chunks[:80]
    ]


def _compact_source_chunks_for_prompt(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted = []
    for chunk in chunks[:40]:
        compacted.append(
            {
                "chunk_id": chunk.get("chunk_id"),
                "chunk_order": chunk.get("chunk_order"),
                "chunk_type": chunk.get("chunk_type"),
                "operation_id": chunk.get("operation_id"),
                "title_ko": chunk.get("title_ko"),
                "service_id": chunk.get("service_id"),
                "signals": chunk.get("signals", []),
                "text_excerpt": (chunk.get("text") or "")[:3500],
            }
        )
    return compacted
