from __future__ import annotations

import hashlib
import json
import re
from typing import Any


HTTP_METHOD_PATH_RE = re.compile(r"\b(?P<method>GET|POST|PUT|DELETE|PATCH)\s+(?P<path>/[^\s|]+)", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s|<>\"]+")
OPERATION_TOKEN_RE = re.compile(r"\b(?P<operation>(?:get|search|list|read|find|check|validate)[A-Za-z0-9_]{4,})\b", re.IGNORECASE)
PATH_OPERATION_RE = re.compile(r"/(?P<operation>(?:get|search|list|read|find|check|validate)[A-Za-z0-9_]{4,})\b", re.IGNORECASE)
CONTROL_VALUE_RE = re.compile(r"(?P<value>[A-Za-z0-9_-]{1,20})\s*[:=]\s*(?P<label>[^,;/|]{1,80})")


def extract_blocks(text: str) -> list[dict[str, Any]]:
    json_blocks = _json_document_blocks(text)
    if json_blocks:
        return json_blocks
    blocks = []
    for index, line in enumerate(text.splitlines()):
        content = line.strip()
        if not content:
            continue
        blocks.append(
            {
                "id": f"block.{index:05d}",
                "index": index,
                "kind": _block_kind(content),
                "text": content,
            }
        )
    return blocks


def detect_api_sections(blocks: list[dict[str, Any]], document_id: str) -> list[dict[str, Any]]:
    candidates = _operation_candidates(blocks)
    if not candidates:
        return [_fallback_section(blocks, document_id)] if blocks else []
    sections = []
    seen: set[str] = set()
    for index, candidate in enumerate(candidates):
        operation = candidate["operation"]
        if operation in seen:
            continue
        seen.add(operation)
        start = max(0, candidate["block_index"] - 20)
        next_block = candidates[index + 1]["block_index"] if index + 1 < len(candidates) else candidate["block_index"] + 220
        end = min(len(blocks), max(candidate["block_index"] + 60, next_block))
        section_blocks = [block for block in blocks if start <= int(block["index"]) < end]
        text = "\n".join(block["text"] for block in section_blocks)
        sections.append(
            {
                "id": f"section.{document_id}.{len(sections):03d}",
                "section_index": len(sections),
                "title": f"{candidate.get('method', 'GET')} /{operation}",
                "method": candidate.get("method") or "GET",
                "path": candidate.get("path") or f"/{operation}",
                "operation_name": operation,
                "score": candidate["score"],
                "block_start": start,
                "block_end": end,
                "text": text[:16000],
                "evidence": {
                    "detector": candidate["detector"],
                    "source_block_id": candidate["block_id"],
                    "source_text": candidate["source_text"],
                    "signature": hashlib.sha256(text.encode("utf-8")).hexdigest()[:12],
                },
            }
        )
    return sections


def extract_structured_evidence(
    blocks: list[dict[str, Any]],
    sections: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "operation_candidates": [_operation_candidate_view(section) for section in sections],
        "field_table_candidates": _field_table_candidates(blocks, sections),
        "example_candidates": _example_candidates(blocks, sections),
        "control_field_candidates": _control_field_candidates(blocks, sections),
    }


def sections_to_chunks(sections: list[dict[str, Any]], document_id: str) -> list[dict[str, Any]]:
    if not sections:
        return []
    chunks = []
    for section in sections:
        chunks.append(
            {
                "id": f"chunk.{document_id}.{int(section['section_index']):03d}",
                "chunk_index": int(section["section_index"]),
                "title": section.get("title") or "api section",
                "text": section.get("text", "")[:16000],
                "evidence": {
                    "chunker": "generic_api_section",
                    "method": section.get("method"),
                    "path": section.get("path"),
                    "operation_name": section.get("operation_name"),
                    **(section.get("evidence") or {}),
                },
            }
        )
    return chunks


def _block_kind(text: str) -> str:
    lowered = text.lower()
    if (text.startswith("|") and text.endswith("|")) or "\t" in text:
        return "table_row"
    if URL_RE.search(text):
        return "url"
    if lowered.startswith(("{", "[", "<response", "<request", "<?xml")):
        return "example"
    if len(text) <= 80 and not any(token in text for token in ".:;|"):
        return "heading"
    return "paragraph"


def _json_document_blocks(text: str) -> list[dict[str, Any]]:
    stripped = text.strip()
    if not stripped.startswith(("{", "[")):
        return []
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return []
    blocks: list[dict[str, Any]] = []
    _append_json_blocks(payload, blocks, "$")
    return blocks


def _append_json_blocks(value: Any, blocks: list[dict[str, Any]], path: str) -> None:
    if isinstance(value, dict):
        if "swagger" in value or "openapi" in value:
            blocks.append(_block(len(blocks), "heading", f"OpenAPI document {value.get('swagger') or value.get('openapi')}"))
        info = value.get("info")
        if isinstance(info, dict):
            for key in ("title", "description", "version"):
                if info.get(key):
                    blocks.append(_block(len(blocks), "paragraph", f"info.{key}: {info[key]}"))
        paths = value.get("paths")
        if isinstance(paths, dict):
            for api_path, path_item in paths.items():
                if not isinstance(path_item, dict):
                    continue
                for method, operation in path_item.items():
                    if str(method).lower() not in {"get", "post", "put", "delete", "patch"} or not isinstance(operation, dict):
                        continue
                    operation_id = operation.get("operationId") or api_path.strip("/").replace("/", "_") or method
                    summary = operation.get("summary") or operation.get("description") or ""
                    blocks.append(_block(len(blocks), "paragraph", f"{str(method).upper()} {api_path} operationId={operation_id} summary={summary}"))
                    parameters = operation.get("parameters")
                    if isinstance(parameters, list):
                        blocks.append(_block(len(blocks), "table_row", "| name | in | required | type | description |"))
                        for parameter in parameters:
                            if not isinstance(parameter, dict):
                                continue
                            blocks.append(
                                _block(
                                    len(blocks),
                                    "table_row",
                                    "| "
                                    + " | ".join(
                                        [
                                            str(parameter.get("name") or ""),
                                            str(parameter.get("in") or ""),
                                            str(parameter.get("required") or ""),
                                            str(parameter.get("type") or parameter.get("schema", {}).get("type") or ""),
                                            str(parameter.get("description") or ""),
                                        ]
                                    )
                                    + " |",
                                )
                            )
        definitions = value.get("definitions") or value.get("components", {}).get("schemas")
        if isinstance(definitions, dict):
            for name, schema in definitions.items():
                blocks.append(_block(len(blocks), "heading", f"schema {name}"))
                if isinstance(schema, dict):
                    _append_schema_properties(schema, blocks, name)
        return
    if isinstance(value, list):
        for index, item in enumerate(value[:200]):
            _append_json_blocks(item, blocks, f"{path}[{index}]")


def _append_schema_properties(schema: dict[str, Any], blocks: list[dict[str, Any]], schema_name: str) -> None:
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    blocks.append(_block(len(blocks), "table_row", "| name | type | required | description |"))
    required = set(schema.get("required") if isinstance(schema.get("required"), list) else [])
    for name, prop in properties.items():
        if not isinstance(prop, dict):
            continue
        blocks.append(
            _block(
                len(blocks),
                "table_row",
                "| "
                + " | ".join(
                    [
                        str(name),
                        str(prop.get("type") or ""),
                        "true" if name in required else "false",
                        str(prop.get("description") or prop.get("title") or schema_name),
                    ]
                )
                + " |",
            )
        )


def _block(index: int, kind: str, text: str) -> dict[str, Any]:
    return {"id": f"block.{index:05d}", "index": index, "kind": kind, "text": text}


def _operation_candidates(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = []
    for block in blocks:
        text = str(block.get("text") or "")
        method_match = HTTP_METHOD_PATH_RE.search(text)
        if method_match:
            candidates.append(
                {
                    "operation": _operation_from_path(method_match.group("path")),
                    "method": method_match.group("method").upper(),
                    "path": method_match.group("path"),
                    "block_index": int(block["index"]),
                    "block_id": block["id"],
                    "source_text": text,
                    "score": 95,
                    "detector": "http_method_path",
                }
            )
            continue
        url_match = URL_RE.search(text)
        if url_match:
            path_match = PATH_OPERATION_RE.search(url_match.group(0))
            if path_match:
                operation = path_match.group("operation")
                candidates.append(
                    {
                        "operation": operation,
                        "method": "GET",
                        "path": f"/{operation}",
                        "block_index": int(block["index"]),
                        "block_id": block["id"],
                        "source_text": text,
                        "score": 90,
                        "detector": "url_path_operation",
                    }
                )
                continue
        token_match = OPERATION_TOKEN_RE.search(text)
        if token_match and _operation_context_score(blocks, int(block["index"]), text) > 0:
            operation = token_match.group("operation")
            candidates.append(
                {
                    "operation": operation,
                    "method": "GET",
                    "path": f"/{operation}",
                    "block_index": int(block["index"]),
                    "block_id": block["id"],
                    "source_text": text,
                    "score": _operation_context_score(blocks, int(block["index"]), text),
                    "detector": "operation_token_with_context",
                }
            )
    return sorted(candidates, key=lambda item: (item["block_index"], -item["score"]))


def _operation_context_score(blocks: list[dict[str, Any]], index: int, text: str) -> int:
    window = " ".join(str(block.get("text") or "") for block in blocks[max(0, index - 8) : index + 8]).lower()
    score = 0
    for token in ("operation", "오퍼레이션", "endpoint", "api", "서비스명", "요청", "응답", "request", "response"):
        if token.lower() in window:
            score += 15
    if text.startswith("|") or "오퍼레이션" in text:
        score += 20
    return min(score, 85)


def _operation_from_path(path: str) -> str:
    match = PATH_OPERATION_RE.search(path)
    if match:
        return match.group("operation")
    return path.strip("/").split("/")[-1] or "operation"


def _fallback_section(blocks: list[dict[str, Any]], document_id: str) -> dict[str, Any]:
    text = "\n".join(str(block.get("text") or "") for block in blocks[:300])
    return {
        "id": f"section.{document_id}.000",
        "section_index": 0,
        "title": "document",
        "method": None,
        "path": None,
        "operation_name": None,
        "score": 0,
        "block_start": 0,
        "block_end": min(len(blocks), 300),
        "text": text[:16000],
        "evidence": {"detector": "fallback_document"},
    }


def _operation_candidate_view(section: dict[str, Any]) -> dict[str, Any]:
    return {
        "section_id": section.get("id"),
        "operation_name": section.get("operation_name"),
        "method": section.get("method"),
        "path": section.get("path"),
        "title": section.get("title"),
        "score": section.get("score"),
        "evidence": section.get("evidence", {}),
    }


def _field_table_candidates(blocks: list[dict[str, Any]], sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = []
    for section in sections:
        rows = [
            block
            for block in blocks
            if section["block_start"] <= int(block["index"]) < section["block_end"] and block.get("kind") == "table_row"
        ]
        grouped = _group_table_rows(rows)
        for table_index, table_rows in enumerate(grouped):
            direction = _table_direction(table_rows)
            candidates.append(
                {
                    "section_id": section["id"],
                    "operation_name": section.get("operation_name"),
                    "table_index": table_index,
                    "direction_hint": direction,
                    "row_count": len(table_rows),
                    "rows": [_table_cells(str(row["text"])) for row in table_rows[:80]],
                    "evidence": {
                        "block_start": table_rows[0]["id"] if table_rows else None,
                        "block_end": table_rows[-1]["id"] if table_rows else None,
                    },
                }
            )
    return candidates


def _group_table_rows(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    previous_index = -10
    for row in rows:
        index = int(row["index"])
        if current and index - previous_index > 3:
            groups.append(current)
            current = []
        current.append(row)
        previous_index = index
    if current:
        groups.append(current)
    return groups


def _table_direction(rows: list[dict[str, Any]]) -> str:
    text = "\n".join(str(row.get("text") or "") for row in rows[:5]).lower()
    if any(token in text for token in ("request", "요청", "parameter", "param", "입력")):
        return "request"
    if any(token in text for token in ("response", "응답", "result", "출력")):
        return "response"
    return "unknown"


def _table_cells(row: str) -> list[str]:
    if "\t" in row and not row.strip().startswith("|"):
        return [cell.strip() for cell in row.split("\t")]
    return [cell.strip() for cell in row.strip().strip("|").split("|")]


def _example_candidates(blocks: list[dict[str, Any]], sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    examples = []
    for section in sections:
        for block in blocks:
            if not (section["block_start"] <= int(block["index"]) < section["block_end"]):
                continue
            text = str(block.get("text") or "")
            if block.get("kind") in {"url", "example"} or URL_RE.search(text):
                examples.append(
                    {
                        "section_id": section["id"],
                        "operation_name": section.get("operation_name"),
                        "kind": block.get("kind"),
                        "text": text[:4000],
                        "evidence": {"block_id": block["id"]},
                    }
                )
    return examples[:500]


def _control_field_candidates(blocks: list[dict[str, Any]], sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    controls = []
    for section in sections:
        for block in blocks:
            if not (section["block_start"] <= int(block["index"]) < section["block_end"]):
                continue
            text = str(block.get("text") or "")
            values = [
                {"value": match.group("value"), "label": match.group("label").strip()}
                for match in CONTROL_VALUE_RE.finditer(text)
            ]
            if len(values) >= 2 or any(token in text for token in ("구분", "선택", "조건", "필수", "enum", "code")):
                controls.append(
                    {
                        "section_id": section["id"],
                        "operation_name": section.get("operation_name"),
                        "text": text[:1000],
                        "values": values[:20],
                        "evidence": {"block_id": block["id"]},
                    }
                )
    return controls[:500]
