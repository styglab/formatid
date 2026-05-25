from __future__ import annotations

import hashlib
import re
from typing import Any


ENDPOINT_PATTERN = re.compile(
    r"(?P<method>GET|POST|PUT|DELETE|PATCH)\s+(?P<path>/[A-Za-z0-9_./{}?-]+)|(?P<path_only>/[A-Za-z0-9_./{}-]*(?:get|search|list|status|validate|info)[A-Za-z0-9_./{}-]*)",
    re.IGNORECASE,
)
OPERATION_NAME_PATTERN = re.compile(r"오퍼레이션명\(영문\)\s*(?P<operation>get[A-Za-z0-9]+)", re.IGNORECASE)


def chunk_by_operation(text: str, document_id: str) -> list[dict[str, Any]]:
    operation_matches = list(OPERATION_NAME_PATTERN.finditer(text))
    if operation_matches:
        return _chunks_from_operation_names(text, document_id, operation_matches)
    matches = list(ENDPOINT_PATTERN.finditer(text))
    if not matches:
        return [
            {
                "id": f"chunk.{document_id}.000",
                "chunk_index": 0,
                "title": "document",
                "text": text[:12000],
                "evidence": {"chunker": "fallback_document"},
            }
        ]
    chunks = []
    for index, match in enumerate(matches):
        start = max(0, match.start() - 1200)
        end = matches[index + 1].start() if index + 1 < len(matches) else min(len(text), match.end() + 5000)
        segment = text[start:end].strip()
        path = match.group("path") or match.group("path_only") or ""
        method = (match.group("method") or "GET").upper()
        chunks.append(
            {
                "id": f"chunk.{document_id}.{index:03d}",
                "chunk_index": index,
                "title": f"{method} {path}",
                "text": segment[:12000],
                "evidence": {
                    "chunker": "endpoint_pattern",
                    "method": method,
                    "path": path,
                    "signature": hashlib.sha256(segment.encode("utf-8")).hexdigest()[:12],
                },
            }
        )
    return chunks


def _chunks_from_operation_names(text: str, document_id: str, matches: list[re.Match[str]]) -> list[dict[str, Any]]:
    chunks = []
    for index, match in enumerate(matches):
        operation = match.group("operation")
        start = max(0, match.start() - 700)
        end = matches[index + 1].start() if index + 1 < len(matches) else min(len(text), match.end() + 9000)
        segment = text[start:end].strip()
        path = f"/{operation}"
        chunks.append(
            {
                "id": f"chunk.{document_id}.{index:03d}",
                "chunk_index": index,
                "title": f"GET {path}",
                "text": segment[:14000],
                "evidence": {
                    "chunker": "operation_name_heading",
                    "method": "GET",
                    "path": path,
                    "operation_name": operation,
                    "signature": hashlib.sha256(segment.encode("utf-8")).hexdigest()[:12],
                },
            }
        )
    return chunks
