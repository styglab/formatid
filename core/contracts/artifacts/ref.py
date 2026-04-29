from __future__ import annotations

from typing import Any, Literal, TypedDict
from uuid import uuid4


ArtifactKind = Literal[
    "document",
    "document_chunks",
    "table_batch",
    "tool_result",
    "retrieval_result",
]


class ArtifactRef(TypedDict, total=False):
    artifact_id: str
    kind: ArtifactKind
    uri: str
    content_type: str
    size_bytes: int
    metadata: dict[str, Any]


def create_artifact_ref(
    *,
    kind: ArtifactKind,
    uri: str,
    content_type: str,
    metadata: dict[str, Any] | None = None,
    artifact_id: str | None = None,
    size_bytes: int | None = None,
) -> ArtifactRef:
    ref: ArtifactRef = {
        "artifact_id": artifact_id or uuid4().hex,
        "kind": kind,
        "uri": uri,
        "content_type": content_type,
        "metadata": dict(metadata or {}),
    }
    if size_bytes is not None:
        ref["size_bytes"] = size_bytes
    return ref
