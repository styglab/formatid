from __future__ import annotations

import hashlib
import os
from typing import Any

import httpx


def sync_semantic_type_registry(semantic_types: list[dict[str, Any]]) -> None:
    if not semantic_types:
        return
    documents = [_semantic_type_document(item) for item in semantic_types]
    vectors = _embed_texts(documents)
    if not vectors:
        return
    collection = _collection_name()
    _recreate_collection(collection, len(vectors[0]))
    points = []
    for semantic_type, vector, document in zip(semantic_types, vectors, documents, strict=False):
        semantic_type_id = str(semantic_type.get("id") or "").strip()
        if not semantic_type_id:
            continue
        points.append(
            {
                "id": _point_id(semantic_type_id),
                "vector": vector,
                "payload": {
                    "id": semantic_type_id,
                    "name": semantic_type.get("name"),
                    "aliases": semantic_type.get("aliases") or [],
                    "description": semantic_type.get("description") or "",
                    "status": semantic_type.get("status") or "",
                    "document": document,
                },
            }
        )
    if not points:
        return
    _request(
        "PUT",
        f"{_qdrant_url()}/collections/{collection}/points",
        json={"points": points},
    )


def search_semantic_type_candidates(
    *,
    query_text: str,
    semantic_types: list[dict[str, Any]],
    limit: int = 5,
    sync_registry: bool = True,
) -> list[dict[str, Any]]:
    query_text = query_text.strip()
    if not query_text:
        return []
    try:
        if sync_registry:
            sync_semantic_type_registry(semantic_types)
        vectors = _embed_texts([query_text])
        if not vectors:
            return _lexical_fallback(query_text, semantic_types, limit=limit)
        response = _request(
            "POST",
            f"{_qdrant_url()}/collections/{_collection_name()}/points/search",
            json={"vector": vectors[0], "limit": limit, "with_payload": True},
        )
        payload = response.json()
        results = payload.get("result") if isinstance(payload, dict) else None
        candidates: list[dict[str, Any]] = []
        for item in results if isinstance(results, list) else []:
            if not isinstance(item, dict):
                continue
            point_payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
            candidates.append(
                {
                    "semantic_type_id": point_payload.get("id"),
                    "semantic_type_name": point_payload.get("name"),
                    "aliases": point_payload.get("aliases") or [],
                    "description": point_payload.get("description") or "",
                    "status": point_payload.get("status") or "",
                    "document": point_payload.get("document") or "",
                    "score": float(item.get("score") or 0.0),
                    "retrieval_method": "qdrant_embedding",
                }
            )
        return candidates
    except Exception:
        return _lexical_fallback(query_text, semantic_types, limit=limit)


def _lexical_fallback(query_text: str, semantic_types: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    query_tokens = _tokens(query_text)
    scored: list[tuple[float, dict[str, Any]]] = []
    for semantic_type in semantic_types:
        document = _semantic_type_document(semantic_type)
        score = _token_overlap_score(query_tokens, _tokens(document))
        if score <= 0.0:
            continue
        scored.append(
            (
                score,
                {
                    "semantic_type_id": semantic_type.get("id"),
                    "semantic_type_name": semantic_type.get("name"),
                    "aliases": semantic_type.get("aliases") or [],
                    "description": semantic_type.get("description") or "",
                    "status": semantic_type.get("status") or "",
                    "document": document,
                    "score": score,
                    "retrieval_method": "lexical_fallback",
                },
            )
        )
    scored.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in scored[:limit]]


def _semantic_type_document(semantic_type: dict[str, Any]) -> str:
    aliases = semantic_type.get("aliases") if isinstance(semantic_type.get("aliases"), list) else []
    parts = [
        str(semantic_type.get("name") or ""),
        " ".join(str(alias) for alias in aliases),
        str(semantic_type.get("description") or ""),
        str(semantic_type.get("documentation") or ""),
    ]
    return "\n".join(part for part in parts if part.strip())


def _embed_texts(texts: list[str]) -> list[list[float]]:
    response = _request(
        "POST",
        _embedding_url(),
        json={"input": texts, "normalize": True},
    )
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    vectors: list[list[float]] = []
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict):
            continue
        embedding = item.get("embedding")
        if isinstance(embedding, list) and embedding:
            vectors.append([float(value) for value in embedding])
    return vectors


def _recreate_collection(collection: str, vector_size: int) -> None:
    _request("DELETE", f"{_qdrant_url()}/collections/{collection}", allow_404=True)
    _request(
        "PUT",
        f"{_qdrant_url()}/collections/{collection}",
        json={"vectors": {"size": vector_size, "distance": "Cosine"}},
    )


def _request(method: str, url: str, *, json: dict[str, Any] | None = None, allow_404: bool = False) -> httpx.Response:
    timeout = httpx.Timeout(20.0, connect=5.0)
    with httpx.Client(timeout=timeout) as client:
        response = client.request(method, url, json=json)
    if allow_404 and response.status_code == 404:
        return response
    response.raise_for_status()
    return response


def _embedding_url() -> str:
    return os.getenv("CONTEXT_PLATFORM_EMBEDDING_URL", "http://embedding-service:8000/embeddings").rstrip("/")


def _qdrant_url() -> str:
    return os.getenv("CONTEXT_PLATFORM_QDRANT_URL", "http://qdrant:6333").rstrip("/")


def _collection_name() -> str:
    return os.getenv("CONTEXT_PLATFORM_SEMANTIC_TYPE_COLLECTION", "context_platform_semantic_types")


def _tokens(text: str) -> set[str]:
    parts = []
    token = []
    for ch in text.lower():
        if ch.isalnum():
            token.append(ch)
        else:
            if token:
                parts.append("".join(token))
                token = []
    if token:
        parts.append("".join(token))
    return {item for item in parts if item}


def _token_overlap_score(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    overlap = left & right
    if not overlap:
        return 0.0
    return len(overlap) / max(len(left), 1)


def _point_id(value: str) -> int:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]
    return int(digest, 16)
