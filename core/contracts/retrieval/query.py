from __future__ import annotations

from typing import TypedDict


class RetrievalQuery(TypedDict, total=False):
    query_text: str
    collection: str
    top_k: int
    filter: dict


class RetrievedChunk(TypedDict, total=False):
    chunk_id: str
    document_id: str
    text: str
    score: float
    source_uri: str


class RetrievalResult(TypedDict, total=False):
    query_text: str
    collection: str
    chunks: list[RetrievedChunk]
