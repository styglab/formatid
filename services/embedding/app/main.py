from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class EmbeddingRequest(BaseModel):
    input: str | list[str]
    normalize: bool | None = None


app = FastAPI(title="Embedding Service", version="0.1.0")


@app.get("/health/ready")
def ready() -> dict[str, Any]:
    model = _model()
    return {
        "status": "ready",
        "model_path": _model_path(),
        "dimensions": model.get_sentence_embedding_dimension(),
    }


@app.post("/embeddings")
def embeddings(request: EmbeddingRequest) -> dict[str, Any]:
    texts = [request.input] if isinstance(request.input, str) else request.input
    if not texts:
        raise HTTPException(status_code=400, detail="input must not be empty")
    model = _model()
    normalize = request.normalize if request.normalize is not None else _normalize_embeddings()
    vectors = model.encode(
        texts,
        normalize_embeddings=normalize,
        convert_to_numpy=True,
        show_progress_bar=False,
    )
    return {
        "model": os.getenv("EMBEDDING_MODEL_NAME", "BGE-m3-ko"),
        "data": [
            {
                "index": index,
                "embedding": vector.astype(float).tolist(),
            }
            for index, vector in enumerate(vectors)
        ],
    }


@lru_cache(maxsize=1)
def _model() -> Any:
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(_model_path(), device=os.getenv("EMBEDDING_DEVICE", "cpu"))


def _model_path() -> str:
    return os.getenv("EMBEDDING_MODEL_PATH", "/data/models/embeddings/BGE-m3-ko")


def _normalize_embeddings() -> bool:
    return os.getenv("EMBEDDING_NORMALIZE", "true").strip().lower() in {"1", "true", "yes", "on"}
