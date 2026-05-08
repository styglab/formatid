from __future__ import annotations

import hashlib
import os

from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, Field


class EmbeddingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input: str | list[str]
    model: str = "mock-embedding"
    dimensions: int = Field(default=16, gt=0, le=4096)


class EmbeddingData(BaseModel):
    index: int
    embedding: list[float]
    object: str = "embedding"


class EmbeddingResponse(BaseModel):
    object: str = "list"
    model: str
    data: list[EmbeddingData]


app = FastAPI(title="Embedding Service", version="0.1.0")


@app.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def ready() -> dict[str, str]:
    return {"status": "ready", "model": os.getenv("EMBEDDING_MODEL", "mock-embedding")}


@app.post("/v1/embeddings", response_model=EmbeddingResponse)
async def create_embeddings(request: EmbeddingRequest) -> EmbeddingResponse:
    inputs = request.input if isinstance(request.input, list) else [request.input]
    return EmbeddingResponse(
        model=request.model,
        data=[
            EmbeddingData(index=index, embedding=_mock_embedding(text, dimensions=request.dimensions))
            for index, text in enumerate(inputs)
        ],
    )


def _mock_embedding(text: str, *, dimensions: int) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return [round(digest[index % len(digest)] / 255, 6) for index in range(dimensions)]
