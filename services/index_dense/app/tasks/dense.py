from __future__ import annotations

import hashlib
import os
from typing import Any

import httpx
from psycopg import sql
from psycopg.rows import dict_row

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.url import get_database_url
from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult
from services.index_dense.app.contracts.dense import DenseIndexOutput, DenseIndexPayload


@task("index.dense.upsert")
async def index_dense_upsert(message: TaskMessage) -> TaskResult:
    payload = DenseIndexPayload.model_validate(message.payload)
    resource_key = str(payload.target.get("resource_key") or payload.source.get("key_value"))
    rows = await _read_source_texts(payload.source)
    dimensions = int(payload.request.get("dimensions", 16))
    vector_rows = await _build_vector_rows(rows=rows, request=payload.request, dimensions=dimensions)
    collection = str(payload.target["collection"])
    await _upsert_qdrant(
        target=payload.target,
        collection=collection,
        resource_key=resource_key,
        rows=vector_rows,
        metadata={**payload.metadata, "task_id": message.task_id, "task_name": message.task_name},
    )
    output = DenseIndexOutput(
        resource_key=resource_key,
        vector_count=len(vector_rows),
        collection=collection,
        dimensions=dimensions,
    )
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output=output.model_dump(),
    )


async def _build_vector_rows(
    *,
    rows: list[dict[str, Any]],
    request: dict[str, Any],
    dimensions: int,
) -> list[dict[str, Any]]:
    endpoint = _request_env(request, "embedding_endpoint_env") or request.get("embedding_endpoint")
    if isinstance(endpoint, str) and endpoint:
        texts = [str(row["text"] or "") for row in rows]
        model = str(_request_env(request, "embedding_model_env") or request.get("embedding_model") or "mock-embedding")
        embeddings = await _create_embeddings(
            endpoint=endpoint,
            texts=texts,
            model=model,
            dimensions=dimensions,
        )
        return [
            {
                "chunk_index": int(row["chunk_index"]),
                "text": str(row["text"] or ""),
                "embedding": embeddings[index],
            }
            for index, row in enumerate(rows)
        ]
    return [
        {
            "chunk_index": int(row["chunk_index"]),
            "text": str(row["text"] or ""),
            "embedding": _fallback_embedding(str(row["text"] or ""), dimensions=dimensions),
        }
        for row in rows
    ]


async def _read_source_texts(source: dict[str, Any]) -> list[dict[str, Any]]:
    conn = await connect(get_database_url(source.get("database_url_env", "POSTGRES_DATABASE_URL")))
    key_column = source.get("key_column", "resource_key")
    text_column = source.get("text_column", "text")
    order_column = source.get("order_column", "chunk_index")
    key_value = source["key_value"]
    try:
        async with conn.cursor(row_factory=dict_row) as cursor:
            if source.get("single_row"):
                await cursor.execute(
                    sql.SQL("SELECT {} FROM {}.{} WHERE {} = %s").format(
                        sql.Identifier(text_column),
                        sql.Identifier(source["schema"]),
                        sql.Identifier(source["table"]),
                        sql.Identifier(key_column),
                    ),
                    (key_value,),
                )
                row = await cursor.fetchone()
                if row is None:
                    return []
                return [{"chunk_index": 0, "text": row[text_column]}]
            await cursor.execute(
                sql.SQL("SELECT {}, {} FROM {}.{} WHERE {} = %s ORDER BY {}").format(
                    sql.Identifier(order_column),
                    sql.Identifier(text_column),
                    sql.Identifier(source["schema"]),
                    sql.Identifier(source["table"]),
                    sql.Identifier(key_column),
                    sql.Identifier(order_column),
                ),
                (key_value,),
            )
            return [
                {"chunk_index": row[order_column], "text": row[text_column]}
                for row in await cursor.fetchall()
            ]
    finally:
        await conn.close()


async def _upsert_qdrant(
    *,
    target: dict[str, Any],
    collection: str,
    resource_key: str,
    rows: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> None:
    endpoint = _target_endpoint(target).rstrip("/")
    if not rows:
        return
    headers = _qdrant_headers(target)
    vector_size = len(rows[0]["embedding"])
    async with httpx.AsyncClient(timeout=float(target.get("timeout_seconds", 30))) as client:
        await client.put(
            f"{endpoint}/collections/{collection}",
            json={"vectors": {"size": vector_size, "distance": target.get("distance", "Cosine")}},
            headers=headers,
        )
        points = [
            {
                "id": _point_id(resource_key=resource_key, chunk_index=int(row["chunk_index"])),
                "vector": row["embedding"],
                "payload": {
                    "resource_key": resource_key,
                    "chunk_index": int(row["chunk_index"]),
                    "text": row["text"],
                    "metadata": metadata,
                },
            }
            for row in rows
        ]
        response = await client.put(
            f"{endpoint}/collections/{collection}/points",
            json={"points": points},
            headers=headers,
        )
        response.raise_for_status()


async def _create_embeddings(*, endpoint: str, texts: list[str], model: str, dimensions: int) -> list[list[float]]:
    if not texts:
        return []
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{endpoint.rstrip('/')}/v1/embeddings",
            json={"input": texts, "model": model, "dimensions": dimensions},
        )
        response.raise_for_status()
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or len(data) != len(texts):
        raise RuntimeError("embedding service returned invalid embedding count")
    ordered = sorted(data, key=lambda item: int(item.get("index", 0)) if isinstance(item, dict) else 0)
    embeddings = []
    for item in ordered:
        embedding = item.get("embedding") if isinstance(item, dict) else None
        embeddings.append(_coerce_embedding(embedding))
    return embeddings


def _fallback_embedding(text: str, *, dimensions: int) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return [round(digest[index % len(digest)] / 255, 6) for index in range(dimensions)]


def _coerce_embedding(value: Any) -> list[float]:
    if not isinstance(value, list):
        raise RuntimeError("embedding source returned invalid embedding value")
    return [float(item) for item in value]


def _target_endpoint(target: dict[str, Any]) -> str:
    endpoint = target.get("endpoint")
    if isinstance(endpoint, str) and endpoint:
        return endpoint
    endpoint_env = target.get("endpoint_env")
    if isinstance(endpoint_env, str) and endpoint_env:
        value = os.getenv(endpoint_env)
        if not value:
            raise RuntimeError(f"{endpoint_env} is not configured")
        return value
    raise RuntimeError("dense index target endpoint or endpoint_env is required")


def _qdrant_headers(target: dict[str, Any]) -> dict[str, str]:
    api_key = target.get("api_key")
    if isinstance(api_key, str) and api_key:
        return {"api-key": api_key}
    api_key_env = target.get("api_key_env")
    if isinstance(api_key_env, str) and api_key_env:
        value = os.getenv(api_key_env)
        if value:
            return {"api-key": value}
    return {}


def _request_env(request: dict[str, Any], key: str) -> str | None:
    env_name = request.get(key)
    if not isinstance(env_name, str) or not env_name:
        return None
    return os.getenv(env_name)


def _point_id(*, resource_key: str, chunk_index: int) -> int:
    digest = hashlib.sha256(f"{resource_key}:{chunk_index}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) % (2**63)
