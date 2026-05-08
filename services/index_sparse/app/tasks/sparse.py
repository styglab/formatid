from __future__ import annotations

import json
import os
from typing import Any

import httpx
from psycopg import sql
from psycopg.rows import dict_row

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.url import get_database_url
from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult
from services.index_sparse.app.contracts.sparse import SparseIndexOutput, SparseIndexPayload


@task("index.sparse.upsert")
async def index_sparse_upsert(message: TaskMessage) -> TaskResult:
    payload = SparseIndexPayload.model_validate(message.payload)
    resource_key = str(payload.target.get("resource_key") or payload.source.get("key_value"))
    rows = await _read_source_texts(payload.source)
    index = str(payload.target["index"])
    await _upsert_opensearch(
        target=payload.target,
        index=index,
        resource_key=resource_key,
        rows=rows,
        metadata={**payload.metadata, "task_id": message.task_id, "task_name": message.task_name},
    )
    output = SparseIndexOutput(resource_key=resource_key, document_count=len(rows), index=index)
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output=output.model_dump(),
    )


async def _read_source_texts(source: dict[str, Any]) -> list[dict[str, Any]]:
    conn = await connect(get_database_url(source.get("database_url_env", "POSTGRES_DATABASE_URL")))
    key_column = source.get("key_column", "resource_key")
    text_column = source.get("text_column", "text")
    order_column = source.get("order_column", "chunk_index")
    key_value = source["key_value"]
    try:
        async with conn.cursor(row_factory=dict_row) as cursor:
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
                {"chunk_index": int(row[order_column]), "text": str(row[text_column] or "")}
                for row in await cursor.fetchall()
            ]
    finally:
        await conn.close()


async def _upsert_opensearch(
    *,
    target: dict[str, Any],
    index: str,
    resource_key: str,
    rows: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> None:
    if not rows:
        return
    endpoint = _target_endpoint(target).rstrip("/")
    headers = {"content-type": "application/x-ndjson"}
    timeout = float(target.get("timeout_seconds", 30))
    verify = bool(target.get("verify_tls", True))
    async with httpx.AsyncClient(timeout=timeout, verify=verify) as client:
        await _ensure_index(client=client, endpoint=endpoint, index=index, headers=headers, target=target)
        response = await client.post(
            f"{endpoint}/_bulk",
            content=_bulk_body(index=index, resource_key=resource_key, rows=rows, metadata=metadata),
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("errors"):
            raise RuntimeError("opensearch bulk indexing completed with item errors")


async def _ensure_index(
    *,
    client: httpx.AsyncClient,
    endpoint: str,
    index: str,
    headers: dict[str, str],
    target: dict[str, Any],
) -> None:
    mapping = {
        "settings": target.get("settings")
        or {
            "index": {
                "similarity": {
                    "default": {
                        "type": "BM25",
                    }
                }
            }
        },
        "mappings": target.get("mappings")
        or {
            "properties": {
                "resource_key": {"type": "keyword"},
                "chunk_index": {"type": "integer"},
                "text": {"type": "text"},
                "metadata": {"type": "object", "enabled": True},
            }
        },
    }
    response = await client.put(f"{endpoint}/{index}", json=mapping, headers=_json_headers(headers))
    if response.status_code not in {200, 201, 400}:
        response.raise_for_status()
    if response.status_code == 400:
        body = response.json()
        error = body.get("error") if isinstance(body, dict) else None
        error_type = error.get("type") if isinstance(error, dict) else None
        if error_type != "resource_already_exists_exception":
            response.raise_for_status()


def _bulk_body(*, index: str, resource_key: str, rows: list[dict[str, Any]], metadata: dict[str, Any]) -> bytes:
    lines: list[str] = []
    for row in rows:
        chunk_index = int(row["chunk_index"])
        doc_id = f"{resource_key}:{chunk_index}"
        lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}, separators=(",", ":")))
        lines.append(
            json.dumps(
                {
                    "resource_key": resource_key,
                    "chunk_index": chunk_index,
                    "text": row["text"],
                    "metadata": metadata,
                },
                separators=(",", ":"),
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


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
    raise RuntimeError("sparse index target endpoint or endpoint_env is required")


def _json_headers(headers: dict[str, str]) -> dict[str, str]:
    return {**headers, "content-type": "application/json"}
