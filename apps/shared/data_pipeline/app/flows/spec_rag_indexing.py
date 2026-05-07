from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import psycopg
from prefect import flow, task
from psycopg import sql
from psycopg.rows import dict_row
from redis import Redis


@dataclass(frozen=True)
class UploadedDocument:
    resource_key: str
    bucket: str
    object_key: str


@task(retries=2, retry_delay_seconds=5)
def list_uploaded_documents() -> list[UploadedDocument]:
    schema = os.getenv("SPEC_RAG_UPLOAD_SCHEMA", "spec_rag")
    table = os.getenv("SPEC_RAG_UPLOAD_TABLE", "documents")
    resource_key = os.getenv("SPEC_RAG_INDEX_RESOURCE_KEY")
    limit = int(os.getenv("SPEC_RAG_INDEX_BATCH_LIMIT", "100"))

    query = sql.SQL(
        "SELECT resource_key, bucket, object_key FROM {}.{} "
        "WHERE bucket IS NOT NULL AND object_key IS NOT NULL "
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
    )
    params: tuple[Any, ...]
    if resource_key:
        query += sql.SQL("AND resource_key = %s ")
        params = (resource_key,)
    else:
        params = ()
    query += sql.SQL("ORDER BY updated_at DESC LIMIT %s")
    params = (*params, limit)

    with psycopg.connect(_postgres_url(), row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

    return [
        UploadedDocument(
            resource_key=str(row["resource_key"]),
            bucket=str(row["bucket"]),
            object_key=str(row["object_key"]),
        )
        for row in rows
    ]


@task
def enqueue_indexing_graphs(documents: list[UploadedDocument]) -> dict[str, Any]:
    queue_name = os.getenv("SPEC_RAG_INDEX_QUEUE", "spec-rag:index")
    redis_url = os.getenv("SERVICE_REDIS_URL", os.getenv("WORKER_REDIS_URL", "redis://redis:6379/0"))
    requested_at = datetime.now(timezone.utc).isoformat()
    correlation_prefix = os.getenv("SPEC_RAG_SCHEDULED_CORRELATION_PREFIX", "spec-rag-hourly-index")
    client = Redis.from_url(redis_url, decode_responses=True)
    enqueued: list[dict[str, str]] = []
    try:
        for document in documents:
            run_id = uuid4().hex
            payload = _trigger_payload(
                document=document,
                run_id=run_id,
                requested_at=requested_at,
                correlation_id=f"{correlation_prefix}:{document.resource_key}:{run_id}",
            )
            client.lpush(queue_name, json.dumps(payload, ensure_ascii=True))
            enqueued.append({"resource_key": document.resource_key, "run_id": run_id})
    finally:
        client.close()
    return {"queue_name": queue_name, "count": len(enqueued), "runs": enqueued}


@flow(name="spec-rag-indexing")
def spec_rag_indexing() -> dict[str, Any]:
    documents = list_uploaded_documents()
    return enqueue_indexing_graphs(documents)


def _trigger_payload(
    *,
    document: UploadedDocument,
    run_id: str,
    requested_at: str,
    correlation_id: str,
) -> dict[str, Any]:
    return {
        "graph_name": "spec_indexing_graph",
        "params": {
            "resource_key": document.resource_key,
            "source": {
                "type": "object_storage",
                "bucket": document.bucket,
                "object_key": document.object_key,
                "endpoint_env": "S3_ENDPOINT",
                "access_key_env": "S3_ACCESS_KEY",
                "secret_key_env": "S3_SECRET_KEY",
                "secure_env": "S3_SECURE",
                "resource_key": document.resource_key,
            },
        },
        "run_id": run_id,
        "request_kind": "start",
        "resume_value": None,
        "requested_by": "shared-data-pipeline",
        "request_id": None,
        "correlation_id": correlation_id,
        "resource_key": document.resource_key,
        "session_id": None,
        "requested_at": requested_at,
        "attempts": 0,
    }


def _postgres_url() -> str:
    if os.getenv("CHECKPOINT_DATABASE_URL"):
        return str(os.environ["CHECKPOINT_DATABASE_URL"])
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


if __name__ == "__main__":
    spec_rag_indexing()
