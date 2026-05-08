from __future__ import annotations

import json
import os
from urllib.parse import urlsplit

from minio import Minio
from psycopg import sql
from psycopg.rows import dict_row

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.url import get_database_url
from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult
from services.chunk.app.contracts.document import DocumentChunkOutput, DocumentChunkPayload


@task("chunk.document.run")
async def chunk_document(message: TaskMessage) -> TaskResult:
    payload = DocumentChunkPayload.model_validate(message.payload)
    resource_key = str(payload.target.get("key_value") or payload.source.get("key_value"))
    text = await _read_text(payload.source)
    chunks = _chunk_text(
        text,
        chunk_size=int(payload.options.get("chunk_size_chars", 1600)),
        overlap=int(payload.options.get("overlap_chars", 200)),
    )
    await _write_chunks(
        target=payload.target,
        resource_key=resource_key,
        chunks=chunks,
        metadata={**payload.metadata, "task_id": message.task_id, "task_name": message.task_name},
    )
    output = DocumentChunkOutput(resource_key=resource_key, chunk_count=len(chunks))
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output=output.model_dump(),
    )


async def _read_text(source: dict) -> str:
    if source.get("type") == "object_storage":
        content = _read_object(source)
        try:
            return content.decode("utf-8").strip()
        except UnicodeDecodeError:
            return content.decode("utf-8", errors="ignore").strip()
    conn = await connect(get_database_url(source.get("database_url_env", "POSTGRES_DATABASE_URL")))
    key_column = source.get("key_column", "resource_key")
    text_column = source.get("text_column", "text")
    try:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                sql.SQL("SELECT {} FROM {}.{} WHERE {} = %s").format(
                    sql.Identifier(text_column),
                    sql.Identifier(source["schema"]),
                    sql.Identifier(source["table"]),
                    sql.Identifier(key_column),
                ),
                (source["key_value"],),
            )
            row = await cursor.fetchone()
    finally:
        await conn.close()
    if row is None:
        raise RuntimeError(f"chunk source text not found: key={source['key_value']}")
    return str(row[text_column] or "")


def _read_object(source: dict) -> bytes:
    endpoint, secure_from_endpoint = _normalize_endpoint(
        _required_env(source.get("endpoint_env") or "S3_ENDPOINT"),
        default_secure=_bool_from_env(source.get("secure_env") or "S3_SECURE", default=False),
    )
    secure = source.get("secure")
    client = Minio(
        endpoint,
        access_key=_required_env(source.get("access_key_env") or "S3_ACCESS_KEY"),
        secret_key=_required_env(source.get("secret_key_env") or "S3_SECRET_KEY"),
        secure=secure_from_endpoint if secure is None else bool(secure),
    )
    response = client.get_object(
        source.get("bucket") or _required_env(source.get("bucket_env") or "S3_BUCKET"),
        source["object_key"],
    )
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


async def _write_chunks(*, target: dict, resource_key: str, chunks: list[str], metadata: dict) -> None:
    conn = await connect(get_database_url(target.get("database_url_env", "POSTGRES_DATABASE_URL")))
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(target["schema"])))
            await cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        resource_key TEXT NOT NULL,
                        chunk_index INTEGER NOT NULL,
                        chunk_text TEXT NOT NULL,
                        char_count INTEGER NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (resource_key, chunk_index)
                    )
                    """
                ).format(sql.Identifier(target["schema"]), sql.Identifier(target["table"]))
            )
            for index, chunk in enumerate(chunks):
                await cursor.execute(
                    sql.SQL(
                        """
                        INSERT INTO {}.{} (resource_key, chunk_index, chunk_text, char_count, metadata)
                        VALUES (%s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (resource_key, chunk_index) DO UPDATE SET
                            chunk_text = EXCLUDED.chunk_text,
                            char_count = EXCLUDED.char_count,
                            metadata = EXCLUDED.metadata,
                            updated_at = NOW()
                        """
                    ).format(sql.Identifier(target["schema"]), sql.Identifier(target["table"])),
                    (resource_key, index, chunk, len(chunk), json.dumps(metadata)),
                )
        await conn.commit()
    finally:
        await conn.close()


def _chunk_text(text: str, *, chunk_size: int, overlap: int) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return []
    chunk_size = max(1, chunk_size)
    overlap = max(0, min(overlap, chunk_size - 1))
    step = chunk_size - overlap
    return [normalized[index : index + chunk_size] for index in range(0, len(normalized), step)]


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not configured")
    return value


def _bool_from_env(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_endpoint(raw_endpoint: str, *, default_secure: bool) -> tuple[str, bool]:
    if "://" not in raw_endpoint:
        return raw_endpoint.strip(), default_secure
    parsed = urlsplit(raw_endpoint)
    endpoint = parsed.netloc or parsed.path
    if not endpoint:
        raise RuntimeError(f"invalid S3 endpoint: {raw_endpoint}")
    return endpoint, parsed.scheme.lower() == "https"
