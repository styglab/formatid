from __future__ import annotations

import json
import os
from io import BytesIO
from urllib.parse import urlsplit

from minio import Minio
from psycopg import sql

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.url import get_database_url
from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult
from services.parser.app.contracts.document import DocumentParseOutput, DocumentParsePayload


@task("parse.document.run")
async def parse_document(message: TaskMessage) -> TaskResult:
    payload = DocumentParsePayload.model_validate(message.payload)
    resource_key = str(payload.target.get("resource_key") or payload.source.get("resource_key"))
    source_bucket = _bucket(payload.source)
    source_object_key = str(payload.source["object_key"])
    content = _storage(payload.source).get_bytes(bucket=source_bucket, object_key=source_object_key)
    text = _extract_text(content=content, object_key=source_object_key)

    target_bucket = _bucket(payload.target)
    target_object_key = str(payload.target["object_key"])
    target_storage = _storage(payload.target)
    target_storage.ensure_bucket(target_bucket)
    target_storage.put_text(bucket=target_bucket, object_key=target_object_key, text=text)

    await _write_metadata(
        target=payload.target,
        resource_key=resource_key,
        bucket=target_bucket,
        object_key=target_object_key,
        text=text,
        metadata={**payload.metadata, "task_id": message.task_id, "task_name": message.task_name},
    )
    output = DocumentParseOutput(
        resource_key=resource_key,
        bucket=target_bucket,
        object_key=target_object_key,
        char_count=len(text),
    )
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output=output.model_dump(),
    )


class ObjectStorage:
    def __init__(self, *, endpoint: str, access_key: str, secret_key: str, secure: bool) -> None:
        self._client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def ensure_bucket(self, bucket: str) -> None:
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)

    def get_bytes(self, *, bucket: str, object_key: str) -> bytes:
        response = self._client.get_object(bucket, object_key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def put_text(self, *, bucket: str, object_key: str, text: str) -> None:
        content = text.encode("utf-8")
        self._client.put_object(
            bucket,
            object_key,
            BytesIO(content),
            length=len(content),
            content_type="text/plain; charset=utf-8",
        )


def _storage(config: dict) -> ObjectStorage:
    endpoint, secure_from_endpoint = _normalize_endpoint(
        _required_env(str(config.get("endpoint_env") or "S3_ENDPOINT")),
        default_secure=_bool_from_env(str(config.get("secure_env") or "S3_SECURE"), default=False),
    )
    secure = config.get("secure")
    return ObjectStorage(
        endpoint=endpoint,
        access_key=_required_env(str(config.get("access_key_env") or "S3_ACCESS_KEY")),
        secret_key=_required_env(str(config.get("secret_key_env") or "S3_SECRET_KEY")),
        secure=secure_from_endpoint if secure is None else bool(secure),
    )


def _bucket(config: dict) -> str:
    value = config.get("bucket")
    if isinstance(value, str) and value:
        return value
    return _required_env(str(config.get("bucket_env") or "S3_BUCKET"))


def _extract_text(*, content: bytes, object_key: str) -> str:
    lowered = object_key.lower()
    if lowered.endswith(".pdf"):
        try:
            from pypdf import PdfReader

            reader = PdfReader(BytesIO(content))
            return "\n".join(page.extract_text() or "" for page in reader.pages).strip()
        except Exception as exc:
            return f"[pdf text extraction failed: {type(exc).__name__}]"
    try:
        return content.decode("utf-8").strip()
    except UnicodeDecodeError:
        return content.decode("utf-8", errors="ignore").strip()


async def _write_metadata(
    *,
    target: dict,
    resource_key: str,
    bucket: str,
    object_key: str,
    text: str,
    metadata: dict,
) -> None:
    metadata_target = target.get("metadata_target")
    if not isinstance(metadata_target, dict):
        return
    conn = await connect(get_database_url(metadata_target.get("database_url_env", "POSTGRES_DATABASE_URL")))
    schema = metadata_target["schema"]
    table = metadata_target["table"]
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            await cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        resource_key TEXT PRIMARY KEY,
                        bucket TEXT NOT NULL,
                        object_key TEXT NOT NULL,
                        text TEXT NOT NULL,
                        char_count INTEGER NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                ).format(sql.Identifier(schema), sql.Identifier(table))
            )
            await cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.{} (resource_key, bucket, object_key, text, char_count, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (resource_key) DO UPDATE SET
                        bucket = EXCLUDED.bucket,
                        object_key = EXCLUDED.object_key,
                        text = EXCLUDED.text,
                        char_count = EXCLUDED.char_count,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    """
                ).format(sql.Identifier(schema), sql.Identifier(table)),
                (resource_key, bucket, object_key, text, len(text), json.dumps(metadata)),
            )
        await conn.commit()
    finally:
        await conn.close()


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
