from __future__ import annotations

import json
from io import BytesIO
from urllib.parse import urlsplit

from minio import Minio
from psycopg import sql

from services.runtime_db.connection import connect
from shared.postgres_url import get_database_url
from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult


@task("extract.text.run")
async def extract_text_and_store(message: TaskMessage) -> TaskResult:
    payload = message.payload
    source = payload["source"]
    target = payload["target"]
    resource_key = target.get("key_value") or target.get("job_id")

    content = _read_s3_object(source)
    text = _extract_text(content=content, object_key=source["object_key"])
    await _write_extracted_text(
        target=target,
        bucket=source.get("bucket") or _required_env(source.get("bucket_env") or "S3_BUCKET"),
        object_key=source["object_key"],
        text=text,
        metadata={**payload.get("metadata", {}), "task_id": message.task_id, "task_name": message.task_name},
    )

    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output={"resource_key": resource_key, "char_count": len(text)},
    )


def _read_s3_object(source: dict) -> bytes:
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


async def _write_extracted_text(*, target: dict, bucket: str, object_key: str, text: str, metadata: dict) -> None:
    conn = await connect(get_database_url(target.get("database_url_env", "POSTGRES_DATABASE_URL")))
    key_column = target.get("key_column", "resource_key")
    key_value = target.get("key_value") or target.get("job_id")
    text_column = target.get("text_column", "text")
    char_count_column = target.get("char_count_column", "char_count")
    bucket_column = target.get("bucket_column", "bucket")
    object_key_column = target.get("object_key_column", "object_key")
    metadata_column = target.get("metadata_column", "metadata")
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.{} ({}, {}, {}, {}, {}, {})
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT ({}) DO UPDATE SET
                        {} = EXCLUDED.{},
                        {} = EXCLUDED.{},
                        {} = EXCLUDED.{},
                        {} = EXCLUDED.{},
                        {} = EXCLUDED.{},
                        updated_at = NOW()
                    """
                ).format(
                    sql.Identifier(target["schema"]),
                    sql.Identifier(target["table"]),
                    sql.Identifier(key_column),
                    sql.Identifier(bucket_column),
                    sql.Identifier(object_key_column),
                    sql.Identifier(text_column),
                    sql.Identifier(char_count_column),
                    sql.Identifier(metadata_column),
                    sql.Identifier(key_column),
                    sql.Identifier(bucket_column),
                    sql.Identifier(bucket_column),
                    sql.Identifier(object_key_column),
                    sql.Identifier(object_key_column),
                    sql.Identifier(text_column),
                    sql.Identifier(text_column),
                    sql.Identifier(char_count_column),
                    sql.Identifier(char_count_column),
                    sql.Identifier(metadata_column),
                    sql.Identifier(metadata_column),
                ),
                (key_value, bucket, object_key, text, len(text), json.dumps(metadata)),
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
