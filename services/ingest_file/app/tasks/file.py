from __future__ import annotations

import os
from io import BytesIO
from urllib.parse import urlsplit

import httpx
from minio import Minio

from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult
from services.ingest_file.app.contracts.file_download import FileDownloadOutput, FileDownloadPayload, FileSource, S3Target
from services.ingest_file.app.tasks.store import GenericPostgresIngestStore, get_database_url, get_value_at_path


@task("ingest.file.download")
async def download_file_and_store(message: TaskMessage) -> TaskResult:
    payload = FileDownloadPayload.model_validate(message.payload)
    source = payload.source
    target = payload.target
    content = await _download(source)
    storage = _build_storage(target)
    bucket = target.bucket or _required_env(target.bucket_env or "S3_BUCKET")
    object_key = target.object_key
    storage.ensure_bucket(bucket)
    storage.put_bytes(
        bucket=bucket,
        object_key=object_key,
        content=content,
        content_type=target.content_type,
    )

    metadata_target = payload.metadata_target
    metadata_stored = False
    if metadata_target is not None:
        metadata_record = {
            "source_url": source.url,
            "filename": source.filename,
            "bucket": bucket,
            "object_key": object_key,
            "size_bytes": len(content),
        }
        resource_key = get_value_at_path(metadata_record, metadata_target.resource_key_path) or object_key
        store = GenericPostgresIngestStore(
            database_url=get_database_url(metadata_target.database_url_env)
        )
        try:
            await store.write_records(
                schema_name=metadata_target.schema_name,
                table_name=metadata_target.table_name,
                records=[metadata_record],
                source_url=source.url,
                resource_keys=[str(resource_key)],
                metadata={**payload.metadata, "task_id": message.task_id, "task_name": message.task_name},
                mode=metadata_target.mode,
                create_table=metadata_target.create_table,
            )
            metadata_stored = True
        finally:
            await store.close()

    output = FileDownloadOutput(
        bucket=bucket,
        object_key=object_key,
        size_bytes=len(content),
        metadata_stored=metadata_stored,
    )
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output=output.model_dump(),
    )


async def _download(source: FileSource) -> bytes:
    async with httpx.AsyncClient(timeout=source.timeout_seconds, follow_redirects=True) as client:
        response = await client.get(source.url, headers=source.headers)
        response.raise_for_status()
        return response.content


class GenericObjectStorage:
    def __init__(self, *, endpoint: str, access_key: str, secret_key: str, secure: bool) -> None:
        self._client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def ensure_bucket(self, bucket: str) -> None:
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)

    def put_bytes(self, *, bucket: str, object_key: str, content: bytes, content_type: str) -> None:
        self._client.put_object(
            bucket,
            object_key,
            BytesIO(content),
            length=len(content),
            content_type=content_type,
        )


def _build_storage(target: S3Target) -> GenericObjectStorage:
    endpoint, secure_from_endpoint = _normalize_endpoint(
        _required_env(target.endpoint_env or "S3_ENDPOINT"),
        default_secure=_bool_from_env(target.secure_env or "S3_SECURE", default=False),
    )
    secure = target.secure
    return GenericObjectStorage(
        endpoint=endpoint,
        access_key=_required_env(target.access_key_env or "S3_ACCESS_KEY"),
        secret_key=_required_env(target.secret_key_env or "S3_SECRET_KEY"),
        secure=secure_from_endpoint if secure is None else bool(secure),
    )


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
