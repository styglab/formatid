from __future__ import annotations

import os
from io import BytesIO
import uuid
from typing import Any
from urllib.parse import urlsplit

from minio import Minio
from psycopg import sql
from psycopg.rows import dict_row

from apps.spec_rag.api.app.core.config import get_settings
from core.runtime.graph_runtime.queue import TriggeredGraphQueue, TriggeredGraphRequest
from core.runtime.graph_runtime.state_store import GraphRunStore
from core.runtime.runtime_db.connection import connect


class SpecRagRepository:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._queue = TriggeredGraphQueue(redis_url=self.settings.redis_url, queue_name=self.settings.graph_queue_name)
        self._graph_runs = GraphRunStore(database_url=self.settings.checkpoint_database_url)

    @property
    def queue_name(self) -> str:
        return self.settings.graph_queue_name

    async def create_document(self, *, filename: str, content_type: str, content: bytes) -> str:
        resource_key = uuid.uuid4().hex
        bucket = _required_env("S3_BUCKET")
        object_key = f"uploads/{resource_key}/{_safe_filename(filename)}"
        text_preview = content.decode("utf-8", errors="ignore").strip()
        storage = _build_storage()
        storage.ensure_bucket(bucket)
        storage.put_bytes(bucket=bucket, object_key=object_key, content=content, content_type=content_type)
        conn = await connect(self.settings.checkpoint_database_url)
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.settings.upload_schema)))
                await cursor.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            resource_key TEXT PRIMARY KEY,
                            filename TEXT NOT NULL,
                            content_type TEXT NOT NULL,
                            text TEXT NOT NULL,
                            bucket TEXT NOT NULL,
                            object_key TEXT NOT NULL,
                            size_bytes INTEGER NOT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                        """
                    ).format(sql.Identifier(self.settings.upload_schema), sql.Identifier(self.settings.upload_table))
                )
                await cursor.execute(
                    sql.SQL(
                        """
                        ALTER TABLE {}.{}
                            ADD COLUMN IF NOT EXISTS bucket TEXT,
                            ADD COLUMN IF NOT EXISTS object_key TEXT
                        """
                    ).format(sql.Identifier(self.settings.upload_schema), sql.Identifier(self.settings.upload_table))
                )
                await cursor.execute(
                    sql.SQL(
                        """
                        INSERT INTO {}.{} (resource_key, filename, content_type, text, bucket, object_key, size_bytes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (resource_key) DO UPDATE SET
                            filename = EXCLUDED.filename,
                            content_type = EXCLUDED.content_type,
                            text = EXCLUDED.text,
                            bucket = EXCLUDED.bucket,
                            object_key = EXCLUDED.object_key,
                            size_bytes = EXCLUDED.size_bytes,
                            updated_at = NOW()
                        """
                    ).format(sql.Identifier(self.settings.upload_schema), sql.Identifier(self.settings.upload_table)),
                    (resource_key, filename, content_type, text_preview, bucket, object_key, len(content)),
                )
            await conn.commit()
        finally:
            await conn.close()
        return resource_key

    async def enqueue_workflow(
        self,
        *,
        run_id: str,
        resource_key: str,
        request_id: str | None,
        correlation_id: str | None,
    ) -> None:
        source = await self._get_uploaded_source(resource_key)
        await self._queue.enqueue(
            TriggeredGraphRequest(
                graph_name="spec_indexing_graph",
                run_id=run_id,
                params={
                    "resource_key": resource_key,
                    "source": {
                        "type": "object_storage",
                        "bucket": source["bucket"],
                        "object_key": source["object_key"],
                        "endpoint_env": "S3_ENDPOINT",
                        "access_key_env": "S3_ACCESS_KEY",
                        "secret_key_env": "S3_SECRET_KEY",
                        "secure_env": "S3_SECURE",
                        "resource_key": resource_key,
                    },
                },
                requested_by="spec-rag-api",
                request_id=request_id,
                correlation_id=correlation_id,
                resource_key=resource_key,
            )
        )

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        return await self._graph_runs.get_run(run_id)

    async def close(self) -> None:
        await self._queue.close()
        await self._graph_runs.close()

    async def _get_uploaded_source(self, resource_key: str) -> dict[str, str]:
        conn = await connect(self.settings.checkpoint_database_url)
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL("SELECT bucket, object_key FROM {}.{} WHERE resource_key = %s").format(
                        sql.Identifier(self.settings.upload_schema),
                        sql.Identifier(self.settings.upload_table),
                    ),
                    (resource_key,),
                )
                row = await cursor.fetchone()
        finally:
            await conn.close()
        if row is None:
            raise RuntimeError(f"uploaded document object path not found: resource_key={resource_key}")
        return {"bucket": str(row["bucket"]), "object_key": str(row["object_key"])}


class ObjectStorage:
    def __init__(self, *, endpoint: str, access_key: str, secret_key: str, secure: bool) -> None:
        self._client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def ensure_bucket(self, bucket: str) -> None:
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)

    def put_bytes(self, *, bucket: str, object_key: str, content: bytes, content_type: str) -> None:
        self._client.put_object(bucket, object_key, BytesIO(content), length=len(content), content_type=content_type)


def _build_storage() -> ObjectStorage:
    endpoint, secure = _normalize_endpoint(_required_env("S3_ENDPOINT"), default_secure=_bool_from_env("S3_SECURE", default=False))
    return ObjectStorage(
        endpoint=endpoint,
        access_key=_required_env("S3_ACCESS_KEY"),
        secret_key=_required_env("S3_SECRET_KEY"),
        secure=secure,
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


def _safe_filename(filename: str) -> str:
    normalized = filename.strip().replace("\\", "/").rsplit("/", 1)[-1]
    return normalized or "upload.bin"
