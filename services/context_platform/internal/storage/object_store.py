from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import uuid4


class ObjectStore:
    def __init__(self) -> None:
        self.endpoint = _clean_endpoint(os.getenv("CONTEXT_PLATFORM_S3_ENDPOINT") or os.getenv("S3_ENDPOINT") or "http://minio:9000")
        self.access_key = os.getenv("CONTEXT_PLATFORM_S3_ACCESS_KEY") or os.getenv("S3_ACCESS_KEY") or "minioadmin"
        self.secret_key = os.getenv("CONTEXT_PLATFORM_S3_SECRET_KEY") or os.getenv("S3_SECRET_KEY") or "minioadmin"
        self.secure = _as_bool(os.getenv("CONTEXT_PLATFORM_S3_SECURE") or os.getenv("S3_SECURE") or "false")
        self.bucket = os.getenv("CONTEXT_PLATFORM_DOCUMENT_BUCKET") or os.getenv("S3_BUCKET") or "context-platform-documents"

    def put_document(self, *, filename: str, content_type: str, data: bytes) -> dict[str, Any]:
        safe_name = Path(filename or f"upload-{uuid4().hex}").name
        key = f"source-documents/{uuid4().hex}-{safe_name}"
        client = self._client()
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)
        client.put_object(
            self.bucket,
            key,
            BytesIO(data),
            length=len(data),
            content_type=content_type or "application/octet-stream",
        )
        return {
            "bucket": self.bucket,
            "key": key,
            "uri": f"s3://{self.bucket}/{key}",
            "endpoint": self.endpoint,
            "filename": safe_name,
            "content_type": content_type or "application/octet-stream",
            "size_bytes": len(data),
        }

    def read_text(self, uri: str) -> str:
        return self.read_bytes(uri).decode("utf-8", errors="replace")

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = _parse_s3_uri(uri)
        response = self._client().get_object(bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def _client(self) -> Any:
        from minio import Minio

        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )


def _clean_endpoint(value: str) -> str:
    return value.removeprefix("http://").removeprefix("https://").rstrip("/")


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"expected s3 uri, got: {uri}")
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri}")
    return bucket, key


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}
