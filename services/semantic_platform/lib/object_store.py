from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ObjectRef:
    bucket: str
    key: str
    uri: str


def default_bucket() -> str:
    return (
        os.getenv("SEMANTIC_PLATFORM_S3_BUCKET")
        or os.getenv("S3_BUCKET")
        or "semantic-platform-sources"
    )


def put_object(*, key: str, body: bytes, content_type: str | None = None, bucket: str | None = None) -> ObjectRef:
    bucket_name = bucket or default_bucket()
    client = _client()
    _ensure_bucket(client, bucket_name)
    client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=body,
        ContentType=content_type or "application/octet-stream",
    )
    return ObjectRef(bucket=bucket_name, key=key, uri=f"s3://{bucket_name}/{key}")


def get_object_bytes(*, key: str, bucket: str | None = None) -> bytes:
    bucket_name = bucket or default_bucket()
    client = _client()
    response = client.get_object(Bucket=bucket_name, Key=key)
    with response["Body"] as body:
        return body.read()


def _client():
    import boto3
    from botocore.config import Config

    endpoint_url = os.getenv("SEMANTIC_PLATFORM_S3_ENDPOINT") or os.getenv("S3_ENDPOINT")
    access_key = os.getenv("SEMANTIC_PLATFORM_S3_ACCESS_KEY") or os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("SEMANTIC_PLATFORM_S3_SECRET_KEY") or os.getenv("S3_SECRET_KEY")
    region = os.getenv("SEMANTIC_PLATFORM_S3_REGION") or os.getenv("S3_REGION") or "us-east-1"
    secure = (os.getenv("SEMANTIC_PLATFORM_S3_SECURE") or os.getenv("S3_SECURE") or "false").lower()
    if endpoint_url is None and secure in {"false", "0", "no"}:
        endpoint_url = "http://minio:9000"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )


def _ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)
