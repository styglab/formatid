from __future__ import annotations

from io import BytesIO
from urllib.parse import urlsplit

from minio import Minio

from apps.g2b_ingest.tasks.config import get_settings


class G2bIngestObjectStorage:
    def __init__(self) -> None:
        settings = get_settings()
        if not settings.s3_endpoint:
            raise RuntimeError("G2B_INGEST_S3_ENDPOINT is not configured")
        if not settings.s3_access_key or not settings.s3_secret_key:
            raise RuntimeError("G2B_INGEST_S3_ACCESS_KEY/G2B_INGEST_S3_SECRET_KEY are not configured")
        if not settings.s3_bucket:
            raise RuntimeError("G2B_INGEST_S3_BUCKET is not configured")

        endpoint, secure = _normalize_endpoint(settings.s3_endpoint, default_secure=settings.s3_secure)
        self._bucket = settings.s3_bucket
        self._client = Minio(
            endpoint,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            region=settings.s3_region,
            secure=secure,
        )

    @property
    def bucket(self) -> str:
        return self._bucket

    def ensure_bucket(self) -> None:
        found = self._client.bucket_exists(self._bucket)
        if not found:
            self._client.make_bucket(self._bucket)

    def put_bytes(
        self,
        *,
        object_key: str,
        content: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        self.ensure_bucket()
        self._client.put_object(
            self._bucket,
            object_key,
            BytesIO(content),
            length=len(content),
            content_type=content_type,
        )


def build_attachment_object_key(
    *,
    notice_date: str,
    bid_ntce_no: str,
    bid_ntce_ord: str,
    file_name: str,
) -> str:
    safe_name = file_name.replace("/", "_")
    return f"bid/attachments/{notice_date}/{bid_ntce_no}/{bid_ntce_ord}/{safe_name}"


def _normalize_endpoint(raw_endpoint: str, *, default_secure: bool) -> tuple[str, bool]:
    if "://" not in raw_endpoint:
        return raw_endpoint.strip(), default_secure

    parsed = urlsplit(raw_endpoint)
    endpoint = parsed.netloc or parsed.path
    if not endpoint:
        raise RuntimeError(f"invalid G2B_INGEST_S3_ENDPOINT: {raw_endpoint}")
    secure = parsed.scheme.lower() == "https"
    return endpoint, secure


def build_notice_date_path_segment(notice_payload: dict) -> str:
    for key in ("bidNtceDt", "rgstDt", "opengDt"):
        value = str(notice_payload.get(key, "")).strip()
        compact = "".join(ch for ch in value if ch.isdigit())
        if len(compact) >= 8:
            return compact[:8]
    return "unknown-date"
