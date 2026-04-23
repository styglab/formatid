from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PostgresIngestTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["postgres"] = "postgres"
    database_url_env: str = Field(default="POSTGRES_DATABASE_URL", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_name: str = Field(alias="schema", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    table_name: str = Field(alias="table", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    mode: Literal["append", "upsert"] = "append"
    create_table: bool = False
    resource_key_path: str | None = None


class FileSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = Field(min_length=1)
    headers: dict[str, str] = Field(default_factory=dict)
    filename: str | None = None
    timeout_seconds: float = Field(default=60, gt=0, le=600)


class S3Target(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["s3"] = "s3"
    endpoint_env: str = Field(default="S3_ENDPOINT", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    access_key_env: str = Field(default="S3_ACCESS_KEY", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    secret_key_env: str = Field(default="S3_SECRET_KEY", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bucket_env: str | None = Field(default="S3_BUCKET", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bucket: str | None = None
    secure_env: str = Field(default="S3_SECURE", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    secure: bool | None = None
    object_key: str = Field(min_length=1)
    content_type: str = "application/octet-stream"

    @field_validator("object_key")
    @classmethod
    def object_key_must_be_relative(cls, value: str) -> str:
        if value.startswith("/") or ".." in value.split("/"):
            raise ValueError("object_key must be a relative object path")
        return value


class FileDownloadPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: FileSource
    target: S3Target
    metadata_target: PostgresIngestTarget | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class FileDownloadOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bucket: str
    object_key: str
    size_bytes: int = Field(ge=0)
    metadata_stored: bool

