from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class S3ObjectSource(BaseModel):
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


class TextTableTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    database_url_env: str = Field(default="POSTGRES_DATABASE_URL", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_name: str = Field(alias="schema", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    table_name: str = Field(alias="table", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    key_value: str | None = Field(default=None, min_length=1)
    job_id: str | None = Field(default=None, min_length=1)
    key_column: str = Field(default="resource_key", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bucket_column: str = Field(default="bucket", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    object_key_column: str = Field(default="object_key", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    text_column: str = Field(default="text", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    char_count_column: str = Field(default="char_count", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    metadata_column: str = Field(default="metadata", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")


class TextExtractStorePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: S3ObjectSource
    target: TextTableTarget
    metadata: dict[str, Any] = Field(default_factory=dict)
