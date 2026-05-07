from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class HttpRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: Literal["GET", "POST"] = "GET"
    url: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    json_body: Any | None = None
    timeout_seconds: float = Field(default=30, gt=0, le=300)


class PostgresIngestTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["postgres"] = "postgres"
    database_url_env: str = Field(default="POSTGRES_DATABASE_URL", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_name: str = Field(alias="schema", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    table_name: str = Field(alias="table", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    mode: Literal["append", "upsert"] = "append"
    create_table: bool = False
    resource_key_path: str | None = None


class ApiResponseMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items_path: str | None = None
    resource_key_path: str | None = None
    store_whole_response: bool = False


class ApiFetchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: HttpRequest
    target: PostgresIngestTarget
    mapping: ApiResponseMapping = Field(default_factory=ApiResponseMapping)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ApiFetchOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stored_count: int = Field(ge=0)
    target: dict[str, str]

