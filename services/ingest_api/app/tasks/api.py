from __future__ import annotations

import httpx

from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult
from services.ingest_api.app.contracts.api_fetch import ApiFetchOutput, ApiFetchPayload, HttpRequest
from services.ingest_api.app.tasks.store import (
    GenericPostgresIngestStore,
    ensure_record_list,
    get_database_url,
    get_value_at_path,
)


@task("ingest.api.fetch")
async def fetch_api_and_store(message: TaskMessage) -> TaskResult:
    payload = ApiFetchPayload.model_validate(message.payload)
    request = payload.request
    target = payload.target
    mapping = payload.mapping

    response_payload = await _fetch(request)
    records_source = response_payload if mapping.store_whole_response else get_value_at_path(
        response_payload,
        mapping.items_path,
    )
    records = ensure_record_list(records_source)
    resource_key_path = mapping.resource_key_path or target.resource_key_path
    resource_keys = (
        [_string_or_none(get_value_at_path(record, resource_key_path)) for record in records]
        if resource_key_path
        else [None for _ in records]
    )

    store = GenericPostgresIngestStore(database_url=get_database_url(target.database_url_env))
    try:
        stored_count = await store.write_records(
            schema_name=target.schema_name,
            table_name=target.table_name,
            records=records,
            source_url=request.url,
            resource_keys=resource_keys,
            metadata={
                **payload.metadata,
                "task_id": message.task_id,
                "task_name": message.task_name,
                "status_code": response_payload.get("_http_status_code") if isinstance(response_payload, dict) else None,
            },
            mode=target.mode,
            create_table=target.create_table,
        )
    finally:
        await store.close()

    output = ApiFetchOutput(
        stored_count=stored_count,
        target={
            "type": "postgres",
            "schema": target.schema_name,
            "table": target.table_name,
        },
    )
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output=output.model_dump(),
    )


async def _fetch(request: HttpRequest) -> dict | list | str:
    async with httpx.AsyncClient(timeout=request.timeout_seconds, follow_redirects=True) as client:
        response = await client.request(
            request.method,
            request.url,
            params=request.params,
            headers=request.headers,
            json=request.json_body,
        )
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "json" in content_type.lower():
            payload = response.json()
            if isinstance(payload, dict):
                payload.setdefault("_http_status_code", response.status_code)
            return payload
        return {
            "_http_status_code": response.status_code,
            "body": response.text,
        }


def _string_or_none(value) -> str | None:
    if value is None:
        return None
    return str(value)
