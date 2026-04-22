from __future__ import annotations

import httpx

from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult
from services.ingest.tasks.store import (
    GenericPostgresIngestStore,
    ensure_record_list,
    get_database_url,
    get_value_at_path,
)


@task("ingest.api.fetch")
async def fetch_api_and_store(message: TaskMessage) -> TaskResult:
    payload = message.payload
    request = payload["request"]
    target = payload["target"]
    mapping = payload.get("mapping", {})

    response_payload = await _fetch(request)
    records_source = response_payload if mapping.get("store_whole_response") else get_value_at_path(
        response_payload,
        mapping.get("items_path"),
    )
    records = ensure_record_list(records_source)
    resource_key_path = mapping.get("resource_key_path") or target.get("resource_key_path")
    resource_keys = (
        [_string_or_none(get_value_at_path(record, resource_key_path)) for record in records]
        if resource_key_path
        else [None for _ in records]
    )

    store = GenericPostgresIngestStore(database_url=get_database_url(target.get("database_url_env", "POSTGRES_DATABASE_URL")))
    try:
        stored_count = await store.write_records(
            schema_name=target["schema_name"],
            table_name=target["table_name"],
            records=records,
            source_url=request["url"],
            resource_keys=resource_keys,
            metadata={
                **payload.get("metadata", {}),
                "task_id": message.task_id,
                "task_name": message.task_name,
                "status_code": response_payload.get("_http_status_code") if isinstance(response_payload, dict) else None,
            },
            mode=target.get("mode", "append"),
            create_table=bool(target.get("create_table", False)),
        )
    finally:
        await store.close()

    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output={
            "stored_count": stored_count,
            "target": {
                "type": "postgres",
                "schema": target["schema_name"],
                "table": target["table_name"],
            },
        },
    )


async def _fetch(request: dict) -> dict | list | str:
    async with httpx.AsyncClient(timeout=float(request.get("timeout_seconds", 30)), follow_redirects=True) as client:
        response = await client.request(
            request.get("method", "GET"),
            request["url"],
            params=request.get("params", {}),
            headers=request.get("headers", {}),
            json=request.get("json_body"),
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
