from __future__ import annotations

import json

from psycopg import sql
from psycopg.rows import dict_row

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.url import get_database_url
from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult


@task("serve.llm.generate")
async def generate_text_and_store(message: TaskMessage) -> TaskResult:
    payload = message.payload
    request = payload.get("request", {})
    source = payload["source"]
    target = payload["target"]

    text = await _read_text(source)
    output_text = _mock_generate(text=text, request=request)
    raw_result = {
        "mock": True,
        "provider": request.get("provider", "mock"),
        "operation": request.get("operation", "generate"),
        "source_char_count": len(text),
        "output_char_count": len(output_text),
        "metadata": payload.get("metadata", {}),
    }
    await _write_generation(target=target, output_text=output_text, raw_result=raw_result)

    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output={
            "resource_key": target.get("key_value"),
            "output_char_count": len(output_text),
            "mock": True,
        },
    )


async def _read_text(source: dict) -> str:
    conn = await connect(get_database_url(source.get("database_url_env", "POSTGRES_DATABASE_URL")))
    try:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                sql.SQL("SELECT {} FROM {}.{} WHERE {} = %s").format(
                    sql.Identifier(source.get("text_column", "text")),
                    sql.Identifier(source["schema"]),
                    sql.Identifier(source["table"]),
                    sql.Identifier(source.get("key_column", "job_id")),
                ),
                (source.get("key_value") or source.get("job_id"),),
            )
            row = await cursor.fetchone()
    finally:
        await conn.close()
    if row is None:
        raise RuntimeError(f"llm source text not found: key={source.get('key_value') or source.get('job_id')}")
    return str(next(iter(row.values())) or "")


async def _write_generation(*, target: dict, output_text: str, raw_result: dict) -> None:
    conn = await connect(get_database_url(target.get("database_url_env", "POSTGRES_DATABASE_URL")))
    output_text_column = target.get("output_text_column", "output_text")
    model_column = target.get("model_column", "model")
    prompt_version_column = target.get("prompt_version_column", "prompt_version")
    raw_result_column = target.get("raw_result_column", "raw_result")
    key_column = target.get("key_column", "resource_key")
    key_value = target.get("key_value") or target.get("job_id")
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.{} ({}, {}, {}, {}, {})
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT ({}) DO UPDATE SET
                        {} = EXCLUDED.{},
                        {} = EXCLUDED.{},
                        {} = EXCLUDED.{},
                        {} = EXCLUDED.{},
                        updated_at = NOW()
                    """
                ).format(
                    sql.Identifier(target["schema"]),
                    sql.Identifier(target["table"]),
                    sql.Identifier(key_column),
                    sql.Identifier(output_text_column),
                    sql.Identifier(model_column),
                    sql.Identifier(prompt_version_column),
                    sql.Identifier(raw_result_column),
                    sql.Identifier(key_column),
                    sql.Identifier(output_text_column),
                    sql.Identifier(output_text_column),
                    sql.Identifier(model_column),
                    sql.Identifier(model_column),
                    sql.Identifier(prompt_version_column),
                    sql.Identifier(prompt_version_column),
                    sql.Identifier(raw_result_column),
                    sql.Identifier(raw_result_column),
                ),
                (
                    key_value,
                    output_text,
                    target.get("model") or "mock-llm",
                    target.get("prompt_version") or "sample-v1",
                    json.dumps(raw_result),
                ),
            )
        await conn.commit()
    finally:
        await conn.close()


def _mock_generate(*, text: str, request: dict) -> str:
    normalized = " ".join(text.split())
    if not normalized:
        return "생성할 입력 텍스트가 없습니다. (mock)"
    max_output_chars = int(request.get("max_output_chars", 600))
    prefix = normalized[:max_output_chars]
    suffix = "" if len(normalized) <= max_output_chars else "..."
    operation = request.get("operation", "generate")
    return f"[mock llm {operation}] {prefix}{suffix}"
