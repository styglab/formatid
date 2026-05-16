from __future__ import annotations

import json
import os
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError


OPENAI_API_URL = os.getenv("SEMANTIC_PLATFORM_LLM_API_URL", "https://api.openai.com/v1/chat/completions")
OPENAI_MODEL = os.getenv("SEMANTIC_PLATFORM_LLM_MODEL", "gpt-4.1-mini")
LLM_TIMEOUT_SECONDS = float(os.getenv("SEMANTIC_PLATFORM_LLM_TIMEOUT_SECONDS", "15"))
LLM_MODES = {"disabled", "codex_manual", "openai"}


def llm_mode() -> str:
    mode = os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = mode.strip().lower()
    return normalized if normalized in LLM_MODES else "disabled"


def generate_execution_plan(query: str, context: dict[str, Any]) -> dict[str, Any] | None:
    mode = llm_mode()
    if mode in {"disabled", "codex_manual"}:
        return None
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an LLM execution planner for a public-data semantic runtime. "
                    "Return only JSON. Use only operation_id values from operation_contracts. "
                    "Plan semantic-level calls, argument bindings, post filters, and integrations. "
                    "Do not invent provider URLs, raw field names, service keys, or API secrets."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "query": query,
                        "planner_context": context,
                        "required_output_schema": _schema_hint(),
                    },
                    ensure_ascii=False,
                ),
            },
        ],
    }
    try:
        body = json.dumps(payload).encode("utf-8")
        http_request = request.Request(
            OPENAI_API_URL,
            data=body,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(http_request, timeout=LLM_TIMEOUT_SECONDS) as response:
            response_body = response.read().decode("utf-8")
        document = json.loads(response_body)
        content = document["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except (HTTPError, URLError, TimeoutError, KeyError, IndexError, json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _schema_hint() -> dict[str, Any]:
    return {
        "intent": {
            "language": "ko|en|unknown",
            "entities": ["Contract"],
            "semantic_types": ["contract_amount"],
            "requested_outputs": ["business_status"],
            "filters": [{"semantic_type": "contract_amount", "operator": ">=", "value": 30000000000}],
        },
        "execution_graph": {
            "type": "dag",
            "nodes": [
                {
                    "id": "contracts",
                    "operation_id": "operation_id_from_context",
                    "capability": "capability_from_operation_contract",
                    "call": {"semantic_arguments": {"page_size": 50}},
                    "post_filters": [],
                    "produces": ["business_registration_number"],
                    "depends_on": [],
                    "argument_bindings": {},
                }
            ],
            "integration": {
                "type": "semantic_join",
                "join_key": "business_registration_number",
                "nodes": ["contracts", "status"],
            },
        },
    }
