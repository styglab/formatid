from __future__ import annotations

import json
import os
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError


OPENAI_API_URL = os.getenv("SEMANTIC_PLATFORM_LLM_API_URL", "https://api.openai.com/v1/chat/completions")
OPENAI_MODEL = os.getenv("SEMANTIC_PLATFORM_LLM_MODEL", "gpt-4.1-mini")
LLM_TIMEOUT_SECONDS = float(os.getenv("SEMANTIC_PLATFORM_LLM_TIMEOUT_SECONDS", "8"))
LLM_MODES = {"disabled", "codex_manual", "openai"}


def llm_enabled() -> bool:
    return _llm_mode() == "openai"


def parse_intent_with_llm(
    query: str,
    candidates: dict[str, Any],
    fallback_intent: dict[str, Any],
    manual_intent: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    mode = _llm_mode()
    if mode == "codex_manual":
        if not isinstance(manual_intent, dict):
            return None
        parsed = dict(manual_intent)
        parsed["parser"] = {
            "name": "codex_manual_intent_parser",
            "mode": mode,
            "source": "manual_payload",
            "fallback": fallback_intent.get("parser", {}),
        }
        return parsed
    if mode != "openai":
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
                    "You extract semantic intent for a public-data semantic catalog. "
                    "Return only JSON. Use only candidate semantic_types, entities, and metrics when possible. "
                    "Do not choose provider endpoints or tool names."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "query": query,
                        "candidate_context": candidates,
                        "fallback_intent": fallback_intent,
                        "required_schema": {
                            "language": "ko|en|unknown",
                            "entities": ["Organization"],
                            "semantic_types": ["contract_amount"],
                            "capabilities": ["capability_id_from_candidate_context"],
                            "semantic_arguments": {
                                "semantic_type_id": "argument value or object"
                            },
                            "filters": [
                                {
                                    "semantic_type": "contract_amount",
                                    "operator": ">=",
                                    "value": 30000000000,
                                    "unit": "KRW",
                                    "source_text": "300억 이상",
                                }
                            ],
                            "metrics": ["contract_amount_sum"],
                            "constraints": [],
                            "confidence": 0.0,
                        },
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
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with request.urlopen(http_request, timeout=LLM_TIMEOUT_SECONDS) as response:
            response_body = response.read().decode("utf-8")
        document = json.loads(response_body)
        content = document["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except (HTTPError, URLError, TimeoutError, KeyError, IndexError, json.JSONDecodeError, ValueError):
        return None

    if not isinstance(parsed, dict):
        return None
    parsed["parser"] = {
        "name": "llm_intent_parser",
        "model": OPENAI_MODEL,
        "mode": _llm_mode(),
        "fallback": fallback_intent.get("parser", {}),
    }
    return parsed


def _llm_mode() -> str:
    mode = os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = mode.strip().lower()
    return normalized if normalized in LLM_MODES else "disabled"
