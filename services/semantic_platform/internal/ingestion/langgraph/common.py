from __future__ import annotations

import os


def resolve_llm_mode() -> str:
    mode = os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = str(mode).strip().lower()
    if normalized in {"disabled", "codex_manual", "openai"}:
        return normalized
    return "disabled"
