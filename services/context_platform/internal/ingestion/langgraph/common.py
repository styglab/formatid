from __future__ import annotations

import os


def resolve_llm_mode(override: str | None = None) -> str:
    mode = override or os.getenv("CONTEXT_PLATFORM_AGENT_MODE") or os.getenv("AGENT_MODE") or os.getenv("CONTEXT_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = str(mode).strip().lower()
    if normalized in {"manual", "agent_manual", "codex_manual"}:
        return "agent_manual"
    if normalized in {"disabled", ""}:
        return "disabled"
    if normalized == "openai":
        raise RuntimeError(
            "openai is no longer supported for Context Platform ingestion; "
            "use AGENT_MODE=manual or --agent-mode manual with an explicit agent response artifact"
        )
    return "disabled"


def is_agent_manual_mode(mode: str | None) -> bool:
    return resolve_llm_mode(mode) == "agent_manual"
