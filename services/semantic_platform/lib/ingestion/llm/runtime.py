from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


_openai_api_key: ContextVar[str | None] = ContextVar("semantic_platform_openai_api_key", default=None)
_llm_mode: ContextVar[str | None] = ContextVar("semantic_platform_llm_mode", default=None)
LLM_MODES = {"disabled", "codex_manual", "openai"}


def openai_api_key() -> str | None:
    return _openai_api_key.get() or os.getenv("OPENAI_API_KEY")


def active_llm_mode() -> str:
    mode = _llm_mode.get() or os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = mode.strip().lower()
    return normalized if normalized in LLM_MODES else "disabled"


@contextmanager
def llm_secret_context(openai_key: str | None = None, mode: str | None = None) -> Iterator[None]:
    token = _openai_api_key.set(openai_key) if openai_key else None
    mode_token = _llm_mode.set(mode) if mode else None
    try:
        yield
    finally:
        if mode_token is not None:
            _llm_mode.reset(mode_token)
        if token is not None:
            _openai_api_key.reset(token)
