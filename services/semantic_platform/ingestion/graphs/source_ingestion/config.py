from __future__ import annotations

import os
from pathlib import Path

DEFAULT_SOURCE = Path("sources/국세청_사업자등록정보 진위확인 및 상태조회 서비스.md")
DEFAULT_OUTPUT_DIR = Path("sources/proposals")
DEFAULT_CHUNKS_OUTPUT_DIR = Path("sources/chunks")
DEFAULT_CATALOG_DIR = Path("services/semantic_platform/catalog")

SOURCE_LLM_API_URL = os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_API_URL", "https://api.openai.com/v1/chat/completions")
SOURCE_LLM_MODEL = os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_MODEL", "gpt-4.1-mini")
SOURCE_LLM_TIMEOUT_SECONDS = float(os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_TIMEOUT_SECONDS", "45"))
SOURCE_LLM_MAX_TEXT_CHARS = int(os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_MAX_TEXT_CHARS", "30000"))
LLM_MODES = {"disabled", "codex_manual", "openai"}
SOURCE_COMMIT_MODES = {"proposal", "direct_apply"}
