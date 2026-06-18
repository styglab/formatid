from __future__ import annotations

from typing import Any

from pydantic import BaseModel


def paged(items: list[dict[str, Any]], page: int, page_size: int) -> dict[str, Any]:
    safe_page = max(page, 1)
    safe_page_size = max(min(page_size, 100), 1)
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    return {
        "items": items[start:end],
        "total": len(items),
        "page": safe_page,
        "page_size": safe_page_size,
    }


def payload_dict(model: BaseModel, *, exclude_unset: bool) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_none=True, exclude_unset=exclude_unset)
    return model.dict(exclude_none=True, exclude_unset=exclude_unset)
