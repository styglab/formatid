from __future__ import annotations

from typing import Any

from shared.checkpoints.postgres import PostgresCheckpointStore


async def set_bid_list_window_checkpoint(
    checkpoint_store: PostgresCheckpointStore,
    name: str,
    value: dict[str, Any],
) -> None:
    existing = await checkpoint_store.get(name)
    merged_value = _merge_bid_list_window_checkpoint(
        existing_value=None if existing is None else existing.get("value"),
        incoming_value=value,
    )
    await checkpoint_store.set(name, merged_value)


def _merge_bid_list_window_checkpoint(
    *,
    existing_value: Any,
    incoming_value: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(existing_value, dict):
        value = dict(incoming_value)
    else:
        value = dict(existing_value)
        value.update(incoming_value)

    existing_status = existing_value.get("status") if isinstance(existing_value, dict) else None
    incoming_status = incoming_value.get("status")
    if existing_status == "succeeded" and incoming_status in {"running", "failed"}:
        value["status"] = "succeeded"
    elif incoming_status is not None:
        value["status"] = incoming_status

    existing_page = _optional_int(existing_value.get("last_completed_page") if isinstance(existing_value, dict) else None)
    incoming_page = _optional_int(incoming_value.get("last_completed_page"))
    if existing_page is not None or incoming_page is not None:
        value["last_completed_page"] = max(page for page in (existing_page, incoming_page) if page is not None)

    total_count = _optional_int(value.get("total_count"))
    num_of_rows = _optional_int(value.get("num_of_rows"))
    last_completed_page = _optional_int(value.get("last_completed_page"))
    if value.get("status") == "succeeded":
        value["next_page_no"] = None
    elif total_count is not None and num_of_rows and last_completed_page is not None:
        value["next_page_no"] = last_completed_page + 1 if last_completed_page * num_of_rows < total_count else None
    return value


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
