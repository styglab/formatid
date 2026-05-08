from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo


G2B_TIMEZONE = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class G2BIngestWindow:
    begin: str
    end: str


def clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def first_present(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if clean(value) is not None:
            return value
    return None


def parse_count(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    cleaned = clean(value)
    if cleaned is None:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_decimal(value: Any) -> Decimal | None:
    cleaned = clean(value)
    if cleaned is None:
        return None
    try:
        return Decimal(cleaned.replace(",", ""))
    except InvalidOperation:
        return None


def parse_g2b_datetime(value: Any) -> datetime | None:
    cleaned = clean(value)
    if cleaned is None:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return parsed.replace(tzinfo=G2B_TIMEZONE)
        except ValueError:
            continue
    return None


def split_name_code(value: Any) -> tuple[str | None, str | None]:
    cleaned = clean(value)
    if cleaned is None:
        return None, None
    if "/" not in cleaned:
        return cleaned, None
    name, code = cleaned.rsplit("/", 1)
    return clean(name), clean(code)
