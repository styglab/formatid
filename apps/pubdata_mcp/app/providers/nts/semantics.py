from __future__ import annotations

from typing import Any


MAX_BATCH_SIZE = 100


def normalize_business_numbers(values: list[str]) -> list[str]:
    if not isinstance(values, list):
        raise ValueError("business_numbers must be a list.")
    normalized = [_business_number(value) for value in values]
    if not normalized:
        raise ValueError("business_numbers must contain at least one business number.")
    if len(normalized) > MAX_BATCH_SIZE:
        raise ValueError("NTS API supports at most 100 business numbers per call.")
    return normalized


def normalize_validation_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("businesses must be a list.")
    if not records:
        raise ValueError("businesses must contain at least one record.")
    if len(records) > MAX_BATCH_SIZE:
        raise ValueError("NTS API supports at most 100 validation records per call.")

    normalized = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("each business record must be an object.")
        normalized.append(_validation_record(record))
    return normalized


def _validation_record(record: dict[str, Any]) -> dict[str, Any]:
    required = {
        "b_no": _business_number(record.get("b_no") or record.get("business_no")),
        "start_dt": _date_yyyymmdd(record.get("start_dt") or record.get("start_date")),
        "p_nm": _required_text(record.get("p_nm") or record.get("owner_name"), "p_nm"),
    }
    optional_fields = {
        "p_nm2": record.get("p_nm2") or record.get("owner_name_2"),
        "b_nm": record.get("b_nm") or record.get("company_name"),
        "corp_no": record.get("corp_no") or record.get("corporation_no"),
        "b_sector": record.get("b_sector") or record.get("business_sector"),
        "b_type": record.get("b_type") or record.get("business_type"),
        "b_adr": record.get("b_adr") or record.get("business_address"),
    }
    return {
        **required,
        **{
            key: "" if value is None else str(value).strip()
            for key, value in optional_fields.items()
        },
    }


def _business_number(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) != 10:
        raise ValueError("business number must be 10 digits.")
    return digits


def _date_yyyymmdd(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) != 8:
        raise ValueError("start date must be YYYYMMDD.")
    return digits


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required.")
    return text
