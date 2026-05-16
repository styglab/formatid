from __future__ import annotations

from typing import Any


def contract_companies(raw_item: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse PPS/G2B corpList into normalized contract company objects."""
    return [
        {
            "sequence": _int(parts, 0),
            "role": _str(parts, 1),
            "joint_contract_type": _str(parts, 2),
            "company_name": _str(parts, 3),
            "representative_name": _str(parts, 4),
            "country": _str(parts, 5),
            "share_rate": _str(parts, 6),
            "creditor_name": _str(parts, 7),
            "contact_name": _str(parts, 8),
            "business_no": _str(parts, 9),
        }
        for parts in _bracket_groups(raw_item.get("corpList"))
    ]


def demand_organizations(raw_item: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse PPS/G2B dminsttList into normalized demand organization objects."""
    return [
        {
            "sequence": _int(parts, 0),
            "organization_code": _str(parts, 1),
            "organization_name": _str(parts, 2),
            "jurisdiction": _str(parts, 3),
            "department_name": _str(parts, 4),
            "officer_name": _str(parts, 5),
            "officer_phone": _str(parts, 6),
        }
        for parts in _bracket_groups(raw_item.get("dminsttList"))
    ]


def _bracket_groups(value: Any) -> list[list[str]]:
    if not value:
        return []
    text = str(value)
    groups = []
    current = []
    in_group = False
    for char in text:
        if char == "[":
            current = []
            in_group = True
            continue
        if char == "]" and in_group:
            groups.append("".join(current).split("^"))
            in_group = False
            continue
        if in_group:
            current.append(char)
    return groups


def _str(parts: list[str], index: int) -> str | None:
    if index >= len(parts):
        return None
    value = parts[index].strip()
    return value or None


def _int(parts: list[str], index: int) -> int | None:
    value = _str(parts, index)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
