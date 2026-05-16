from __future__ import annotations

from typing import Any

from apps.g2b.semantic.model import SemanticTag


def infer_bid_notice_tags(
    *,
    bid: dict[str, Any],
    license_limits: list[dict[str, Any]],
    participation_regions: list[dict[str, Any]],
) -> list[str]:
    tags = {SemanticTag.GOVERNMENT_PROCUREMENT}

    if bid.get("budget") is not None:
        tags.add(SemanticTag.BUDGET_DISCLOSED)
    if license_limits:
        tags.add(SemanticTag.REGULATED_LICENSE)
    if participation_regions:
        tags.add(SemanticTag.REGION_RESTRICTED)

    for license_limit in license_limits:
        name = str(license_limit.get("license_limit_name") or "")
        if "폐기물" in name:
            tags.add(SemanticTag.WASTE_MANAGEMENT)
        if "의료폐기물" in name:
            tags.add(SemanticTag.MEDICAL_WASTE)

    return sorted(tags)
