from __future__ import annotations

from typing import Any

from core.semantic import EntityRef, SemanticDocument, SemanticObject, SemanticRelationship

from apps.g2b.ontology import EntityType, Relationship, infer_bid_notice_tags


def build_bid_notice_semantic_object(
    bid: dict[str, Any],
    *,
    license_limits: list[dict[str, Any]] | None = None,
    participation_regions: list[dict[str, Any]] | None = None,
    success_bids: list[dict[str, Any]] | None = None,
) -> SemanticObject:
    license_limits = license_limits or []
    participation_regions = participation_regions or []
    success_bids = success_bids or []
    entity_id = _bid_entity_id(bid)
    relationships: list[SemanticRelationship] = []

    organization_name = bid.get("organization_name")
    if organization_name:
        relationships.append(
            {
                "predicate": Relationship.ISSUED_BY,
                "target": _entity_ref(EntityType.AGENCY, str(organization_name), str(organization_name)),
            }
        )

    demand_org_name = bid.get("demand_org_name")
    if demand_org_name:
        relationships.append(
            {
                "predicate": Relationship.REQUESTED_BY,
                "target": _entity_ref(EntityType.AGENCY, str(demand_org_name), str(demand_org_name)),
            }
        )

    category = bid.get("category")
    category_label = bid.get("category_label")
    if category:
        relationships.append(
            {
                "predicate": Relationship.CATEGORIZED_AS,
                "target": _entity_ref(
                    EntityType.PROCUREMENT_CATEGORY,
                    str(category),
                    str(category_label or category),
                ),
            }
        )

    for license_limit in license_limits:
        license_name = str(license_limit.get("license_limit_name") or "unknown")
        license_code = license_limit.get("license_limit_code")
        license_constraint_id = str(license_code or license_name)
        relationships.append(
            {
                "predicate": Relationship.REQUIRES,
                "target": _entity_ref(
                    EntityType.LICENSE_CONSTRAINT,
                    license_constraint_id,
                    license_name,
                    attributes={
                        "license_code": license_code,
                        "allowed_industries": license_limit.get("allowed_industries") or [],
                        "main_field_groups": license_limit.get("main_field_groups") or [],
                    },
                ),
                "attributes": {
                    "limit_group_no": license_limit.get("limit_group_no"),
                    "limit_serial_no": license_limit.get("limit_serial_no"),
                },
            }
        )
        for industry in license_limit.get("allowed_industries") or []:
            industry_name = str(industry.get("name") or "unknown")
            industry_code = industry.get("code")
            relationships.append(
                {
                    "predicate": Relationship.ALLOWS_INDUSTRY,
                    "target": _entity_ref(
                        EntityType.ALLOWED_INDUSTRY,
                        str(industry_code or industry_name),
                        industry_name,
                        attributes={"industry_code": industry_code},
                    ),
                    "attributes": {
                        "license_constraint_id": license_constraint_id,
                        "license_constraint_name": license_name,
                        "limit_group_no": license_limit.get("limit_group_no"),
                        "limit_serial_no": license_limit.get("limit_serial_no"),
                    },
                }
            )

    for region in participation_regions:
        region_name = str(region.get("region_name") or "unknown")
        region_code = region.get("region_code")
        relationships.append(
            {
                "predicate": Relationship.RESTRICTED_TO,
                "target": _entity_ref(
                    EntityType.PARTICIPATION_REGION,
                    str(region_code or region_name),
                    region_name,
                    attributes={"region_code": region_code},
                ),
                "attributes": {
                    "limit_group_no": region.get("limit_group_no"),
                    "limit_serial_no": region.get("limit_serial_no"),
                },
            }
        )

    for success_bid in success_bids:
        success_bid_id = str(success_bid.get("resource_key") or success_bid.get("id") or "unknown")
        winner_name = success_bid.get("winner_name")
        relationships.append(
            {
                "predicate": Relationship.RESULT_OF,
                "target": _entity_ref(
                    EntityType.SUCCESSFUL_BID,
                    success_bid_id,
                    str(winner_name or success_bid.get("title") or success_bid_id),
                    attributes={
                        "winning_amount": success_bid.get("winning_amount"),
                        "winning_rate": success_bid.get("winning_rate"),
                        "final_success_date": success_bid.get("final_success_date"),
                    },
                ),
            }
        )
        if winner_name:
            relationships.append(
                {
                    "predicate": Relationship.AWARDED_TO,
                    "target": _entity_ref(
                        EntityType.COMPANY,
                        str(success_bid.get("winner_business_no") or winner_name),
                        str(winner_name),
                        attributes={
                            "business_no": success_bid.get("winner_business_no"),
                            "ceo_name": success_bid.get("winner_ceo_name"),
                            "address": success_bid.get("winner_address"),
                        },
                    ),
                    "attributes": {
                        "successful_bid_id": success_bid_id,
                        "winning_amount": success_bid.get("winning_amount"),
                        "winning_rate": success_bid.get("winning_rate"),
                    },
                }
            )

    tags = infer_bid_notice_tags(
        bid=bid,
        license_limits=license_limits,
        participation_regions=participation_regions,
    )
    return {
        "entity_type": EntityType.BID_NOTICE,
        "entity_id": entity_id,
        "label": str(bid.get("title") or entity_id),
        "attributes": {
            "category": bid.get("category"),
            "category_label": bid.get("category_label"),
            "bid_notice_no": bid.get("bid_notice_no"),
            "bid_notice_order": bid.get("bid_notice_order"),
            "budget": bid.get("budget"),
            "published_at": bid.get("published_at"),
            "deadline_at": bid.get("deadline_at"),
            "opening_at": bid.get("opening_at"),
            "contract_method": bid.get("contract_method"),
            "bid_method": bid.get("bid_method"),
            "notice_kind": bid.get("notice_kind"),
            "detail_url": bid.get("detail_url"),
        },
        "relationships": relationships,
        "semantic_tags": tags,
    }


def build_bid_notice_semantic_document(semantic_object: SemanticObject) -> SemanticDocument:
    attributes = semantic_object["attributes"]
    relationships = semantic_object["relationships"]
    license_labels = [
        relationship["target"].get("label")
        for relationship in relationships
        if relationship["predicate"] == Relationship.REQUIRES
    ]
    allowed_industry_labels = [
        relationship["target"].get("label")
        for relationship in relationships
        if relationship["predicate"] == Relationship.ALLOWS_INDUSTRY
    ]
    region_labels = [
        relationship["target"].get("label")
        for relationship in relationships
        if relationship["predicate"] == Relationship.RESTRICTED_TO
    ]
    winner_labels = [
        relationship["target"].get("label")
        for relationship in relationships
        if relationship["predicate"] == Relationship.AWARDED_TO
    ]
    text_parts = [
        f"입찰공고: {semantic_object['label']}",
        f"업무구분: {attributes.get('category_label') or attributes.get('category')}",
    ]
    if attributes.get("deadline_at"):
        text_parts.append(f"마감일시: {attributes['deadline_at']}")
    if license_labels:
        text_parts.append(f"필요 면허/업종 제한: {', '.join(str(label) for label in license_labels if label)}")
    if allowed_industry_labels:
        text_parts.append(f"대체 허용 업종: {', '.join(str(label) for label in allowed_industry_labels if label)}")
    if region_labels:
        text_parts.append(f"참가가능지역 제한: {', '.join(str(label) for label in region_labels if label)}")
    if winner_labels:
        text_parts.append(f"낙찰업체: {', '.join(str(label) for label in winner_labels if label)}")

    return {
        "document_id": f"bid_notice:{semantic_object['entity_id']}",
        "entity": {
            "entity_type": semantic_object["entity_type"],
            "entity_id": semantic_object["entity_id"],
            "label": semantic_object["label"],
        },
        "title": semantic_object["label"],
        "text": "\n".join(text_parts),
        "metadata": {
            "category": attributes.get("category"),
            "category_label": attributes.get("category_label"),
            "deadline_at": attributes.get("deadline_at"),
            "semantic_tags": semantic_object["semantic_tags"],
        },
        "relationships": relationships,
        "semantic_tags": semantic_object["semantic_tags"],
    }


def _bid_entity_id(bid: dict[str, Any]) -> str:
    return str(
        bid.get("resource_key")
        or bid.get("id")
        or f"{bid.get('category', 'unknown')}:{bid.get('bid_notice_no', 'unknown')}:{bid.get('bid_notice_order', '000')}"
    )


def _entity_ref(
    entity_type: str,
    entity_id: str,
    label: str | None,
    *,
    attributes: dict[str, Any] | None = None,
) -> EntityRef:
    ref: EntityRef = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "label": label,
    }
    if attributes:
        ref["attributes"] = attributes
    return ref
