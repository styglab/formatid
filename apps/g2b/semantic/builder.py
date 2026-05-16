from __future__ import annotations

from typing import Any

from core.semantic import build_semantic_document, build_semantic_object, relationship

from apps.g2b.semantic.model import EntityType, Relationship
from apps.g2b.semantic.rules import infer_bid_notice_tags
from apps.g2b.semantic.spec import G2B_SEMANTIC_SPEC


def build_bid_notice_semantic_object(
    bid: dict[str, Any],
    *,
    license_limits: list[dict[str, Any]] | None = None,
    participation_regions: list[dict[str, Any]] | None = None,
    success_bids: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    license_limits = license_limits or []
    participation_regions = participation_regions or []
    return build_semantic_object(
        G2B_SEMANTIC_SPEC,
        EntityType.BID_NOTICE,
        bid,
        relationships=[
            *_license_relationships(license_limits),
            *_region_relationships(participation_regions),
            *_award_relationships(success_bids or []),
        ],
        semantic_tags=infer_bid_notice_tags(
            bid=bid,
            license_limits=license_limits,
            participation_regions=participation_regions,
        ),
    )


def build_success_bid_semantic_object(success_bid: dict[str, Any]) -> dict[str, Any]:
    return build_semantic_object(
        G2B_SEMANTIC_SPEC,
        EntityType.SUCCESSFUL_BID,
        success_bid,
    )


def build_contract_semantic_object(
    contract: dict[str, Any],
    *,
    companies: list[dict[str, Any]] | None = None,
    demand_organizations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return build_semantic_object(
        G2B_SEMANTIC_SPEC,
        EntityType.CONTRACT,
        contract,
        relationships=[
            *_contract_company_relationships(companies or []),
            *_contract_demand_org_relationships(demand_organizations or []),
        ],
    )


def build_bid_notice_semantic_document(semantic_object: dict[str, Any]) -> dict[str, Any]:
    document = build_semantic_document(G2B_SEMANTIC_SPEC, semantic_object)
    document["document_id"] = f"bid_notice:{semantic_object['entity_id']}"
    return document


def _license_relationships(license_limits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relationships = []
    for limit in license_limits:
        license_name = str(limit.get("license_limit_name") or "unknown")
        constraint_id = str(limit.get("license_limit_code") or license_name)
        limit_context = {
            "limit_group_no": limit.get("limit_group_no"),
            "limit_serial_no": limit.get("limit_serial_no"),
        }
        relationships.append(
            relationship(
                Relationship.REQUIRES,
                EntityType.LICENSE_CONSTRAINT,
                license_name,
                entity_id=constraint_id,
                target_attributes={
                    "license_code": limit.get("license_limit_code"),
                    "allowed_industries": limit.get("allowed_industries") or [],
                    "main_field_groups": limit.get("main_field_groups") or [],
                },
                relationship_attributes=limit_context,
            )
        )
        relationships.extend(_allowed_industry_relationships(limit, constraint_id, license_name, limit_context))
    return relationships


def _contract_company_relationships(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relationships = []
    for company in companies:
        company_name = str(company.get("company_name") or "unknown")
        relationships.append(
            relationship(
                Relationship.CONTRACTED_WITH,
                EntityType.COMPANY,
                company_name,
                entity_id=str(company.get("business_no") or company_name),
                target_attributes={
                    "business_no": company.get("business_no"),
                    "ceo_name": company.get("ceo_name"),
                    "country_name": company.get("country_name"),
                },
                relationship_attributes={
                    "role_name": company.get("role_name"),
                    "contract_type_name": company.get("contract_type_name"),
                    "share_rate": company.get("share_rate"),
                },
            )
        )
    return relationships


def _contract_demand_org_relationships(demand_organizations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relationships = []
    for organization in demand_organizations:
        organization_name = str(organization.get("organization_name") or "unknown")
        relationships.append(
            relationship(
                Relationship.REQUESTED_BY,
                EntityType.AGENCY,
                organization_name,
                entity_id=str(organization.get("organization_code") or organization_name),
                target_attributes={
                    "organization_code": organization.get("organization_code"),
                    "jurisdiction_name": organization.get("jurisdiction_name"),
                    "department_name": organization.get("department_name"),
                    "officer_name": organization.get("officer_name"),
                },
            )
        )
    return relationships


def _allowed_industry_relationships(
    limit: dict[str, Any],
    constraint_id: str,
    license_name: str,
    limit_context: dict[str, Any],
) -> list[dict[str, Any]]:
    relationships = []
    for industry in limit.get("allowed_industries") or []:
        industry_name = str(industry.get("name") or "unknown")
        relationships.append(
            relationship(
                Relationship.ALLOWS_INDUSTRY,
                EntityType.ALLOWED_INDUSTRY,
                industry_name,
                entity_id=str(industry.get("code") or industry_name),
                target_attributes={"industry_code": industry.get("code")},
                relationship_attributes={
                    **limit_context,
                    "license_constraint_id": constraint_id,
                    "license_constraint_name": license_name,
                },
            )
        )
    return relationships


def _region_relationships(regions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relationships = []
    for region in regions:
        region_name = str(region.get("region_name") or "unknown")
        relationships.append(
            relationship(
                Relationship.RESTRICTED_TO,
                EntityType.PARTICIPATION_REGION,
                region_name,
                entity_id=str(region.get("region_code") or region_name),
                target_attributes={"region_code": region.get("region_code")},
                relationship_attributes={
                    "limit_group_no": region.get("limit_group_no"),
                    "limit_serial_no": region.get("limit_serial_no"),
                },
            )
        )
    return relationships


def _award_relationships(success_bids: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relationships = []
    for success_bid in success_bids:
        success_semantic = build_success_bid_semantic_object(success_bid)
        relationships.append(
            relationship(
                Relationship.RESULT_OF,
                EntityType.SUCCESSFUL_BID,
                success_semantic["label"],
                entity_id=success_semantic["entity_id"],
                target_attributes=success_semantic["attributes"],
            )
        )
        relationships.extend(
            item for item in success_semantic["relationships"] if item["predicate"] == Relationship.AWARDED_TO
        )
    return relationships
