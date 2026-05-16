from __future__ import annotations

from dataclasses import dataclass

from apps.g2b.semantic.model import EntityType


DEFAULT_SCHEMA = "g2b"


@dataclass(frozen=True)
class CanonicalTable:
    entity_type: str
    raw_table: str | None
    normalized_table: str
    identity_columns: tuple[str, ...]
    relationship_columns: tuple[str, ...] = ()


BID_NOTICE_TABLE = CanonicalTable(
    entity_type=EntityType.BID_NOTICE,
    raw_table="bid_public_notice_raw",
    normalized_table="bid_public_notice",
    identity_columns=("category", "bid_notice_no", "bid_notice_order"),
    relationship_columns=("notice_agency_code", "notice_agency_name", "demand_agency_code", "demand_agency_name"),
)

LICENSE_CONSTRAINT_TABLE = CanonicalTable(
    entity_type=EntityType.LICENSE_CONSTRAINT,
    raw_table="bid_public_notice_license_limit_raw",
    normalized_table="bid_public_notice_license_limit",
    identity_columns=("bid_notice_no", "bid_notice_order", "limit_group_no", "limit_serial_no"),
    relationship_columns=("license_limit_code", "license_limit_name", "allowed_industries"),
)

PARTICIPATION_REGION_TABLE = CanonicalTable(
    entity_type=EntityType.PARTICIPATION_REGION,
    raw_table="bid_public_notice_participation_region_raw",
    normalized_table="bid_public_notice_participation_region",
    identity_columns=("bid_notice_no", "bid_notice_order", "limit_group_no", "limit_serial_no"),
    relationship_columns=("region_code", "region_name"),
)

SUCCESSFUL_BID_TABLE = CanonicalTable(
    entity_type=EntityType.SUCCESSFUL_BID,
    raw_table="successful_bid_raw",
    normalized_table="successful_bid",
    identity_columns=("category", "bid_notice_no", "bid_notice_order", "bid_classification_no", "rebid_no"),
    relationship_columns=("winner_business_no", "winner_name", "demand_org_code", "demand_org_name"),
)

CONTRACT_TABLE = CanonicalTable(
    entity_type=EntityType.CONTRACT,
    raw_table="contract_raw",
    normalized_table="contract",
    identity_columns=("category", "unified_contract_no"),
    relationship_columns=("contract_org_code", "contract_org_name", "bid_notice_no"),
)

CONTRACT_COMPANY_TABLE = CanonicalTable(
    entity_type=EntityType.COMPANY,
    raw_table=None,
    normalized_table="contract_company",
    identity_columns=("contract_resource_key", "sequence_no"),
    relationship_columns=("business_no", "company_name", "role_name"),
)

CONTRACT_DEMAND_ORG_TABLE = CanonicalTable(
    entity_type=EntityType.AGENCY,
    raw_table=None,
    normalized_table="contract_demand_organization",
    identity_columns=("contract_resource_key", "sequence_no"),
    relationship_columns=("organization_code", "organization_name"),
)

PROCUREMENT_COMPANY_TABLE = CanonicalTable(
    entity_type=EntityType.COMPANY,
    raw_table="procurement_company_raw",
    normalized_table="procurement_company",
    identity_columns=("business_no",),
    relationship_columns=("company_name", "region_code", "region_name"),
)

PROCUREMENT_COMPANY_INDUSTRY_TABLE = CanonicalTable(
    entity_type=EntityType.INDUSTRY,
    raw_table="procurement_company_industry_raw",
    normalized_table="procurement_company_industry",
    identity_columns=("business_no", "industry_code"),
    relationship_columns=("industry_code", "industry_name"),
)

CANONICAL_TABLES = {
    EntityType.BID_NOTICE: BID_NOTICE_TABLE,
    EntityType.LICENSE_CONSTRAINT: LICENSE_CONSTRAINT_TABLE,
    EntityType.PARTICIPATION_REGION: PARTICIPATION_REGION_TABLE,
    EntityType.SUCCESSFUL_BID: SUCCESSFUL_BID_TABLE,
    EntityType.CONTRACT: CONTRACT_TABLE,
    EntityType.COMPANY: PROCUREMENT_COMPANY_TABLE,
    EntityType.INDUSTRY: PROCUREMENT_COMPANY_INDUSTRY_TABLE,
}


CANONICAL_COLUMN_NOTES = {
    "resource_key": "Pipeline-wide stable row identity used for idempotent upsert.",
    "category": "Normalized procurement category: GOODS, SERVICE, CONSTRUCTION, or FOREIGN.",
    "bid_notice_no": "G2B bid notice number. This is a domain identifier, not a DB surrogate key.",
    "bid_notice_order": "G2B bid notice revision/order.",
    "bid_classification_no": "Successful-bid classification number from the award API.",
    "rebid_no": "Rebid number from the award API.",
    "notice_agency_code": "Notice issuer agency code when available.",
    "notice_agency_name": "Notice issuer agency name when available.",
    "demand_agency_code": "Demand/requesting agency code when available.",
    "demand_agency_name": "Demand/requesting agency name.",
    "license_limit_name": "Required license or industry constraint label.",
    "license_limit_code": "Required license or industry constraint code.",
    "allowed_industries": "Alternative industries accepted for the license constraint.",
    "region_name": "Eligible participation region label.",
    "region_code": "Eligible participation region code.",
    "winner_name": "Successful bidder company name.",
    "winner_business_no": "Successful bidder business registration number.",
    "winning_amount": "Final successful bid amount in KRW.",
    "winning_rate": "Final successful bid rate against expected price.",
    "unified_contract_no": "G2B unified contract number.",
    "decision_contract_no": "G2B decision contract number.",
    "contract_ref_no": "G2B contract reference number.",
    "contract_name": "Contract title or project name.",
    "contract_resource_key": "Stable contract identity used by relation tables.",
    "contract_org_code": "Code of the agency that signed or manages the contract.",
    "contract_org_name": "Name of the agency that signed or manages the contract.",
    "current_contract_amount": "Current contract amount in KRW.",
    "total_contract_amount": "Total contract amount in KRW.",
    "business_no": "Company business registration number.",
    "company_name": "Procurement company name.",
    "industry_code": "Procurement company registered industry code.",
    "industry_name": "Procurement company registered industry name.",
}
