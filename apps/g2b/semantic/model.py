from __future__ import annotations

SEMANTIC_MODEL_VERSION = "g2b.procurement.v1"


class EntityType:
    BID_NOTICE = "BidNotice"
    AGENCY = "Agency"
    COMPANY = "Company"
    SUCCESSFUL_BID = "SuccessfulBid"
    CONTRACT = "Contract"
    LICENSE_CONSTRAINT = "LicenseConstraint"
    ALLOWED_INDUSTRY = "AllowedIndustry"
    PARTICIPATION_REGION = "ParticipationRegion"
    PROCUREMENT_CATEGORY = "ProcurementCategory"
    INDUSTRY = "Industry"


class Relationship:
    ISSUED_BY = "issued_by"
    REQUESTED_BY = "requested_by"
    RESULT_OF = "result_of"
    AWARDED_TO = "awarded_to"
    CONTRACTED_BY = "contracted_by"
    CONTRACTED_WITH = "contracted_with"
    REQUIRES = "requires"
    ALLOWS_INDUSTRY = "allows_industry"
    RESTRICTED_TO = "restricted_to"
    CATEGORIZED_AS = "categorized_as"
    LOCATED_IN = "located_in"
    REGISTERED_INDUSTRY = "registered_industry"


class SemanticTag:
    GOVERNMENT_PROCUREMENT = "government_procurement"
    REGULATED_LICENSE = "regulated_license"
    REGION_RESTRICTED = "region_restricted"
    MEDICAL_WASTE = "medical_waste"
    WASTE_MANAGEMENT = "waste_management"
    BUDGET_DISCLOSED = "budget_disclosed"


ENTITY_DEFINITIONS = {
    EntityType.BID_NOTICE: "G2B bid notice",
    EntityType.AGENCY: "G2B notice or demand agency",
    EntityType.COMPANY: "Company involved in bidding or award results",
    EntityType.SUCCESSFUL_BID: "Successful bid award result",
    EntityType.CONTRACT: "G2B contract execution record",
    EntityType.LICENSE_CONSTRAINT: "License or industry participation constraint",
    EntityType.ALLOWED_INDUSTRY: "Industry allowed as an alternative participation qualification",
    EntityType.PARTICIPATION_REGION: "Participation region constraint",
    EntityType.PROCUREMENT_CATEGORY: "Procurement category such as goods, service, construction, or foreign",
    EntityType.INDUSTRY: "Procurement company registered industry",
}

RELATION_DEFINITIONS = {
    Relationship.ISSUED_BY: {
        "source": EntityType.BID_NOTICE,
        "target": EntityType.AGENCY,
        "description": "Agency that issued the bid notice",
    },
    Relationship.REQUESTED_BY: {
        "source": EntityType.BID_NOTICE,
        "target": EntityType.AGENCY,
        "description": "Demand agency that requested the procurement",
    },
    Relationship.RESULT_OF: {
        "source": [EntityType.BID_NOTICE, EntityType.SUCCESSFUL_BID],
        "target": [EntityType.SUCCESSFUL_BID, EntityType.BID_NOTICE],
        "description": "Relationship between a bid notice and its award result",
    },
    Relationship.AWARDED_TO: {
        "source": EntityType.SUCCESSFUL_BID,
        "target": EntityType.COMPANY,
        "description": "Company that won the award",
    },
    Relationship.CONTRACTED_BY: {
        "source": EntityType.CONTRACT,
        "target": EntityType.AGENCY,
        "description": "Agency that signed or manages the contract",
    },
    Relationship.CONTRACTED_WITH: {
        "source": EntityType.CONTRACT,
        "target": EntityType.COMPANY,
        "description": "Company participating in the contract",
    },
    Relationship.REQUIRES: {
        "source": EntityType.BID_NOTICE,
        "target": EntityType.LICENSE_CONSTRAINT,
        "description": "License or industry qualification required for participation",
    },
    Relationship.ALLOWS_INDUSTRY: {
        "source": EntityType.LICENSE_CONSTRAINT,
        "target": EntityType.ALLOWED_INDUSTRY,
        "description": "Alternative industry qualification accepted by a license constraint",
    },
    Relationship.RESTRICTED_TO: {
        "source": EntityType.BID_NOTICE,
        "target": EntityType.PARTICIPATION_REGION,
        "description": "Region allowed for participation",
    },
    Relationship.CATEGORIZED_AS: {
        "source": EntityType.BID_NOTICE,
        "target": EntityType.PROCUREMENT_CATEGORY,
        "description": "Procurement category classification",
    },
    Relationship.LOCATED_IN: {
        "source": EntityType.COMPANY,
        "target": EntityType.PARTICIPATION_REGION,
        "description": "Company location region",
    },
    Relationship.REGISTERED_INDUSTRY: {
        "source": EntityType.COMPANY,
        "target": EntityType.INDUSTRY,
        "description": "Industry registered to a procurement company",
    },
}

VOCABULARY = {
    EntityType.BID_NOTICE: {
        "aliases": ["입찰", "공고", "입찰공고", "tender", "procurement notice"],
    },
    EntityType.SUCCESSFUL_BID: {
        "aliases": ["낙찰", "낙찰결과", "award", "winner"],
    },
    EntityType.CONTRACT: {
        "aliases": ["계약", "계약현황", "contract"],
    },
    EntityType.LICENSE_CONSTRAINT: {
        "aliases": ["면허", "자격", "업종제한", "참가자격", "license"],
    },
    EntityType.PARTICIPATION_REGION: {
        "aliases": ["지역", "참가가능지역", "지역제한", "region"],
    },
    EntityType.COMPANY: {
        "aliases": ["업체", "조달업체", "낙찰업체", "company", "supplier"],
    },
    EntityType.INDUSTRY: {
        "aliases": ["업종", "등록업종", "industry"],
    },
}

TOOL_SPECS = {
    "search_bid": {
        "returns": EntityType.BID_NOTICE,
        "usage": "Use for bid notice search. Questions about posted notices use published_at; deadline questions use deadline_at.",
        "default_sort": "published_at desc",
        "date_semantics": {
            "오늘 올라온 공고": "published_at",
            "공고일": "published_at",
            "마감": "deadline_at",
            "개찰": "opening_at",
        },
        "supports": [
            "category",
            "keyword",
            "notice_kind",
            "agency",
            "demand_org",
            "budget",
            "published_at",
            "deadline_at",
            "opening_at",
            "license_limits",
            "participation_regions",
            "success_bids",
            "semantic",
            "semantic_tags",
            "requires_license",
            "restricted_region",
        ],
    },
    "search_success_bid": {
        "returns": EntityType.SUCCESSFUL_BID,
        "usage": "Use for successful bid or award result search. Questions about award date use final_success_date.",
        "default_sort": "registered_at desc",
        "date_semantics": {
            "낙찰일": "final_success_date",
            "개찰일": "actual_opening_at",
            "등록일": "registered_at",
        },
        "supports": [
            "category",
            "keyword",
            "bid_notice_no",
            "winner",
            "demand_org",
            "registered_at",
            "final_success_date",
            "winning_amount",
            "semantic",
        ],
    },
    "search_contract": {
        "returns": EntityType.CONTRACT,
        "usage": "Use for contract execution search. Questions like '오늘 계약한 건' must use contract_date, not registered_at.",
        "default_sort": "contract_date desc",
        "date_semantics": {
            "오늘 계약": "contract_date",
            "계약일": "contract_date",
            "계약체결일": "contract_concluded_date",
            "등록일": "registered_at",
        },
        "supports": [
            "category",
            "keyword",
            "contract_no",
            "bid_notice_no",
            "contract_org",
            "company",
            "business_no",
            "contract_date",
            "contract_concluded_date",
            "registered_at",
            "amount",
            "semantic",
        ],
    },
    "get_bid_context": {
        "returns": EntityType.BID_NOTICE,
        "usage": "Use for one exact bid notice with semantic context, license constraints, regions, and award links.",
        "supports": ["bid_notice_no", "bid_notice_order", "semantic_document"],
    },
    "get_procurement_lifecycle": {
        "returns": "ProcurementLifecycle",
        "usage": "Use when the user asks for a bid notice lifecycle across notice, constraints, award, contract, companies, and demand organizations.",
        "supports": ["bid_notice_no", "bid_notice_order", "category"],
    },
    "get_tool_capabilities": {
        "returns": "ToolCapabilities",
        "supports": ["semantic_model_version", "entities", "relationships", "vocabulary", "tools"],
    },
}
