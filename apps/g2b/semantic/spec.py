from __future__ import annotations

from apps.g2b.semantic.model import EntityType, Relationship, SEMANTIC_MODEL_VERSION

G2B_SEMANTIC_SPEC = {
    "version": SEMANTIC_MODEL_VERSION,
    "entities": {
        EntityType.BID_NOTICE: {
            "id": {"first_of": ["id", {"template": "{category}:{bid_notice_no}:{bid_notice_order}"}]},
            "label": "title",
            "attributes": {
                "title": "title",
                "category": "category",
                "category_label": "category_label",
                "bid_notice_no": "bid_notice_no",
                "bid_notice_order": "bid_notice_order",
                "budget": "budget",
                "published_at": "published_at",
                "deadline_at": "deadline_at",
                "opening_at": "opening_at",
                "contract_method": "contract_method",
                "bid_method": "bid_method",
                "notice_kind": "notice_kind",
                "detail_url": "detail_url",
            },
            "relationships": [
                {
                    "predicate": Relationship.ISSUED_BY,
                    "target_entity": EntityType.AGENCY,
                    "target_id": {"first_of": ["notice_agency_code", "notice_agency_name", "organization_name"]},
                    "target_label": {"first_of": ["notice_agency_name", "organization_name"]},
                    "target_attributes": {
                        "agency_code": "notice_agency_code",
                    },
                },
                {
                    "predicate": Relationship.REQUESTED_BY,
                    "target_entity": EntityType.AGENCY,
                    "target_id": {"first_of": ["demand_agency_code", "demand_agency_name", "demand_org_name"]},
                    "target_label": {"first_of": ["demand_agency_name", "demand_org_name"]},
                    "target_attributes": {
                        "agency_code": "demand_agency_code",
                    },
                },
                {
                    "predicate": Relationship.CATEGORIZED_AS,
                    "target_entity": EntityType.PROCUREMENT_CATEGORY,
                    "target_id": "category",
                    "target_label": "category_label",
                },
            ],
        },
        EntityType.SUCCESSFUL_BID: {
            "id": "id",
            "label": {"first_of": ["winner_name", "title", "id"]},
            "attributes": {
                "category": "category",
                "category_label": "category_label",
                "bid_notice_no": "bid_notice_no",
                "bid_notice_order": "bid_notice_order",
                "winner_name": "winner_name",
                "winner_business_no": "winner_business_no",
                "winning_amount": "winning_amount",
                "winning_rate": "winning_rate",
                "registered_at": "registered_at",
                "final_success_date": "final_success_date",
            },
            "relationships": [
                {
                    "predicate": Relationship.RESULT_OF,
                    "target_entity": EntityType.BID_NOTICE,
                    "target_id": {"template": "{category}:{bid_notice_no}:{bid_notice_order}"},
                    "target_label": "title",
                    "target_attributes": {
                        "bid_notice_no": "bid_notice_no",
                        "bid_notice_order": "bid_notice_order",
                    },
                },
                {
                    "predicate": Relationship.AWARDED_TO,
                    "target_entity": EntityType.COMPANY,
                    "target_id": "winner_business_no",
                    "target_label": "winner_name",
                    "target_attributes": {
                        "business_no": "winner_business_no",
                        "ceo_name": "winner_ceo_name",
                        "address": "winner_address",
                    },
                    "attributes": {
                        "winning_amount": "winning_amount",
                        "winning_rate": "winning_rate",
                    },
                },
                {
                    "predicate": Relationship.REQUESTED_BY,
                    "target_entity": EntityType.AGENCY,
                    "target_id": "demand_org_name",
                    "target_label": "demand_org_name",
                },
                {
                    "predicate": Relationship.CATEGORIZED_AS,
                    "target_entity": EntityType.PROCUREMENT_CATEGORY,
                    "target_id": "category",
                    "target_label": "category_label",
                },
            ],
            "semantic_tags": ["successful_bid"],
        },
        EntityType.CONTRACT: {
            "id": "id",
            "label": {"first_of": ["contract_name", "unified_contract_no", "id"]},
            "attributes": {
                "category": "category",
                "category_label": "category_label",
                "unified_contract_no": "unified_contract_no",
                "decision_contract_no": "decision_contract_no",
                "contract_ref_no": "contract_ref_no",
                "contract_name": "contract_name",
                "bid_notice_no": "bid_notice_no",
                "contract_method": "contract_method",
                "current_contract_amount": "current_contract_amount",
                "total_contract_amount": "total_contract_amount",
                "contract_date": "contract_date",
                "contract_concluded_date": "contract_concluded_date",
                "registered_at": "registered_at",
                "detail_url": "detail_url",
            },
            "relationships": [
                {
                    "predicate": Relationship.CONTRACTED_BY,
                    "target_entity": EntityType.AGENCY,
                    "target_id": {"first_of": ["contract_org_code", "contract_org_name"]},
                    "target_label": "contract_org_name",
                    "target_attributes": {
                        "organization_code": "contract_org_code",
                        "department_name": "contract_org_department_name",
                        "officer_name": "contract_org_officer_name",
                    },
                },
                {
                    "predicate": Relationship.RESULT_OF,
                    "target_entity": EntityType.BID_NOTICE,
                    "target_id": {"template": "{category}:{bid_notice_no}:000"},
                    "target_label": "bid_notice_no",
                    "target_attributes": {
                        "bid_notice_no": "bid_notice_no",
                    },
                },
                {
                    "predicate": Relationship.CATEGORIZED_AS,
                    "target_entity": EntityType.PROCUREMENT_CATEGORY,
                    "target_id": "category",
                    "target_label": "category_label",
                },
            ],
            "semantic_tags": ["contract"],
        },
    },
    "documents": {
        EntityType.BID_NOTICE: {
            "attribute_lines": [
                {"label": "입찰공고", "attribute": "title"},
                {"label": "업무구분", "attribute": "category_label"},
                {"label": "마감일시", "attribute": "deadline_at"},
            ],
            "relationship_lines": [
                {"label": "필요 면허/업종 제한", "predicate": Relationship.REQUIRES},
                {"label": "대체 허용 업종", "predicate": Relationship.ALLOWS_INDUSTRY},
                {"label": "참가가능지역 제한", "predicate": Relationship.RESTRICTED_TO},
                {"label": "낙찰업체", "predicate": Relationship.AWARDED_TO},
            ],
            "metadata_attributes": ["category", "category_label", "deadline_at"],
        }
    },
}
