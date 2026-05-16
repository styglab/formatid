from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from apps.g2b.pipeline.app.steps.bid_notices import CATEGORY_LABELS
from apps.g2b.pipeline.app.steps.common import clean, parse_decimal, parse_g2b_datetime


CONTRACT_URLS = {
    "SERVICE": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch",
    "GOODS": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngPPSSrch",
    "CONSTRUCTION": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch",
    "FOREIGN": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListFrgcptPPSSrch",
}


def contract_resource_key(category: str, record: dict[str, Any]) -> str:
    unified_no = clean(record.get("untyCntrctNo"))
    decision_no = clean(record.get("dcsnCntrctNo"))
    ref_no = clean(record.get("cntrctRefNo"))
    contract_no = unified_no or decision_no or ref_no or "unknown"
    return f"{category}:{contract_no}"


def parse_contract_company_list(value: Any) -> list[dict[str, Any]]:
    companies = []
    for parts in _parse_bracket_records(value):
        companies.append(
            {
                "sequence_no": _get(parts, 0),
                "role_name": _get(parts, 1),
                "contract_type_name": _get(parts, 2),
                "company_name": _get(parts, 3),
                "ceo_name": _get(parts, 4),
                "country_name": _get(parts, 5),
                "share_rate": parse_decimal(_get(parts, 6)),
                "display_company_name": _get(parts, 7),
                "business_no": _get(parts, 9),
                "raw_parts": parts,
            }
        )
    return companies


def parse_contract_demand_org_list(value: Any) -> list[dict[str, Any]]:
    organizations = []
    for parts in _parse_bracket_records(value):
        organizations.append(
            {
                "sequence_no": _get(parts, 0),
                "organization_code": _get(parts, 1),
                "organization_name": _get(parts, 2),
                "jurisdiction_name": _get(parts, 3),
                "department_name": _get(parts, 4),
                "officer_name": _get(parts, 5),
                "raw_parts": parts,
            }
        )
    return organizations


def normalize_contract_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["raw_payload"]
    category = str(row["category"])
    return {
        "resource_key": row["resource_key"] or contract_resource_key(category, raw),
        "category": category,
        "category_label": CATEGORY_LABELS.get(category, category),
        "unified_contract_no": clean(raw.get("untyCntrctNo")),
        "decision_contract_no": clean(raw.get("dcsnCntrctNo")),
        "contract_ref_no": clean(raw.get("cntrctRefNo")),
        "contract_name": clean(raw.get("cntrctNm")) or clean(raw.get("cnstwkNm")) or "(계약명 없음)",
        "business_div_name": clean(raw.get("bsnsDivNm")),
        "is_common_contract": _parse_yes_no(raw.get("cmmnCntrctYn")),
        "long_term_division_name": clean(raw.get("lngtrmCtnuDivNm")),
        "contract_concluded_date": parse_g2b_datetime(raw.get("cntrctCnclsDate")),
        "contract_period": clean(raw.get("cntrctPrd")),
        "base_law_name": clean(raw.get("baseLawNm")),
        "total_contract_amount": parse_decimal(raw.get("totCntrctAmt")),
        "current_contract_amount": parse_decimal(raw.get("thtmCntrctAmt")),
        "guarantee_rate": parse_decimal(raw.get("grntymnyRate")),
        "contract_info_url": clean(raw.get("cntrctInfoUrl")),
        "payment_division_name": clean(raw.get("payDivNm")),
        "request_no": clean(raw.get("reqNo")),
        "bid_notice_no": clean(raw.get("ntceNo")),
        "contract_org_code": clean(raw.get("cntrctInsttCd")),
        "contract_org_name": clean(raw.get("cntrctInsttNm")),
        "contract_org_jurisdiction_name": clean(raw.get("cntrctInsttJrsdctnDivNm")),
        "contract_org_department_name": clean(raw.get("cntrctInsttChrgDeptNm")),
        "contract_org_officer_name": clean(raw.get("cntrctInsttOfclNm")),
        "contract_org_officer_phone_no": clean(raw.get("cntrctInsttOfclTelNo")),
        "contract_org_officer_fax_no": clean(raw.get("cntrctInsttOfclFaxNo")),
        "detail_url": clean(raw.get("cntrctDtlInfoUrl")),
        "creditor_name": clean(raw.get("crdtrNm")),
        "base_details": clean(raw.get("baseDtls")),
        "contract_method": clean(raw.get("cntrctCnclsMthdNm")),
        "registered_at": parse_g2b_datetime(raw.get("rgstDt")),
        "changed_at": parse_g2b_datetime(raw.get("chgDt")),
        "delay_compensation_rate": parse_decimal(raw.get("dfrcmpnstRt")),
        "public_procurement_classification_no": clean(raw.get("pubPrcrmntClsfcNo")),
        "public_procurement_classification_name": clean(raw.get("pubPrcrmntClsfcNm")),
        "public_procurement_mid_classification_name": clean(raw.get("pubPrcrmntMidClsfcNm")),
        "public_procurement_large_classification_name": clean(raw.get("pubPrcrmntLrgClsfcNm")),
        "contract_date": parse_g2b_datetime(raw.get("cntrctDate")),
        "is_info_business": _parse_yes_no(raw.get("infoBizYn")),
        "source_url": row["source_url"],
        "raw_id": row["id"],
        "updated_at": datetime.now(UTC),
    }


def _parse_bracket_records(value: Any) -> list[list[str]]:
    text = clean(value)
    if text is None:
        return []
    return [[part.strip() for part in item.split("^")] for item in re.findall(r"\[([^\]]*)\]", text)]


def _get(parts: list[str], index: int) -> str | None:
    if index >= len(parts):
        return None
    return clean(parts[index])


def _parse_yes_no(value: Any) -> bool | None:
    cleaned = clean(value)
    if cleaned is None:
        return None
    if cleaned.upper() == "Y":
        return True
    if cleaned.upper() == "N":
        return False
    return None
