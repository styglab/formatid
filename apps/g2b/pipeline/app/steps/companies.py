from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from apps.g2b.pipeline.app.steps.common import clean, parse_g2b_datetime, parse_int

COMPANY_BASIC_URL = "http://apis.data.go.kr/1230000/ao/UsrInfoService02/getPrcrmntCorpBasicInfo02"
COMPANY_INDUSTRY_URL = "http://apis.data.go.kr/1230000/ao/UsrInfoService02/getPrcrmntCorpIndstrytyInfo02"


def company_basic_resource_key(record: dict[str, Any]) -> str:
    return clean(record.get("bizno")) or "unknown"


def company_industry_resource_key(record: dict[str, Any]) -> str:
    business_no = clean(record.get("bizno")) or "unknown"
    industry_code = clean(record.get("indstrytyCd")) or clean(record.get("indstrytyNm")) or "unknown"
    return f"{business_no}:{industry_code}"


def normalize_company_basic_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["raw_payload"]
    return {
        "business_no": clean(raw.get("bizno")) or "unknown",
        "company_name": clean(raw.get("corpNm")) or "(업체명 없음)",
        "english_company_name": clean(raw.get("engCorpNm")),
        "ceo_name": clean(raw.get("ceoNm")),
        "opened_at": parse_g2b_datetime(raw.get("opbizDt")),
        "region_code": clean(raw.get("rgnCd")),
        "region_name": clean(raw.get("rgnNm")),
        "zip_code": clean(raw.get("zip")),
        "address": clean(raw.get("adrs")),
        "detail_address": clean(raw.get("dtlAdrs")),
        "phone_no": clean(raw.get("telNo")),
        "fax_no": clean(raw.get("faxNo")),
        "country_name": clean(raw.get("cntryNm")),
        "homepage_url": clean(raw.get("hmpgAdrs")),
        "manufacturing_division_code": clean(raw.get("mnfctDivCd")),
        "manufacturing_division_name": clean(raw.get("mnfctDivNm")),
        "employee_count": parse_int(raw.get("emplyeNum")),
        "business_division_codes": _split_csv(raw.get("corpBsnsDivCd")),
        "business_division_names": _split_csv(raw.get("corpBsnsDivNm")),
        "head_office_division_name": clean(raw.get("hdoffceDivNm")),
        "source_registered_at": parse_g2b_datetime(raw.get("rgstDt")),
        "source_changed_at": parse_g2b_datetime(raw.get("chgDt")),
        "essential_no_cert_registered": clean(raw.get("esntlNoCertRgstYn")),
        "raw_id": row["id"],
        "last_checked_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }


def normalize_company_industry_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["raw_payload"]
    return {
        "business_no": clean(raw.get("bizno")) or "unknown",
        "industry_name": clean(raw.get("indstrytyNm")) or "(업종명 없음)",
        "industry_code": clean(raw.get("indstrytyCd")) or "unknown",
        "registered_at": parse_g2b_datetime(raw.get("rgstDt")),
        "valid_until": parse_g2b_datetime(raw.get("vldPrdExprtDt")),
        "system_registered_at": parse_g2b_datetime(raw.get("systmRgstDt")),
        "source_changed_at": parse_g2b_datetime(raw.get("chgDt")),
        "status_name": clean(raw.get("indstrytyStatsNm")),
        "is_representative": clean(raw.get("rprsntIndstrytyYn")) == "Y",
        "system_changed_at": parse_g2b_datetime(raw.get("systmChgDt")),
        "raw_id": row["id"],
        "last_checked_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }


def _split_csv(value: Any) -> list[str]:
    cleaned = clean(value)
    if cleaned is None:
        return []
    return [item.strip() for item in cleaned.split(",") if item.strip()]
