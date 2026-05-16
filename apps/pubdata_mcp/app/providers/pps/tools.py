from __future__ import annotations

from typing import Any

from apps.pubdata_mcp.app.common.specs import get_evidence_spec, get_response_spec
from apps.pubdata_mcp.app.providers.pps.client import call_pps_api
from apps.pubdata_mcp.app.providers.pps.constants import BID_ENDPOINTS, CONTRACT_ENDPOINTS
from apps.pubdata_mcp.app.providers.pps.semantics import (
    build_bid_notice_params,
    build_contract_params,
    normalize_category,
)


def search_bid_notices(
    category: str,
    date_from: str,
    date_to: str,
    keyword: str | None = None,
    page_no: int = 1,
    num_of_rows: int = 10,
) -> dict[str, Any]:
    """Call the PPS/G2B bid notice API directly without normalized tables."""
    normalized_category = normalize_category(category)
    params = build_bid_notice_params(
        date_from=date_from,
        date_to=date_to,
        keyword=keyword,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )
    return call_pps_api(
        BID_ENDPOINTS[normalized_category],
        params,
        "search_pps_bid_notices_live",
    )


def search_contracts(
    category: str,
    contract_date_from: str,
    contract_date_to: str,
    keyword: str | None = None,
    page_no: int = 1,
    num_of_rows: int = 10,
) -> dict[str, Any]:
    """Call the PPS/G2B contract API directly without normalized tables."""
    normalized_category = normalize_category(category)
    params = build_contract_params(
        contract_date_from=contract_date_from,
        contract_date_to=contract_date_to,
        keyword=keyword,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )
    return call_pps_api(
        CONTRACT_ENDPOINTS[normalized_category],
        params,
        "search_pps_contracts_live",
        response_spec=get_response_spec("search_pps_contracts_live"),
        evidence_metadata={
            **get_evidence_spec("search_pps_contracts_live"),
            "date_basis": "contract_date",
            "category": normalized_category,
        },
    )
