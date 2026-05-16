from __future__ import annotations

from typing import Any

from apps.pubdata_mcp.app.common.specs import get_evidence_spec, get_response_spec
from apps.pubdata_mcp.app.providers.nts.client import call_nts_api
from apps.pubdata_mcp.app.providers.nts.constants import STATUS_URL, VALIDATE_URL
from apps.pubdata_mcp.app.providers.nts.semantics import (
    normalize_business_numbers,
    normalize_validation_records,
)


def check_business_status(business_numbers: list[str]) -> dict[str, Any]:
    """Check NTS business registration status for up to 100 business numbers."""
    body = {"b_no": normalize_business_numbers(business_numbers)}
    tool_name = "check_nts_business_status_live"
    return call_nts_api(
        STATUS_URL,
        body,
        tool_name,
        response_spec=get_response_spec(tool_name),
        evidence_metadata=get_evidence_spec(tool_name),
    )


def validate_business_registration(businesses: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate NTS business registration details for up to 100 records."""
    body = {"businesses": normalize_validation_records(businesses)}
    tool_name = "validate_nts_business_registration_live"
    return call_nts_api(
        VALIDATE_URL,
        body,
        tool_name,
        response_spec=get_response_spec(tool_name),
        evidence_metadata=get_evidence_spec(tool_name),
    )
