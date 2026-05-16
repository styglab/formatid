from __future__ import annotations

import os
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg.sql import Identifier, SQL

from apps.g2b.schema import (
    BID_NOTICE_TABLE,
    CONTRACT_COMPANY_TABLE,
    CONTRACT_DEMAND_ORG_TABLE,
    CONTRACT_TABLE,
    DEFAULT_SCHEMA,
    LICENSE_CONSTRAINT_TABLE,
    PARTICIPATION_REGION_TABLE,
    SUCCESSFUL_BID_TABLE,
)
from apps.g2b.semantic import (
    ENTITY_DEFINITIONS,
    RELATION_DEFINITIONS,
    SEMANTIC_MODEL_VERSION,
    TOOL_SPECS,
    VOCABULARY,
    build_bid_notice_semantic_document,
    build_bid_notice_semantic_object,
    build_contract_semantic_object,
    build_success_bid_semantic_object,
)


G2B_TIMEZONE = ZoneInfo(os.getenv("G2B_TIMEZONE", "Asia/Seoul"))
VALID_CATEGORIES = {"SERVICE", "GOODS", "CONSTRUCTION", "FOREIGN"}
CATEGORY_ALIASES = {
    "SERVICE": "SERVICE",
    "SERVICES": "SERVICE",
    "용역": "SERVICE",
    "일반용역": "SERVICE",
    "GOODS": "GOODS",
    "GOOD": "GOODS",
    "THINGS": "GOODS",
    "THING": "GOODS",
    "물품": "GOODS",
    "CONSTRUCTION": "CONSTRUCTION",
    "WORK": "CONSTRUCTION",
    "WORKS": "CONSTRUCTION",
    "공사": "CONSTRUCTION",
    "FOREIGN": "FOREIGN",
    "FRGCPT": "FOREIGN",
    "외자": "FOREIGN",
    "외자구매": "FOREIGN",
}
SORT_COLUMNS = {
    "published_at": "published_at",
    "deadline_at": "deadline_at",
    "budget": "budget",
}
SUCCESS_BID_SORT_COLUMNS = {
    "registered_at": "registered_at",
    "final_success_date": "final_success_date",
    "winning_amount": "winning_amount",
    "winning_rate": "winning_rate",
}
CONTRACT_SORT_COLUMNS = {
    "registered_at": "registered_at",
    "contract_date": "contract_date",
    "contract_concluded_date": "contract_concluded_date",
    "current_contract_amount": "current_contract_amount",
    "total_contract_amount": "total_contract_amount",
}


def search_bids(
    *,
    category: str | None = None,
    keyword: str | None = None,
    notice_kind: str | None = None,
    exclude_cancelled: bool = True,
    contract_method: str | None = None,
    bid_method: str | None = None,
    bid_notice_no: str | None = None,
    bid_notice_order: str | None = None,
    published_from: str | None = None,
    published_to: str | None = None,
    deadline_from: str | None = None,
    deadline_to: str | None = None,
    opening_from: str | None = None,
    opening_to: str | None = None,
    notice_agency_name: str | None = None,
    demand_agency_name: str | None = None,
    organization_name: str | None = None,
    demand_org_name: str | None = None,
    has_budget: bool | None = None,
    min_budget: int | float | str | None = None,
    max_budget: int | float | str | None = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "published_at",
    sort_order: str = "desc",
    include_license_limits: bool = False,
    include_participation_regions: bool = False,
    include_success_bids: bool = False,
    include_semantic: bool = False,
    semantic_tags: list[str] | str | None = None,
    requires_license: str | None = None,
    restricted_region: str | None = None,
) -> dict[str, Any]:
    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA)
    table_name = os.getenv("G2B_NORMALIZED_TABLE", BID_NOTICE_TABLE.normalized_table)
    license_limit_table_name = os.getenv("G2B_LICENSE_LIMIT_NORMALIZED_TABLE", LICENSE_CONSTRAINT_TABLE.normalized_table)
    participation_region_table_name = os.getenv(
        "G2B_PARTICIPATION_REGION_NORMALIZED_TABLE",
        PARTICIPATION_REGION_TABLE.normalized_table,
    )
    success_bid_table_name = os.getenv("G2B_SUCCESS_BID_NORMALIZED_TABLE", SUCCESSFUL_BID_TABLE.normalized_table)
    max_limit = _parse_positive_int(os.getenv("G2B_MCP_MAX_LIMIT", "50"), field_name="G2B_MCP_MAX_LIMIT")
    limit = max(1, min(_parse_positive_int(limit, field_name="limit"), max_limit))
    offset = _parse_non_negative_int(offset, field_name="offset")
    order_sql = _build_order_by(sort_by=sort_by, sort_order=sort_order)

    where_sql, params = _build_filters(
        category=category,
        keyword=keyword,
        notice_kind=notice_kind,
        exclude_cancelled=exclude_cancelled,
        contract_method=contract_method,
        bid_method=bid_method,
        bid_notice_no=bid_notice_no,
        bid_notice_order=bid_notice_order,
        published_from=published_from,
        published_to=published_to,
        deadline_from=deadline_from,
        deadline_to=deadline_to,
        opening_from=opening_from,
        opening_to=opening_to,
        notice_agency_name=notice_agency_name,
        demand_agency_name=demand_agency_name,
        organization_name=organization_name,
        demand_org_name=demand_org_name,
        has_budget=has_budget,
        min_budget=min_budget,
        max_budget=max_budget,
        schema_name=schema_name,
        license_limit_table_name=license_limit_table_name,
        participation_region_table_name=participation_region_table_name,
        semantic_tags=_parse_list_filter(semantic_tags, field_name="semantic_tags"),
        requires_license=requires_license,
        restricted_region=restricted_region,
    )

    table = SQL("{}.{}").format(Identifier(schema_name), Identifier(table_name))
    count_query = SQL("SELECT count(*) FROM {} AS bid_notice {}").format(table, where_sql)
    rows_query = SQL(
        """
        SELECT
            bid_notice.resource_key,
            bid_notice.category,
            bid_notice.category_label,
            bid_notice.bid_notice_no,
            bid_notice.bid_notice_order,
            bid_notice.title,
            bid_notice.notice_agency_code,
            bid_notice.notice_agency_name,
            bid_notice.demand_agency_code,
            bid_notice.demand_agency_name,
            bid_notice.budget,
            bid_notice.published_at,
            bid_notice.deadline_at,
            bid_notice.opening_at,
            bid_notice.contract_method,
            bid_notice.bid_method,
            bid_notice.notice_kind,
            bid_notice.detail_url
        FROM {} AS bid_notice {}
        {}
        LIMIT %s
        OFFSET %s
        """
    ).format(table, where_sql, order_sql)

    with psycopg.connect(_database_url(), row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(count_query, params)
            count = int(cursor.fetchone()["count"])
            cursor.execute(rows_query, (*params, limit, offset))
            rows = cursor.fetchall()
            load_license_limits = include_license_limits or include_semantic
            load_participation_regions = include_participation_regions or include_semantic
            load_success_bids = include_success_bids or include_semantic
            license_limits = (
                _load_license_limits(cursor, schema_name, license_limit_table_name, rows)
                if load_license_limits
                else {}
            )
            participation_regions = (
                _load_participation_regions(cursor, schema_name, participation_region_table_name, rows)
                if load_participation_regions
                else {}
            )
            success_bids = (
                _load_success_bids(cursor, schema_name, success_bid_table_name, rows)
                if load_success_bids
                else {}
            )

    bids = [
        _format_bid(
            row,
            license_limits=license_limits if include_license_limits or include_semantic else None,
            participation_regions=participation_regions if include_participation_regions or include_semantic else None,
            success_bids=success_bids if include_success_bids or include_semantic else None,
        )
        for row in rows
    ]
    if include_semantic:
        bids = [_attach_bid_semantic(bid) for bid in bids]

    return {
        "source": "normalized_db",
        **({"semantic_model_version": SEMANTIC_MODEL_VERSION} if include_semantic else {}),
        "evidence": _search_evidence(
            tool="search_bid",
            tables=[f"{schema_name}.{table_name}"],
            filters={
                "category": category,
                "keyword": keyword,
                "published_from": published_from,
                "published_to": published_to,
                "deadline_from": deadline_from,
                "deadline_to": deadline_to,
                "opening_from": opening_from,
                "opening_to": opening_to,
                "bid_notice_no": bid_notice_no,
                "notice_agency_name": notice_agency_name or organization_name,
                "demand_agency_name": demand_agency_name or demand_org_name,
                "requires_license": requires_license,
                "restricted_region": restricted_region,
            },
            date_basis=_date_basis(
                ("published_at", published_from, published_to),
                ("deadline_at", deadline_from, deadline_to),
                ("opening_at", opening_from, opening_to),
            ),
            sort_by=sort_by,
            sort_order=sort_order,
        ),
        "count": count,
        "returned": len(rows),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rows) < count,
        "sort": {
            "by": sort_by,
            "order": sort_order.lower(),
        },
        "bids": bids,
    }


def search_success_bids(
    *,
    category: str | None = None,
    keyword: str | None = None,
    bid_notice_no: str | None = None,
    bid_notice_order: str | None = None,
    winner_name: str | None = None,
    winner_business_no: str | None = None,
    demand_org_name: str | None = None,
    registered_from: str | None = None,
    registered_to: str | None = None,
    final_success_from: str | None = None,
    final_success_to: str | None = None,
    min_winning_amount: int | float | str | None = None,
    max_winning_amount: int | float | str | None = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "registered_at",
    sort_order: str = "desc",
    include_semantic: bool = False,
) -> dict[str, Any]:
    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA)
    table_name = os.getenv("G2B_SUCCESS_BID_NORMALIZED_TABLE", SUCCESSFUL_BID_TABLE.normalized_table)
    max_limit = _parse_positive_int(os.getenv("G2B_MCP_MAX_LIMIT", "50"), field_name="G2B_MCP_MAX_LIMIT")
    limit = max(1, min(_parse_positive_int(limit, field_name="limit"), max_limit))
    offset = _parse_non_negative_int(offset, field_name="offset")
    order_sql = _build_order_by(
        sort_by=sort_by,
        sort_order=sort_order,
        sort_columns=SUCCESS_BID_SORT_COLUMNS,
    )
    where_sql, params = _build_success_bid_filters(
        category=category,
        keyword=keyword,
        bid_notice_no=bid_notice_no,
        bid_notice_order=bid_notice_order,
        winner_name=winner_name,
        winner_business_no=winner_business_no,
        demand_org_name=demand_org_name,
        registered_from=registered_from,
        registered_to=registered_to,
        final_success_from=final_success_from,
        final_success_to=final_success_to,
        min_winning_amount=min_winning_amount,
        max_winning_amount=max_winning_amount,
    )
    table = SQL("{}.{}").format(Identifier(schema_name), Identifier(table_name))
    count_query = SQL("SELECT count(*) FROM {} {}").format(table, where_sql)
    rows_query = SQL(
        """
        SELECT
            resource_key,
            category,
            category_label,
            bid_notice_no,
            bid_notice_order,
            bid_classification_no,
            rebid_no,
            notice_division_code,
            title,
            participant_count,
            winner_name,
            winner_business_no,
            winner_ceo_name,
            winner_address,
            winner_phone_no,
            winning_amount,
            winning_rate,
            actual_opening_at,
            demand_org_code,
            demand_org_name,
            registered_at,
            final_success_date,
            final_success_company_officer
        FROM {} {}
        {}
        LIMIT %s
        OFFSET %s
        """
    ).format(table, where_sql, order_sql)

    with psycopg.connect(_database_url(), row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(count_query, params)
            count = int(cursor.fetchone()["count"])
            cursor.execute(rows_query, (*params, limit, offset))
            rows = cursor.fetchall()

    success_bids = [_format_success_bid(row) for row in rows]
    if include_semantic:
        success_bids = [_attach_success_bid_semantic(success_bid) for success_bid in success_bids]

    return {
        "source": "normalized_db",
        **({"semantic_model_version": SEMANTIC_MODEL_VERSION} if include_semantic else {}),
        "evidence": _search_evidence(
            tool="search_success_bid",
            tables=[f"{schema_name}.{table_name}"],
            filters={
                "category": category,
                "keyword": keyword,
                "bid_notice_no": bid_notice_no,
                "winner_name": winner_name,
                "winner_business_no": winner_business_no,
                "demand_org_name": demand_org_name,
                "registered_from": registered_from,
                "registered_to": registered_to,
                "final_success_from": final_success_from,
                "final_success_to": final_success_to,
            },
            date_basis=_date_basis(
                ("registered_at", registered_from, registered_to),
                ("final_success_date", final_success_from, final_success_to),
            ),
            sort_by=sort_by,
            sort_order=sort_order,
        ),
        "count": count,
        "returned": len(rows),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rows) < count,
        "sort": {
            "by": sort_by,
            "order": sort_order.lower(),
        },
        "success_bids": success_bids,
    }


def search_contracts(
    *,
    category: str | None = None,
    keyword: str | None = None,
    unified_contract_no: str | None = None,
    bid_notice_no: str | None = None,
    contract_org_name: str | None = None,
    company_name: str | None = None,
    business_no: str | None = None,
    registered_from: str | None = None,
    registered_to: str | None = None,
    contract_date_from: str | None = None,
    contract_date_to: str | None = None,
    contract_concluded_from: str | None = None,
    contract_concluded_to: str | None = None,
    contract_from: str | None = None,
    contract_to: str | None = None,
    min_amount: int | float | str | None = None,
    max_amount: int | float | str | None = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "contract_date",
    sort_order: str = "desc",
    include_companies: bool = False,
    include_demand_organizations: bool = False,
    include_semantic: bool = False,
) -> dict[str, Any]:
    schema_name = os.getenv("G2B_NORMALIZED_SCHEMA", DEFAULT_SCHEMA)
    table_name = os.getenv("G2B_CONTRACT_NORMALIZED_TABLE", CONTRACT_TABLE.normalized_table)
    company_table_name = os.getenv("G2B_CONTRACT_COMPANY_TABLE", CONTRACT_COMPANY_TABLE.normalized_table)
    demand_org_table_name = os.getenv("G2B_CONTRACT_DEMAND_ORG_TABLE", CONTRACT_DEMAND_ORG_TABLE.normalized_table)
    max_limit = _parse_positive_int(os.getenv("G2B_MCP_MAX_LIMIT", "50"), field_name="G2B_MCP_MAX_LIMIT")
    limit = max(1, min(_parse_positive_int(limit, field_name="limit"), max_limit))
    offset = _parse_non_negative_int(offset, field_name="offset")
    order_sql = _build_order_by(
        sort_by=sort_by,
        sort_order=sort_order,
        sort_columns=CONTRACT_SORT_COLUMNS,
    )
    where_sql, params = _build_contract_filters(
        category=category,
        keyword=keyword,
        unified_contract_no=unified_contract_no,
        bid_notice_no=bid_notice_no,
        contract_org_name=contract_org_name,
        company_name=company_name,
        business_no=business_no,
        registered_from=registered_from,
        registered_to=registered_to,
        contract_date_from=contract_date_from or contract_from,
        contract_date_to=contract_date_to or contract_to,
        contract_concluded_from=contract_concluded_from,
        contract_concluded_to=contract_concluded_to,
        min_amount=min_amount,
        max_amount=max_amount,
        schema_name=schema_name,
        company_table_name=company_table_name,
    )
    table = SQL("{}.{}").format(Identifier(schema_name), Identifier(table_name))
    count_query = SQL("SELECT count(*) FROM {} AS contract {}").format(table, where_sql)
    rows_query = SQL(
        """
        SELECT
            resource_key,
            category,
            category_label,
            unified_contract_no,
            decision_contract_no,
            contract_ref_no,
            contract_name,
            business_div_name,
            total_contract_amount,
            current_contract_amount,
            bid_notice_no,
            contract_org_code,
            contract_org_name,
            contract_org_jurisdiction_name,
            contract_org_department_name,
            contract_org_officer_name,
            contract_method,
            registered_at,
            changed_at,
            contract_date,
            contract_concluded_date,
            detail_url
        FROM {} AS contract {}
        {}
        LIMIT %s
        OFFSET %s
        """
    ).format(table, where_sql, order_sql)

    with psycopg.connect(_database_url(), row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(count_query, params)
            count = int(cursor.fetchone()["count"])
            cursor.execute(rows_query, (*params, limit, offset))
            rows = cursor.fetchall()
            load_companies = include_companies or include_semantic
            load_demand_orgs = include_demand_organizations or include_semantic
            companies = (
                _load_contract_companies(cursor, schema_name, company_table_name, rows)
                if load_companies
                else {}
            )
            demand_organizations = (
                _load_contract_demand_orgs(cursor, schema_name, demand_org_table_name, rows)
                if load_demand_orgs
                else {}
            )

    contracts = [
        _format_contract(
            row,
            companies=companies if include_companies or include_semantic else None,
            demand_organizations=demand_organizations if include_demand_organizations or include_semantic else None,
        )
        for row in rows
    ]
    if include_semantic:
        contracts = [_attach_contract_semantic(contract) for contract in contracts]

    return {
        "source": "normalized_db",
        **({"semantic_model_version": SEMANTIC_MODEL_VERSION} if include_semantic else {}),
        "evidence": _search_evidence(
            tool="search_contract",
            tables=[f"{schema_name}.{table_name}"],
            filters={
                "category": category,
                "keyword": keyword,
                "unified_contract_no": unified_contract_no,
                "bid_notice_no": bid_notice_no,
                "contract_org_name": contract_org_name,
                "company_name": company_name,
                "business_no": business_no,
                "registered_from": registered_from,
                "registered_to": registered_to,
                "contract_date_from": contract_date_from or contract_from,
                "contract_date_to": contract_date_to or contract_to,
                "contract_concluded_from": contract_concluded_from,
                "contract_concluded_to": contract_concluded_to,
            },
            date_basis=_date_basis(
                ("registered_at", registered_from, registered_to),
                ("contract_date", contract_date_from or contract_from, contract_date_to or contract_to),
                ("contract_concluded_date", contract_concluded_from, contract_concluded_to),
            ),
            sort_by=sort_by,
            sort_order=sort_order,
        ),
        "count": count,
        "returned": len(rows),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rows) < count,
        "sort": {
            "by": sort_by,
            "order": sort_order.lower(),
        },
        "contracts": contracts,
    }


def summarize_bid_context(
    *,
    bid_notice_no: str,
    bid_notice_order: str | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    result = search_bids(
        category=category,
        bid_notice_no=bid_notice_no,
        bid_notice_order=bid_notice_order,
        exclude_cancelled=False,
        limit=1,
        include_semantic=True,
    )
    if not result["bids"]:
        return {
            "source": "normalized_db",
            "semantic_model_version": SEMANTIC_MODEL_VERSION,
            "found": False,
            "bid": None,
            "summary": None,
            "semantic_document": None,
        }

    bid = result["bids"][0]
    semantic_document = build_bid_notice_semantic_document(bid["semantic"])
    return {
        "source": "normalized_db",
        "semantic_model_version": SEMANTIC_MODEL_VERSION,
        "found": True,
        "bid": bid,
        "summary": semantic_document["text"],
        "semantic_document": semantic_document,
    }


def get_procurement_lifecycle(
    *,
    bid_notice_no: str,
    bid_notice_order: str | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    bid_context = summarize_bid_context(
        bid_notice_no=bid_notice_no,
        bid_notice_order=bid_notice_order,
        category=category,
    )
    success_bids = search_success_bids(
        category=category,
        bid_notice_no=bid_notice_no,
        bid_notice_order=bid_notice_order,
        limit=20,
        include_semantic=True,
    )
    contracts = search_contracts(
        category=category,
        bid_notice_no=bid_notice_no,
        limit=20,
        include_companies=True,
        include_demand_organizations=True,
        include_semantic=True,
    )
    return {
        "source": "normalized_db",
        "semantic_model_version": SEMANTIC_MODEL_VERSION,
        "found": bool(bid_context.get("found") or success_bids.get("success_bids") or contracts.get("contracts")),
        "bid_context": bid_context,
        "success_bids": success_bids,
        "contracts": contracts,
        "evidence": {
            "tool": "get_procurement_lifecycle",
            "tables": [
                BID_NOTICE_TABLE.normalized_table,
                LICENSE_CONSTRAINT_TABLE.normalized_table,
                PARTICIPATION_REGION_TABLE.normalized_table,
                SUCCESSFUL_BID_TABLE.normalized_table,
                CONTRACT_TABLE.normalized_table,
                CONTRACT_COMPANY_TABLE.normalized_table,
                CONTRACT_DEMAND_ORG_TABLE.normalized_table,
            ],
            "filters": {
                "bid_notice_no": bid_notice_no,
                "bid_notice_order": bid_notice_order,
                "category": category,
            },
        },
    }


def get_tool_capabilities() -> dict[str, Any]:
    return {
        "semantic_model_version": SEMANTIC_MODEL_VERSION,
        "entities": ENTITY_DEFINITIONS,
        "relationships": RELATION_DEFINITIONS,
        "vocabulary": VOCABULARY,
        "tools": TOOL_SPECS,
    }


def _search_evidence(
    *,
    tool: str,
    tables: list[str],
    filters: dict[str, Any],
    date_basis: str | None,
    sort_by: str,
    sort_order: str,
) -> dict[str, Any]:
    return {
        "tool": tool,
        "tables": tables,
        "filters": {key: value for key, value in filters.items() if value not in (None, "")},
        "date_basis": date_basis,
        "sort": {"by": sort_by, "order": sort_order.lower()},
    }


def _date_basis(*fields: tuple[str, Any, Any]) -> str | None:
    for field_name, from_value, to_value in fields:
        if from_value or to_value:
            return field_name
    return None


def _attach_bid_semantic(bid: dict[str, Any]) -> dict[str, Any]:
    semantic_input = _semantic_bid_input(bid)
    semantic = build_bid_notice_semantic_object(
        semantic_input,
        license_limits=bid.get("license_limits") or [],
        participation_regions=bid.get("participation_regions") or [],
        success_bids=[_semantic_success_bid_input(success_bid) for success_bid in bid.get("success_bids") or []],
    )
    return {
        **bid,
        "semantic": semantic,
    }


def _attach_success_bid_semantic(success_bid: dict[str, Any]) -> dict[str, Any]:
    return {
        **success_bid,
        "semantic": build_success_bid_semantic_object(_semantic_success_bid_input(success_bid)),
    }


def _attach_contract_semantic(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        **contract,
        "semantic": build_contract_semantic_object(
            _semantic_contract_input(contract),
            companies=contract.get("companies") or [],
            demand_organizations=contract.get("demand_organizations") or [],
        ),
    }


def _semantic_bid_input(bid: dict[str, Any]) -> dict[str, Any]:
    metadata = bid.get("metadata") or {}
    return {
        "id": bid.get("id"),
        "category": bid.get("category"),
        "category_label": bid.get("category_label"),
        "bid_notice_no": metadata.get("bid_notice_no"),
        "bid_notice_order": metadata.get("bid_notice_order"),
        "title": bid.get("title"),
        "notice_agency_code": (bid.get("notice_agency") or {}).get("code"),
        "notice_agency_name": (bid.get("notice_agency") or {}).get("name"),
        "demand_agency_code": (bid.get("demand_agency") or {}).get("code"),
        "demand_agency_name": (bid.get("demand_agency") or {}).get("name"),
        "budget": bid.get("budget"),
        "published_at": bid.get("published_at"),
        "deadline_at": bid.get("deadline_at"),
        "opening_at": bid.get("opening_at"),
        "contract_method": bid.get("contract_method"),
        "bid_method": bid.get("bid_method"),
        "notice_kind": bid.get("notice_kind"),
        "detail_url": bid.get("detail_url"),
    }


def _semantic_success_bid_input(success_bid: dict[str, Any]) -> dict[str, Any]:
    metadata = success_bid.get("metadata") or {}
    winner = success_bid.get("winner") or {}
    demand_org = success_bid.get("demand_org") or {}
    return {
        "id": success_bid.get("id"),
        "category": success_bid.get("category"),
        "category_label": success_bid.get("category_label"),
        "bid_notice_no": metadata.get("bid_notice_no"),
        "bid_notice_order": metadata.get("bid_notice_order"),
        "title": success_bid.get("title"),
        "winner_name": winner.get("name"),
        "winner_business_no": winner.get("business_no"),
        "winner_ceo_name": winner.get("ceo_name"),
        "winner_address": winner.get("address"),
        "winning_amount": success_bid.get("winning_amount"),
        "winning_rate": success_bid.get("winning_rate"),
        "registered_at": success_bid.get("registered_at"),
        "final_success_date": success_bid.get("final_success_date"),
        "demand_org_name": demand_org.get("name"),
    }


def _semantic_contract_input(contract: dict[str, Any]) -> dict[str, Any]:
    metadata = contract.get("metadata") or {}
    contract_org = contract.get("contract_org") or {}
    amounts = contract.get("amounts") or {}
    return {
        "id": contract.get("id"),
        "category": contract.get("category"),
        "category_label": contract.get("category_label"),
        "unified_contract_no": metadata.get("unified_contract_no"),
        "decision_contract_no": metadata.get("decision_contract_no"),
        "contract_ref_no": metadata.get("contract_ref_no"),
        "contract_name": contract.get("contract_name"),
        "bid_notice_no": metadata.get("bid_notice_no"),
        "contract_method": contract.get("contract_method"),
        "current_contract_amount": amounts.get("current_contract_amount"),
        "total_contract_amount": amounts.get("total_contract_amount"),
        "contract_date": contract.get("contract_date"),
        "contract_concluded_date": contract.get("contract_concluded_date"),
        "registered_at": contract.get("registered_at"),
        "detail_url": contract.get("detail_url"),
        "contract_org_code": contract_org.get("code"),
        "contract_org_name": contract_org.get("name"),
        "contract_org_department_name": contract_org.get("department_name"),
        "contract_org_officer_name": contract_org.get("officer_name"),
    }


def _database_url() -> str:
    database_url = os.getenv("G2B_MCP_DATABASE_URL")
    if not database_url:
        raise RuntimeError("G2B_MCP_DATABASE_URL is not configured")
    return database_url


def _build_filters(
    *,
    category: str | None,
    keyword: str | None,
    notice_kind: str | None,
    exclude_cancelled: bool,
    contract_method: str | None,
    bid_method: str | None,
    bid_notice_no: str | None,
    bid_notice_order: str | None,
    published_from: str | None,
    published_to: str | None,
    deadline_from: str | None,
    deadline_to: str | None,
    opening_from: str | None,
    opening_to: str | None,
    notice_agency_name: str | None,
    demand_agency_name: str | None,
    organization_name: str | None,
    demand_org_name: str | None,
    has_budget: bool | None,
    min_budget: int | float | str | None,
    max_budget: int | float | str | None,
    schema_name: str,
    license_limit_table_name: str,
    participation_region_table_name: str,
    semantic_tags: list[str],
    requires_license: str | None,
    restricted_region: str | None,
) -> tuple[SQL, tuple[Any, ...]]:
    clauses: list[SQL] = []
    params: list[Any] = []

    if category:
        category = _normalize_category(category)
        clauses.append(SQL("category = %s"))
        params.append(category)
    if keyword and keyword.strip():
        clauses.append(SQL("(title ILIKE %s OR notice_agency_name ILIKE %s OR demand_agency_name ILIKE %s)"))
        pattern = f"%{keyword.strip()}%"
        params.extend([pattern, pattern, pattern])
    if notice_kind and notice_kind.strip():
        clauses.append(SQL("notice_kind = %s"))
        params.append(notice_kind.strip())
    elif exclude_cancelled:
        clauses.append(SQL("(notice_kind IS NULL OR notice_kind NOT ILIKE %s)"))
        params.append("%취소%")
    if contract_method and contract_method.strip():
        clauses.append(SQL("contract_method ILIKE %s"))
        params.append(f"%{contract_method.strip()}%")
    if bid_method and bid_method.strip():
        clauses.append(SQL("bid_method ILIKE %s"))
        params.append(f"%{bid_method.strip()}%")
    if bid_notice_no and bid_notice_no.strip():
        clauses.append(SQL("bid_notice_no = %s"))
        params.append(bid_notice_no.strip())
    if bid_notice_order and bid_notice_order.strip():
        clauses.append(SQL("bid_notice_order = %s"))
        params.append(bid_notice_order.strip())
    notice_agency_filter = notice_agency_name or organization_name
    demand_agency_filter = demand_agency_name or demand_org_name
    if notice_agency_filter and notice_agency_filter.strip():
        clauses.append(SQL("notice_agency_name ILIKE %s"))
        params.append(f"%{notice_agency_filter.strip()}%")
    if demand_agency_filter and demand_agency_filter.strip():
        clauses.append(SQL("demand_agency_name ILIKE %s"))
        params.append(f"%{demand_agency_filter.strip()}%")
    if published_from:
        clauses.append(SQL("published_at >= %s"))
        params.append(_parse_datetime(published_from))
    if published_to:
        clauses.append(SQL("published_at <= %s"))
        params.append(_parse_datetime(published_to, end_of_day=True))
    if deadline_from:
        clauses.append(SQL("deadline_at >= %s"))
        params.append(_parse_datetime(deadline_from))
    if deadline_to:
        clauses.append(SQL("deadline_at <= %s"))
        params.append(_parse_datetime(deadline_to, end_of_day=True))
    if opening_from:
        clauses.append(SQL("opening_at >= %s"))
        params.append(_parse_datetime(opening_from))
    if opening_to:
        clauses.append(SQL("opening_at <= %s"))
        params.append(_parse_datetime(opening_to, end_of_day=True))
    if has_budget is True:
        clauses.append(SQL("budget IS NOT NULL"))
    elif has_budget is False:
        clauses.append(SQL("budget IS NULL"))
    if min_budget is not None:
        clauses.append(SQL("budget >= %s"))
        params.append(_parse_decimal(min_budget))
    if max_budget is not None:
        clauses.append(SQL("budget <= %s"))
        params.append(_parse_decimal(max_budget))
    if requires_license and requires_license.strip():
        clauses.append(_exists_license_clause(schema_name, license_limit_table_name, _license_keyword_condition()))
        pattern = f"%{requires_license.strip()}%"
        params.extend([pattern, pattern, pattern])
    if restricted_region and restricted_region.strip():
        clauses.append(_exists_region_clause(schema_name, participation_region_table_name, "region_name ILIKE %s"))
        params.append(f"%{restricted_region.strip()}%")
    for tag in semantic_tags:
        if tag == "budget_disclosed":
            clauses.append(SQL("budget IS NOT NULL"))
        elif tag == "regulated_license":
            clauses.append(_exists_license_clause(schema_name, license_limit_table_name))
        elif tag == "region_restricted":
            clauses.append(_exists_region_clause(schema_name, participation_region_table_name))
        elif tag == "medical_waste":
            clauses.append(_exists_license_clause(schema_name, license_limit_table_name, _license_keyword_condition()))
            params.extend(["%의료폐기물%", "%의료폐기물%", "%의료폐기물%"])
        elif tag == "waste_management":
            clauses.append(_exists_license_clause(schema_name, license_limit_table_name, _license_keyword_condition()))
            params.extend(["%폐기물%", "%폐기물%", "%폐기물%"])
        elif tag == "government_procurement":
            continue
        else:
            raise ValueError(f"Unsupported semantic tag filter: {tag}")

    if not clauses:
        return SQL(""), tuple(params)
    return SQL("WHERE ") + SQL(" AND ").join(clauses), tuple(params)


def _exists_license_clause(schema_name: str, table_name: str, extra_condition: str | None = None) -> SQL:
    extra_sql = SQL("")
    if extra_condition:
        extra_sql = SQL(" AND ") + SQL(extra_condition)
    return SQL(
        """
        EXISTS (
            SELECT 1
            FROM {}.{} AS license_limit
            WHERE license_limit.bid_notice_no = bid_notice.bid_notice_no
              AND license_limit.bid_notice_order = bid_notice.bid_notice_order
              {}
        )
        """
    ).format(Identifier(schema_name), Identifier(table_name), extra_sql)


def _license_keyword_condition() -> str:
    return """
    (
        license_limit.license_limit_name ILIKE %s
        OR license_limit.license_limit_code ILIKE %s
        OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(license_limit.allowed_industries) AS allowed_industry
            WHERE allowed_industry->>'name' ILIKE %s
        )
    )
    """


def _exists_region_clause(schema_name: str, table_name: str, extra_condition: str | None = None) -> SQL:
    extra_sql = SQL("")
    if extra_condition:
        extra_sql = SQL(" AND ") + SQL(extra_condition)
    return SQL(
        """
        EXISTS (
            SELECT 1
            FROM {}.{} AS participation_region
            WHERE participation_region.bid_notice_no = bid_notice.bid_notice_no
              AND participation_region.bid_notice_order = bid_notice.bid_notice_order
              {}
        )
        """
    ).format(Identifier(schema_name), Identifier(table_name), extra_sql)


def _build_success_bid_filters(
    *,
    category: str | None,
    keyword: str | None,
    bid_notice_no: str | None,
    bid_notice_order: str | None,
    winner_name: str | None,
    winner_business_no: str | None,
    demand_org_name: str | None,
    registered_from: str | None,
    registered_to: str | None,
    final_success_from: str | None,
    final_success_to: str | None,
    min_winning_amount: int | float | str | None,
    max_winning_amount: int | float | str | None,
) -> tuple[SQL, tuple[Any, ...]]:
    clauses: list[SQL] = []
    params: list[Any] = []

    if category:
        category = _normalize_category(category)
        clauses.append(SQL("category = %s"))
        params.append(category)
    if keyword and keyword.strip():
        pattern = f"%{keyword.strip()}%"
        clauses.append(SQL("(title ILIKE %s OR winner_name ILIKE %s OR demand_org_name ILIKE %s)"))
        params.extend([pattern, pattern, pattern])
    if bid_notice_no and bid_notice_no.strip():
        clauses.append(SQL("bid_notice_no = %s"))
        params.append(bid_notice_no.strip())
    if bid_notice_order and bid_notice_order.strip():
        clauses.append(SQL("bid_notice_order = %s"))
        params.append(bid_notice_order.strip())
    if winner_name and winner_name.strip():
        clauses.append(SQL("winner_name ILIKE %s"))
        params.append(f"%{winner_name.strip()}%")
    if winner_business_no and winner_business_no.strip():
        clauses.append(SQL("winner_business_no = %s"))
        params.append(winner_business_no.strip())
    if demand_org_name and demand_org_name.strip():
        clauses.append(SQL("demand_org_name ILIKE %s"))
        params.append(f"%{demand_org_name.strip()}%")
    if registered_from:
        clauses.append(SQL("registered_at >= %s"))
        params.append(_parse_datetime(registered_from))
    if registered_to:
        clauses.append(SQL("registered_at <= %s"))
        params.append(_parse_datetime(registered_to, end_of_day=True))
    if final_success_from:
        clauses.append(SQL("final_success_date >= %s"))
        params.append(_parse_datetime(final_success_from))
    if final_success_to:
        clauses.append(SQL("final_success_date <= %s"))
        params.append(_parse_datetime(final_success_to, end_of_day=True))
    if min_winning_amount is not None:
        clauses.append(SQL("winning_amount >= %s"))
        params.append(_parse_decimal(min_winning_amount))
    if max_winning_amount is not None:
        clauses.append(SQL("winning_amount <= %s"))
        params.append(_parse_decimal(max_winning_amount))

    if not clauses:
        return SQL(""), tuple(params)
    return SQL("WHERE ") + SQL(" AND ").join(clauses), tuple(params)


def _build_contract_filters(
    *,
    category: str | None,
    keyword: str | None,
    unified_contract_no: str | None,
    bid_notice_no: str | None,
    contract_org_name: str | None,
    company_name: str | None,
    business_no: str | None,
    registered_from: str | None,
    registered_to: str | None,
    contract_date_from: str | None,
    contract_date_to: str | None,
    contract_concluded_from: str | None,
    contract_concluded_to: str | None,
    min_amount: int | float | str | None,
    max_amount: int | float | str | None,
    schema_name: str,
    company_table_name: str,
) -> tuple[SQL, tuple[Any, ...]]:
    clauses: list[SQL] = []
    params: list[Any] = []

    if category:
        category = _normalize_category(category)
        clauses.append(SQL("category = %s"))
        params.append(category)
    if keyword and keyword.strip():
        pattern = f"%{keyword.strip()}%"
        clauses.append(SQL("(contract_name ILIKE %s OR contract_org_name ILIKE %s OR bid_notice_no ILIKE %s)"))
        params.extend([pattern, pattern, pattern])
    if unified_contract_no and unified_contract_no.strip():
        clauses.append(SQL("unified_contract_no = %s"))
        params.append(unified_contract_no.strip())
    if bid_notice_no and bid_notice_no.strip():
        clauses.append(SQL("bid_notice_no = %s"))
        params.append(bid_notice_no.strip())
    if contract_org_name and contract_org_name.strip():
        clauses.append(SQL("contract_org_name ILIKE %s"))
        params.append(f"%{contract_org_name.strip()}%")
    if company_name and company_name.strip():
        clauses.append(_exists_contract_company_clause(schema_name, company_table_name, "company_name ILIKE %s"))
        params.append(f"%{company_name.strip()}%")
    if business_no and business_no.strip():
        clauses.append(_exists_contract_company_clause(schema_name, company_table_name, "business_no = %s"))
        params.append(business_no.strip())
    if registered_from:
        clauses.append(SQL("registered_at >= %s"))
        params.append(_parse_datetime(registered_from))
    if registered_to:
        clauses.append(SQL("registered_at <= %s"))
        params.append(_parse_datetime(registered_to, end_of_day=True))
    if contract_date_from:
        clauses.append(SQL("contract_date >= %s"))
        params.append(_parse_datetime(contract_date_from))
    if contract_date_to:
        clauses.append(SQL("contract_date <= %s"))
        params.append(_parse_datetime(contract_date_to, end_of_day=True))
    if contract_concluded_from:
        clauses.append(SQL("contract_concluded_date >= %s"))
        params.append(_parse_datetime(contract_concluded_from))
    if contract_concluded_to:
        clauses.append(SQL("contract_concluded_date <= %s"))
        params.append(_parse_datetime(contract_concluded_to, end_of_day=True))
    if min_amount is not None:
        clauses.append(SQL("current_contract_amount >= %s"))
        params.append(_parse_decimal(min_amount))
    if max_amount is not None:
        clauses.append(SQL("current_contract_amount <= %s"))
        params.append(_parse_decimal(max_amount))

    if not clauses:
        return SQL(""), tuple(params)
    return SQL("WHERE ") + SQL(" AND ").join(clauses), tuple(params)


def _exists_contract_company_clause(schema_name: str, table_name: str, extra_condition: str) -> SQL:
    return SQL(
        """
        EXISTS (
            SELECT 1
            FROM {}.{} AS contract_company
            WHERE contract_company.contract_resource_key = contract.resource_key
              AND {}
        )
        """
    ).format(Identifier(schema_name), Identifier(table_name), SQL(extra_condition))


def _parse_datetime(value: str, *, end_of_day: bool = False) -> datetime:
    normalized = value.strip()
    if normalized.isdigit():
        if len(normalized) == 8:
            parsed = datetime.strptime(normalized, "%Y%m%d")
            if end_of_day:
                parsed = datetime.combine(parsed.date(), time.max.replace(microsecond=0))
            return parsed.replace(tzinfo=G2B_TIMEZONE)
        if len(normalized) == 12:
            return datetime.strptime(normalized, "%Y%m%d%H%M").replace(tzinfo=G2B_TIMEZONE)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=G2B_TIMEZONE)
        except ValueError:
            continue
    try:
        parsed_date = datetime.strptime(normalized, "%Y-%m-%d").date()
        parsed = datetime.combine(parsed_date, time.max.replace(microsecond=0) if end_of_day else time.min)
        return parsed.replace(tzinfo=G2B_TIMEZONE)
    except ValueError:
        pass
    raise ValueError(f"Invalid datetime: {value}")


def _normalize_category(value: str) -> str:
    normalized = value.strip().upper()
    category = CATEGORY_ALIASES.get(normalized) or CATEGORY_ALIASES.get(value.strip())
    if category is None:
        raise ValueError(f"Invalid category: {value}")
    return category


def _parse_decimal(value: int | float | str) -> Decimal:
    try:
        return Decimal(str(value).replace(",", "").strip())
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal: {value}") from exc


def _parse_positive_int(value: int | str, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{field_name} must be greater than 0")
    return parsed


def _parse_non_negative_int(value: int | str, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be greater than or equal to 0")
    return parsed


def _parse_list_filter(value: list[str] | str | None, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [item.strip() for item in value.split(",")]
    elif isinstance(value, list):
        values = [str(item).strip() for item in value]
    else:
        raise ValueError(f"{field_name} must be a list or comma-separated string")
    return [item for item in values if item]


def _build_order_by(
    *,
    sort_by: str,
    sort_order: str,
    sort_columns: dict[str, str] | None = None,
) -> SQL:
    sort_columns = sort_columns or SORT_COLUMNS
    sort_key = sort_by.strip().lower()
    sort_column = sort_columns.get(sort_key)
    if sort_column is None:
        raise ValueError(f"Invalid sort_by: {sort_by}")

    normalized_order = sort_order.strip().lower()
    if normalized_order not in {"asc", "desc"}:
        raise ValueError(f"Invalid sort_order: {sort_order}")

    direction = SQL("ASC") if normalized_order == "asc" else SQL("DESC")
    id_direction = SQL("ASC") if normalized_order == "asc" else SQL("DESC")
    return SQL("ORDER BY {} {} NULLS LAST, id {}").format(Identifier(sort_column), direction, id_direction)


def _load_license_limits(
    cursor: psycopg.Cursor,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    notice_keys = [(row["bid_notice_no"], row["bid_notice_order"]) for row in rows]
    if not notice_keys:
        return {}

    placeholders = SQL(", ").join(SQL("(%s, %s)") for _ in notice_keys)
    query = SQL(
        """
        SELECT
            bid_notice_no,
            bid_notice_order,
            category,
            business_div_name,
            registered_at,
            limit_group_no,
            limit_serial_no,
            license_limit_name,
            license_limit_code,
            allowed_industries,
            main_field_groups
        FROM {}.{}
        WHERE (bid_notice_no, bid_notice_order) IN ({})
        ORDER BY bid_notice_no, bid_notice_order, limit_group_no NULLS LAST, limit_serial_no NULLS LAST
        """
    ).format(Identifier(schema_name), Identifier(table_name), placeholders)
    params = tuple(value for notice_key in notice_keys for value in notice_key)
    cursor.execute(query, params)

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in cursor.fetchall():
        key = (row["bid_notice_no"], row["bid_notice_order"])
        grouped.setdefault(key, []).append(_format_license_limit(row))
    return grouped


def _load_participation_regions(
    cursor: psycopg.Cursor,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    notice_keys = [(row["bid_notice_no"], row["bid_notice_order"]) for row in rows]
    if not notice_keys:
        return {}

    placeholders = SQL(", ").join(SQL("(%s, %s)") for _ in notice_keys)
    query = SQL(
        """
        SELECT
            bid_notice_no,
            bid_notice_order,
            category,
            business_div_name,
            registered_at,
            limit_group_no,
            limit_serial_no,
            region_name,
            region_code
        FROM {}.{}
        WHERE (bid_notice_no, bid_notice_order) IN ({})
        ORDER BY bid_notice_no, bid_notice_order, limit_group_no NULLS LAST, limit_serial_no NULLS LAST
        """
    ).format(Identifier(schema_name), Identifier(table_name), placeholders)
    params = tuple(value for notice_key in notice_keys for value in notice_key)
    cursor.execute(query, params)

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in cursor.fetchall():
        key = (row["bid_notice_no"], row["bid_notice_order"])
        grouped.setdefault(key, []).append(_format_participation_region(row))
    return grouped


def _load_success_bids(
    cursor: psycopg.Cursor,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    notice_keys = [(row["bid_notice_no"], row["bid_notice_order"]) for row in rows]
    if not notice_keys:
        return {}

    placeholders = SQL(", ").join(SQL("(%s, %s)") for _ in notice_keys)
    query = SQL(
        """
        SELECT
            resource_key,
            category,
            category_label,
            bid_notice_no,
            bid_notice_order,
            bid_classification_no,
            rebid_no,
            notice_division_code,
            title,
            participant_count,
            winner_name,
            winner_business_no,
            winner_ceo_name,
            winner_address,
            winner_phone_no,
            winning_amount,
            winning_rate,
            actual_opening_at,
            demand_org_code,
            demand_org_name,
            registered_at,
            final_success_date,
            final_success_company_officer
        FROM {}.{}
        WHERE (bid_notice_no, bid_notice_order) IN ({})
        ORDER BY bid_notice_no, bid_notice_order, registered_at DESC NULLS LAST
        """
    ).format(Identifier(schema_name), Identifier(table_name), placeholders)
    params = tuple(value for notice_key in notice_keys for value in notice_key)
    cursor.execute(query, params)

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in cursor.fetchall():
        key = (row["bid_notice_no"], row["bid_notice_order"])
        grouped.setdefault(key, []).append(_format_success_bid(row))
    return grouped


def _load_contract_companies(
    cursor: psycopg.Cursor,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    resource_keys = [row["resource_key"] for row in rows]
    if not resource_keys:
        return {}

    cursor.execute(
        SQL(
            """
            SELECT
                contract_resource_key,
                sequence_no,
                role_name,
                contract_type_name,
                company_name,
                display_company_name,
                ceo_name,
                country_name,
                share_rate,
                business_no
            FROM {}.{}
            WHERE contract_resource_key = ANY(%s)
            ORDER BY contract_resource_key, sequence_no
            """
        ).format(Identifier(schema_name), Identifier(table_name)),
        (resource_keys,),
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in cursor.fetchall():
        grouped.setdefault(row["contract_resource_key"], []).append(_format_contract_company(row))
    return grouped


def _load_contract_demand_orgs(
    cursor: psycopg.Cursor,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    resource_keys = [row["resource_key"] for row in rows]
    if not resource_keys:
        return {}

    cursor.execute(
        SQL(
            """
            SELECT
                contract_resource_key,
                sequence_no,
                organization_code,
                organization_name,
                jurisdiction_name,
                department_name,
                officer_name
            FROM {}.{}
            WHERE contract_resource_key = ANY(%s)
            ORDER BY contract_resource_key, sequence_no
            """
        ).format(Identifier(schema_name), Identifier(table_name)),
        (resource_keys,),
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in cursor.fetchall():
        grouped.setdefault(row["contract_resource_key"], []).append(_format_contract_demand_org(row))
    return grouped


def _format_bid(
    row: dict[str, Any],
    *,
    license_limits: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    participation_regions: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    success_bids: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    bid = {
        "id": row["resource_key"],
        "category": row["category"],
        "category_label": row["category_label"],
        "title": row["title"],
        "notice_agency": {
            "code": row["notice_agency_code"],
            "name": row["notice_agency_name"],
        },
        "demand_agency": {
            "code": row["demand_agency_code"],
            "name": row["demand_agency_name"],
        },
        "budget": int(row["budget"]) if row["budget"] is not None else None,
        "published_at": _format_datetime(row["published_at"]),
        "deadline_at": _format_datetime(row["deadline_at"]),
        "opening_at": _format_datetime(row["opening_at"]),
        "contract_method": row["contract_method"],
        "bid_method": row["bid_method"],
        "notice_kind": row["notice_kind"],
        "detail_url": row["detail_url"],
        "metadata": {
            "bid_notice_no": row["bid_notice_no"],
            "bid_notice_order": row["bid_notice_order"],
        },
    }
    if license_limits is not None:
        key = (row["bid_notice_no"], row["bid_notice_order"])
        bid["license_limits"] = license_limits.get(key, [])
    if participation_regions is not None:
        key = (row["bid_notice_no"], row["bid_notice_order"])
        bid["participation_regions"] = participation_regions.get(key, [])
    if success_bids is not None:
        key = (row["bid_notice_no"], row["bid_notice_order"])
        bid["success_bids"] = success_bids.get(key, [])
    return bid


def _format_license_limit(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "category": row["category"],
        "business_div_name": row["business_div_name"],
        "registered_at": _format_datetime(row["registered_at"]),
        "limit_group_no": row["limit_group_no"],
        "limit_serial_no": row["limit_serial_no"],
        "license_limit_name": row["license_limit_name"],
        "license_limit_code": row["license_limit_code"],
        "allowed_industries": row["allowed_industries"] or [],
        "main_field_groups": row["main_field_groups"] or [],
    }


def _format_participation_region(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "category": row["category"],
        "business_div_name": row["business_div_name"],
        "registered_at": _format_datetime(row["registered_at"]),
        "limit_group_no": row["limit_group_no"],
        "limit_serial_no": row["limit_serial_no"],
        "region_name": row["region_name"],
        "region_code": row["region_code"],
    }


def _format_success_bid(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["resource_key"],
        "category": row["category"],
        "category_label": row["category_label"],
        "title": row["title"],
        "participant_count": row["participant_count"],
        "winner": {
            "name": row["winner_name"],
            "business_no": row["winner_business_no"],
            "ceo_name": row["winner_ceo_name"],
            "address": row["winner_address"],
            "phone_no": row["winner_phone_no"],
        },
        "winning_amount": int(row["winning_amount"]) if row["winning_amount"] is not None else None,
        "winning_rate": float(row["winning_rate"]) if row["winning_rate"] is not None else None,
        "actual_opening_at": _format_datetime(row["actual_opening_at"]),
        "registered_at": _format_datetime(row["registered_at"]),
        "final_success_date": _format_datetime(row["final_success_date"]),
        "final_success_company_officer": row["final_success_company_officer"],
        "demand_org": {
            "code": row["demand_org_code"],
            "name": row["demand_org_name"],
        },
        "metadata": {
            "bid_notice_no": row["bid_notice_no"],
            "bid_notice_order": row["bid_notice_order"],
            "bid_classification_no": row["bid_classification_no"],
            "rebid_no": row["rebid_no"],
            "notice_division_code": row["notice_division_code"],
        },
    }


def _format_contract(
    row: dict[str, Any],
    *,
    companies: dict[str, list[dict[str, Any]]] | None = None,
    demand_organizations: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    contract = {
        "id": row["resource_key"],
        "category": row["category"],
        "category_label": row["category_label"],
        "contract_name": row["contract_name"],
        "business_div_name": row["business_div_name"],
        "amounts": {
            "total_contract_amount": int(row["total_contract_amount"]) if row["total_contract_amount"] is not None else None,
            "current_contract_amount": int(row["current_contract_amount"]) if row["current_contract_amount"] is not None else None,
        },
        "contract_org": {
            "code": row["contract_org_code"],
            "name": row["contract_org_name"],
            "jurisdiction_name": row["contract_org_jurisdiction_name"],
            "department_name": row["contract_org_department_name"],
            "officer_name": row["contract_org_officer_name"],
        },
        "contract_method": row["contract_method"],
        "registered_at": _format_datetime(row["registered_at"]),
        "changed_at": _format_datetime(row["changed_at"]),
        "contract_date": _format_datetime(row["contract_date"]),
        "contract_concluded_date": _format_datetime(row["contract_concluded_date"]),
        "detail_url": row["detail_url"],
        "metadata": {
            "unified_contract_no": row["unified_contract_no"],
            "decision_contract_no": row["decision_contract_no"],
            "contract_ref_no": row["contract_ref_no"],
            "bid_notice_no": row["bid_notice_no"],
        },
    }
    if companies is not None:
        contract["companies"] = companies.get(row["resource_key"], [])
    if demand_organizations is not None:
        contract["demand_organizations"] = demand_organizations.get(row["resource_key"], [])
    return contract


def _format_contract_company(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "sequence_no": row["sequence_no"],
        "role_name": row["role_name"],
        "contract_type_name": row["contract_type_name"],
        "company_name": row["company_name"],
        "display_company_name": row["display_company_name"],
        "ceo_name": row["ceo_name"],
        "country_name": row["country_name"],
        "share_rate": float(row["share_rate"]) if row["share_rate"] is not None else None,
        "business_no": row["business_no"],
    }


def _format_contract_demand_org(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "sequence_no": row["sequence_no"],
        "organization_code": row["organization_code"],
        "organization_name": row["organization_name"],
        "jurisdiction_name": row["jurisdiction_name"],
        "department_name": row["department_name"],
        "officer_name": row["officer_name"],
    }


def _format_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is not None:
        return value.astimezone(G2B_TIMEZONE).isoformat()
    return value.replace(tzinfo=G2B_TIMEZONE).isoformat()
