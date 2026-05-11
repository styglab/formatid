from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from apps.g2b.mcp.app.adapters.db import search_bids, search_success_bids

mcp = FastMCP("g2b-mcp")


@mcp.custom_route("/health/ready", methods=["GET"])
async def ready(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ready"})


@mcp.tool
def search_bid(
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
    organization_name: str | None = None,
    demand_org_name: str | None = None,
    has_budget: bool | None = None,
    min_budget: int | None = None,
    max_budget: int | None = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "published_at",
    sort_order: str = "desc",
    include_license_limits: bool = False,
    include_participation_regions: bool = False,
    include_success_bids: bool = False,
) -> dict:
    """
    category: SERVICE(용역) | GOODS(물품) | CONSTRUCTION(공사) | FOREIGN(외자/외자구매). Optional.
    keyword: title or organization keyword. Optional.
    notice_kind: exact notice kind such as 등록공고 | 변경공고 | 재공고 | 취소공고. Optional.
    exclude_cancelled: excludes notice_kind containing 취소 by default.
    contract_method: contract method keyword such as 제한경쟁 | 일반경쟁 | 수의계약. Optional.
    bid_method: bid method keyword such as 전자입찰 | 직찰. Optional.
    bid_notice_no/bid_notice_order: exact notice identity filters. Optional.
    published_from/published_to: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Optional.
    deadline_from/deadline_to: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Optional.
    opening_from/opening_to: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Optional.
    demand_org_name: demand organization keyword. Optional.
    has_budget: true for budget only, false for no-budget only. Optional.
    offset: zero-based result offset. Optional.
    sort_by: published_at | deadline_at | budget. Optional.
    sort_order: asc | desc. Optional.
    include_license_limits: include normalized license limit conditions for each bid. Optional.
    include_participation_regions: include normalized participation region conditions for each bid. Optional.
    include_success_bids: include normalized successful bid results for each bid. Optional.
    """
    try:
        return search_bids(
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
            organization_name=organization_name,
            demand_org_name=demand_org_name,
            has_budget=has_budget,
            min_budget=min_budget,
            max_budget=max_budget,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order,
            include_license_limits=include_license_limits,
            include_participation_regions=include_participation_regions,
            include_success_bids=include_success_bids,
        )
    except ValueError as exc:
        return {
            "error": {
                "type": "invalid_request",
                "message": str(exc),
            }
        }


@mcp.tool
def search_success_bid(
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
    min_winning_amount: int | None = None,
    max_winning_amount: int | None = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "registered_at",
    sort_order: str = "desc",
) -> dict:
    """
    Search successful bid results from normalized G2B award data.

    category: SERVICE(용역) | GOODS(물품) | CONSTRUCTION(공사) | FOREIGN(외자/외자구매). Optional.
    keyword: title, winner, or demand organization keyword. Optional.
    bid_notice_no/bid_notice_order: exact notice identity filters. Optional.
    winner_name/winner_business_no: successful bidder filters. Optional.
    registered_from/registered_to: award result registration date range. Optional.
    final_success_from/final_success_to: final award date range. Optional.
    min_winning_amount/max_winning_amount: winning amount range in KRW. Optional.
    sort_by: registered_at | final_success_date | winning_amount | winning_rate. Optional.
    sort_order: asc | desc. Optional.
    """
    try:
        return search_success_bids(
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
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order,
        )
    except ValueError as exc:
        return {
            "error": {
                "type": "invalid_request",
                "message": str(exc),
            }
        }
