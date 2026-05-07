from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from apps.g2b_mcp.app.adapters.db import search_bids
from apps.g2b_mcp.app.adapters.live import search_live_bids

mcp = FastMCP("g2b-mcp")


@mcp.custom_route("/health/ready", methods=["GET"])
async def ready(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ready"})


@mcp.tool
def search_bid(
    category: str | None = None,
    keyword: str | None = None,
    published_from: str | None = None,
    published_to: str | None = None,
    deadline_from: str | None = None,
    deadline_to: str | None = None,
    organization_name: str | None = None,
    min_budget: int | None = None,
    max_budget: int | None = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "published_at",
    sort_order: str = "desc",
) -> dict:
    """
    category: SERVICE | GOODS | CONSTRUCTION. Optional.
    keyword: title or organization keyword. Optional.
    published_from/published_to: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Optional.
    deadline_from/deadline_to: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Optional.
    offset: zero-based result offset. Optional.
    sort_by: published_at | deadline_at | budget. Optional.
    sort_order: asc | desc. Optional.
    """
    try:
        result = search_bids(
            category=category,
            keyword=keyword,
            published_from=published_from,
            published_to=published_to,
            deadline_from=deadline_from,
            deadline_to=deadline_to,
            organization_name=organization_name,
            min_budget=min_budget,
            max_budget=max_budget,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        if result["count"] > 0:
            return result
        try:
            live_result = search_live_bids(
                category=category,
                keyword=keyword,
                published_from=published_from,
                published_to=published_to,
                deadline_from=deadline_from,
                deadline_to=deadline_to,
                organization_name=organization_name,
                min_budget=min_budget,
                max_budget=max_budget,
                limit=limit,
                offset=offset,
                sort_by=sort_by,
                sort_order=sort_order,
            )
            live_result["fallback_from"] = "normalized_db_empty"
            return live_result
        except RuntimeError as exc:
            result["fallback_error"] = {
                "type": "live_api_unavailable",
                "message": str(exc),
            }
            return result
    except ValueError as exc:
        return {
            "error": {
                "type": "invalid_request",
                "message": str(exc),
            }
        }
