from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from apps.g2b_mcp.app.adapters.db import search_bids

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
) -> dict:
    """
    category: SERVICE | GOODS | CONSTRUCTION. Optional.
    keyword: title or organization keyword. Optional.
    published_from/published_to: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Optional.
    deadline_from/deadline_to: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Optional.
    """
    return search_bids(
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
    )
