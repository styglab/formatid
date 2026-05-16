from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from core.mcp_runtime import register_from_yaml


mcp = FastMCP("g2b-mcp")


@mcp.custom_route("/health/ready", methods=["GET"])
async def ready(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ready"})


register_from_yaml(mcp, "apps/g2b/mcp/tools.yaml")
