from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from core.runtime.mcp import register_from_yaml_dir
from apps.pubdata_mcp.domain.catalog import load_catalog


mcp = FastMCP("pubdata-mcp")


@mcp.custom_route("/health/ready", methods=["GET"])
async def ready(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ready"})


@mcp.resource(
    "pubdata://semantic-platform",
    name="public_data_semantic_platform",
    description="Semantic layer catalog and runtime metadata for cross-domain public-data interoperability.",
    mime_type="application/json",
)
def public_data_semantic_platform() -> dict:
    return load_catalog()


register_from_yaml_dir(mcp, "apps/pubdata_mcp/specs")
