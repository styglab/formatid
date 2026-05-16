# semantic_platform_api

Semantic platform API.

This API exposes the semantic operating layer for public API metadata. It serves
the canonical catalog and builds compact runtime context packages for LLM/MCP
planners.

Current source-driven domains:

- `business`: NTS business registration status and validation.
- `procurement`: PPS/G2B bid notices and contracts.
- `finance`: FSC corporate financial statement APIs.
- `environment`: KECO AirKorea CAI and pollutant measurements.
- `weather`: KMA short-term forecast APIs.

Endpoints:

- `GET /health/ready`
- `GET /semantic/catalog`
- `GET /semantic/meta`
- `GET /semantic/domains`
- `GET /semantic/resolve?q=사업자번호`
- `POST /semantic/resolve`
- `POST /semantic/capabilities/find`
- `POST /semantic/join/plan`
- `POST /planner/intent`
- `POST /planner/plan`
- `POST /runtime/context`
