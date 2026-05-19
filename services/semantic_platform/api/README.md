# semantic_platform_api

Semantic platform API.

This API exposes the semantic operating layer for public API metadata. It serves
the canonical catalog and builds compact runtime context packages for LLM/MCP
planners.

Endpoints:

- `GET /health/ready`
- `GET /semantic/catalog`
- `GET /semantic/execution/contracts`
- `GET /semantic/meta`
- `GET /semantic/capability-documents`
- `POST /semantic/capability-documents/rebuild`
- `POST /semantic/capability-documents/embed`
- `POST /semantic/capabilities/retrieve`
- `GET /semantic/execution/checks`
- `POST /semantic/execution/checks`
- `GET /planner/execution-graphs`
- `POST /planner/execution-graphs`
- `POST /planner/plan`
- `POST /planner/execution-plan`
- `POST /runtime/context`
