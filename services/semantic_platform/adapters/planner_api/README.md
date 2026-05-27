# semantic_platform_planner_api

Runtime-facing semantic planner API for MCP/executor clients.

This adapter is separate from the admin API. It exposes only approved catalog
runtime surfaces and execution planning:

- `GET /health/ready`
- `GET /semantic/catalog`
- `GET /semantic/execution/contracts`
- `POST /semantic/capabilities/retrieve`
- `GET /semantic/execution/checks`
- `POST /semantic/execution/checks`
- `POST /semantic/planner/execution-plan`
- `POST /semantic/planner/runtime-context`

It must not expose source upload, secret CRUD, ingestion, proposal review, or
catalog mutation endpoints. Catalog version list/export/restore is an admin
governance concern and must stay on `semantic-platform-api`.
