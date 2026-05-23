# semantic_platform_api

Semantic platform API.

This API exposes the semantic operating layer for public API metadata. It serves
the canonical catalog and builds compact runtime context packages for LLM/MCP
planners.

Endpoints:

- `GET /health/ready`
- `GET /semantic/catalog`
- `GET /catalog`
- `GET /catalog/sections/{section}?limit=100&offset=0&q=`
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

`/catalog` is kept for compatibility. Dashboard and larger clients should prefer
`/catalog/sections/{section}` so catalog tables can be paged at the API layer.
Supported sections include:

- `semantic_types`
- `entities`
- `entity_identifiers`
- `capabilities`
- `capability_documents`
- `capability_entity_links`
- `capability_dependencies`
- `resources`
- `operations`
- `operation_fields`
- `operation_contracts`
- `operation_variants`
- `field_mappings`
- `capability_implementations`
- `semantic_join_rules`
- `planning_examples`
