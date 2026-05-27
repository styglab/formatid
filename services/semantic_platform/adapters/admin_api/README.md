# semantic_platform_admin_api

Semantic platform admin/control-plane API.

This API exposes dashboard and governance surfaces for public API metadata. It
serves source upload, ingestion, proposal review/apply/reject, catalog browsing
and governance edits, and run tracking.

MCP/runtime clients should use `semantic-platform-planner-api` instead of this
adapter for planning and approved execution contract reads.

Structure:

- `app/main.py`: FastAPI routes and HTTP concerns
- `app/gateway.py`: thin gateway from routes into `lib/storage`, `lib/planner`,
  and `lib/context`

Endpoints:

- `GET /health/ready`
- `GET /semantic/catalog`
- `GET /catalog`
- `GET /catalog/sections/{section}?limit=100&offset=0&q=`
- `PATCH /catalog/sections/{section}/{item_id}`
- `GET /catalog/sections/{section}/{item_id}/delete-plan`
- `POST /catalog/sections/{section}/{item_id}/delete`
- `GET /catalog/versions`
- `GET /catalog/versions/{version_id}`
- `GET /catalog/versions/{version_id}/diff`
- `GET /catalog/versions/{version_id}/export`
- `POST /catalog/versions/{version_id}/restore`
- `GET /semantic/execution/contracts`
- `GET /semantic/meta`
- `GET /sources`
- `POST /sources/upload`
- `POST /sources/{source_id}/ingest`
- `GET /secrets`
- `POST /secrets`
- `GET /semantic/capability-documents`
- `POST /semantic/capability-documents/rebuild`
- `POST /semantic/capability-documents/embed`
- `POST /semantic/capabilities/retrieve` (compatibility; prefer planner API)
- `GET /semantic/execution/checks`
- `POST /semantic/execution/checks`
- `GET /planner/execution-graphs`
- `POST /planner/execution-graphs`
- `POST /planner/plan` (compatibility; prefer planner API)
- `POST /planner/execution-plan` (compatibility; prefer planner API)
- `POST /runtime/context` (compatibility; prefer planner API)

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

Editable catalog sections are intentionally limited to semantic governance
objects: `planning_examples`, `capabilities`, `semantic_types`, `entities`,
`semantic_join_rules`, `capability_entity_links`, and
`capability_dependencies`. Execution catalog objects remain read-only until
contract validation and stronger delete planning are added.

Catalog versions:

- Applying a proposal or changing governed catalog rows creates a version.
- Version snapshots use `approved_declarative_catalog_v1`.
- Snapshots include declarative catalog sections only; derived capability
  documents/vectors, endpoint checks, proposals, sources, evidence, runs, and
  secrets are excluded.
- Restore applies the selected snapshot to current catalog tables and creates a
  new active version with `reason=version_restore`; it does not mutate old
  version rows.

Secret handling:

- Source upload stores only `auth_secret_refs` such as
  `secret.data_go_kr.service_key`.
- `auth_parameter_names` stores provider request parameter names such as
  `serviceKey` or `authkey`.
- `POST /secrets` accepts the secret value as write-only input. `GET /secrets`
  returns masked metadata and `has_value`, never the secret value.

Ingestion mode:

- Dashboard/service ingestion uses the API service environment:
  `LLM_MODE=openai` and `OPENAI_API_KEY`.
- Dashboard requests must not send `manual_llm_response`; the API calls OpenAI.
- Worker/CLI development may send `llm_mode=codex_manual` only together with a
  `manual_llm_response` JSON object. That request is recorded as codex-manual
  even if the API service is otherwise configured for OpenAI.
- If no manual response is supplied, the API rejects ingestion unless OpenAI
  service mode is ready. This prevents empty proposals caused by accidental
  `disabled` or `codex_manual` server mode.
- For batch codex-manual development, send one ingestion request per source and
  one manual response payload per source. Review units remain
  capability-scoped proposals; do not collapse several source documents into
  one proposal.
