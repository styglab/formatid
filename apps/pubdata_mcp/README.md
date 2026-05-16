# pubdata_mcp

Public-data MCP app for direct API-backed tools.

This app exposes direct API-backed tools plus `semantic_query`, the MCP-facing
entrypoint for public-data semantic questions. It should not become the semantic
planner itself. Semantic planning and runtime context belong in
`services/semantic_platform`.

Current direct tool shape:

```txt
MCP tool -> provider adapter -> public API call -> normalized response + semantic metadata
```

It does not use normalized tables, Postgres, or cached data. It still keeps
provider adapters so raw public API calls can be executed. Semantic mapping
contracts are owned by `semantic_platform` and loaded at runtime.

## Structure

```txt
apps/pubdata_mcp/
  specs/         # MCP tool specs: inputs, handlers, response normalization, evidence
  app/providers/ # provider adapters: client, params, parsers, tool handlers
  app/common/    # app-local loaders, catalog access, normalization helpers
```

The approved semantic platform is owned by `services/semantic_platform/api`.
`pubdata_mcp` reads it through `SEMANTIC_PLATFORM_API_URL`.

Target orchestration shape:

```txt
MCP Client
  -> pubdata_mcp semantic_query
  -> semantic_platform parser/resolver/planner
  -> pubdata_mcp provider adapters or generated MCP tools
  -> pubdata_mcp semantic integration/response assembly
  -> structured semantic result + evidence
```

Current `semantic_query` returns a provider-neutral semantic DAG and
implementation readiness. Full server-side execution is the next step.

Shared MCP tool registration lives in `core/mcp_runtime`. It is reusable
runtime code for loading YAML specs and registering FastMCP tools. It must not
contain public-data provider logic.

### Terms

- `semantic catalog`: what the data means across domains. It is owned by
  `services/semantic_platform/api` and defines core entities, domain entities,
  properties, identifiers, semantic capabilities, approved execution contracts,
  operation field mappings, and cross-domain crosswalks.
- `specs`: how external APIs are exposed as MCP tools. A spec declares tool
  inputs, handler path, response normalization, catalog field references, and
  evidence metadata.
- `providers`: how each external API behaves. Provider code contains HTTP
  clients, endpoint constants, argument-to-API parameter mapping, and parsers
  for provider-specific formats.
- `core/mcp_runtime`: reusable MCP registration utilities used by apps. It
  reads specs and registers tools, but it does not know PPS, NTS, or any
  public-data domain.

Provider code is grouped by data source:

```txt
app/providers/pps/
  client.py      # HTTP call and public API resultCode handling
  constants.py   # endpoints and aliases
  semantics.py   # tool arguments -> public API params
  parsers.py     # provider-specific raw field parsers
  transforms.py  # public API response -> MCP response
  tools.py       # YAML handler functions
```

## Tools

Tool contracts are declared in:

- `apps/pubdata_mcp/specs/**/*.yaml`

Specs are grouped by provider and capability. For example:

- `apps/pubdata_mcp/specs/pps/procurement.yaml`
- `apps/pubdata_mcp/specs/nts/business_registration.yaml`

Current example tools:

- `semantic_query`
- `semantic_parse_intent`
- `semantic_get_context`
- `search_pps_bid_notices_live`
- `search_pps_contracts_live`
- `check_nts_business_status_live`
- `validate_nts_business_registration_live`

## Semantic Catalog

Provider-specific fields are connected through the federated semantic catalog:

- `services/semantic_platform/catalog/core/*.yaml`
- `services/semantic_platform/catalog/domains/*/*.yaml`
- `services/semantic_platform/catalog/resources/*.yaml`
- `services/semantic_platform/catalog/mappings/*.yaml`
- `services/semantic_platform/catalog/capabilities.yaml`

MCP resources:

- `pubdata://semantic-platform`

Use semantic resolver tools instead of sending the full catalog to the client:

- `semantic_query`
- `semantic_parse_intent`
- `semantic_get_context`
- `semantic_resolve`
- `semantic_find_capabilities`
- `semantic_plan_join`

`semantic_query` is the preferred product entrypoint. Direct provider tools are
kept for debugging, fallback, and explicit low-level calls.

## Boundary Rules

`pubdata_mcp` is the imperative execution runtime. It may know provider
endpoints, auth, pagination, retry behavior, raw response shape, and physical
field names.

`pubdata_mcp` must not own semantic planning. It executes semantic capabilities
selected by `semantic_platform`.

Planning ownership:

```text
semantic_platform
  - semantic intent parsing
  - capability resolution
  - provider-neutral DAG planning
  - optional LLM-assisted synonym/intent enrichment

pubdata_mcp
  - MCP transport
  - semantic_query facade
  - capability -> provider implementation resolution
  - API auth, pagination, retries, raw parsing
  - operation field -> SemanticType normalization using approved contracts
```

The LLM, when enabled, belongs behind `semantic_platform` planner APIs as a
helper. It should not be the only source of planning truth, and the MCP server
should not embed cross-domain semantic planning rules.

Provider-specific implementation metadata belongs to `semantic_platform`:

- `services/semantic_platform/catalog/execution/capability_implementations.yaml`
- `services/semantic_platform/catalog/execution/operation_field_mappings.yaml`

`pubdata_mcp` reads those contracts through
`GET /semantic/execution/contracts` and may use
`SEMANTIC_PLATFORM_EXECUTION_CONTRACTS_PATH` only as a local development
fallback. It must not own proposal generation or mutate the semantic catalog.
Do not put execution metadata under `specs/`; that directory is loaded as
FastMCP tool specs and every YAML file there must contain a top-level `tools`
list.

The semantic planner should only plan with normalized semantic types and
capability ids such as `search_contracts`. Provider/tool ids such as
`search_pps_contracts_live` are approved execution-contract details, not
planner nodes.

## Environment

Required env:

```text
PPS_PUBLIC_API_KEY=...
NTS_BUSINESSMAN_API_KEY=...
```

`G2B_PUBLIC_API_KEY` and `PUBLIC_API_KEY` are also accepted for local compatibility.
For NTS, `ODCLOUD_API_KEY` is also accepted.
