# pubdata_mcp

Optional LLM MCP Adapter for the Context Platform.

`pubdata_mcp` does not own the Source Catalog, Canonical Model, Binding Layer,
Capability Catalog, proposal generation, capability naming, planning rules, or
provider-selection rules. It exposes high-level MCP tools that call the
server-side Planner Service implemented under `services/context_platform`.

```text
LLM client
  -> pubdata_mcp plan_request
  -> context-platform-planner-api /planner/plan
  -> validated plan
  -> pubdata_mcp execute_plan
  -> context-platform-planner-api /planner/execute
  -> canonical result
```

## Boundary

`context_platform` owns:

- source ingestion
- Source Catalog
- Canonical Model
- Binding Layer
- Capability Catalog
- proposal review and lifecycle
- server-side planning
- plan validation
- validated plan execution

`pubdata_mcp` owns:

- MCP transport
- Planner Service client calls
- high-level plan/execute/explain tools
- developer/debug read tools for approved planner context
- structured MCP responses

`pubdata_mcp` must not:

- execute raw source operations directly
- expose `execute_operation`
- infer provider/domain choices from Korean/provider terms
- own canonical definitions or capability ranking
- mutate catalog or proposal state
- bypass Planner Service for execution

## Structure

```text
apps/pubdata_mcp/
  adapters/
    api/
      main.py
      infra/
  domain/
    catalog.py      # Planner Service / approved context API client
    execution.py    # legacy compatibility code; do not expand raw execution
  specs/catalog.yaml
  manifests/
```

## Target Tools

Primary MCP tools:

- `plan_request`
- `execute_plan`
- `explain_plan`

Developer/debug tools:

- `search_capabilities`
- `get_capability`
- `get_canonical_model`
- `get_operation_bindings`

Forbidden tool:

- `execute_operation`

## Planner APIs

Runtime calls go to `context-platform-planner-api`:

```text
POST /planner/plan
POST /planner/execute
GET /planner/plans/{plan_id}
POST /planner/validate
```

`execute_plan` must send a plan id or validated plan payload to Planner Service.
It must not compile source parameters or call provider APIs locally.

## Result Schema

MCP responses should remain structured and planner-oriented. Stable fields:

- `status`
- `plan_id`
- `selected_capability_id`
- `selected_source_operation_id`
- `canonical_inputs`
- `parameter_bindings`
- `expected_outputs`
- `confidence`
- `requires_confirmation`
- `validation`
- `results`
- `errors`
- `evidence`

Natural-language answer synthesis is a caller/UI concern. The adapter should
return enough structured context for the LLM client to explain the plan and
result.
