# pubdata_mcp

MCP-facing runtime for public-data semantic execution.

`pubdata_mcp` does not own the semantic catalog, proposal generation, canonical
types, capability naming, or planning rules. It reads approved execution
contracts from `services/semantic_platform` and executes the plan it receives.

The current product direction is retrieval-first:

```text
Question
  -> semantic_platform capability retrieval
  -> semantic_platform planner
  -> execution graph
  -> pubdata_mcp generic operation executor
```

`pubdata_mcp` does not search the vector index and does not decide which
capability should be used. It receives a selected execution graph and runs it
against approved execution contracts.

## Boundary

```text
MCP Client
  -> pubdata_mcp semantic_query
  -> semantic_platform planner/context APIs
  -> pubdata_mcp generic operation executor
  -> public API
  -> normalized semantic result + evidence
```

`semantic_platform` owns:

- source ingestion
- catalog/proposal review
- catalog version snapshot/export/restore
- canonical semantic types
- capabilities and operation variants
- operation contracts and field mappings
- LLM-first execution planning

`pubdata_mcp` owns:

- MCP transport
- loading approved execution contracts
- compiling semantic arguments into raw API arguments
- auth injection from environment variables
- generic HTTP execution
- response normalization using approved mappings
- structured execution evidence

## Structure

```text
apps/pubdata_mcp/
  specs/catalog.yaml    # semantic MCP tools only
  app/common/catalog.py # semantic_platform API client
  app/common/execution.py
```

Provider-specific MCP tools and provider adapter modules were removed. Public
API behavior should come from approved `operation_contract`,
`operation_variant`, and `field_mapping` rows, not from hard-coded provider
tools.

## Tools

Current MCP tools:

- `semantic_query`
- `semantic_smoke_test_operation`

`semantic_query` is the product entrypoint. `semantic_smoke_test_operation` is
for validating an approved operation or variant.

## Runtime Data

`pubdata_mcp` reads:

```text
GET /semantic/execution/contracts
```

from `semantic-platform-planner-api`, not the admin API. Runtime planning uses:

```text
POST /semantic/planner/execution-plan
```

The response must include:

- resources
- operation_contracts
- operation_variants
- operation_field_mappings
- capability_implementations

## Environment

API keys stay in env files. The generic executor uses only auth metadata from
the approved operation contract:

```text
operation_contract.auth.parameter
operation_contract.auth.in
operation_contract.auth.env_names
```

Provider-specific fallback env names must not be added to runtime code. If a
source needs a different key name, declare it in the reviewed operation
contract.

## Contract Interpreter

The executor interprets reviewed contracts; it does not infer provider rules.
Provider-specific response shapes must be declared in
`operation_contract.response`:

```json
{
  "items_path": "response.body.items.item",
  "count_path": "response.body.totalCount",
  "success": {
    "path": "response.header.resultCode",
    "equals": "00",
    "message_path": "response.header.resultMsg"
  },
  "error": {
    "code_path": "response.header.resultCode",
    "not_equals": "00",
    "message_path": "response.header.resultMsg"
  }
}
```

Fields may use path expressions such as `data[].tax_type`, `[].cur_unit`, or
`response.body.items.item[].untyCntrctNo`. The path evaluator is generic; the
path values themselves come from approved catalog data.

## Request Transform / Validation

Planner output contains semantic values. It is allowed to produce
`phone_number: "01022223333"` or `phone_number: "010-2222-3333"`. The final raw
provider representation is decided by the selected operation contract.

The executor supports declarative request rules:

- `enum_mapping`
- `strip` / `strip_chars`
- `remove_whitespace`
- `digits_only`
- `uppercase` / `lowercase`
- `date_format`
- `phone_format`
- `pattern`
- `enum`
- `min_length`
- `max_length`

Example:

```json
{
  "request": {
    "query": {
      "phone": {
        "semantic_type": "phone_number",
        "transform": {
          "name": "phone_format",
          "style": "kr_mobile_hyphen"
        },
        "pattern": "^01[016789]-[0-9]{3,4}-[0-9]{4}$"
      }
    }
  }
}
```

The executor applies the transform, validates the result, and returns
`validation_error` before any provider call if the declared rule fails. This
keeps endpoint-specific formatting in reviewed catalog data instead of runtime
provider code.

## Result Schema

`semantic_query` returns structured MCP-friendly data. The stable top-level
fields include:

- `status`
- `result_status`
- `selected_capabilities`
- `execution_graph`
- `results`
- `errors`
- `evidence`

`result_status` is more specific than execution status and may include values
such as `executed_with_items`, `executed_empty`, `provider_error`,
`validation_error`, `not_executable`, or `capability_not_found`.
