# semantic_platform

Semantic operating layer for public API metadata, LLM runtime context, and MCP
tool planning.

This service is not an ontology platform. It is a semantic capability platform:
public API metadata is normalized into small semantic types, resources,
relations, and capabilities so the platform can plan tool use and joins for an
LLM/MCP client.

The target runtime is a semantic query engine, not a CRUD API gateway. Clients
should eventually ask semantic questions while the server handles parsing,
capability resolution, execution planning, API orchestration, semantic
normalization, and integration.

## Design Principles

- Everything is metadata.
- `SemanticType` is the primary normalization unit.
- `Capability` is the only execution abstraction visible to planners.
- Entity count stays small; semantic types may grow.
- Canonical metadata and generated metadata are separate.
- LLM planners read capability graph and runtime context, not raw API specs.
- Planning is LLM-first and contract-validated: an LLM proposes semantic
  execution plans, while catalog metadata and operation contracts constrain what
  can actually execute.
- GraphDB/RDF/OWL are future export targets, not the initial source of truth.
- `semantic_platform` is declarative semantic intelligence.
- `pubdata_mcp` is imperative execution runtime.
- `semantic_platform` must not contain planner logic that depends on provider
  endpoints, auth, pagination, retry behavior, or physical operation field names.

## Structure

```text
services/semantic_platform/
  catalog/            # semantic source of truth
    core/             # canonical entities, semantic types, identifiers, relations
    domains/          # small domain extensions that depend on core
    resources/        # provider-neutral resource metadata only
    mappings/         # semantic normalization and cross-domain join rules
    execution/        # reviewed provider implementation and field-mapping contracts
    capabilities.yaml # planner-facing capability graph
    generated/        # disposable generated artifacts only

  ingestion/          # API spec crawling/parsing/extraction
  worker/             # manual background workers for ingestion/proposal jobs
  semantic_mapper/    # LLM-assisted semantic mapping with contract validation
  runtime/            # runtime retrieval and context packaging
  planner/            # capability graph based planning
  mcp/                # generated MCP schemas/tools/adapters
  storage/            # Postgres JSONB/pgvector abstraction
  api/                # current FastAPI service
```

## Platform Boundary

The repository already has platform services such as Postgres, Redis, Nginx,
`platform_api`, and dashboards. `semantic_platform` is a platform semantic
control plane. It owns public-data semantic meaning, capability planning,
proposal review, and runtime context for apps such as `pubdata_mcp`.

`pubdata_mcp` is the MCP-facing app. It should stay thin:

- expose MCP transport and tool manifests
- call `semantic_platform` for semantic context/planning
- execute provider adapters for public APIs
- return normalized results with semantic metadata

The product dependency direction is:

```text
MCP Client
  -> pubdata_mcp
  -> semantic_platform planner/context APIs
  -> pubdata_mcp provider tools/API adapters
  -> pubdata_mcp semantic integration/response assembly
```

This avoids putting semantic planning inside the client and avoids turning MCP
into a flat collection of endpoint wrappers.

Direct calls to `semantic_platform` are acceptable for internal debugging,
catalog inspection, and admin workflows. Product MCP clients should normally
communicate with `pubdata_mcp` only.

## Capability Boundary

Capabilities are first-class semantic abstractions. The planner must only refer
to semantic capability ids such as:

```text
search_contracts
check_business_status
get_air_quality
```

It must not refer to provider/tool ids such as:

```text
pps.search_contracts
search_pps_contracts_live
g2b.getBidNotice
```

Provider and tool implementations live in `pubdata_mcp`, for example:

```text
search_contracts -> pps/search_pps_contracts_live
check_business_status -> nts/check_nts_business_status_live
```

This keeps provider replacement, API version changes, pagination, retries, and
auth out of the semantic layer.

## Join Boundary

`semantic_platform` knows semantic joins:

```text
Organization --WON--> Contract
join_key: business_registration_number
```

`semantic_platform` owns reviewed physical operation mappings as declarative
execution contracts:

```text
bidwinnrBizno -> business_registration_number
b_no -> business_registration_number
```

Physical operation fields must not be required by planner logic.

Operation raw field mappings are stored in `semantic_platform`, not in
`pubdata_mcp`:

```text
services/semantic_platform/catalog/execution/operation_field_mappings.yaml
services/semantic_platform/catalog/execution/capability_implementations.yaml
```

`pubdata_mcp` may load those approved contracts through the semantic platform
API and use them for execution, but it must not generate proposals or mutate
catalog files.

## Core Models

The MVP keeps the canonical model intentionally small:

- `Resource`: an API, dataset, document, or MCP tool surface.
- `Entity`: a small real-world object class such as `Organization`, `Location`, or `Contract`.
- `SemanticType`: normalized field meaning such as `business_registration_number` or `contract_amount`.
- `Relation`: a reasoning edge such as `Organization WON Contract`.

## Runtime Entry Point

`POST /runtime/context` builds compact LLM/MCP context:

```json
{
  "query": "대기오염 심한 지역의 건설 프로젝트",
  "limit": 6
}
```

The response contains relevant semantic types, entities, capabilities, relations,
join keys, and execution hints. Provider/tool implementation details are
resolved by `pubdata_mcp`.

`POST /planner/plan` builds a provider-neutral semantic execution DAG:

```json
{
  "query": "300억 이상 기업들의 조달 참여 규모",
  "limit": 6
}
```

The DAG contains ordered semantic capability nodes such as `search_contracts`.
It deliberately does not contain provider endpoints, auth parameters, or MCP
tool names such as `search_pps_contracts_live`.

## Target Semantic Query Flow

The final runtime should support a single semantic orchestration entrypoint,
for example:

```text
semantic_query(query: string, constraints?: object)
```

Target flow:

```text
Client Question
  -> Semantic Parser
  -> Capability Resolver
  -> Semantic Planner
  -> Execution Graph
  -> Provider Executors
  -> Semantic Integrator
  -> Structured Result
```

The client should not need to know provider endpoints or tool names such as
`search_pps_contracts_live`. It should ask for a semantic outcome, and the
server should decide which semantic capability can satisfy it.

Planning ownership:

```text
semantic_platform
  - parses semantic intent
  - resolves entities, semantic types, relations, and capabilities
  - builds provider-neutral execution DAG
  - may use an LLM for ambiguous language and synonym expansion

pubdata_mcp
  - exposes semantic_query to MCP clients
  - asks semantic_platform for the plan
  - resolves capabilities to provider executors
  - handles auth, pagination, retries, raw parsing, and normalization
  - assembles execution evidence and final response
```

So the planner is inside `semantic_platform`. The MCP server contains the
execution orchestrator, not the semantic planner. An LLM can be used inside
`semantic_platform` as a helper, but the capability graph, operation contracts,
and validators remain the execution boundary.

LLM intent parsing is optional and disabled by default:

```text
LLM_MODE=disabled
SEMANTIC_PLATFORM_LLM_MODE=disabled
SEMANTIC_PLATFORM_LLM_MODEL=gpt-4.1-mini
OPENAI_API_KEY=...
```

When mode is `disabled`, LLM-backed paths return an explicit skipped result or
fall back to catalog-only metadata responses. When mode is `codex_manual`, the
same graph/API path still runs, but the external LLM response must be supplied
explicitly at the call boundary, such as `manual_plan`, `manual_intent`, or
`--manual-llm-response`.

When mode is `openai`, the LLM returns structured semantic intent through the
configured OpenAI-compatible API:

```json
{
  "entities": ["Organization", "Contract"],
  "semantic_types": ["contract_amount"],
  "semantic_arguments": {"business_registration_number": "1234567890"},
  "filters": [{"semantic_type": "contract_amount", "operator": ">=", "value": 30000000000}],
  "metrics": ["contract_amount_sum"]
}
```

The LLM output is sanitized against catalog candidates before planning.
`semantic_arguments` are copied into matching capability DAG nodes so
`pubdata_mcp` can later map canonical arguments to provider raw arguments using
`operation_field_mappings`.

## LLM Development Mode

LLM-backed paths separate mode selection from secret values.

```text
LLM_MODE=disabled      # no external LLM
LLM_MODE=codex_manual  # Codex/manual proposal workflow
LLM_MODE=openai        # call OpenAI; requires OPENAI_API_KEY
```

`OPENAI_API_KEY` is a secret only. Do not use values such as `codex` inside
`OPENAI_API_KEY` to choose behavior.

When mode is `disabled`, runtime code should return an explicit `skipped` or
`not_generated` result. When mode is `codex_manual`, Codex may manually act as
the LLM, but the manual JSON must be passed into the active request or graph
run. Runtime code must not auto-discover query-hash or document-id fixtures.

Source ingestion manual payloads should use:

```json
{
  "proposal_builder": "codex_manual_llm",
  "status": "pending_review"
}
```

Intent parsing manual payloads are plain structured intent JSON objects:

```json
{
  "language": "ko",
  "entities": ["Organization"],
  "semantic_types": ["business_registration_number"],
  "semantic_arguments": {"business_registration_number": "1234567890"},
  "filters": [],
  "metrics": [],
  "constraints": [],
  "confidence": 0.9
}
```

This rule applies to source ingestion, semantic mapping, intent parsing,
proposal generation, and future LLM-assisted planning.

## Worker Boundary

`api/` serves request/response semantic APIs. `worker/` owns manual or future
scheduled background jobs.

Current worker:

```text
worker/flows/source_ingestion.py
  scan sources
  check sha256 registry
  run ingestion/source_graph.py for changed files
  write source API/operation chunks
  write proposal artifacts or directly apply catalog changes
```

Per-source ingestion graph:

```text
read_source
  -> extract_text
  -> split_source_chunks
  -> write_source_chunks
  -> load_catalog_context
  -> analyze_source_with_llm
  -> write_proposals       # proposal commit mode
```

Trusted automation can use the same graph with only the final commit node
swapped:

```text
read_source
  -> extract_text
  -> split_source_chunks
  -> write_source_chunks
  -> load_catalog_context
  -> analyze_source_with_llm
  -> apply_catalog_changes # direct_apply commit mode
```

The default must remain `proposal`. `direct_apply` is for trusted sources or
controlled automation where review is intentionally skipped.

`split_source_chunks` is a structural chunker. It finds API/operation candidate
sections from operation headings, endpoint paths, operation ids, and
request/response schema signals. It must not decide canonical semantics,
providers, capabilities, or field mappings. Those decisions happen in proposal
generation and review.

Chunk output:

```text
sources/chunks/<document_id>.chunks.jsonl
```

Run manually:

```bash
python3 -m services.semantic_platform.worker.flows.source_ingestion
python3 -m services.semantic_platform.worker.flows.source_ingestion --commit-mode direct_apply
LLM_MODE=codex_manual python3 -m services.semantic_platform.worker.flows.source_ingestion \
  --source sources/some_api_guide.docx \
  --manual-llm-response /tmp/source_llm_response.json
```

The worker is also exposed as a Prefect manual deployment:

```text
semantic-platform-source-ingestion/manual
```

There is no schedule. Prefect is used for manual triggering, run history,
concurrency control, and future retries/observability. `ingestion/source_graph.py`
remains the per-source LangGraph.

### Components To Add

```text
runtime/
  semantic_parser/       # query -> entities, semantic types, filters, metrics
  capability_resolver/   # intent + context -> candidate capabilities/resources
  execution_runtime/     # execute semantic DAG nodes through provider adapters/tools
  integrator/            # semantic joins, aggregation, result structuring

planner/
  execution_planner/     # LLM-first query -> executable semantic plan
  intent_parser/         # legacy intent parsing helpers; not the execution planner

mcp/
  tools/
    semantic_query.yaml  # generated MCP entrypoint contracts, when needed
```

### Semantic Query Responsibilities

`semantic_query` should perform:

1. Parse the user query into semantic intent:

   ```json
   {
     "entities": ["Organization", "Contract"],
     "semantic_types": ["contract_amount", "business_registration_number"],
     "filters": [{"semantic_type": "contract_amount", "operator": ">=", "value": 30000000000}],
     "metrics": ["contract_amount_sum"]
   }
   ```

2. Resolve required semantic capabilities from `catalog/capabilities.yaml`.
3. Build a DAG without hard-coding endpoint, provider, or tool names in planner logic.
4. Let `pubdata_mcp` resolve semantic capabilities to provider tools/API adapters.
5. Normalize raw fields through `SemanticType`.
6. Join results by semantic identifiers such as `business_registration_number`.
7. Return structured evidence and semantic result data.

### Current Versus Target State

Current:

```text
semantic_query(query)
  -> returns provider-neutral semantic DAG and implementation readiness

semantic_get_context(query)
  -> returns semantic types, entities, capabilities, relations, join keys

specific API tools
  -> check_nts_business_status_live
  -> search_pps_contracts_live
```

Target:

```text
semantic_query(query)
  -> builds and executes semantic plan
  -> calls required provider tools internally
  -> integrates normalized results
```

In short, the current implementation is the retrieval/context layer. The next
major step is the semantic query engine.

## Implementation Roadmap

1. Keep `catalog/` simple and canonical.
   `SemanticType`, `Resource`, `Relation`, and capability metadata remain the
   primary source of truth.

2. Add operation contracts.
   `catalog/execution/operation_contracts.yaml` describes approved executable
   operations: provider, resource, method, path, request semantic arguments,
   response semantic fields, extractors, and planner-selected control
   parameters such as `inquiry_basis -> inqryDiv`.

3. Add LLM execution planning.
   The planner reads catalog context and operation contracts, then returns a
   semantic execution DAG. It must select approved `operation_id` values rather
   than hard-code provider rules.

4. Represent plans as metadata, for example:

  ```json
  {
    "nodes": [
       {
         "id": "contracts",
         "operation_id": "pps.contract_info.getCntrctInfoListCnstwk",
         "call": {"semantic_arguments": {"page_size": 50}},
         "post_filters": [{"semantic_type": "contract_amount", "operator": ">=", "value": 30000000000}]
       },
       {
         "id": "status",
         "operation_id": "nts.business_registration.status",
         "depends_on": ["contracts"],
         "argument_bindings": {
           "business_registration_number": {
             "from_node": "contracts",
             "semantic_type": "business_registration_number",
             "mode": "batch"
           }
         }
       }
    ],
    "integration": {"type": "semantic_join", "join_key": "business_registration_number"}
  }
  ```

5. Add execution runtime.
   `pubdata_mcp` executes `operation_id` contracts, compiles semantic arguments
   to provider requests, handles auth/retry/pagination, normalizes responses,
   and executes planner-declared argument bindings.

6. Add semantic integration.
   Consume normalized semantic results, join on semantic identifiers, aggregate
   metrics, and return structured evidence.

7. Add `semantic_query` MCP tool.
   Keep existing direct API tools for debugging and fallback, but make
   `semantic_query` the preferred client entrypoint.

## Current Source-Driven Coverage

- `business`: NTS business registration status/validation.
- `procurement`: PPS/G2B bid notices and contracts.
- `finance`: FSC corporate financial information.
- `environment`: KECO AirKorea CAI and pollutant measurements.
- `weather`: KMA short-term forecast.

## Operational Rule

Only `catalog/core`, `catalog/domains`, `catalog/resources`,
`catalog/mappings`, and `catalog/capabilities.yaml` are canonical.

Everything under `catalog/generated` is disposable and must not be treated as
source of truth.
