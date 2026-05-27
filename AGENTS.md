# AGENTS.md

## Purpose

This repository is an AI-ready data platform. Keep the structure simple and keep domain meaning out of generic runtime code.

```text
Raw API documents -> semantic capability graph -> LLM planning -> MCP execution
```

The current folder ownership guide is documented in
`docs/folder_structure_ko.md`. Keep this file and that guide aligned when the
top-level structure changes.

## Layers

### core/

Generic platform code only:

- `core/catalog`: manifest discovery for platform and app services
- `core/runtime/app_service`: reusable app-service lifecycle, health, logging, request/event/run stores
- `core/runtime/runtime_db`: Postgres connection, checkpoint, and service observability schema helpers
- `core/observability`: shared log and retention helpers
- `core/contracts`: stable cross-app contracts such as execution identity

Do not put app names, procurement fields, business rules, or app orchestration in `core/*`.

### services/

Runnable platform services and backing capabilities:

- `postgres`
- `redis`
- `nginx`
- `platform_api`
- `platform_dashboard`

Services expose generic platform capabilities. They must not contain app-specific business logic,
except for platform-level control planes such as `services/semantic_platform`.
App-required services such as `prefect`, `minio`, and `qdrant` are enabled only when an app or platform control plane declares them.

Current semantic platform service:

- `services/semantic_platform`

Semantic platform source layout:

- `services/semantic_platform/adapters/*`: runnable adapters only
  (`admin_api`, `planner_api`, `dashboard`, `worker`)
- `services/semantic_platform/lib/*`: internal semantic platform libraries
  (`ingestion`, `planner`, `context`, `storage`)
- `services/semantic_platform/manifests/*`: compose/catalog service
  declarations for the semantic platform boundary

### apps/

Apps own orchestration, business rules, persistence, ontology, semantic transformers, MCP/RAG/domain APIs, and app-specific pipeline workers.

Current apps:

- `apps/pubdata_mcp`

### Semantic platform boundary

`services/semantic_platform` is declarative semantic intelligence:

- Postgres-backed semantic source of truth
- capability catalog for retrieval-first tool routing
- capability embeddings/vector index ownership
- `SemanticType`, small `Entity`, `Relation`, and semantic `Capability` definitions
- capability graph and semantic execution DAG planning
- runtime context packaging for LLM/MCP planners
- semantic join rules expressed by semantic identifiers
- reviewed execution contracts stored in Postgres `sp_operation_contracts`
  - capability -> provider/tool implementation metadata
  - raw provider field -> `SemanticType` mappings
- LLM-first execution planning:
  - select `operation_id` values from approved operation contracts
  - produce semantic arguments, argument bindings, post filters, and integration plans
  - describe what to execute and how semantic data flows between steps

Catalog concerns must stay separate:

- Capability Catalog: retrieval-facing capability metadata, examples, aliases,
  inputs, outputs, and tags
- Execution Catalog: resources, operations, contracts, variants, field mappings,
  implementations, and endpoint checks
- Governance Context: naming decisions, conflicts, lineage, review status, and
  merge/deprecation decisions

`load_catalog_context` in ingestion is a context-packaging step that gives the
LLM existing capability catalog, execution summary, and governance context
before it proposes changes. It is not a runtime planner and not a separate
catalog.

It must not contain imperative provider execution code:

- provider HTTP clients
- API auth/key handling
- pagination loops or retry/timeout behavior
- provider SDK/client quirks

`apps/pubdata_mcp` is imperative execution runtime:

- MCP transport and tool manifests
- provider clients/adapters
- auth, retries, pagination, and transport quirks
- raw response parsing
- provider field -> `SemanticType` normalization using approved contracts
- semantic capability implementation dispatch using approved contracts
- compile planner `operation_id` + semantic arguments into provider calls
- execute argument bindings and semantic integrations declared by the planner

It must not own cross-domain planning, global capability ranking, semantic
join-path reasoning, proposal generation, catalog mutation, canonical semantic
definitions, or provider-selection rules such as "공사 means this PPS operation".

Capability ids in semantic_platform must be provider-neutral, for example
`search_contracts`, not `pps.search_contracts`. Provider/tool implementation
mappings and provider field mappings belong in semantic_platform Postgres
tables as approved declarative contracts. `pubdata_mcp` may read those
contracts through `/semantic/execution/contracts`, but must not own them.
`apps/pubdata_mcp/specs/*` is only for MCP tool specs with top-level `tools`.

Planner and executor contract:

```text
Client question
  -> pubdata_mcp
  -> services/semantic_platform/adapters/planner_api LLM execution planner
  -> semantic execution plan
  -> pubdata_mcp operation executor
  -> provider APIs
  -> semantic normalization / integration / answer
```

Semantic platform API services are split by plane:

- `semantic-platform-api`: admin/control plane for dashboard, source upload,
  ingestion, proposals, catalog governance, and run tracking.
- `semantic-platform-planner-api`: runtime plane for MCP/executor clients. It
  exposes approved catalog/contract reads, capability retrieval, endpoint check
  records, runtime context, and execution planning. It must not expose source
  upload, secret CRUD, ingestion, proposal review, or catalog mutation.

`services/semantic_platform` may include operation metadata in a plan:

- `operation_id`
- method/path copied from `operation_contracts`
- semantic arguments
- `argument_bindings`
- post filters
- semantic integration plan

Planner capability coverage is an LLM judgment, not a deterministic
keyword/rule system. Runtime code must not hard-code domain or intent rules
such as "공사 means construction", "변경 이력 means change_history", or
"if these Korean words are present then this capability is missing." Retrieval
may provide candidate capability documents and approved variants, and validation
may verify that selected ids exist in the supplied context, but deciding whether
the retrieved candidates actually satisfy the user's question, or whether no
capability is sufficient, belongs to the LLM planner response.

When no retrieved capability satisfies the user's requested meaning, the LLM
planner should return an explicit not-found plan rather than selecting the
nearest partial match:

```json
{
  "planner": {"status": "not_found", "reason": "capability_not_found"},
  "execution_graph": {"type": "dag", "status": "not_found", "nodes": []},
  "errors": [{"code": "capability_not_found"}]
}
```

Operation contracts must model provider control parameters declaratively. For
example, parameters such as `inqryDiv` must not be hard-coded in executor code
or added as unconditional defaults when the API spec defines multiple meanings.
They are operation-local control fields. The same raw name can mean different
things in different operations, so LLM ingestion must interpret them from the
current operation's request table, descriptions, examples, and endpoint
verification evidence. Represent them as planner-selected semantic controls or,
when one physical endpoint has several meanings, as separate operation variants:

```yaml
inqryDiv:
  kind: control
  semantic_type: inquiry_basis
  planner_selects: true
  enum_mapping:
    contract_date: "1"
```

The LLM planner chooses `inquiry_basis`; `pubdata_mcp` only applies the
declared `enum_mapping`.

If one endpoint can produce multiple semantic capabilities depending on fixed
control values, model it as:

```text
operation_contract
  -> operation_variant
    -> capability_implementation
```

Each `operation_variant` has its own `variant_id`, `capability_id`,
`fixed_semantic_arguments`, `fixed_raw_arguments`, and verification sample.
Planner output should prefer `variant_id`; `pubdata_mcp` must execute the
selected variant without guessing provider choices. Endpoint checks must be
stored per capability/variant, not only per physical endpoint.

During source ingestion, the graph must expose operation-scoped
`operation_variant_candidates` to the LLM when control fields are found. The LLM
decides whether values such as `1`, `2`, or `A` are separate capabilities based
on evidence. Runtime code must not contain branches like "if inqryDiv is 1 then
contract-date search"; that belongs in reviewed catalog data as a variant.

Source ingestion proposals must be capability-scoped review units. A source
run may create many proposals shaped as
`proposal.<source_document_id>.<capability_id>.review`; source evidence
snapshots remain source-scoped. Capability proposals must carry provenance that
lets reviewers trace the capability to source document, source sections,
operation ids, variant ids, endpoint method/path metadata, and evidence
snapshot id.

`apps/pubdata_mcp` must not infer these choices from Korean/provider terms.
It executes selected operation contracts, handles auth/retry/pagination/HTTP,
normalizes responses, applies planner-declared bindings, and joins results.

### LLM development mode

LLM-backed features must separate mode selection from secret values.
`OPENAI_API_KEY` is a secret only. Do not use API key values such as `codex` as
control signals.

Use `LLM_MODE` as the global mode and app-specific overrides such as
`SEMANTIC_PLATFORM_LLM_MODE` only when needed.

Supported modes:

- `disabled`: do not call an external LLM; return explicit skipped/not-generated
  results
- `codex_manual`: do not call an external LLM; Codex may manually act as the LLM
  during development by supplying the LLM response payload explicitly
- `openai`: call the OpenAI API and require `OPENAI_API_KEY`

Secret values must not be committed to env files, manifests, payloads, or
generated artifacts.

When `LLM_MODE=codex_manual` during development:

- Codex may manually act as the LLM for the current task and create a
  reviewable artifact
- the manual LLM output must be passed through the active graph/API boundary
  (`manual_plan`, `manual_intent`, `manual_llm_response`, or
  `--manual-llm-response`)
- runtime code must not auto-discover query-hash or document-id fixture files
- manual artifacts must be labeled, for example
  `proposal_builder: codex_manual_llm`
- manual artifacts must include source/evidence references
- manual artifacts must remain `pending_review` unless the user explicitly asks
  to apply them
- manual reasoning must not be converted into hard-coded provider rules,
  field mappings, or catalog mutations inside graph/runtime code

This applies to source ingestion, semantic mapping, proposal generation,
execution planning, and any future LLM-assisted planning path.

## Rules

- Domain logic belongs in `apps/*`.
- Generic service/runtime logic belongs in `services/*` or `core/*`.
- App ontology, relationship names, semantic tags, and semantic document builders belong in `apps/<app>/app/semantic`.
- Semantic planning belongs in `services/semantic_platform/lib/planner`; provider execution belongs in `apps/pubdata_mcp`.
- `services/semantic_platform/adapters/worker` owns manual/background ingestion jobs and Prefect manual deployments. Do not add schedules until explicitly requested.
- Prefect control plane manifests live in `services/prefect`; app-specific Prefect workers live under `apps/<app>`.
- Compose is generated from manifests. Do not hand-edit `deploy/compose/docker-compose.yml` except to inspect generated output.
- Secret values must stay in env files, not manifests or payloads.
- Retired code belongs under `tmp/retired_apps` or `tmp/retired_core` and must
  not be imported by active runtime paths.
- `tmp/*` is a scratch/working area. Codex must not ask for confirmation before
  creating, updating, validating, copying, or deleting non-secret temporary
  artifacts there. This explicitly includes codex_manual LLM payloads, generated
  request/response JSON, one-off inspection files, transient validation output,
  and other disposable work files. Do the work directly and clean it up when it
  is no longer useful. Do not put secret values in `tmp/*`.
- When validating `tmp/*` artifacts, prefer already-approved commands without
  shell redirection, command substitution, pipes, or heredocs. For example, run
  `python3 -m json.tool tmp/path/payload.json` directly and read the tool output
  instead of redirecting to `/tmp/*.out`, because redirection can trigger a
  sandbox approval prompt even for otherwise approved validation commands.

## Recommended App Structure

```text
apps/<app>/
  app/
    flows/          # orchestration wiring
    tasks/          # execution boundaries
    steps/          # pure transformation/domain logic
    repositories/   # app data access
    semantic/       # app ontology and semantic projection
    service/        # app runner helpers
  infra/
  manifests/
```

## Commands

```bash
python3 scripts/generate_compose.py
python3 scripts/ops.py validate-config
python3 scripts/ops.py lint-boundaries
python3 scripts/ops.py check-all
```

## Acceptance Checklist

- [ ] Correct layer placement
- [ ] No app logic in `core/*` or `services/*`
- [ ] App semantic meaning is under `apps/<app>/app/semantic`
- [ ] Manifest updated when services/apps change
- [ ] Generated compose is in sync
- [ ] `python3 scripts/ops.py validate-config` passes
- [ ] `python3 scripts/ops.py lint-boundaries` passes

END OF FILE
