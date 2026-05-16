# AGENTS.md

## Purpose

This repository is an AI-ready data platform. Keep the structure simple and keep domain meaning in apps.

```text
Raw sources -> canonical data -> app semantic layer -> MCP / RAG / domain apps
```

## Layers

### core/

Generic platform code only:

- `core/catalog`: manifest discovery for platform and app services
- `core/runtime/app_service`: reusable app-service lifecycle, health, logging, request/event/run stores
- `core/runtime/runtime_db`: Postgres connection, checkpoint, and service observability schema helpers
- `core/observability`: shared log and retention helpers
- `core/contracts`: stable cross-app contracts such as execution identity
- `core/semantic`: generic semantic object/document contracts

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

### apps/

Apps own orchestration, business rules, persistence, ontology, semantic transformers, MCP/RAG/domain APIs, and app-specific pipeline workers.

Current apps:

- `apps/g2b/pipeline`
- `apps/g2b/mcp`
- `apps/pubdata_mcp`

### Semantic platform boundary

`services/semantic_platform` is declarative semantic intelligence:

- semantic source of truth under `catalog/*`
- `SemanticType`, small `Entity`, `Relation`, and semantic `Capability` definitions
- capability graph and semantic execution DAG planning
- runtime context packaging for LLM/MCP planners
- semantic join rules expressed by semantic identifiers
- reviewed execution contracts under `catalog/execution/*`
  - capability -> provider/tool implementation metadata
  - raw provider field -> `SemanticType` mappings
- LLM-first execution planning:
  - select `operation_id` values from approved operation contracts
  - produce semantic arguments, argument bindings, post filters, and integration plans
  - describe what to execute and how semantic data flows between steps

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

Capability ids in `services/semantic_platform/catalog/capabilities.yaml` must be
provider-neutral, for example `search_contracts`, not `pps.search_contracts`.
Provider/tool implementation mappings and provider field mappings belong in
`services/semantic_platform/catalog/execution/*` as approved declarative contracts.
`pubdata_mcp` may read those contracts, but must not own them.
`apps/pubdata_mcp/specs/*` is only for MCP tool specs with top-level `tools`.

Planner and executor contract:

```text
Client question
  -> pubdata_mcp
  -> services/semantic_platform LLM execution planner
  -> semantic execution plan
  -> pubdata_mcp operation executor
  -> provider APIs
  -> semantic normalization / integration / answer
```

`services/semantic_platform` may include operation metadata in a plan:

- `operation_id`
- method/path copied from `operation_contracts`
- semantic arguments
- `argument_bindings`
- post filters
- semantic integration plan

Operation contracts must model provider control parameters declaratively. For
example, parameters such as `inqryDiv` must not be hard-coded in executor code
or added as unconditional defaults when the API spec defines multiple meanings.
Represent them as planner-selected semantic controls:

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

This applies to source ingestion, semantic mapping, intent parsing, proposal
generation, and any future LLM-assisted planning path.

## Rules

- Domain logic belongs in `apps/*`.
- Generic service/runtime logic belongs in `services/*` or `core/*`.
- App ontology, relationship names, semantic tags, and semantic document builders belong in `apps/<app>/app/semantic`.
- Semantic planning belongs in `services/semantic_platform`; provider execution belongs in `apps/pubdata_mcp`.
- `services/semantic_platform/worker` owns manual/background ingestion jobs and Prefect manual deployments. Do not add schedules until explicitly requested.
- `core/semantic` contains contracts only.
- Prefect control plane manifests live in `services/prefect`; app-specific Prefect workers live under `apps/<app>`.
- Compose is generated from manifests. Do not hand-edit `deploy/compose/docker-compose.yml` except to inspect generated output.
- Secret values must stay in env files, not manifests or payloads.

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
