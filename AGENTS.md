# AGENTS.md

## Purpose

This repository is an AI-ready data platform. Keep the structure simple and keep
domain meaning out of generic runtime code.

```text
Source documents / manual authoring
  -> Source Graph
  -> Meaning Graph
  -> Representation Model
  -> Resolution Graph
  -> Capability Graph
  -> Execution Graph
  -> optional LLM MCP Adapter
```

The current folder ownership guide is documented in
`docs/folder_structure_ko.md`. Keep this file and that guide aligned when the
top-level structure changes.

## Layers

### core/

Generic platform code only:

- `core/catalog`: manifest discovery for platform and app services
- `core/runtime/app_service`: reusable app-service lifecycle, health, logging,
  request/event/run stores
- `core/runtime/runtime_db`: Postgres connection, checkpoint, and service
  observability schema helpers
- `core/observability`: shared log and retention helpers
- `core/contracts`: stable cross-app contracts such as execution identity

Do not put app names, provider fields, business rules, source-specific planning,
or app orchestration in `core/*`.

### services/

Runnable platform services and backing capabilities:

- `postgres`
- `redis`
- `nginx`
- `platform_api`
- `platform_dashboard`
- `prefect`
- `minio`
- `context_platform`

Services expose generic platform capabilities. They must not contain
app-specific business logic except for platform-level control planes such as
`services/context_platform`.

App-required services such as `prefect`, `minio`, and `qdrant` are enabled only
when an app or platform control plane declares them.

### apps/

Apps own app orchestration, provider execution, business rules, app persistence,
MCP/RAG/domain APIs, and app-specific pipeline workers.

Current apps:

- `apps/pubdata_mcp`

## Context Platform

Current implementation:

- module name: `context_platform`
- implementation path: `services/context_platform`
- dashboard route: `/context-platform`
- API route prefix: `/context-platform/api`
- planner route prefix: `/context-platform/planner`

Use Context Platform terminology in code, docs, UI labels, route names, manifest
names, and service names. Active routes and runtime paths must use the Context
Platform naming scheme.

### Source Layout

```text
services/context_platform/
  adapters/
    admin_api/
    planner_api/
    dashboard/
    worker/
  internal/
    meaning/
    representation/
    resolution/
    capability/
    execution/
    governance/
    ingestion/
    planner/
    context/
    storage/
  manifests/
```

- `adapters/*`: runnable boundaries only
- `internal/*`: reusable Context Platform libraries
- `manifests/*`: compose/catalog service declarations

## Frozen Architecture

Context Platform is a Meaning Resolution Platform:

```text
Context Platform is a Meaning Resolution Platform that connects Concepts,
Canonical Representations, Representation Schemas, Sources, Capabilities, and
Executions into an Executable Meaning Graph.
```

The Executable Meaning Graph consists of:

1. Meaning Graph
2. Representation Model
3. Source Graph
4. Resolution Graph
5. Capability Graph
6. Execution Graph
7. Evidence / Governance
8. optional LLM MCP Adapter

Source operations are the executable operations. Use `source_operations` as the
single operation table. Do not create a separate Operation Registry.

Concepts are core meaning nodes, not a standalone vocabulary product. Do not
create a separate Concept Registry product boundary. Implement concepts,
concept schemes, concept relations, value domains, and meaning scopes as part
of the Context Platform Meaning Graph.

Core principles:

- Concept is meaning.
- ObjectType and PropertyType are structure.
- CanonicalRepresentation is a template for expressing a Concept as structure.
- RepresentationSchema defines datatype, regex, enum, cardinality, examples,
  and validation for a representation.
- Binding is source-to-representation resolution.
- Capability is an executable contract.
- ExecutionPlan and ExecutionRun are concrete execution instances.
- Projection is a consumer-facing output shape.
- Evidence makes every assertion reviewable.
- MeaningScope prevents Concept from becoming an unbounded catch-all.

## Source Graph

Source Graph stores raw source systems, documents, source operations,
parameters, fields, constraints, and source errors.

Tables:

- `sources`
- `source_documents`
- `source_operations`
- `source_parameters`
- `source_fields`

An API source operation represents an executable endpoint such as:

```text
GET /company/info
```

Non-API sources, such as PDFs, CSVs, database extracts, DCAT metadata, or field
dictionaries, may produce source fields without executable operations.
Field-only sources can propose Meaning Graph, Representation Model, and
Resolution Graph changes. They become runtime-executable only after an
executable source operation is linked through the Capability Graph.

Source documents are uploaded through MinIO-backed storage. Dashboard upload is
source intake only: it creates a queued agent-ingestion request and must not
start semantic ingestion or draft generation by itself. Worker/agent ingestion
uses Prefect/manual boundaries and CPU-oriented document parsing by default.

## Meaning Graph

Meaning Graph stores the semantic meaning the platform understands.

Core concepts include company, revenue, operating income, total assets,
corporate registration number, business registration number, fiscal year,
business registration status, KRW, and source-specific value concepts.

Tables may be introduced as new registry storage or mapped onto compatible
existing canonical tables during migration:

- meaning scopes
- concept schemes
- concepts
- concept relations
- value domains
- value domain values

`concept.kind` must be explicit. Allowed kinds include:

- `object_concept`
- `metric_concept`
- `identifier_concept`
- `status_concept`
- `value_concept`
- `unit_concept`
- `time_concept`
- `account_concept`
- `document_concept`
- `operation_concept`

Concept relation types must be explicit; do not collapse meaning edges into a
single `related_to` bucket. Use typed relations such as `broader`, `narrower`,
`exact_match`, `close_match`, `has_unit`, `has_value_domain`, `has_value`,
`applicable_to_object`, `represents_identifier_type`, `derived_from`, and
`requires_context`.

## Representation Model

Representation Model stores the standard structures and validation schemas used
to carry meaning.

Tables include:

- `object_types`
- `property_types`
- `link_types`
- `canonical_representations`
- `representation_schemas`
- `external_projections`

Retired `canonical_*` tables are not runtime source of truth. Compatibility API
methods may expose read-only aliases during UI migration, but new writes must
target ObjectType, PropertyType, LinkType, CanonicalRepresentation,
RepresentationSchema, or ExternalProjection.

CanonicalRepresentation answers this question:

```text
How is this Concept represented as ObjectType + PropertyType + fixed context +
required context?
```

RepresentationSchema answers this question:

```text
What datatype, regex/pattern, enum/value domain, cardinality, required/default
rules, examples, and validation constraints apply to this representation?
```

Example:

```text
concept.finance.revenue
  -> repr.finance.revenue.observation_amount
  -> object.observation + property.observed_amount
  -> fixed context: concept = concept.finance.revenue
  -> schema: decimal money amount, minimum 0, currency context required
```

`revenue_amount` is not a canonical property by default. It is a capability
output key or external projection unless there is a reviewed reason to add a
specialized representation.

Type, regex, enum, validation, cardinality, defaults, and examples do not live
on Concept. A Concept such as `concept.identifier.kr_business_registration_number`
is meaning only. A representation schema can define variants such as plain
10-digit string (`^\d{10}$`) or dashed display string (`^\d{3}-\d{2}-\d{5}$`).

Keep these layers separate:

- `PropertyType`: structural value slot and broad range, such as
  `identifier_value` as string.
- `RepresentationSchema`: concept-specific typing and validation, such as Korean
  business registration number as a 10-digit string.
- `SourceConstraint`: source/API-specific transport rule, such as body array
  cardinality or a maximum batch size of 100.

Modeling can still be imported/exported through the schema-language shape:

```text
Schema -> Class -> Slot -> Type -> Enum
```

Use the schema language as an import/export and validation format only. The
runtime source of truth is PostgreSQL. The dashboard should present business
modeling terms such as class, field, type, enum, relation, and validation; it
does not need to expose schema-language branding in navigation or primary page
copy.

LinkML is an authoring, import/export, and validation language. The runtime
source of truth is PostgreSQL.

## Resolution Graph

Resolution Graph connects source parameters and source fields to canonical
representations and representation context.

Tables:

- `field_bindings`
- `context_bindings`
- `parameter_bindings`
- `transform_rules`
- `resolution_rules`

Retired `bindings` and `binding_evidence` tables must not be used for new
runtime behavior.

Every binding/resolution edge must include:

- source context: `source + operation + path` when an operation exists
- source parameter id or source field id
- target canonical representation, representation context key, or required
  input concept
- filled property or context key when applicable
- confidence
- status
- evidence
- transformation or normalization rule when needed

Identical raw names such as `id`, `name`, or `type` must not be treated as
global meanings without source context.

Provider control parameters are binding metadata, not runtime branches. For
example, `inqryDiv` should be modeled as an operation-local control field with
declared values or transformations. Runtime code must apply reviewed binding
data; it must not infer provider meaning from keywords.

Ingestion must not solve semantic ambiguity by adding deterministic
provider/domain keyword rules to runtime code. The parser may extract source
structure, and generic validators may enforce shapes, but business meaning,
including whether a source term is a business field, provider control,
transport field, response envelope, or capability signal, belongs in the
LLM/manual response and the reviewable proposal bundle. When an LLM/manual
decision is `skip` or `skip_binding`, downstream proposal payloads must not
reintroduce fallback concepts, representations, canonical classes, slots, or
bindings for that skipped term.

## Capability Graph

Capability Graph is planner-facing and describes executable meaning contracts.

Tables:

- `capabilities`
- `capability_inputs`
- `capability_outputs`
- `capability_steps`
- `capability_constraints`

A capability describes WHAT can be achieved. A source operation describes HOW it
is executed.

Example:

```text
capability: cap.company.finance.get_revenue
requires: concept.identifier.kr_corporate_registration_number
requires: concept.time.fiscal_year
provides: concept.finance.revenue
representation: repr.finance.revenue.observation_amount
output_key: revenue_amount
operation: source_operations.id = op_company_info_get
```

Capabilities must be provider-neutral. Use ids such as
`company.contact.lookup`, not provider-prefixed ids. Executable capabilities
must resolve to a source operation before plan execution.

When one endpoint has multiple meanings based on fixed control values, model the
reviewed data explicitly with operation-local variants or capability-step
metadata. The Planner chooses the reviewed capability/operation/control values;
execution applies them without guessing provider intent.

## Execution Graph

Execution Graph stores concrete plan and run instances. Keep it separate from
Capability Graph.

- Capability is an executable contract.
- ExecutionPlan is a concrete plan selected for one request.
- ExecutionRun is a concrete execution instance.
- ExecutionResult and traces are runtime evidence, not catalog definitions.

Plans must preserve selected capability, selected source operation, selected
representation, input values, parameter bindings, expected outputs, validation
result, confidence, and confirmation state.

## Planner Service

Planner is a server-side service, not a registry.

Planner responsibilities:

- parse user intent
- resolve concept candidates
- select canonical representations
- infer required input concepts
- infer desired output concepts
- search capabilities
- resolve source operation
- bind parameters
- validate plan
- execute validated plan

Planner APIs:

- `POST /planner/plan`
- `POST /planner/execute`
- `GET /planner/plans/{plan_id}`
- `POST /planner/validate`

A plan must include:

- `plan_id`
- `selected_capability_id`
- `selected_source_operation_id`
- `selected_representation_id` when applicable
- concept input values
- parameter bindings
- expected outputs
- confidence
- `requires_confirmation`
- validation result

All runtime execution must go through validated plans. Raw source operation
execution must not be exposed to LLM clients.

Planner capability coverage is an LLM judgment. Runtime code may retrieve
candidate capabilities and validate ids, but must not hard-code domain or
provider keyword rules such as "if this Korean word appears, choose this
capability." When no capability satisfies the user request, the planner should
return an explicit not-found plan.

## LLM MCP Adapter

The LLM MCP Adapter is optional and sits outside the core architecture. It
exposes the Planner Service to LLM clients.

Primary tools:

- `plan_request`
- `execute_plan`
- `explain_plan`

Developer/debug tools:

- `search_capabilities`
- `get_capability`
- `get_canonical_model`
- `get_operation_bindings`

Do not expose:

- `execute_operation`

The adapter must execute only validated plans through the Planner Service.

## Ingestion And Review

Generated artifacts use the lifecycle:

```text
proposed -> reviewed -> approved -> published
```

This applies to:

- concepts, concept schemes, concept relations, and value domains
- object types, property types, link types, and canonical representations
- source operations, source parameters, and source fields
- field bindings, context bindings, parameter bindings, and transform rules
- capabilities, capability inputs/outputs, and capability-operation links
- execution tests, evidence, and governance metadata

The Workbench is a source intake and review surface. It may show source queue,
agent status, extracted assets, proposal bundles, validation, and approval, but
it must not directly generate semantic drafts from the UI. Generated output for
one source should converge into reviewable proposal bundles. Approval is a
governance action, not an automatic result of LLM generation.

For API documents, endpoint verification records are evidence. Verification may
call candidate endpoints with configured secrets, redacted request samples, and
stored response summaries. Verification must not expose secret values in logs,
payloads, proposals, or generated artifacts.

For non-API documents, ingestion should still extract fields, terms, structure,
and candidate Meaning Graph, Representation Model, and Resolution Graph
changes. Operation and execution validation can remain pending until an
executable source operation is available.

## API Planes

Context Platform API services are split by plane:

- `context-platform-api`: admin/control plane for dashboard, source upload,
  agent-ingestion queue records, proposals, catalog governance, and run tracking
- `context-platform-planner-api`: runtime plane for LLM/MCP clients and other
  callers that need approved catalog reads, plan creation, validation, and
  validated execution

The planner plane must not expose source upload, secret CRUD, ingestion queue
mutation, proposal review, or catalog mutation.

## Agent-Assisted Ingestion

Context Platform ingestion is operator/agent assisted. The platform parses
documents, builds evidence, validates contracts, stores proposal bundles, and
supports review/approval. It must not directly call an external LLM to decide
business meaning.

LangExtract is allowed as an explicit operator/agent-side source contract
drafting tool. Use it to extract grounded `source_operation`,
`source_parameter`, and `source_response_field` facts from document chunks.
Do not use LangExtract output to bypass review, mutate the catalog directly,
or decide Concepts, Representations, Bindings, Capabilities, or business
meaning without an agent response artifact and proposal review.

Supported ingestion modes:

- `disabled`: do not use an agent response; return explicit skipped or
  not-generated results where semantic judgment is required
- `agent_manual` or `manual`: wait for an explicit agent response artifact and
  process it through the active graph/API boundary
- `codex_manual`: legacy alias for `agent_manual`

`openai` is not a supported Context Platform ingestion mode. `OPENAI_API_KEY`
is a secret only and must never be used as a control signal.

When `agent_manual` is used:

- Codex or another operator agent may inspect the generated request/evidence and
  create a reviewable agent response artifact
- Codex or another operator agent may use
  `scripts/ops.py context-platform draft-source-contract` to create a
  LangExtract-backed `source_structure` draft, then pass the reviewed artifact
  through `--agent-response`
- agent output must pass through the active graph/API boundary
  (`manual_plan`, `manual_intent`, `manual_llm_response`,
  `--agent-response`, or the legacy `--manual-llm-response`)
- runtime code must not auto-discover query-hash or document-id fixture files
- agent artifacts must be labeled, for example
  `proposal_builder: agent_manual`
- agent artifacts must include source/evidence references
- agent artifacts must remain pending review unless the user explicitly asks to
  apply them
- agent reasoning must not become hard-coded provider rules, field mappings, or
  catalog mutations inside runtime code
- agent output must represent only the response at the active graph boundary;
  Codex must not patch runtime code with one-off keyword rules to make a
  specific source ingest cleanly
- if an agent response marks a term as `skip` or `skip_binding`, later ingestion
  stages must preserve that decision and must not fill in generic fallback
  canonical names such as `record`, `request_context`, or `api_response` for the
  skipped term

This applies to source ingestion, canonical modeling, binding generation,
proposal generation, execution planning, and future agent-assisted planner
paths.

## Runtime Boundaries

`services/context_platform` may own:

- catalog CRUD
- ingestion orchestration
- proposal generation
- endpoint verification evidence
- meaning graph and representation model import/export
- capability search
- plan creation, validation, and validated execution API

`services/context_platform` must not become app-specific provider runtime code.
Keep provider-specific business behavior, client quirks, and domain execution
logic in apps or dedicated execution adapters behind reviewed contracts.

`apps/pubdata_mcp`, when used, may own:

- MCP transport and tool manifests
- calls into `context-platform-planner-api`
- LLM-facing high-level planner tools
- display/explanation of validated plans

`apps/pubdata_mcp` must not own:

- meaning graph or representation model definitions
- global capability ranking
- cross-domain planning
- proposal generation
- catalog mutation
- provider/domain keyword selection rules
- raw source operation execution through MCP

## Rules

- Domain logic belongs in `apps/*`.
- Generic service/runtime logic belongs in `services/*` or `core/*`.
- Context Platform implementation belongs in `services/context_platform`.
- Planning and validated plan execution APIs belong in
  `services/context_platform/internal/planner` and its adapters.
- `services/context_platform/adapters/worker` owns manual/background ingestion
  jobs and Prefect manual deployments. Do not add schedules until explicitly
  requested.
- Prefect control plane manifests live in `services/prefect`.
- MinIO is the file upload/object storage path for source documents.
- Compose is generated from manifests. Do not hand-edit
  `deploy/compose/docker-compose.yml` except to inspect generated output.
- Secret values must stay in env files, not manifests or payloads.
- Retired code belongs under `tmp/retired_apps` or `tmp/retired_core` and must
  not be imported by active runtime paths.
- `tmp/*` is a scratch/working area. Codex must not ask for confirmation before
  creating, updating, validating, copying, or deleting non-secret temporary
  artifacts there. This includes agent response artifacts, generated
  request/response JSON, one-off inspection files, transient validation output,
  and other disposable work files. Do not put secret values in `tmp/*`.

## Recommended App Structure

```text
apps/<app>/
  adapters/
    <adapter>/
      infra/
  domain/
    flows/
    tasks/
    steps/
    repositories/
    context/
    service/
  manifests/
```

## Commands

```bash
python3 scripts/generate_compose.py
python3 scripts/ops.py validate-config
python3 scripts/ops.py lint-boundaries
python3 scripts/ops.py check-all
```

Context Platform ingestion from the host:

```bash
python3 scripts/ops.py context-platform ingest-source "<source-file-path>"
```

The shortest command uses `--agent-mode env` by default. If no agent response is
provided, ingestion may stop at the generated request/evidence boundary. Metadata
can be supplied when needed:

```bash
python3 scripts/ops.py context-platform ingest-source "<source-file-path>" \
  --name "<source-name>" \
  --provider "<provider-name>" \
  --agent-mode manual \
  --agent-response "<agent-response-json>"
```

Use `--agent-mode manual --agent-response <json-path>` when Codex or another
operator agent supplies the response artifact. `--llm-mode codex_manual` and
`--manual-llm-response` are accepted only as legacy aliases. Use
`--llm-mode disabled` only for explicit deterministic fallback.

For a document uploaded through dashboard Source Intake, process the queued run
by id:

```bash
python3 scripts/ops.py context-platform ingest-queued-source "<run-id>" \
  --agent-mode manual \
  --agent-response "<agent-response-json>"
```

## Acceptance Checklist

- [ ] Correct layer placement
- [ ] No app logic in `core/*`
- [ ] Context Platform code uses `context_platform` paths and Context Platform
      terminology
- [ ] `source_operations` remains the single executable operation table
- [ ] LLM clients execute only validated plans
- [ ] Manifest updated when services/apps change
- [ ] Generated compose is in sync
- [ ] `python3 scripts/ops.py validate-config` passes
- [ ] `python3 scripts/ops.py lint-boundaries` passes

END OF FILE
