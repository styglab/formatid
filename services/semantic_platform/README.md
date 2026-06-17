# Semantic Platform

Semantic Platform is the platform control plane for canonical semantic models,
provider-neutral capabilities, approved execution contracts, and planner-ready
context packages.

It is not a data catalog and it is not an MCP registry. API documents can propose
semantic/capability/execution changes, and operators can also author them
directly through the admin plane. Runtime clients only read approved context from
the planner plane.

```text
API documents / manual authoring
  -> context change proposals
  -> governance review
  -> canonical semantic model
  -> semantic platform graph
  -> LLM execution planning
  -> app executor
```

## Boundary

Implementation module name: `semantic_platform`
Current implementation path: `services/semantic_platform`

`semantic_platform` owns declarative context:

- canonical semantic types and relationships
- provider-neutral capabilities and input/output types
- operation contracts, operation variants, and field/control mappings
- context change proposals, review state, provenance, and lineage
- planner context packaging and execution plan validation

It must not own provider execution:

- provider HTTP clients
- API key handling beyond service configuration
- retry, pagination, and provider SDK quirks
- raw provider response parsing at runtime

Provider execution belongs in apps such as `apps/pubdata_mcp`.

## Layout

```text
adapters/
  admin_api/     control-plane API for authoring, ingestion, review, governance
  planner_api/   runtime API for approved context and execution planning
  dashboard/     human authoring/review UI
  worker/        manual/background ingestion jobs
internal/
  model/         Entity, Aspect, Relationship, ContextChangeProposal primitives
  semantic/      canonical semantic model services
  capability/    provider-neutral capability catalog services
  execution/     operation contracts, variants, field/control mappings
  authoring/     direct manual create/update command services
  governance/    proposal review, approval, conflicts, lineage
  ingestion/     API document -> evidence -> proposal
  planner/       question -> semantic execution plan
  context/       planner/executor context packaging
  storage/       Postgres repository/schema helpers
manifests/       compose/catalog declarations
```

## Current Implementation Status

The current codebase is not fully at the target architecture yet.

- `admin_api`: basic authoring CRUD and proposal review loop are implemented
- `storage.repository`: Postgres-backed semantic types, sources, operations, mappings,
  capabilities, relationships, and proposal approval flow exist
- `planner_api`: endpoint surface exists, but execution planning and approved
  contract packaging are still mostly stubs
- `dashboard`: route-based control plane exists with workflow-first onboarding,
  semantic registry, execution registry, governance, release, and reference
  sections, while CRUD
  workflows and planner/runtime plane remain incomplete
- source upload now creates an onboarding run, evidence snapshot, proposal
  bundle, and initial work queue task for operation/schema discovery

Current dashboard CRUD moved out of the legacy prototype for:

- execution sources, including file upload
- semantic types
- canonical model entities, attributes, and relations
- mappings
- capabilities

Implementation-gap details and the current dashboard guidance are documented in:

- [docs/architecture/semantic_platform_overview_ko.md](/workspace/docs/architecture/semantic_platform_overview_ko.md)
- [docs/architecture/semantic_platform_implementation_ko.md](/workspace/docs/architecture/semantic_platform_implementation_ko.md)
- [docs/architecture/semantic_platform_dashboard_ko.md](/workspace/docs/architecture/semantic_platform_dashboard_ko.md)

## Product Workflow Direction

The dashboard should be organized around Source Onboarding Runs, not standalone
field mapping suggestions. The authoring model should be source-first:

```text
Source
  -> Asset / Access Path
    -> Schema / Fields
    -> Controls (optional)
    -> Operations (optional)
```

A source upload or ingestion execution creates an onboarding run, captures an
evidence snapshot, discovers assets/access paths/structures, and produces a
proposal bundle spanning semantic types, canonical model changes, field
mappings/transforms, variants, capabilities, and capability-operation bindings.

```text
Source upload
  -> onboarding run
  -> evidence snapshot
  -> discovery and AI suggestion batch
  -> proposal bundle
  -> review / approve
  -> publish approved runtime snapshot
```

Field mapping remains a task inside this workflow. Capability-to-operation or
capability-to-variant bindings are a separate registry concern from operation
field mappings.

## Onboarding Workflow Principles

The intended onboarding model is a stage-based workflow, not a one-shot upload
wizard.

- a run is the workflow container for one source evidence package
- each run is broken into stages
- each stage contains reviewable tasks
- every task supports AI draft generation
- AI may draft proposals, but it must not directly finalize runtime truth

Recommended stages:

1. `source_review`
2. `asset_discovery`
3. `structure_review`
4. `semantic_mapping`
5. `controls_and_variants`
6. `operation_and_binding_modeling`
7. `proposal_review`
8. `publish_readiness`

Recommended task behavior:

- operators can complete tasks manually
- operators can ask AI to generate a draft for any task
- drafts must carry rationale, confidence, and evidence references
- draft output remains review-state data until explicit approval/publish

This means the system should distinguish:

- discovery facts that can be stored automatically
  - runs
  - evidence snapshots
  - discovered operations
  - extracted fields
  - observed control values
- semantic proposals that require review
  - semantic types
  - canonical model links
  - mappings and transforms
  - variants
  - capabilities
  - capability bindings

Control fields need a dedicated path. They are not just ordinary field mappings;
they can imply semantic controls or operation variants, so onboarding should
treat them as first-class review subjects.
