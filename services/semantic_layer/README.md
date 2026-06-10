# Semantic Layer

Semantic Layer is the platform control plane for canonical semantic models,
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
  -> semantic layer graph
  -> LLM execution planning
  -> app executor
```

## Boundary

Service name: `semantic_layer`
Current implementation path: `services/semantic_layer`

`semantic_layer` owns declarative context:

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
lib/
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
