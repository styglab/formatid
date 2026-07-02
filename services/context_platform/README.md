# Context Platform

`services/context_platform` is the current implementation path for the Context
Platform.

Context Platform is a Meaning Resolution Platform that connects Concepts,
Canonical Representations, Representation Schemas, Sources, Capabilities, and
Executions into an Executable Meaning Graph.

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

## Core Rule

Source operations are the executable operations.

```text
Use source_operations as the single operation table.
Do not create a separate Operation Registry.
```

## Core Principles

- Concept is meaning.
- ObjectType and PropertyType are structure.
- CanonicalRepresentation is a template.
- RepresentationSchema is datatype and validation for that template.
- Binding is source-to-representation resolution.
- Capability is an executable contract.
- ExecutionPlan and ExecutionRun are concrete execution instances.
- Projection is a consumer-facing shape.
- Evidence makes every assertion reviewable.
- MeaningScope prevents Concept from becoming an unbounded catch-all.

## Boundary

Implementation module name: `context_platform`
Current implementation path: `services/context_platform`

`context_platform` owns the Meaning Resolution Platform:

- source systems, source documents, source operations, parameters, fields, and constraints
- meaning graph concepts, concept schemes, concept relations, value domains, and meaning scopes
- representation model object types, property types, link types, and canonical representations
- representation schemas for datatype, enum, regex, cardinality, examples, and validation
- source-to-representation resolution bindings and transform rules
- planner-facing capabilities and capability steps
- execution plan/run/result APIs
- proposal, review, evidence, metadata aspect, and lifecycle state

It must not become provider-specific runtime code. Provider HTTP clients, API
key handling beyond service configuration, retry behavior, pagination quirks,
and one-off provider response parsing belong behind reviewed execution
contracts or app/dedicated execution adapters.

Raw provider calls are allowed only as part of validated plan execution or
internal verification evidence.

## Logical Model

```text
Executable Meaning Graph
  1. Meaning Graph
  2. Representation Model
  3. Source Graph
  4. Resolution Graph
  5. Capability Graph
  6. Execution Graph
  7. Evidence / Governance
```

Current tables are the Meaning Resolution source of truth. Retired canonical
compatibility tables may still appear in old API method names, but runtime
storage must use the graph tables below.

Important tables:

- `sources`
- `source_documents`
- `source_operations`
- `source_parameters`
- `source_fields`
- `meaning_scopes`
- `concept_schemes`
- `concepts`
- `concept_relations`
- `value_domains`
- `value_domain_values`
- `object_types`
- `property_types`
- `link_types`
- `canonical_representations`
- `representation_schemas`
- `external_projections`
- `field_bindings`
- `context_bindings`
- `parameter_bindings`
- `transform_rules`
- `resolution_rules`
- `capabilities`
- `capability_inputs`
- `capability_outputs`
- `capability_steps`
- `capability_constraints`
- execution plan/run/result/trace tables
- proposal and review tables

Target concepts:

- `Concept`
- `CanonicalRepresentation`
- `RepresentationSchema`
- `FieldBinding`
- `ContextBinding`
- `ParameterBinding`
- `Capability`
- `ExecutionPlan`
- `ExecutionRun`
- `MetadataAspect`

## Layout

```text
adapters/
  admin_api/     control-plane API for source upload, ingestion, review, CRUD
  planner_api/   runtime API for planning, validation, approved reads, execution
  dashboard/     human authoring/review UI
  worker/        manual/background ingestion jobs
internal/
  meaning/       meaning graph services
  representation/ representation model services
  source/        source graph services
  resolution/    source-to-representation resolution services
  capability/    provider-neutral capability graph services
  execution/     execution plan/run/result services
  governance/    evidence, aspects, review, approval, lineage
  ingestion/     source document -> proposal workflow
  planner/       user request -> validated plan -> execution
  context/       planner context packaging
  storage/       Postgres repository/schema helpers
manifests/       compose/catalog declarations
```

The current code may not yet have every directory above. New work should move
toward this layout without breaking existing imports.

## Planner Service

Planner API:

- `POST /planner/plan`
- `POST /planner/execute`
- `GET /planner/plans/{plan_id}`
- `POST /planner/validate`

Planner responsibilities:

- parse user intent
- resolve concept candidates
- select canonical representations
- search capabilities
- compile execution plans
- validate plans
- execute validated plans
- normalize results through reviewed bindings
- project results into consumer-facing output shapes

`execute_plan` must reject unvalidated or invalid plans.

## LLM MCP Adapter

The optional LLM MCP Adapter exposes Planner Service tools to an LLM client.

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

The LLM client must not execute raw source operations directly.

## Product Workflow Direction

```text
upload source document
  -> IngestionPipelineGraph
     -> parse source operations, parameters, fields
     -> propose concepts and canonical representations
     -> propose field/context/parameter bindings
     -> propose capabilities and capability outputs
     -> verify endpoint/capability evidence
     -> create proposal bundle
  -> review
  -> approve and publish
```

Generated artifacts are never approved automatically.

## References

- [Context Platform overview](/workspace/docs/architecture/context_platform_overview_ko.md)
- [Data model](/workspace/docs/architecture/context_platform_registry_model_ko.md)
- [Meaning Resolution Platform](/workspace/docs/architecture/meaning-resolution-platform.md)
- [Executable Meaning Graph](/workspace/docs/architecture/executable-meaning-graph.md)
- [ADR-0001](/workspace/docs/adr/ADR-0001-meaning-resolution-platform.md)
