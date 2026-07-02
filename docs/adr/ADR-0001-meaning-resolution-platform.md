# ADR-0001: Context Platform As A Meaning Resolution Platform

## Status

Accepted.

## Context

The previous Context Platform design was centered on Source Catalog, Canonical
Model, Binding Layer, Capability Catalog, and Planner Service. That was useful,
but it made canonical slots carry too much semantic responsibility. It also
made source field mapping ambiguous when several fields shared the same value
shape but represented different business meanings.

For example, company financial APIs can expose revenue, operating income, and
total assets as monetary fields. Binding all of them directly to
`Observation.observed_amount` loses the distinction between the metrics. Making
every metric a first-class canonical property causes the canonical model to
grow without a clear boundary.

## Decision

Context Platform is redefined as:

```text
Context Platform is a Meaning Resolution Platform that connects Concepts,
Canonical Representations, Representation Schemas, Sources, Capabilities, and
Executions into an Executable Meaning Graph.
```

The architecture is:

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

The core principles are:

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

## Consequences

`revenue_amount` is not a canonical property by default. It is a capability
output key or external projection. The canonical representation of revenue can
be `Observation.observed_amount` with fixed context
`concept = concept.finance.revenue`.

Binding targets should move from canonical slots/class-slot usages toward
CanonicalRepresentation. The migration must preserve compatibility with current
`canonical_*`, `bindings`, `capabilities`, and `source_operations` tables until
the new model is fully implemented.

Datatype, regex, enum, cardinality, examples, and validation constraints belong
to RepresentationSchema, not Concept. Concept remains semantic meaning only.
PropertyType may carry broad structural range; RepresentationSchema carries
concept-specific constraints; SourceConstraint carries source/API transport
constraints.

MetadataAspect follows the DataHub-style aspect pattern for flexible metadata,
but planner-critical information must remain in typed tables.

LinkML remains the schema authoring/import/export/validation format. PostgreSQL
remains the runtime source of truth.

DataHub, OpenMetadata, dbt/Cube, and MCP are integration targets or reference
patterns, not core stores introduced by this decision.
