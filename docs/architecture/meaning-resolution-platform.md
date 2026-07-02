# Meaning Resolution Platform

## Definition

Context Platform is a Meaning Resolution Platform that connects Concepts,
Canonical Representations, Representation Schemas, Sources, Capabilities, and
Executions into an Executable Meaning Graph.

The product is not a standalone data catalog, canonical schema repository, or
metric layer. It resolves user and agent intent into reviewed meanings, maps
those meanings to source systems, compiles executable plans, and normalizes
results back into canonical representations.

## Core Model

```text
Concept
  -> CanonicalRepresentation
  -> RepresentationSchema
  -> Source Binding
  -> Capability
  -> ExecutionPlan / ExecutionRun
```

The model keeps these responsibilities separate:

- Concept is meaning.
- ObjectType and PropertyType are structure.
- CanonicalRepresentation is a template.
- RepresentationSchema is datatype and validation for that template.
- Binding is source-to-representation resolution.
- Capability is an executable contract.
- ExecutionPlan is a concrete plan instance.
- ExecutionRun is a concrete execution instance.
- Projection is a consumer-facing shape.
- Evidence makes every assertion reviewable.
- MeaningScope prevents Concept from becoming an unbounded catch-all.

## Why Representation Exists

Without CanonicalRepresentation, source fields tend to bind directly to
canonical slots and the platform loses business meaning. For example,
`enpSaleAmt`, `enpBzopPft`, and `enpTastAmt` can all be monetary values, but
they mean revenue, operating income, and total assets respectively.

The correct shape is:

```text
concept.finance.revenue
  -> repr.finance.revenue.observation_amount
  -> object.observation + property.observed_amount
  -> schema.finance.revenue.money_amount
  -> capability output key: revenue_amount
```

`revenue_amount` is not the canonical property. It is a projection or capability
output key.

## Why RepresentationSchema Exists

Concept is meaning, not data type. A Korean business registration number can be
represented as `1234567890`, `123-45-67890`, or a normalized internal string.
Those are different representation schemas for the same concept.

```text
concept.identifier.kr_business_registration_number
  -> repr.identifier.kr_business_registration_number.identifier_value
  -> schema.identifier.kr_business_registration_number.plain_10_digit
     datatype = string
     pattern = ^\d{10}$
```

Type, regex, enum, required, cardinality, default, examples, and validation rules
belong in RepresentationSchema. Source-specific constraints such as request
location, body shape, and batch limits belong in SourceConstraint.

## Relationship To External Patterns

DataHub contributes the Entity/Aspect pattern. Context Platform uses typed core
tables for planner-critical information and a MetadataAspect store for
ownership, lineage, quality, samples, policy tags, and review notes.

OpenMetadata contributes the AI context layer and governance pattern. Context
Platform may integrate with OpenMetadata later, but it remains the source of
truth for planning and execution.

Palantir Foundry Ontology contributes the object/property/link/action idea.
Context Platform generalizes Action into Capability because it must support
lookup, search, validation, status checks, computation, transformation, and
mutation.

Semantic layer tools such as dbt Semantic Layer and Cube contribute the
metric/dimension/projection pattern. Context Platform treats those as export or
projection targets, not as the core store.

## Non-Goals

Do not introduce these as part of the core redesign:

- RDF/OWL/triplestore
- Neo4j or another graph database
- DataHub/OpenMetadata as the core store
- MCP server implementation
- full domain modeling
- broad public API ingestion automation
- dbt/Cube export implementation

The design must leave these integrations possible later.
