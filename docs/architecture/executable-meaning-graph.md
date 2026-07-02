# Executable Meaning Graph

## Graph Layers

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

## Meaning Graph

Meaning Graph stores the semantic nodes the platform can resolve.

Core nodes:

- MeaningScope
- ConceptScheme
- Concept
- ConceptRelation
- ValueDomain
- ValueDomainValue

`Concept.kind` is required. Recommended kinds:

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

Concept relations must be typed. Use relation types such as `broader`,
`narrower`, `exact_match`, `close_match`, `has_unit`, `has_value_domain`,
`has_value`, `applicable_to_object`, `represents_identifier_type`,
`derived_from`, and `requires_context`.

## Representation Model

Representation Model stores structure and representation-level schema.

Core nodes:

- ObjectType
- PropertyType
- LinkType
- CanonicalRepresentation
- RepresentationSchema
- ExternalProjection

CanonicalRepresentation is a template. It says how a Concept is represented as
an ObjectType, PropertyType, fixed context, and required context.

RepresentationSchema defines the datatype and validation for that template:
regex/pattern, enum or value domain, cardinality, required/default rules,
examples, min/max, precision, scale, and other constraints. These constraints do
not belong on Concept.

Keep the layers separate:

- PropertyType defines a structural slot and broad range.
- RepresentationSchema defines concept-specific validation for a representation.
- SourceConstraint defines source/API-specific transport constraints.

## Source Graph

Source Graph stores source reality.

Core nodes:

- SourceSystem
- SourceOperation
- SourceParameter
- SourceField
- SourceConstraint
- SourceError

`source_operations` remains the single executable operation table. Do not create
a separate Operation Registry.

## Resolution Graph

Resolution Graph maps source reality to meaning and representation.

Core nodes:

- FieldBinding
- ContextBinding
- ParameterBinding
- TransformRule
- ResolutionRule

Bindings must be source-context-aware. Identical raw field names across
providers or operations are not the same meaning unless reviewed binding data
says so.

## Capability Graph

Capability Graph stores planner-facing executable contracts.

Core nodes:

- Capability
- CapabilityInput
- CapabilityOutput
- CapabilityStep
- CapabilityConstraint

Capabilities are provider-neutral. A capability can be implemented by one or
more source operations, functions, transforms, or sub-capabilities.

## Execution Graph

Execution Graph stores concrete runtime instances.

Core nodes:

- ExecutionPlan
- PlanStep
- ExecutionRun
- ExecutionStepRun
- ExecutionResult
- ExecutionTrace

Capability and execution must stay separate. Catalog review approves
capabilities; runtime validation approves plans; execution produces runs and
results.

## Evidence / Governance

Evidence and governance make generated or inferred graph edges reviewable.

Core nodes:

- Evidence
- ReviewEvent
- MetadataAspect
- LineageEdge
- QualityCheck
- PolicyTag

MetadataAspect is for extensible metadata only. Planner-critical graph edges
must stay in typed tables.
