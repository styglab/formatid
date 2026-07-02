# Context Platform LinkML Registry Model

## 결정

LinkML은 Context Platform의 runtime DB가 아니다. LinkML은 Meaning Graph,
Representation Model, Source Graph, Resolution Graph, Capability Graph,
Execution Graph, Governance Graph를 선언하고 교환하기 위한 schema language다.

Runtime source of truth는 PostgreSQL이다.

```text
PostgreSQL runtime registry
  <-> LinkML-compatible YAML/JSON import/export
```

기존 foundation/common business LinkML은 유지한다.

```text
tmp/context_platform/foundation_canonical_model.linkml.yaml
tmp/context_platform/common_business_model.linkml.yaml
```

신규 registry schema는 별도 LinkML 파일로 분리한다.

```text
linkml/
  meaning_registry.linkml.yaml
  representation_registry.linkml.yaml
  source_registry.linkml.yaml
  resolution_registry.linkml.yaml
  capability_registry.linkml.yaml
  execution_registry.linkml.yaml
  governance_registry.linkml.yaml
```

## Foundation Model

Foundation은 작게 유지한다.

권장 foundation 객체:

- Entity
- Identifier
- Observation
- Relationship
- Document
- ConceptScheme
- Concept
- Money
- Quantity
- TimeInterval
- Evidence

현재 foundation 모델에 있는 `ClassificationScheme`과 `Classification`은
compatibility 이름으로 유지할 수 있다. 제품 용어와 신규 설계에서는 각각
`ConceptScheme`, `Concept`로 취급한다.

`Observation`은 이번 설계의 중심 구조다. metric, status, reported value를
concept, subject, time, source, evidence context와 함께 표현할 수 있어야 한다.

예:

```text
concept.finance.revenue
  -> repr.finance.revenue.observation_amount
  -> Observation.observed_amount
```

## Common Business Model

`common_business_model.linkml.yaml`은 shared business kernel이다.

권장 객체:

- Organization
- Company
- Person
- Dataset
- API

Company, Person, Organization 같은 구체 business object는 foundation에 넣지
않고 common business schema가 foundation을 import해서 정의한다.

## Registry Schema Modules

### meaning_registry.linkml.yaml

정의:

- MeaningScope
- ConceptScheme
- Concept
- ConceptRelation
- ValueDomain
- ValueDomainValue

`Concept.kind`는 필수다. `object_concept`, `metric_concept`,
`identifier_concept`, `status_concept`, `value_concept`, `unit_concept`,
`time_concept`, `account_concept`, `document_concept`, `operation_concept` 같은
명시적 kind를 사용한다.

### representation_registry.linkml.yaml

정의:

- ObjectType
- PropertyType
- LinkType
- CanonicalRepresentation
- RepresentationSchema
- ExternalProjection

`CanonicalRepresentation`은 instance가 아니라 template이다. Concept를 어떤
ObjectType/PropertyType/fixed context/required context 조합으로 표현하는지
정의한다.

`RepresentationSchema`는 CanonicalRepresentation의 datatype, pattern,
structured pattern, enum/value domain, cardinality, required/default, examples,
minimum/maximum, precision/scale, validation rules를 정의한다. Concept에는 regex나
datatype을 두지 않는다.

### source_registry.linkml.yaml

정의:

- SourceSystem
- SourceOperation
- SourceParameter
- SourceField
- SourceConstraint
- SourceError

`source_operations`가 executable operation의 단일 원천이다. LinkML schema가
생기더라도 별도 Operation Registry를 만들지 않는다.

### resolution_registry.linkml.yaml

정의:

- FieldBinding
- ContextBinding
- ParameterBinding
- TransformRule
- ResolutionRule

Binding target은 CanonicalRepresentation을 기본으로 한다. 필요한 경우
representation의 value property, context key, required input concept를 명시한다.

### capability_registry.linkml.yaml

정의:

- Capability
- CapabilityInput
- CapabilityOutput
- CapabilityStep
- CapabilityConstraint

Capability는 endpoint가 아니라 executable meaning contract다.

### execution_registry.linkml.yaml

정의:

- ExecutionPlan
- PlanStep
- ExecutionRun
- ExecutionStepRun
- ExecutionResult
- ExecutionTrace

Capability catalog object와 runtime execution instance를 분리한다.

### governance_registry.linkml.yaml

정의:

- Evidence
- ReviewEvent
- MetadataAspect
- LineageEdge
- QualityCheck
- PolicyTag

MetadataAspect는 확장 메타데이터용이다. planner-critical 정보는 typed registry
schema에 둔다.

## Modeling Rules

### Concept First

의미는 Concept로 먼저 표현한다.

```text
concept.finance.revenue
concept.identifier.kr_corporate_registration_number
concept.tax.business_registration_status
concept.time.fiscal_year
```

### Representation As Template

CanonicalRepresentation은 Concept를 표준 구조에 올리는 템플릿이다.

```text
repr.finance.revenue.observation_amount
  concept = concept.finance.revenue
  carrier_object_type = object.observation
  value_property = property.observed_amount
```

### RepresentationSchema For Type And Validation

Type, regex, enum, validation은 Concept가 아니라 RepresentationSchema에 둔다.

예:

```text
concept.identifier.kr_business_registration_number
  -> repr.identifier.kr_business_registration_number.identifier_value
  -> schema.identifier.kr_business_registration_number.plain_10_digit
     datatype = string
     pattern = ^\d{10}$
```

LinkML의 `types`, `enums`, `slots`, `slot_usage`, `pattern`,
`structured_pattern`, `required`, `minimum_value`, `maximum_value` 같은 제약은
RepresentationSchema로 흡수된다. `PropertyType`은 구조적 slot과 broad range를
표현하고, `RepresentationSchema`는 Concept-specific validation을 표현한다.

Source API의 body/query/header 위치, batch limit, request shape 같은 transport
제약은 RepresentationSchema가 아니라 SourceConstraint에 둔다.

### Direct Property Is Not The Default

`revenue_amount`를 먼저 canonical property로 만들지 않는다.

기본 표현:

```text
concept.finance.revenue
  -> repr.finance.revenue.observation_amount
  -> Observation.observed_amount
  -> capability output key: revenue_amount
```

필요하면 나중에 별도 `ExternalProjection` 또는 특수
`CanonicalRepresentation`으로 `FinancialSummary.revenue_amount`를 추가한다.

### Identifier Pattern

외부 기관이나 source가 발급한 번호는 direct Company property로 무한히 늘리지
않는다.

예:

```text
concept.identifier.kr_corporate_registration_number
  -> repr.identifier.kr_corporate_registration_number.identifier_value
  -> Identifier.identifier_value
```

### Status / Code Pattern

상태와 코드는 Concept + ValueDomain으로 모델링한다.

예:

```text
concept.tax.business_registration_status
value_domain.nts.business_status_code
  01 -> concept.tax.active_business
  02 -> concept.tax.suspended_business
  03 -> concept.tax.closed_business
```

### Observation Pattern

기간, 단위, 출처, evidence, concept context가 필요한 fact는 Observation으로
모델링한다.

예:

```text
revenue
operating_income
total_assets
employee_count
business_registration_status
```

## LinkML Import Rules

- Foundation schema는 작게 유지한다.
- Registry schema는 foundation을 import한다.
- Domain schema는 foundation과 필요한 registry schema를 import한다.
- LinkML import/export는 proposal workflow를 거친다.
- LinkML 문서가 runtime DB를 대체하지 않는다.
- UI primary navigation에는 LinkML branding을 노출하지 않는다.

## Non-goals

- LinkML을 runtime database로 사용하지 않는다.
- RDF/OWL/triplestore를 도입하지 않는다.
- 독립 vocabulary product를 만들지 않는다.
- Concept와 CanonicalRepresentation을 JSON aspect에만 묻지 않는다.
- regex, datatype, enum, validation을 Concept에 직접 두지 않는다.
