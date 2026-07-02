# Context Platform Data Model

## 기준 구조

Context Platform의 데이터 모델은 Executable Meaning Graph를 기준으로 한다.

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

이 문서는 최종 방향을 정의한다. 실제 migration은 기존 `context_platform` schema와
현재 테이블을 분석한 뒤, 중복 테이블을 만들지 않는 방식으로 진행한다.

## 핵심 결정

- Context Platform은 Meaning Resolution Platform이다.
- Concept가 의미의 중심이다.
- CanonicalRepresentation은 Concept를 표준 구조로 표현하는 템플릿이다.
- RepresentationSchema는 해당 템플릿의 datatype, regex, enum, cardinality,
  examples, validation constraints를 정의한다.
- Binding target은 canonical slot이 아니라 CanonicalRepresentation을 기본으로 한다.
- Capability는 planner-facing executable contract다.
- ExecutionPlan/ExecutionRun은 concrete runtime instance다.
- `source_operations`가 executable operation의 단일 원천이다.
- LinkML은 schema authoring/import/export/validation 용도다.
- PostgreSQL이 runtime source of truth다.
- MetadataAspect는 확장 메타데이터용이다.
- planner-critical 정보는 JSONB aspect 안에만 저장하지 않는다.

## Logical Namespaces

설계상 namespace:

```text
meaning.*
repr.*
source.*
resolution.*
capability.*
execution.*
governance.*
```

실제 Postgres schema 분리는 repo의 migration 구조와 기존 DB를 확인한 뒤 결정한다.
기존 단일 `context_platform` schema를 유지해야 하면 테이블명 또는 view로 논리
namespace를 표현한다.

## MVP Tables

### Meaning Graph

- `meaning.scopes`
- `meaning.concept_schemes`
- `meaning.concepts`
- `meaning.concept_relations`
- `meaning.value_domains`
- `meaning.value_domain_values`

`meaning.concepts` 필수 필드:

- `stable_key`
- `meaning_scope_id`
- `domain_id`
- `scheme_id`
- `kind`
- `code`
- `label_ko`
- `label_en`
- `definition`
- `status`

권장 `Concept.kind`:

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

초기 `MeaningScope`:

- `meaning_scope.global`
- `meaning_scope.company`
- `meaning_scope.finance`
- `meaning_scope.tax`
- `meaning_scope.identifier`
- `meaning_scope.time`
- `meaning_scope.currency`

### Representation Model

- `repr.object_types`
- `repr.property_types`
- `repr.link_types`
- `repr.canonical_representations`
- `repr.representation_schemas`
- `repr.external_projections`

`repr.canonical_representations` 필수 필드:

- `stable_key`
- `concept_id`
- `carrier_object_type_id`
- `value_property_type_id`
- `fixed_context_json`
- `required_context_json`
- `representation_kind`
- `priority`
- `is_preferred`
- `domain_id`
- `status`
- `evidence_id`

`CanonicalRepresentation`은 instance가 아니다. 예:

```text
repr.finance.revenue.observation_amount
  concept = concept.finance.revenue
  carrier = object.observation
  value_property = property.observed_amount
  fixed_context.concept = concept.finance.revenue
```

`repr.representation_schemas` 필수 필드:

- `stable_key`
- `representation_id`
- `datatype`
- `value_domain_id`
- `pattern`
- `structured_pattern_json`
- `cardinality`
- `required`
- `default_json`
- `examples_json`
- `validation_json`
- `status`
- `evidence_id`

SQL 초안:

```sql
CREATE TABLE repr.representation_schemas (
  id uuid PRIMARY KEY,
  stable_key text UNIQUE NOT NULL,
  representation_id uuid NOT NULL REFERENCES repr.canonical_representations(id),
  datatype text NOT NULL,
  value_domain_id uuid NULL REFERENCES meaning.value_domains(id),
  pattern text NULL,
  structured_pattern_json jsonb NOT NULL DEFAULT '{}',
  cardinality text NULL,
  required boolean NULL,
  default_json jsonb NULL,
  examples_json jsonb NOT NULL DEFAULT '[]',
  validation_json jsonb NOT NULL DEFAULT '{}',
  status text NOT NULL DEFAULT 'draft',
  evidence_id uuid NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

`RepresentationSchema`는 Concept가 아니라 Representation에 붙는다.

예:

```text
schema.identifier.kr_business_registration_number.plain_10_digit
  representation = repr.identifier.kr_business_registration_number.identifier_value
  datatype = string
  pattern = ^\d{10}$

schema.finance.revenue.money_amount
  representation = repr.finance.revenue.observation_amount
  datatype = decimal
  minimum = 0
  precision = 18
  scale = 2

schema.tax.business_registration_status.nts_code
  representation = repr.tax.business_registration_status.observed_value
  datatype = enum
  value_domain = value_domain.nts.business_status_code
```

층위 구분:

- `PropertyType`: 구조적 value slot과 broad range
- `RepresentationSchema`: 특정 Concept 표현의 타입/검증
- `SourceConstraint`: source/API transport 제약

### Source Graph

- `source.systems`
- `source.operations`
- `source.parameters`
- `source.fields`
- `source.constraints`
- `source.errors`

현재 구현의 `sources`, `source_operations`, `source_parameters`, `source_fields`는
이 logical namespace의 compatibility storage로 취급한다.

`source.operations`는 실제 API endpoint/function/job다. 별도 Operation Registry를
만들지 않는다.

### Resolution Graph

- `resolution.field_bindings`
- `resolution.context_bindings`
- `resolution.parameter_bindings`
- `resolution.transform_rules`
- `resolution.resolution_rules`

역할:

- `FieldBinding`: SourceField가 Representation의 value property를 채운다.
- `ContextBinding`: SourceField가 Representation의 context key를 채운다.
- `ParameterBinding`: Required Concept가 SourceParameter로 들어간다.
- `TransformRule`: parse, normalize, cast, code normalization을 선언한다.

기존 `bindings` 테이블은 migration 전까지 compatibility storage로 사용하되,
target representation과 role metadata를 잃지 않아야 한다.

### Capability Graph

- `capability.capabilities`
- `capability.inputs`
- `capability.outputs`
- `capability.steps`
- `capability.constraints`

Capability는 SourceOperation이 아니다. Capability는 의미 기반 실행 계약이고,
SourceOperation은 구현 step이다.

`capability.outputs`는 다음을 가져야 한다.

- `output_key`
- `concept_id`
- `representation_id`
- `value_path`
- `unit_path`
- `period_path`
- `is_primary`

`revenue_amount`는 canonical property가 아니라 `output_key` 또는 projection key다.

### Execution Graph

- `execution.plans`
- `execution.plan_steps`
- `execution.runs`
- `execution.step_runs`
- `execution.results`
- `execution.traces`

Catalog object와 runtime instance를 분리한다.

```text
Capability = executable contract
ExecutionPlan = selected concrete plan
ExecutionRun = concrete run instance
```

### Governance Graph

- `governance.evidence_items`
- `governance.review_events`
- `governance.metadata_aspects`
- `governance.lineage_edges`
- `governance.quality_checks`
- `governance.policy_tags`

`MetadataAspect`는 ownership, lineage, quality, sample values, policy tags,
review note, extraction trace 같은 확장 메타데이터에만 사용한다.

## Stable Key Rules

Concept:

```text
concept.company
concept.finance.revenue
concept.finance.operating_income
concept.identifier.kr_corporate_registration_number
concept.identifier.kr_business_registration_number
concept.time.fiscal_year
concept.tax.business_registration_status
concept.currency.krw
```

Representation:

```text
repr.finance.revenue.observation_amount
repr.identifier.kr_corporate_registration_number.identifier_value
repr.tax.business_registration_status.observed_value
```

Representation Schema:

```text
schema.finance.revenue.money_amount
schema.identifier.kr_business_registration_number.plain_10_digit
schema.identifier.kr_business_registration_number.dashed_display
schema.tax.business_registration_status.nts_code
```

Source:

```text
source.data_go_kr.fsc
op.data_go_kr.fsc.get_summ_fina_stat_v2
field.data_go_kr.fsc.get_summ_fina_stat_v2.item.enpSaleAmt
```

Binding:

```text
bind.data_go_kr.fsc.enpSaleAmt.to.repr.finance.revenue
bind.data_go_kr.fsc.curCd.to.revenue.currency
bind.data_go_kr.fsc.bizYear.to.revenue.fiscal_year
```

Capability:

```text
cap.company.finance.get_revenue
cap.company.finance.get_summary_financial_facts
cap.company.tax.check_business_registration_status
cap.company.tax.validate_business_registration
```

Rules:

- Stable keys use lowercase dot namespaces.
- Original source names remain separate from stable keys.
- Stable keys do not change after publish.
- Labels, aliases, and mappings are search/governance metadata.

## Examples

### 기업 매출

```text
SourceField:
  field.data_go_kr.fsc.get_summ_fina_stat_v2.item.enpSaleAmt

Concept:
  concept.finance.revenue

CanonicalRepresentation:
  repr.finance.revenue.observation_amount
  = object.observation + property.observed_amount

RepresentationSchema:
  schema.finance.revenue.money_amount
  = decimal, minimum 0, currency context required

Bindings:
  enpSaleAmt -> observed_amount
  curCd -> currency
  bizYear -> fiscal_year
  fnclDcd -> statement_type

Capability:
  cap.company.finance.get_revenue

Capability Output:
  revenue_amount
```

### 사업자등록 상태

```text
SourceField:
  field.nts.businessman.status.data.b_stt_cd

Concept:
  concept.tax.business_registration_status

ValueDomain:
  value_domain.nts.business_status_code
    01 -> concept.tax.active_business
    02 -> concept.tax.suspended_business
    03 -> concept.tax.closed_business

CanonicalRepresentation:
  repr.tax.business_registration_status.observed_value

RepresentationSchema:
  schema.tax.business_registration_status.nts_code
  = enum value_domain.nts.business_status_code

Capability:
  cap.company.tax.check_business_registration_status
```

## Resolver Interfaces

MVP 내부 service interface:

```text
resolve_intent(text) -> ConceptCandidate[]
resolve_concept(concept_key) -> Concept
find_representations(concept_key) -> CanonicalRepresentation[]
find_capabilities(provides_concept, required_context) -> Capability[]
compile_plan(capability, inputs) -> ExecutionPlan
execute_plan(plan) -> ExecutionRun
normalize_result(raw_result, bindings) -> CanonicalObject[]
project_result(canonical_object, projection) -> Response
```

## Migration Guardrails

- 기존 Context Platform 테이블을 무시하고 중복 테이블을 만들지 않는다.
- 먼저 `sources`, `source_operations`, `source_fields`, `bindings`,
  `capabilities`, `capability_outputs`와의 compatibility path를 분석한다.
- 대규모 교체가 필요하면 V2 migration plan을 별도 문서로 낸다.
- 새 runtime code에 provider/domain keyword rule을 하드코딩하지 않는다.
- Secret value는 env 파일에만 두고 manifest, payload, proposal, generated
  artifact에 넣지 않는다.
