# Context Platform 개요

## 정의

`services/context_platform`은 Context Platform의 현재 구현 경로다.

Context Platform은 모델 저장소가 아니라 Meaning Resolution Platform이다.

```text
Context Platform is a Meaning Resolution Platform that connects Concepts,
Canonical Representations, Representation Schemas, Sources, Capabilities, and
Executions into an Executable Meaning Graph.
```

한국어 정의:

```text
Context Platform은 Concept를 중심으로 표준 표현, 표현 스키마, 원천 데이터,
실행 가능한 Capability, 실제 실행 이력, 근거/거버넌스를 연결하는 Meaning
Resolution Platform이다.
```

## 기준 흐름

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

핵심 규칙:

```text
Concept is meaning.
ObjectType and PropertyType are structure.
CanonicalRepresentation is a template.
RepresentationSchema is datatype and validation for that template.
Binding is source-to-representation resolution.
Capability is an executable contract.
ExecutionPlan and ExecutionRun are concrete execution instances.
Evidence makes every assertion reviewable.
```

## Executable Meaning Graph

### 1. Meaning Graph

플랫폼이 이해하는 의미를 저장한다.

주요 객체:

- `MeaningScope`
- `ConceptScheme`
- `Concept`
- `ConceptRelation`
- `ValueDomain`
- `ValueDomainValue`

예:

```text
concept.finance.revenue
concept.identifier.kr_corporate_registration_number
concept.time.fiscal_year
concept.tax.business_registration_status
concept.currency.krw
```

`Concept`는 class도 field도 metric 전용 객체도 아니다. 의미 자체다.
`Concept.kind`와 `MeaningScope`를 반드시 둬서 Concept가 무제한 쓰레기통이
되지 않게 한다.

### 2. Representation Model

의미를 담는 표준 구조를 정의한다.

주요 객체:

- `ObjectType`
- `PropertyType`
- `LinkType`
- `CanonicalRepresentation`
- `RepresentationSchema`
- `ExternalProjection`

`CanonicalRepresentation`은 instance가 아니라 템플릿이다.

예:

```text
concept.finance.revenue
  -> repr.finance.revenue.observation_amount
  -> object.observation + property.observed_amount
```

`revenue_amount`는 기본적으로 canonical property가 아니다. Capability output key
또는 external projection key다.

`RepresentationSchema`는 그 템플릿에 적용되는 타입과 검증 규칙이다.
datatype, regex/pattern, enum/value domain, cardinality, required/default,
examples, minimum/maximum 같은 제약은 Concept가 아니라 RepresentationSchema에
둔다.

예:

```text
concept.identifier.kr_business_registration_number
  = 사업자등록번호라는 의미

repr.identifier.kr_business_registration_number.identifier_value
  = Identifier.identifier_value로 표현한다는 템플릿

schema.identifier.kr_business_registration_number.plain_10_digit
  = datatype string, pattern ^\d{10}$
```

`PropertyType`은 구조적으로 가능한 넓은 range를 가진다. `RepresentationSchema`는
특정 Concept 표현에 적용되는 세부 제약을 가진다. `SourceConstraint`는 특정
source/API transport 제약을 가진다.

### 3. Source Graph

원천 시스템과 API 구조를 있는 그대로 보존한다.

주요 객체:

- `SourceSystem`
- `SourceOperation`
- `SourceParameter`
- `SourceField`
- `SourceConstraint`
- `SourceError`

`source_operations`는 executable operation의 단일 테이블이다. 별도 Operation
Registry를 만들지 않는다.

### 4. Resolution Graph

Source와 Meaning/Representation을 연결한다.

주요 객체:

- `FieldBinding`
- `ContextBinding`
- `ParameterBinding`
- `TransformRule`
- `ResolutionRule`

예:

```text
field.data_go_kr.fsc.get_summ_fina_stat_v2.item.enpSaleAmt
  -> repr.finance.revenue.observation_amount
  -> fills property.observed_amount

field.data_go_kr.fsc.get_summ_fina_stat_v2.item.curCd
  -> repr.finance.revenue.observation_amount
  -> fills context currency
```

Binding은 전역 raw name 매핑이 아니다. 항상 source, operation, path context를
가져야 한다.

### 5. Capability Graph

Planner가 선택하는 실행 가능한 의미 계약을 저장한다.

주요 객체:

- `Capability`
- `CapabilityInput`
- `CapabilityOutput`
- `CapabilityStep`
- `CapabilityConstraint`

Capability는 endpoint가 아니다. SourceOperation이 실제 API endpoint/function/job이고,
Capability는 의미 기반 실행 계약이다.

예:

```text
cap.company.finance.get_revenue
  requires concept.identifier.kr_corporate_registration_number
  requires concept.time.fiscal_year
  provides concept.finance.revenue
  provides repr.finance.revenue.observation_amount
  output_key revenue_amount
  step op.data_go_kr.fsc.get_summ_fina_stat_v2
```

### 6. Execution Graph

실제 plan과 run instance를 저장한다.

주요 객체:

- `ExecutionPlan`
- `PlanStep`
- `ExecutionRun`
- `ExecutionStepRun`
- `ExecutionResult`
- `ExecutionTrace`

Capability는 “할 수 있는 것”이고, ExecutionPlan/ExecutionRun은 “이번 요청에서
실제로 선택하고 실행한 것”이다. 둘을 섞지 않는다.

### 7. Evidence / Governance

모든 assertion과 generated artifact의 근거, 리뷰, 정책, 품질, lineage를 관리한다.

주요 객체:

- `Evidence`
- `ReviewEvent`
- `MetadataAspect`
- `LineageEdge`
- `QualityCheck`
- `PolicyTag`

`MetadataAspect`는 DataHub의 aspect 패턴처럼 확장 메타데이터를 담는다. 단,
planner-critical 정보인 Concept, CanonicalRepresentation, Binding, Capability,
SourceOperation은 aspect JSON 안에만 묻지 않는다.

## Planner Runtime Flow

예:

```text
"이 회사 2024년 매출 알려줘"
```

흐름:

1. Intent parsing: `매출` -> `concept.finance.revenue`
2. Entity resolution: 회사 문맥 -> Company instance / identifier 확보
3. Representation selection: `repr.finance.revenue.observation_amount`
4. Capability discovery: `cap.company.finance.get_revenue`
5. Input fulfillment: 법인등록번호, fiscal year
6. Execution planning: source operation과 parameter binding 선택
7. Validated execution
8. Result normalization: source field -> canonical representation
9. Projection: `revenue_amount` output key로 반환

Planner API:

- `POST /planner/plan`
- `POST /planner/execute`
- `GET /planner/plans/{plan_id}`
- `POST /planner/validate`

Raw source operation execution은 LLM client에 노출하지 않는다. 항상 validated plan을
통해 실행한다.

## Proposal Workflow

Generated artifact는 자동 승인하지 않는다.

Lifecycle:

```text
proposed -> reviewed -> approved -> published
```

적용 대상:

- concepts, concept schemes, concept relations, value domains
- object types, property types, canonical representations
- source operations, source parameters, source fields
- field/context/parameter bindings
- capabilities, capability inputs/outputs, capability steps
- execution tests, evidence, governance metadata

## API Plane 분리

### Admin / Control Plane

- service: `context-platform-api`
- source upload
- API document ingestion
- proposal 생성
- review / governance
- dashboard 지원

### Planner / Runtime Plane

- service: `context-platform-planner-api`
- approved graph read
- capability search
- plan creation
- plan validation
- validated plan execution

Planner plane은 source upload, secret CRUD, ingestion trigger, proposal review,
catalog mutation을 노출하지 않는다.

## Non-goals

우선 만들지 않는다.

- RDF/OWL/triplestore 도입
- Neo4j 또는 graph DB 도입
- DataHub/OpenMetadata를 core store로 사용
- MCP 서버 구현
- 전체 도메인 모델링
- 모든 공공 API ingestion 자동화
- BI/dbt/Cube export 구현
- LLM client의 raw source operation 직접 실행

DataHub/OpenMetadata는 나중에 metadata catalog integration target이 될 수 있다.
MCP는 나중에 Capability를 tool로 노출하는 adapter layer로 붙인다.

## 관련 문서

- 데이터 모델: [context_platform_registry_model_ko.md](/workspace/docs/architecture/context_platform_registry_model_ko.md)
- 설계 기준: [meaning-resolution-platform.md](/workspace/docs/architecture/meaning-resolution-platform.md)
- 그래프 모델: [executable-meaning-graph.md](/workspace/docs/architecture/executable-meaning-graph.md)
- ADR: [ADR-0001-meaning-resolution-platform.md](/workspace/docs/adr/ADR-0001-meaning-resolution-platform.md)
