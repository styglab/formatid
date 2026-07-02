# Context Platform 구현 현황

## 현재 상태 요약

현재 구현 경로는 `services/context_platform`이다.

제품/아키텍처 기준은 Meaning Resolution Platform으로 재정의한다.

```text
Executable Meaning Graph
  -> Meaning Graph
  -> Representation Model
  -> Source Graph
  -> Resolution Graph
  -> Capability Graph
  -> Execution Graph
  -> Evidence / Governance
```

기존 canonical/binding compatibility storage는 런타임 source of truth가 아니다.
새 테이블이나 서비스를 추가할 때는 Meaning Resolution Platform의 책임 분리를
깨지 않는 범위에서만 확장한다.

## 구현된 영역

### 저장소 / 승인 루프

`services/context_platform/internal/storage/context_repository.py`

현재 구현됨:

- source CRUD/read model
- source document upload metadata
- source operation / parameter / field 저장
- meaning scope / concept / value domain 저장
- object type / property type / representation / representation schema 저장
- field binding / context binding / parameter binding 저장
- capability 저장
- proposal / proposal bundle / review decision 저장
- onboarding run / evidence snapshot 저장
- overview 집계

현재 Meaning Resolution storage는 새 graph table을 기준으로 한다. 옛
`canonical_*`, `bindings`, `binding_evidence`, `capability_operations` 테이블은
reset/schema 적용 후 제거된다.

목표 mapping:

```text
object_types               -> ObjectType
property_types             -> PropertyType
link_types                 -> LinkType
canonical_representations  -> Concept-to-structure template
representation_schemas     -> datatype/regex/enum/validation rules
field_bindings             -> SourceField fills representation value
context_bindings           -> SourceField fills representation context
parameter_bindings         -> SourceParameter receives required concept
capabilities               -> executable meaning contract
capability_steps           -> source operation/function implementation step
capability_outputs         -> output_key + concept + representation target
```

### Admin API

`services/context_platform/adapters/admin_api/app/context_platform.py`

현재 구현됨:

- overview
- source CRUD
- source upload
- source operation 조회
- source field 조회
- compatibility read APIs for canonical type / enum / slot / class / class-slot usage
- LinkML-compatible model export
- capabilities CRUD
- binding CRUD
- proposals 조회

### Dashboard

현재 대시보드는 control plane이 존재하지만 IA와 명칭은 새 설계로 이동 중이다.

목표 IA:

```text
Overview
Sources
Workbench
Meaning Graph
Representations
Resolution
Capabilities
Executions
Governance
Release
```

## 미완성 영역

### Meaning / Representation Storage

구현 기준:

- MeaningScope
- ConceptScheme
- Concept
- ConceptRelation
- ValueDomain
- ValueDomainValue
- ObjectType
- PropertyType
- LinkType
- CanonicalRepresentation
- RepresentationSchema
- ExternalProjection

### Resolution Storage

구현 기준:

- FieldBinding
- ContextBinding
- ParameterBinding
- TransformRule
- ResolutionRule

`bindings`는 제거하고 목적별 typed table을 사용한다.

### Capability / Execution 분리

필요한 것:

- CapabilityInput/Output/Step이 concept와 representation을 명시
- ExecutionPlan/ExecutionRun/ExecutionResult가 catalog object와 분리
- validated plan만 execution 가능

### Planner API

`services/context_platform/adapters/planner_api/app/main.py`

목표 API:

- `POST /planner/plan`
- `POST /planner/execute`
- `GET /planner/plans/{plan_id}`
- `POST /planner/validate`

Planner 책임:

- intent -> ConceptCandidate
- Concept -> CanonicalRepresentation
- Concept/context -> Capability
- Capability/input -> ExecutionPlan
- validated plan -> ExecutionRun
- raw result -> canonical object -> projection

## Migration Guardrails

- `source_operations`가 유일한 operation table이다.
- 별도 Operation Registry를 만들지 않는다.
- Concept는 core meaning node지만 독립 vocabulary product로 만들지 않는다.
- LinkML은 registry schema import/export language다.
- PostgreSQL이 runtime registry다.
- MetadataAspect는 governance extension이다.
- planner-critical 정보는 JSONB aspect 안에만 두지 않는다.
- runtime code에 provider/domain keyword rule을 하드코딩하지 않는다.

## 현재 우선순위

1. 설계 문서와 AGENTS.md를 Meaning Resolution Platform 기준으로 정렬
2. 기존 테이블과 새 logical model의 compatibility map 작성
3. LinkML registry schema 초안 작성
4. RepresentationSchema를 type/regex/enum/validation의 소유 계층으로 반영
5. DB migration plan 작성
6. resolver service skeleton 작성
7. ingestion proposal payload에 Concept/RepresentationSchema/Resolution 정보를 보존
8. Planner Service의 `/planner/*` API를 Execution Graph 기준으로 정리

## 관련 문서

- 데이터 모델: [context_platform_registry_model_ko.md](/workspace/docs/architecture/context_platform_registry_model_ko.md)
- LinkML Registry Model: [context_platform_linkml_canonical_model_ko.md](/workspace/docs/architecture/context_platform_linkml_canonical_model_ko.md)
- 개요: [context_platform_overview_ko.md](/workspace/docs/architecture/context_platform_overview_ko.md)
- 설계 기준: [meaning-resolution-platform.md](/workspace/docs/architecture/meaning-resolution-platform.md)
- ADR: [ADR-0001-meaning-resolution-platform.md](/workspace/docs/adr/ADR-0001-meaning-resolution-platform.md)
