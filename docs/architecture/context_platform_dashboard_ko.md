# Context Platform Dashboard 운영/UX 메모

## 목적

이 문서는 `services/context_platform/adapters/dashboard`가 Meaning Resolution
Platform 설계를 어떻게 보여줘야 하는지 정리한다.

## 목표 IA

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

현재 route나 구현에는 이전 용어가 남아 있을 수 있다. 새 화면과 문서에서는
Executable Meaning Graph 용어를 우선한다.

## UX 원칙

### 1. Source Intake와 Review를 분리

- Sources는 등록된 source system/document/operation/field의 자산 축이다.
- Workbench는 source upload를 agent ingestion queue로 접수하고, agent가 만든
  proposal bundle을 검토/승인하는 작업 축이다.
- Workbench는 semantic draft 생성 버튼을 제공하지 않는다. Concept,
  Representation, Resolution, Capability 생성 판단은 agent response artifact와
  proposal bundle 경계를 통과해야 한다.

### 2. Catalog와 Runtime을 분리

- Meaning Graph, Representation Model, Source Graph, Resolution Graph,
  Capability Graph는 reviewable graph 자산이다.
- Execution Graph는 plan/run/result runtime visibility 영역이다.
- Planner는 registry가 아니라 service다.

### 3. Governance와 Release를 분리

- Governance: proposals, reviews, audit, evidence
- Release: approved graph snapshot publish, compare, promote, rollback

## Workbench 중심 Workflow

```text
Source upload
  -> queued agent ingestion request
  -> agent/manual ingestion creates evidence and proposal bundle
  -> dashboard review
  -> validate
  -> approve
  -> publish
```

Workbench의 핵심 단위는 final proposal bundle이다. 업로드는 intake이고,
ingestion authoring은 agent/worker 경계에서 수행한다.

## 화면별 책임

### Sources

- source system/document/operation/parameter/field visibility
- source operation method/path/auth/constraint 확인
- source field path, original name, label, evidence 확인

### Meaning Graph

- MeaningScope, ConceptScheme, Concept, ConceptRelation, ValueDomain 관리
- `Concept.kind`와 relation type을 명확히 보여준다.
- Concept가 class/field/metric 전용 객체가 아니라 meaning node임을 유지한다.

### Representations

- ObjectType, PropertyType, LinkType, CanonicalRepresentation 관리
- RepresentationSchema의 datatype, regex, enum, validation 관리
- `CanonicalRepresentation`이 instance가 아니라 template임을 보여준다.
- Concept에 regex/datatype이 직접 붙지 않게 UI에서 계층을 분리한다.
- `revenue_amount` 같은 consumer key와 canonical property를 혼동하지 않게 한다.

### Resolution

- FieldBinding, ContextBinding, ParameterBinding, TransformRule 관리
- source context `source + operation + path`를 항상 보여준다.
- target representation, fills property/context/input concept를 명확히 보여준다.

### Capabilities

- CapabilityInput, CapabilityOutput, CapabilityStep, CapabilityConstraint 관리
- Capability가 SourceOperation이 아니라 executable meaning contract임을 보여준다.
- output key, concept, representation을 함께 보여준다.

### Executions

- ExecutionPlan, PlanStep, ExecutionRun, ExecutionResult visibility
- `execute_plan`은 validated plan만 실행한다는 상태 표현 필요
- raw `execute_operation` UI action은 만들지 않는다.

### Governance

- proposal bundle review
- evidence, reviewer note, impact scope
- aspect, lineage, quality, policy metadata

## Proposal 대상

- concepts, concept schemes, concept relations, value domains
- object types, property types, canonical representations
- source operations, parameters, fields
- field/context/parameter bindings
- capabilities, inputs, outputs, steps
- evidence, metadata aspects, review events

Lifecycle:

```text
proposed -> reviewed -> approved -> published
```

## 현재 우선 개선 순서

1. Workbench를 Source Intake / Agent Ingestion Queue / Proposal Review로 재구성
2. Proposal bundle review에서 evidence와 impact scope 강화
3. Capability output에 output key, concept, representation을 함께 표시
4. Execution plan/run/result 화면 구현

## 관련 문서

- 개요: [context_platform_overview_ko.md](/workspace/docs/architecture/context_platform_overview_ko.md)
- 데이터 모델: [context_platform_registry_model_ko.md](/workspace/docs/architecture/context_platform_registry_model_ko.md)
- 설계 기준: [meaning-resolution-platform.md](/workspace/docs/architecture/meaning-resolution-platform.md)
