# Semantic Platform Dashboard 운영/UX 메모

## 목적

이 문서는 `services/semantic_platform/adapters/dashboard`의 현재 구조와
운영 개선 방향을 한 곳에 정리한다.

기존 backlog 문서와 UX 메모 문서를 합친 요약본이다.

## 현재 구조

```text
Home
  Overview

Onboarding
  Sources
  Onboarding Runs

Semantic Platform
  Semantic Types
  Canonical Model
  Mappings
  Lineage

Agent Layer
  Capabilities
  Operation Catalog
  Variants

Governance
  Proposal Bundles
  Proposals
  Reviews
  Audit

Release
  Publish

Reference
  Prototype
```

## 현재 원칙

### 1. Onboarding과 Semantic Platform을 분리

- Onboarding은 source/run workflow
- Semantic Platform은 의미 모델링

### 2. Agent Layer를 따로 둔다

- capability
- operation catalog
- variant

이 영역은 planner/executor 관점이다.

### 3. Governance와 Release를 분리

- Governance
  - proposals
  - reviews
  - audit
- Release
  - publish

## 현재 UX 패턴

핵심 화면은 아래 패턴을 우선한다.

- full-width table
- row click
- drawer detail / edit

특히 `Mappings`는 현재 기준 화면이다.

## 현재 CRUD 상태

새 route에서 직접 가능한 것:

- Sources
- Semantic Types
- Canonical Model
- Mappings
- Capabilities
- Variants

## 아직 부족한 점

### Onboarding

- source upload 이후 guided next-step이 약함
- source run / ingestion history가 약함
- asset/access path visibility가 약함
- structure coverage가 더 필요

### Semantic Platform

- semantic/canonical 차이를 더 분명히 보여줘야 함
- mappings bulk 작업이 부족
- lineage는 아직 얕음

### Agent Layer

- capability IO 설명력 보강 필요
- operation catalog contract 표현 강화 필요
- variants 비교 UX 부족

### Governance

- bulk review 부족
- reviewer note / impact scope 부족
- richer audit ledger 부족

### Release

- snapshot compare / promote / rollback 흐름 부족

## 현재 우선 개선 순서

1. `Mappings` 기준 화면 마감
2. `Sources -> Source Detail / Run Detail` onboarding UX 강화
3. `Proposals` workbench 고도화
4. `Canonical Model` 관계 모델링 UX 보강
5. `Publish / Lineage` 심화
6. planner/runtime plane 구현

## 현재 라우트 기준

```text
app/semantic/discovery/*
app/semantic/semantic-platform/*
app/semantic/agent/*
app/semantic/governance/*
app/semantic/release/*
app/semantic/reference/*
```

`authoring/*` 같은 내부 이전 경로는 정리 대상이다.

## 관련 문서

- 개요: [semantic_platform_overview_ko.md](/workspace/docs/architecture/semantic_platform_overview_ko.md)
- 구현 현황: [semantic_platform_implementation_ko.md](/workspace/docs/architecture/semantic_platform_implementation_ko.md)

## 방향 전환: Mapping 중심에서 Onboarding Run 중심으로

현재 `Work Queue`와 `Mappings` UX는 field mapping authoring에는 유효하지만,
제품의 중심 workflow로 보기에는 범위가 좁다. 앞으로의 중심 화면은
`Onboarding Runs`와 `Proposal Bundles`다.

새 IA:

```text
Home
  Overview

Onboarding
  Sources
  Onboarding Runs

Execution Registry
  Capabilities
  Operations
  Variants
  Capability Bindings

Governance
  Proposal Bundles
  Proposals
  Reviews
  Audit
```

`Work Queue`는 다음 역할로 낮춘다.

- onboarding run에서 생성된 field-level task 처리
- unmapped field의 semantic type/transform 선택
- proposal bundle 안의 일부 proposal 수정

`Operations`, `Schemas`, `Work Queue`는 독립 메인 메뉴가 아니라 source/run
detail 안의 탭이나 하위 view로 이동한다.

핵심 authoring 모델:

```text
Source
  -> Asset / Access Path
    -> Schema / Fields
    -> Controls (optional)
    -> Operations (optional)
```

즉 API형 source는 operation view가 보이고, CSV형 source는 구조/필드 중심으로
진행된다.

### Onboarding Runs 화면

목적:

- source upload/ingestion run 단위 진행 상황을 보여준다.
- evidence, discovered assets, structures, optional operations, suggestions, proposals를 한 화면에서 연결한다.

필수 정보:

- run id
- source
- status
- assets discovered
- structures discovered
- mappings suggested/created
- proposals generated
- evidence snapshot status

### Proposal Bundles 화면

목적:

- 한 onboarding run에서 나온 proposal 묶음을 reviewer에게 제공한다.
- 개별 proposal을 보기 전 source evidence와 영향 범위를 먼저 보여준다.

필수 정보:

- bundle id
- source/run
- proposal count
- entity type breakdown
- pending/approved/rejected count
- evidence snapshot link

### Capability Bindings 화면

목적:

- capability와 operation/variant implementation의 연결을 field mapping과 분리해 보여준다.

필수 정보:

- capability
- operation
- variant
- binding status
- input/output semantic coverage
- evidence/proposal status

## 다음 구현 우선순위

1. Onboarding Runs 정식 테이블/API/UI 고도화
2. Source Detail에 `Assets / Structures / Proposals / Review Tasks` 통합
3. Evidence Snapshot 상세 화면과 source section/field/sample 연결
4. Proposal Bundle 상세 review/apply workflow
5. Work Queue를 onboarding run detail 하위 task view로 이동
6. run-scoped AI suggestion batch 저장
7. Capability Binding authoring/proposal workflow
8. approved runtime snapshot publish
