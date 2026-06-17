# Semantic Platform 개요

## 목적

`semantic_platform`은 단순 Data Catalog도 아니고 MCP Registry도 아니다.

목표는 AI agent와 실행 runtime이 다음을 이해할 수 있는 승인된 semantic context를
제공하는 것이다.

```text
API documents / manual authoring
  -> semantic types
  -> canonical model
  -> mappings
  -> capabilities
  -> operations / variants
  -> governance
  -> planner context
  -> app executor
```

## 서비스 경계

구현 모듈과 경로:

- module: `semantic_platform`
- path: `services/semantic_platform`

`semantic_platform`이 소유하는 것:

- semantic types registry
- canonical model registry
- mapping registry
- capability registry
- operations registry
- governance registry
- planner/runtime context packaging

`semantic_platform`이 소유하지 않는 것:

- provider HTTP client
- auth, retry, pagination
- provider SDK quirks
- raw runtime response parsing

이 실행 책임은 `apps/pubdata_mcp` 같은 app runtime에 있다.

## 핵심 레지스트리

### 1. Semantic Types

의미 단위 정의.

예:

- `COMPANY_ID`
- `BUSINESS_NUMBER`
- `BID_NOTICE_ID`

### 2. Canonical Model

비즈니스 객체 구조 정의.

- entity
- attribute
- relation
- identity system

예:

- `Company`
- `Company.company_id`
- `BidNotice.notice_id`

### 3. Mappings

핵심 단위는 `field -> semantic_type`가 아니라 아래다.

```text
source + operation + path
  -> semantic type
  -> canonical attribute
```

### 4. Capabilities

planner-facing intent.

예:

- `search_contracts`
- `get_company_info`

### 5. Operations / Variants

실행 가능한 contract와 의미 분기 단위.

- operation
- field/control mapping
- variant
- capability implementation

### 6. Governance

- proposal
- review
- audit
- lifecycle
- provenance

## 두 가지 관점

### 개념/런타임 관점

```text
Semantic Types
-> Canonical Model
-> Capabilities
-> Operations
-> Mappings
-> Governance
```

### 등록/온보딩 관점

```text
Source
-> Asset / Access Path
-> Schema / Field Paths
-> Semantic Types
-> Canonical Links
-> Mappings
-> Capabilities
-> Governance
```

두 관점을 섞지 않는 것이 중요하다.

## API plane 분리

### Admin / Control Plane

- `semantic-platform-api`
- source upload
- CRUD
- proposals
- review
- governance

### Planner / Runtime Plane

- `semantic-platform-planner-api`
- approved context read
- contract read
- execution planning

planner plane은 mutation을 노출하지 않는다.

## 관련 문서

- 구현 현황: [semantic_platform_implementation_ko.md](/workspace/docs/architecture/semantic_platform_implementation_ko.md)
- 대시보드 운영/UX: [semantic_platform_dashboard_ko.md](/workspace/docs/architecture/semantic_platform_dashboard_ko.md)

## Source Onboarding Run 중심 Workflow

현재 제품 방향은 `Mapping AI Suggestion`을 독립 중심 workflow로 두지 않는다.
Mapping은 Source Onboarding Run 안의 field-level authoring task다.

핵심 authoring 축은 `operation-first`가 아니라 `source / asset-first`다.

```text
Source
  -> Asset / Access Path
    -> Schema / Fields
    -> Controls (optional)
    -> Operations (optional)
```

의미는 다음과 같다.

- `Source`
  - 업로드한 문서, 파일, 명세 묶음
- `Asset / Access Path`
  - 실제 분석 대상
  - API는 endpoint group, request/response path, spec section
  - CSV는 file, table, sheet
- `Schema / Fields`
  - 해당 대상의 구조
- `Controls`
  - 의미를 바꾸는 제어값이 있을 때만 존재
- `Operations`
  - 실행 가능한 호출 단위가 있을 때만 존재

권장 흐름:

```text
Source upload
-> onboarding run 생성
-> evidence snapshot 생성
-> asset / access path / schema / field / control discovery
-> AI suggestion batch 생성
-> proposal bundle 생성
   - semantic type proposals
   - canonical model proposals
   - field mapping + transform proposals
   - operation variant proposals
   - capability proposals
   - capability-operation binding proposals
-> reviewer가 proposal 또는 bundle 단위로 승인
-> approved runtime snapshot publish
-> planner/executor는 approved snapshot만 사용
```

핵심 원칙:

```text
Source-driven proposal, platform-governed approval
```

소스 문서는 제안을 만들 수 있지만 정답이 아니다. Semantic Type, Canonical
Model, Capability, Operation Variant, Mapping, Capability Binding은 모두
review/publish lifecycle을 통과해야 runtime context가 된다.

## Stage/Task 기반 Onboarding Workflow

onboarding은 단순 업로드 후 배치 실행 화면이 아니라, 사용자가 현재 단계와
작업 단위를 따라가며 진행하는 guided workflow여야 한다.

핵심 모델:

- `Onboarding Run`
  - 하나의 source evidence를 semantic proposal로 전환하는 workflow container
- `Stage`
  - 의미 있는 작업 단계
- `Task`
  - 사용자가 실제로 검토/승인/수정하는 작업 단위
- `Proposal Bundle`
  - run에서 생성된 proposal 묶음

권장 stage:

1. `source_review`
2. `asset_discovery`
3. `structure_review`
4. `semantic_mapping`
5. `controls_and_variants`
6. `operation_and_binding_modeling`
7. `proposal_review`
8. `publish_readiness`

권장 task 예:

- `confirm_source_metadata`
- `discover_assets`
- `review_extracted_structures`
- `resolve_unmapped_fields`
- `classify_control_fields`
- `review_operation_and_binding_candidates`
- `approve_proposal_bundle`
- `publish_runtime_snapshot`

run detail UI는 자유 탐색 중심보다 다음 순서를 우선해야 한다.

```text
current stage
-> open tasks
-> inspect evidence
-> review AI draft / manual edit
-> approve or reject
-> next stage
```

### Dashboard IA 원칙

사이드바는 리소스 분해보다 workflow 중심으로 유지해야 한다.

권장 주축:

- `Sources`
- `Onboarding Runs`
- `Proposal Bundles`

`Operations`, `Schemas`, `Work Queue`는 전역 메뉴보다 source/run 상세 안의
하위 탭으로 내려야 한다.

예:

- `Source Detail`
  - `Overview`
  - `Assets`
  - `Structures`
  - `Operations` when present
  - `Controls / Variants` when present
  - `Proposals`
  - `Review Tasks`

- `Run Detail`
  - `Evidence`
  - `Assets`
  - `Structures`
  - `Operations / Access Paths` when present
  - `Proposal Bundle`
  - `Review Tasks`

### 모든 Task는 AI Draft를 지원

설계 원칙:

- 모든 onboarding task는 AI draft 생성을 지원한다.
- 사용자는 수동으로 작업할 수도 있고, AI draft를 받아 수정/승인할 수도 있다.
- AI draft는 proposal 또는 draft artifact이며, approved truth가 아니다.

즉 `AI 보조`는 특정 화면 기능이 아니라 workflow-native capability다.

각 task는 최소 아래를 가져야 한다.

- `task_type`
- `stage`
- `status`
- `supports_ai_draft`
- `draft_status`
- `draft_payload`
- `draft_rationale`
- `draft_confidence`
- `evidence_refs`
- `recommended_action`

권장 상태:

- `pending`
- `running`
- `ai_drafted`
- `needs_review`
- `approved`
- `rejected`
- `blocked`
- `completed`

### 자동화와 승인 경계

다음은 discovery 산출물로 자동 저장 가능하다.

- source metadata
- onboarding run
- evidence snapshot
- execution operation
- operation field
- observed control values
- endpoint/request/response evidence

다음은 proposal로만 다뤄야 한다.

- semantic type 생성/연결
- canonical entity/attribute 생성/연결
- field mapping
- transform spec
- enum mapping
- operation variant
- capability
- capability binding

원칙:

```text
Every onboarding task supports AI draft generation,
but semantic/runtime truth changes only through explicit review and publish transitions.
```

### Control Field 특별 규칙

control field는 일반 field mapping의 하위 문제가 아니다.

예:

- `inqryDiv`
- `typeCd`
- `searchMode`

이 필드는 onboarding에서 별도 decision path를 가져야 한다.

- observed value 수집
- semantic control 후보 생성
- enum meaning 후보 생성
- variant split 후보 생성
- reviewer가 mapping 또는 variant 경로를 결정

따라서 `controls_and_variants` stage는 optional 부가 기능이 아니라 core stage다.

### Onboarding Run

`Onboarding Run`은 source upload 또는 ingestion execution의 결과를 묶는 작업 단위다.

포함해야 하는 정보:

- source id / uploaded document reference
- evidence snapshot id
- discovered operation count
- discovered schema/field/control count
- semantic suggestion batch status
- generated proposal ids
- reviewer handoff status

### Evidence Snapshot

`Evidence Snapshot`은 AI suggestion과 reviewer 판단의 근거다.

포함해야 하는 정보:

- source document hash / reference uri
- parsed sections
- operation evidence
- request/response/control tables
- sample values
- endpoint verification evidence
- extraction warnings

### Proposal Bundle

`Proposal Bundle`은 한 onboarding run에서 나온 registry 변경 proposal 묶음이다.
개별 proposal만 보면 맥락을 잃기 때문에 reviewer는 bundle 단위로 영향 범위를
먼저 보고 필요한 proposal을 drill-down해야 한다.

예:

```text
bundle.ppspublicapi.2026-06-16
  create semantic type BidClosingAt
  create canonical attribute ProcurementNotice.bid_closing_at
  create mapping body.item.bidClseDt -> BidClosingAt
  create variant search_by_notice_date
  bind capability search_contract_notices -> op_search_bid_notices
```

## Capability Binding과 Field Mapping 분리

두 종류의 매핑은 분리해야 한다.

### Capability -> Operation / Variant

의미:

```text
planner-facing capability를 실행하려면 어떤 operation contract 또는 variant를 쓰는가
```

예:

```text
search_contract_notices
-> op_search_bid_notices
-> variant_search_by_notice_date
```

이 registry는 `Capability Bindings` 또는 `Capability Implementations`로 다룬다.

### Operation Field -> Semantic Type -> Canonical Attribute

의미:

```text
operation request/response/control field가 어떤 semantic meaning과 canonical attribute를 갖는가
```

예:

```text
op_get_bid_notice_detail.body.item.bidClseDt
-> BidClosingAt
-> ProcurementNotice.bid_closing_at
-> date_parse yyyyMMddHHmmss
```

planner/runtime 연결은 다음처럼 이어진다.

```text
capability_id
-> capability binding
-> operation_id + optional variant_id
-> request/response/control field mappings
-> semantic_type_id
-> canonical_attribute_id
```

## UI 중심 변경

대시보드 중심 workflow는 다음 순서로 재정렬한다.

```text
Home
  Overview

Onboarding
  Sources
  Onboarding Runs

Semantic Registry
  Semantic Types
  Canonical Model
  Mappings
  Lineage

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

Release
  Publish
```

Work Queue는 독립 제품 중심이 아니라 onboarding run의 하위 task view다.
