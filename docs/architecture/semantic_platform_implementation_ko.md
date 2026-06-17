# Semantic Platform 구현 현황

## 현재 상태 요약

현재 `semantic_platform`은 다음처럼 보는 것이 정확하다.

```text
admin/control plane:
  기본 CRUD + proposal review 루프 존재

planner/runtime plane:
  API surface는 있으나 핵심 응답은 대부분 스텁

dashboard:
  새 route 기반 control plane 존재
  legacy prototype reference도 유지
```

## 구현된 영역

### 저장소 / 승인 루프

`services/semantic_platform/internal/storage/repository.py`

구현됨:

- semantic types CRUD proposal
- canonical entities / attributes / relations CRUD proposal
- execution sources CRUD proposal
- execution operations 조회
- operation fields 조회
- operation variants CRUD proposal
- capabilities CRUD proposal
- field mappings CRUD proposal
- semantic relationships CRUD proposal
- proposals 조회
- approve / reject / apply
- overview 집계

핵심은 즉시 반영이 아니라 `proposal -> review -> apply` 흐름이라는 점이다.

### Admin API

`services/semantic_platform/adapters/admin_api/app/main.py`

구현됨:

- overview
- semantic types CRUD
- execution sources CRUD
- execution source upload
- execution assets 조회
- execution operations 조회
- operation fields 조회
- operation variants CRUD
- capabilities CRUD
- mappings CRUD
- semantic relationships CRUD
- canonical model CRUD
- proposals 조회
- proposal approve / reject

### Dashboard

현재 대시보드 IA:

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
  Proposals
  Reviews
  Audit

Release
  Publish

Reference
  Prototype
```

새 route에서 직접 CRUD 가능한 자원:

- execution sources
- semantic types
- canonical entities / attributes / relations
- mappings
- capabilities
- operation variants

## 미완성 영역

### Planner API

`services/semantic_platform/adapters/planner_api/app/main.py`

현재:

- `/semantic/planner/execution-plan`
  - `not_found` 스텁
- `/semantic/execution/contracts`
  - 빈 payload 중심
- `/runtime-context`
  - 최소 metadata만 반환

즉 runtime plane은 아직 본격 구현 전이다.

### Runtime context packaging

아직 없는 것:

- approved capabilities package
- approved contracts package
- approved variants package
- approved mappings package
- runtime snapshot package

### Governance 깊이

현재는 review queue MVP 수준이다.

부족한 것:

- richer review history
- version history
- conflict handling
- publish snapshot compare
- deeper lineage drill-down

### Legacy prototype

`app/semantic/reference/prototype/page.tsx`

유지 이유:

- migration reference

문제:

- 구조 분해 대상이었고
- 더 이상 메인 경로는 아니다

## 현재 우선순위

1. dashboard 핵심 화면 완성도 향상
2. source onboarding / schema review / mapping UX 강화
3. governance depth 보강
4. planner/runtime plane 실구현

## 관련 문서

- 개요: [semantic_platform_overview_ko.md](/workspace/docs/architecture/semantic_platform_overview_ko.md)
- 대시보드 운영/UX: [semantic_platform_dashboard_ko.md](/workspace/docs/architecture/semantic_platform_dashboard_ko.md)
