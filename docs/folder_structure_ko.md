# 폴더 구조 설명

이 저장소는 공공 API wrapper나 단순 MCP Registry가 아니다. 목표는 AI
Agent가 조직 내 Capability를 이해하고, 계획하고, 실행 가능한 컨텍스트를
얻을 수 있도록 하는 Semantic Layer Platform이다.

```text
API documents / manual authoring
  -> canonical semantic model
  -> capability catalog
  -> execution contracts / variants
  -> semantic layer graph
  -> LLM execution planning
  -> MCP/app execution runtime
```

## 최상위 구조

```text
.
  apps/
  core/
  data/
  deploy/
  docs/
  scripts/
  services/
  tests/
  tmp/
```

## services/semantic_layer

문서상 서비스명은 `semantic_layer`로 부른다. 현재 구현 경로는
`services/semantic_layer`를 유지한다.

`services/semantic_layer`는 플랫폼 control plane이다. 특정 app이 아니라
여러 MCP/RAG/domain app이 공유하는 canonical semantic model, capability
context graph, execution contract, governance context를 소유한다.

```text
services/semantic_layer/
  adapters/
    admin_api/
    planner_api/
    dashboard/
    worker/
  lib/
    model/
    semantic/
    capability/
    execution/
    authoring/
    governance/
    ingestion/
    planner/
    context/
    storage/
  manifests/
```

책임:

- canonical semantic type과 relationship 관리
- provider-neutral capability catalog 관리
- source-neutral execution contract 관리
  - source
  - asset
  - access path
  - operation contract
  - operation variant
  - field/control mapping
- API document ingestion을 통한 context change proposal 생성
- 사람이 직접 semantic/capability/contract를 등록, 수정, 리뷰, 승인
- proposal, review status, provenance, lineage, conflict 관리
- approved context를 planner/executor용 package로 제공
- LLM execution planner와 plan validation 제공

Execution Contracts는 API 전용 모델이 아니다. 현재는 API 문서를 주 입력으로
삼더라도, 향후 table/view/query/file/stream 같은 source type까지 수용할 수
있어야 한다. 따라서 execution contract는 `API endpoint 중심`이 아니라
`source -> asset -> access path -> operation -> field mapping` 구조로
설계한다.

하지 말아야 할 일:

- provider HTTP client 구현
- API key/provider auth 실행 로직
- provider pagination/retry loop
- raw provider response runtime parsing
- provider/domain keyword 선택 규칙 하드코딩

## apps/pubdata_mcp

`apps/pubdata_mcp`는 실행 runtime이다.

책임:

- MCP transport 제공
- `semantic-layer-planner-api`에서 approved plan/context 읽기
- selected `operation_id` / `variant_id`를 provider call로 컴파일
- provider HTTP 호출, 인증, retry, pagination, raw response parsing
- approved field mapping으로 semantic normalization 수행
- planner-declared binding과 integration 적용

하지 말아야 할 일:

- canonical semantic definition 소유
- global capability ranking
- cross-domain planning
- proposal 생성
- catalog mutation
- provider/domain 선택 규칙 하드코딩

## core

`core`는 도메인 없는 공통 코드만 둔다.

```text
core/catalog
core/contracts
core/observability
core/runtime
  app_service
  mcp
  runtime_db
```

`core/*`에는 app 이름, procurement field, business rule, capability planning을
넣지 않는다.

## services

서비스는 플랫폼 공통 기능과 control plane을 제공한다.

현재 주요 서비스:

```text
services/postgres
services/redis
services/nginx
services/platform_api
services/platform_dashboard
services/embedding
services/prefect
services/minio
services/qdrant
services/semantic_layer
```

App-required service인 `prefect`, `minio`, `qdrant`는 app/control plane이
선언할 때만 활성화된다.

## deploy

`deploy/compose/docker-compose.yml`은 generated file이다. 직접 수정하지 않고
manifest 변경 후 아래 명령으로 재생성한다.

```bash
python3 scripts/generate_compose.py
```

## scripts

외부 entrypoint:

```bash
python3 scripts/generate_compose.py
python3 scripts/ops.py <command>
```

권장 검증:

```bash
python3 scripts/ops.py validate-config
python3 scripts/ops.py lint-boundaries
python3 scripts/ops.py check-all
```

Semantic Layer 운영 명령:

```bash
python3 scripts/ops.py semantic-layer reset
python3 scripts/ops.py semantic-layer seed-registry
```

## tests

권장 테스트 구조:

```text
tests/semantic_layer
tests/apps/pubdata_mcp
```

## tmp

`tmp/*`는 scratch/retired 영역이다. active runtime path로 import하지 않는다.
비밀이 아닌 임시 산출물, codex_manual payload, 일회성 검증 파일은 사용자에게
다시 묻지 않고 생성, 수정, 검증, 삭제할 수 있다.

## 설계 원칙

- semantic/capability planning은 `services/semantic_layer/lib/planner`
- provider execution은 `apps/pubdata_mcp`
- generic runtime은 `core`
- source documents는 Semantic Layer source registry/object storage
- generated compose는 `deploy/compose`
- retired code는 `tmp`

LLM은 capability 선택, not-found 판단, execution DAG 생성을 담당한다.
Runtime code는 도메인 키워드나 provider-specific 의미 판단을 하드코딩하지
않는다. Deterministic code는 승인된 contract의 validation, transform,
execution, normalization만 수행한다.
