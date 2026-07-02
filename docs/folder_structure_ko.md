# 폴더 구조 설명

이 저장소는 LLM과 runtime이 조직 내 capability를 이해하고, 계획하고,
validated plan으로 실행할 수 있도록 하는 Context Platform 중심의 데이터
플랫폼이다.

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

## 공통 원칙

멀티 컨테이너 app/service는 같은 책임을 같은 위치에 둔다.

- `adapters/`: 외부 실행 경계
  - API
  - worker
  - dashboard
  - consumer
- `internal/`: service 내부 공통 로직
- `domain/`: app 내부 도메인/실행 로직
- `infra/`: 해당 실행 단위의 빌드/런타임/배포 자산
  - Dockerfile
  - adapter-local requirements
  - adapter-local env
- `manifests/`: compose 생성에 사용하는 선언

`infra/`는 루트에 두는 것이 기본이 아니라 실제 배포 단위에 붙인다.
컨테이너가 `manifests`에 선언되는 구조에서는 각 `adapters/<adapter>` 아래에
`infra/`를 둔다.

## services/context_platform

현재 구현 모듈과 폴더 이름은 `context_platform`이며 구현 경로는
`services/context_platform`다.

```text
services/context_platform/
  adapters/
    admin_api/
      app/
      infra/
    planner_api/
      app/
      infra/
    dashboard/
      app/
      components/
      infra/
    worker/
      flows/
      infra/
  internal/
    meaning/
    representation/
    source/
    resolution/
    capability/
    execution/
    governance/
    ingestion/
    planner/
    context/
    storage/
  manifests/
```

책임:

- Source Graph 관리
  - source
  - source document
  - source operation
  - source parameter
  - source field
- Meaning Graph 관리
  - meaning scope
  - concept scheme
  - concept
  - concept relation
  - value domain
  - value domain value
- Representation Model 관리
  - object type
  - property type
  - link type
  - canonical representation
  - representation schema
  - external projection
  - LinkML schema import/export
- Resolution Graph 관리
  - `source + operation + path`
  - source parameter 또는 source field
  - canonical representation
  - field/context/parameter binding
  - transformation / normalization rule
  - confidence
  - status
  - provenance
- provider-neutral Capability Graph 관리
  - required concepts
  - provided concepts
  - provided representations
  - output keys
  - capability-operation link
- Execution Graph 관리
  - execution plan
  - plan step
  - execution run
  - result / trace
- Planner Service 관리
  - `/planner/plan`
  - `/planner/validate`
  - `/planner/execute`
  - `/planner/plans/{plan_id}`
- source document upload와 agent-ingestion queue 관리
- proposal, review status, provenance, lineage, conflict, lifecycle, version 관리
- approved context를 Planner Service에 제공
- validated plan execution 제공

`source_operations`가 executable operation의 단일 테이블이다. 별도 실행
operation 테이블을 만들지 않는다. Concept는 의미이고,
CanonicalRepresentation은 그 의미를 ObjectType/PropertyType/context 조합으로
표현하는 템플릿이다. RepresentationSchema는 그 템플릿의 datatype, regex, enum,
cardinality, examples, validation constraints를 담당한다.

API 문서가 아닌 소스도 처리 대상이다. PDF, CSV, 데이터베이스 스키마,
필드 사전, DCAT 문서처럼 실행 endpoint가 없는 경우에는 `source_fields`,
Concept, CanonicalRepresentation, Binding proposal을 먼저 만들 수 있다. 실행
가능한 capability가 되려면 나중에 `source_operations` 링크가 필요하다.

Binding은 전역 raw name 매핑이 아니다. 동일한 `id`, `name`, `type` 필드는
source와 operation이 다르면 의미가 달라질 수 있다. 따라서 binding의 최소
context는 다음을 포함한다.

```text
source + operation + path
  -> canonical representation / context / parameter target
```

operation이 없는 문서에서는 source document와 field path를 evidence로
보존하고, 실행 검증은 pending 상태로 둔다.

Ingestion은 semantic ambiguity를 runtime keyword rule로 해결하지 않는다.
parser/docling은 source structure를 추출하고, generic validator는 schema shape만
검증한다. source term이 business field인지, provider control인지, transport인지,
response envelope인지, capability signal인지는 LLM/manual response와 reviewable
proposal bundle에 남긴다. `skip` 또는 `skip_binding`으로 판단된 term은 후속
proposal 생성 단계에서 fallback concept/representation/class/slot/binding을 다시
채우면 안 된다.

하지 말아야 할 일:

- provider/domain keyword 선택 규칙 하드코딩
- LLM MCP adapter에서 raw `execute_operation` 노출
- source operation과 별도의 실행 operation registry 구성
- 독립 vocabulary 제품 구성
- dashboard에 schema-language branding을 주요 navigation으로 노출
- Context Platform naming scheme과 맞지 않는 active route/runtime path 추가

## Context Platform Adapters

### admin_api

Dashboard와 운영자가 사용하는 control plane이다.

책임:

- source upload
- agent-ingestion queue/run 생성
- proposal 조회/리뷰
- catalog CRUD
- governance 상태 관리
- endpoint check evidence 조회

Planner runtime 전용 API는 여기에서 분리한다.

### planner_api

LLM MCP Adapter 또는 다른 runtime caller가 사용하는 runtime plane이다.

책임:

- approved catalog read
- capability search
- plan 생성
- plan validate
- validated plan execute

하지 말아야 할 일:

- source upload
- secret CRUD
- ingestion queue mutation
- proposal review
- catalog mutation

### dashboard

Dashboard는 두 흐름을 분리해서 보여준다.

- Overview: 현재 catalog와 governance 상태의 KPI
- Workbench: source intake, agent ingestion queue, proposal review 작업 화면

Workbench는 KPI 중심 화면이 아니라 source별 intake/review 화면이다. Semantic
draft 생성은 대시보드가 직접 하지 않고 agent response artifact와 proposal bundle
경계를 통과해야 한다.

권장 흐름:

```text
Upload
  -> Agent Ingestion Queue
  -> Proposal Bundle
  -> Validate
  -> Approve
  -> Publish
```

Agent가 만든 proposal bundle이 review 입력이 된다. 최종 승인은 proposal bundle
단위의 governance action으로 처리한다.

### worker

Worker는 Prefect 기반 ingestion 실행 경계다.

책임:

- MinIO에서 source document 읽기
- docling CPU-oriented parsing
- LangGraph 기반 단계 실행
- LLM extraction mode 적용
- concept/representation/resolution/capability proposal 생성
- API 문서 endpoint verification evidence 생성

Worker 환경값은 worker adapter의 env 파일에서 관리한다. Secret 값은
manifest, payload, proposal, log에 쓰지 않는다.

## apps/pubdata_mcp

`apps/pubdata_mcp`는 optional LLM MCP Adapter 역할을 할 수 있다.

책임:

- MCP transport 제공
- `context-platform-planner-api` 호출
- `plan_request`, `execute_plan`, `explain_plan` 같은 high-level tool 제공
- developer/debug 조회 tool 제공
- validated plan만 실행 요청

하지 말아야 할 일:

- canonical model definition 소유
- global capability ranking
- cross-domain planning
- proposal 생성
- catalog mutation
- provider/domain 선택 규칙 하드코딩
- raw source operation 직접 실행
- `execute_operation` tool 노출

권장 app 구조:

```text
apps/<app>/
  adapters/
    <adapter>/
      infra/
  domain/
    flows/
    tasks/
    steps/
    repositories/
    context/
    service/
  manifests/
```

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

`core/*`에는 app 이름, provider field, business rule, capability planning을
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
services/prefect
services/minio
services/context_platform
```

App-required service는 app/control plane이 선언할 때만 활성화한다.

## deploy

`deploy/compose/docker-compose.yml`은 generated file이다. 직접 수정하지 않고
manifest 변경 후 아래 명령으로 재생성한다.

```bash
python3 scripts/generate_compose.py
```

Nginx를 통해 접근하는 경로는 `/context-platform` 기준이다.

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

Context Platform 운영 명령:

```bash
python3 scripts/ops.py context-platform reset
python3 scripts/ops.py context-platform seed-registry
python3 scripts/ops.py context-platform ingest-source "<source-file-path>"
python3 scripts/ops.py context-platform ingest-queued-source "<run-id>"
```

`ingest-source`는 source path만 넘기면 기본 agent mode로 실행된다. agent
response artifact가 없으면 semantic 판단이 필요한 지점에서 request/evidence를
남기고 멈출 수 있다. 필요하면 metadata와 agent response를 추가로 넘긴다.

```bash
python3 scripts/ops.py context-platform ingest-source "<source-file-path>" \
  --name "<source-name>" \
  --provider "<provider-name>" \
  --agent-mode manual \
  --agent-response "<agent-response-json>"
```

`ingest-source`는 host에서 실행한다. 명령은 source file을 worker container로
전달하고, worker 내부에서 MinIO upload와 ingestion을 수행한다.
Dashboard Source Intake에서 업로드된 문서는 `ingest-queued-source <run-id>`로
worker ingestion을 실행한다.

## tests

권장 테스트 구조:

```text
tests/context_platform
tests/apps/pubdata_mcp
```

## tmp

`tmp/*`는 scratch/retired 영역이다. active runtime path로 import하지 않는다.
비밀이 아닌 임시 산출물, agent response artifact, 일회성 검증 파일은 사용자에게
다시 묻지 않고 생성, 수정, 검증, 삭제할 수 있다. Secret 값은 `tmp/*`에 두지
않는다.

## 설계 원칙

- Context Platform 구현은 `services/context_platform`
- provider execution과 app-specific business behavior는 `apps/*`
- generic runtime은 `core`
- source document upload는 MinIO
- background ingestion은 Prefect worker
- generated compose는 `deploy/compose`
- retired code는 `tmp/retired_apps` 또는 `tmp/retired_core`

LLM은 capability 선택, not-found 판단, canonical/binding/capability 제안을
담당한다. Runtime code는 도메인 키워드나 provider-specific 의미 판단을
하드코딩하지 않는다. Deterministic code는 승인된 contract의 validation,
transform, execution, normalization만 수행한다.
