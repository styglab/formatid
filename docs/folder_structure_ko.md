# 폴더 구조 설명

이 저장소는 공공 API 문서를 단순히 저장하거나 API wrapper를 만드는
프로젝트가 아니다. 목표는 API 문서를 LLM이 추론 가능한 의미 구조로
변환하고, MCP 실행 런타임이 승인된 실행 계약만 호출하는
Semantic Agentic Data Platform이다.

큰 흐름은 다음과 같다.

```text
Raw API documents
  -> semantic capability graph
  -> LLM execution planning
  -> MCP execution runtime
  -> provider API calls
  -> semantic normalization / integration
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
  sources/
  tests/
  tmp/
```

## apps/

앱은 특정 실행 표면이나 도메인 런타임을 소유한다.

현재 active app은 `apps/pubdata_mcp` 하나다.

```text
apps/pubdata_mcp/
  app/
    common/
    main.py
  specs/
  infra/
  manifests/
```

`apps/pubdata_mcp`의 책임:

- MCP transport 제공
- semantic query tool 제공
- 승인된 operation contract 실행
- provider HTTP 호출
- 인증, retry, pagination, response parsing
- provider response를 semantic result로 정규화

하지 말아야 할 일:

- global capability ranking
- cross-domain planning
- proposal 생성
- semantic catalog mutation
- provider 선택 규칙 하드코딩

즉, `pubdata_mcp`는 의미를 판단하는 곳이 아니라 승인된 계획을 실행하는
곳이다.

## services/

서비스는 플랫폼 공통 기능과 control plane을 제공한다.

주요 서비스:

```text
services/postgres
services/redis
services/nginx
services/platform_api
services/platform_dashboard
services/embedding
services/prefect
services/semantic_platform
```

`services/semantic_platform`은 이 저장소의 의미 계층 control plane이다.
`apps/`로 두지 않는 이유는 특정 앱이 아니라 여러 MCP/RAG/domain app이
공유해야 하는 semantic source of truth이기 때문이다.

`services/semantic_platform`의 책임:

- source document ingestion
- proposal/review workflow
- capability catalog
- semantic entity registry
- field/semantic type registry
- operation/resource/contract/variant catalog
- capability embedding/vector index
- LLM execution planner
- semantic join/dependency graph
- dashboard/API/worker

하지 말아야 할 일:

- provider HTTP client 구현
- API key handling
- provider pagination loop
- provider-specific execution branch

provider 실행은 `apps/pubdata_mcp`가 담당한다.

내부 경계는 다음처럼 고정한다.

```text
api/        HTTP API 경계만 담당
dashboard/  UI만 담당
ingestion/  source document -> evidence/proposal/apply
planner/    question -> semantic execution plan
runtime/    planner/runtime context helper
storage/    Postgres schema/repository
worker/     optional Prefect background/manual runner
```

경계 규칙:

- `api/`는 세부 catalog mutation을 직접 구현하지 않고 repository/domain
  API를 호출한다.
- `dashboard/`는 provider/domain routing 규칙을 갖지 않는다.
- `ingestion/`은 evidence 수집을 위한 endpoint probe는 할 수 있지만,
  provider execution runtime이 되면 안 된다.
- `planner/`는 provider API를 호출하지 않는다.
- `storage/`는 semantic/domain 추론을 하지 않는다.
- `worker/`는 ingestion을 감싸는 optional runner이며 별도 ingestion
  구현을 갖지 않는다.

## core/

`core`는 도메인 없는 공통 코드만 둔다.

현재 active 구조:

```text
core/catalog
core/contracts
core/observability
core/runtime
  app_service
  mcp
  runtime_db
```

### core/catalog

manifest 기반 catalog loader다.

사용처:

- compose 생성
- config validation
- platform dashboard service 목록

`services/*/manifests`와 `apps/*/manifests`를 읽어 active service와 app
service를 계산한다.

### core/contracts

앱과 서비스가 공유하는 작은 계약을 둔다.

현재는 execution identity가 핵심이다.

- `request_id`
- `correlation_id`
- `run_id`
- `resource_key`
- `session_id`

### core/runtime/mcp

YAML tool spec을 읽어서 FastMCP tool로 등록하는 공통 런타임이다.

현재 `apps/pubdata_mcp`가 사용한다.

역할:

- `apps/pubdata_mcp/specs/*.yaml` 로딩
- handler import
- tool input signature 생성
- FastMCP tool 등록

MCP tool 정의를 코드에 직접 하드코딩하지 않고 spec 기반으로 유지하기
위한 최소 공통 계층이다.

### core/observability

공통 로그, correlation, retention helper다.

`platform_api`와 `core/runtime`이 사용한다.

### core/runtime

공통 runtime helper다.

- 시간 처리
- Postgres 연결/URL
- checkpoint store
- service request/event/run store
- app service middleware

## sources/

원본 API 문서를 둔다.

문서는 그대로 실행 대상으로 쓰지 않는다. ingestion graph가 문서를 읽고
LLM이 의미 구조를 제안한 뒤, review/apply를 통해 catalog에 반영한다.

## data/

로컬 runtime data와 모델 파일을 둔다.

현재 중요한 경로:

```text
data/models/embeddings/
data/postgres/
data/prefect/
```

embedding model은 컨테이너에 복사하지 않고 volume mount로 연결하는
방향이다.

## deploy/

compose와 nginx route 같은 배포 산출물을 둔다.

`deploy/compose/docker-compose.yml`은 generated file이다.
직접 수정하지 말고 아래 명령으로 생성한다.

```bash
python3 scripts/generate_compose.py
```

## scripts/

외부에서 직접 호출하는 entrypoint는 두 개만 사용한다.

```bash
python3 scripts/generate_compose.py
python3 scripts/ops.py <command>
```

`scripts/ops/*`는 `scripts/ops.py`의 구현 모듈이다.
CI나 shell에서 직접 호출하지 않는다.

## tests/

현재 active 테스트는 두 영역이다.

```text
tests/semantic_platform
tests/apps/pubdata_mcp
```

`semantic_platform` 테스트는 catalog, ingestion contract, planner validation을
검증한다.

`pubdata_mcp` 테스트는 executor, argument validation, response
normalization, secret redaction, MCP top-level schema를 검증한다.

## tmp/

퇴역 코드와 임시 산출물을 둔다.

현재 보존된 퇴역 경로:

```text
tmp/retired_apps/
tmp/retired_core/
```

`tmp` 아래 코드는 active runtime path가 아니다. 참고용으로만 본다.

## 설계 원칙

- semantic planning은 `services/semantic_platform`
- provider execution은 `apps/pubdata_mcp`
- generic runtime은 `core`
- source documents는 `sources`
- local models/runtime data는 `data`
- generated compose는 `deploy/compose`
- retired code는 `tmp`

LLM은 capability 선택, not-found 판단, execution DAG 생성을 담당한다.
runtime code는 도메인 키워드나 provider-specific 의미 판단을 하드코딩하지
않는다. deterministic code는 승인된 contract의 validation, transform,
execution, normalization만 수행한다.
