## Project Overview

이 프로젝트는 Redis 기반 generic worker runtime과 app service runtime을 분리해서 운영합니다.

- Execution Layer: queue consume, retry/DLQ, heartbeat, execution history, catalog, observability
- Service Layer: reusable worker tasks (`ingest`, `extract`, `serve`) and platform-facing services (`runtime-api`, `runtime-dashboard`)
- Application Layer: app-specific orchestration under `apps/*`

앱별 상세 문서는 각 앱 컨테이너 폴더에서 관리합니다.

- [G2B API](apps/g2b/api/README.md)
- [G2B Pipeline Scheduler](apps/g2b/pipeline_scheduler/README.md)
- [G2B Pipeline Worker](apps/g2b/pipeline_worker/README.md)

## Run

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up -d --build
```

종료:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml down
```

로그 확인:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f runtime-api
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f runtime-dashboard
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f g2b-api
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f g2b-pipeline-scheduler
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f g2b-pipeline-worker
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f ingest-api-worker
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f ingest-file-worker
```

## Services

Compose 서비스는 manifest에서 생성합니다.

- `redis`: queue, heartbeat, task status, DLQ 저장
- `postgres`: 내부 checkpoint와 runtime observability 저장
- `runtime-api`: health/checkpoint/observability/dashboard 조회 API
- `runtime-dashboard`: runtime 상태 모니터링 UI
- `g2b-pipeline-scheduler`: G2B scheduled ingest graph 실행
- `g2b-pipeline-worker`: G2B triggered document graph 실행
- `ingest-api-worker`: `ingest:api` 큐 소비
- `ingest-file-worker`: `ingest:file` 큐 소비
- app services: `apps/**/manifests/services/*.json`에 정의

활성 platform service와 worker는 `apps/**/manifests/app.json`의 `requires`에서 선택합니다.
`postgres`, `redis`, `runtime-api`, `runtime-dashboard`는 기본 platform service입니다.

새 앱이나 앱 컨테이너를 만들 때는 먼저 `apps/<app>/<container>/manifests/app.json`에 필요한 실행 의존성을 선언합니다.

```json
{
  "app": "example.pipeline",
  "description": "Example pipeline app",
  "requires": {
    "workers": [
      "ingest-api-worker",
      "ingest-file-worker"
    ],
    "platform_services": [
      "qdrant"
    ]
  }
}
```

- `requires.workers`: 이 앱 때문에 compose에 포함되어야 하는 worker service 이름입니다.
- `requires.platform_services`: 기본 platform service 외에 추가로 필요한 platform service 이름입니다.
- 기본 platform service는 `postgres`, `redis`, `runtime-api`, `runtime-dashboard`이며 별도로 선언하지 않아도 포함됩니다.
- `requires`를 수정한 뒤에는 `python3 scripts/generate_compose.py`와 `python3 scripts/ops.py validate-config`를 실행합니다.

사용 가능한 이름은 manifest catalog에서 확인합니다.

```bash
python3 scripts/ops.py catalog
```

원본 파일 위치:

- platform/runtime services: `core/**/manifests/*.json`, `services/*/manifests/*.json`
- worker services: `services/*/manifests/workers/*.json`

Manifest 원본:

- `core/**/manifests/*.json`
- `services/*/manifests/*.json`
- `services/*/manifests/workers/*.json`
- `apps/**/manifests/app.json`
- `apps/**/manifests/services/*.json`
- `apps/**/manifests/tasks.json`
- `services/*/manifests/tasks.json`

`deploy/compose/docker-compose.yml`은 생성 파일입니다. manifest를 수정한 뒤 compose를 다시 만들려면:

```bash
python3 scripts/generate_compose.py
```

생성 파일 drift만 확인하려면:

```bash
python3 scripts/generate_compose.py --check
```

구조 검증, boundary lint, Python compile, compose config를 한 번에 확인하려면:

```bash
python3 scripts/ops.py check-all
```

## Environment

공통 env 파일은 `deploy/compose/env` 아래에 있습니다.

- `compose.env`: host port와 로컬 bind mount 경로
- `postgres.env`: 내부 compose Postgres 접속 정보
- `worker.common.env`: worker 공통 설정
- `deploy/compose/env/workers/*.env`: worker별 queue/env 설정
- `runtime_api.env`: runtime API 설정

앱별 env는 runnable 단위의 `apps/<app>/<api|pipeline>/infra/env`에서 관리합니다.

내부 Postgres 기본값:

```env
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

서비스 내부 checkpoint 접속 URL은 `POSTGRES_*` 값으로 조립합니다. `CHECKPOINT_DATABASE_URL`을 직접 지정하면 그 값을 우선 사용합니다.

## Generic Pipeline Tasks

Generic worker는 app-specific 판단을 하지 않습니다. 앱 서비스가 source/target/metadata를 payload로 구성해서 enqueue하고, worker는 선언된 작업만 실행합니다.

현재 worker task:

- `ingest.api.fetch -> ingest:api`
- `ingest.file.download -> ingest:file`
- `extract.text.run -> extract:text`
- `serve.llm.generate -> serve:llm`

API fetch 후 Postgres ingest 테이블에 저장:

```bash
python3 scripts/ops.py enqueue ingest.api.fetch --payload '{
  "request": {
    "method": "GET",
    "url": "http://runtime-api:8000/health/live"
  },
  "target": {
    "type": "postgres",
    "database_url_env": "POSTGRES_DATABASE_URL",
    "schema": "public",
    "table": "generic_api_ingest",
    "mode": "append",
    "create_table": true
  },
  "metadata": {
    "source": "manual"
  }
}'
```

파일 다운로드 후 S3/MinIO에 저장:

```bash
python3 scripts/ops.py enqueue ingest.file.download --payload '{
  "source": {
    "url": "https://example.com/sample.pdf",
    "filename": "sample.pdf"
  },
  "target": {
    "type": "s3",
    "endpoint_env": "S3_ENDPOINT",
    "access_key_env": "S3_ACCESS_KEY",
    "secret_key_env": "S3_SECRET_KEY",
    "bucket_env": "S3_BUCKET",
    "secure_env": "S3_SECURE",
    "object_key": "generic/sample.pdf",
    "content_type": "application/pdf"
  }
}'
```

## App Service Runtime

App service runtime은 실행 방식에 종속되지 않는 공통 lifecycle만 제공합니다.

- `AppServiceRuntime`: logging, signal handling, service heartbeat, graceful shutdown
- `CronServiceRunner`: APScheduler cron job adapter, Redis lock, 실행 duration/error 기록
- `ApiServiceRuntime`: API형 서비스 startup/shutdown과 request/event store helper
- `ServiceRequestMiddleware`: FastAPI 요청 duration/status/error 기록
- `ServiceRunStore`: cron/batch 실행 기록
- `ServiceRequestStore`: API형 서비스 요청 기록
- `ServiceEventStore`: cron/API/consumer 공통 이벤트 기록

앱 서비스 타입은 manifest의 `service_type`으로 구분합니다.

- `cron`: 주기 배치 서비스
- `api`: HTTP/SSE API 서비스
- `consumer`: queue/stream consumer 서비스
- `service`: 아직 타입을 세분화하지 않은 일반 앱 서비스

## Catalog

Task와 service 정의는 manifest를 source of truth로 사용합니다.

- task catalog: `apps/*/manifests/tasks.json`, `services/*/manifests/tasks.json`
- app metadata: `apps/*/manifests/app.json`
- app service manifest: `apps/*/manifests/services/*.json`
- worker manifest: `services/*/manifests/workers/*.json`

각 task는 catalog에서 다음 정책을 가집니다.

- `service_name`
- `queue_name`
- `payload_schema`
- `max_retries`
- `retryable`
- `backoff_seconds`
- `timeout_seconds`
- `dlq_enabled`
- `dlq_requeue_limit`
- `dlq_requeue_keep_attempts`

worker는 자신이 담당하는 queue의 task만 실행합니다.

## Add An App

새 앱은 `apps/<app>` 아래에 둡니다.

기본 구성:

- `apps/<app>/api`: runnable API app
- `apps/<app>/pipeline`: runnable pipeline app
- `apps/<app>/<runtime>/app`: 앱 오케스트레이터, API, graph, steps
- `apps/<app>/<runtime>/manifests/app.json`: 앱 메타데이터와 dashboard/env 연결
- `apps/<app>/<runtime>/manifests/services/<service>.json`: 앱 서비스 컨테이너 정의
- `apps/<app>/<runtime>/infra/env`: 앱별 env
- `apps/<app>/<runtime>/infra/image`: 앱 서비스 이미지

앱 서비스 manifest 예:

```json
{
  "service_name": "example-api",
  "service_type": "api",
  "dockerfile": "apps/example/infra/image/Dockerfile",
  "ports": ["8010:8000"]
}
```

새 서비스를 추가한 뒤에는 compose를 재생성하고 설정을 검증합니다.

```bash
python3 scripts/generate_compose.py
python3 -c 'import json; from scripts.ops.validation import validate_config; print(json.dumps(validate_config(), ensure_ascii=False, indent=2))'
```

## Runtime Data

Redis에 저장되는 데이터:

- queue: `ingest:api`, `ingest:file`, `extract:text`, `serve:llm`
- DLQ: `<queue_name>:dlq`
- worker heartbeat: `worker:heartbeat:<queue_name>:<worker_id>`
- app service heartbeat: `service:heartbeat:<service_id>`
- task status: `task:status:<task_id>`
- dedupe key: `task:dedupe:<service_name>:<task_name>:<dedupe_key>`
- queue pause: `task:queue_pause:<queue_name>`

내부 Postgres observability 데이터:

- `checkpoints`
- `service_runs`
- `service_requests`
- `service_events`
- `task_executions`
- `task_execution_events`
- `external_api_quota_blocks`

`task_executions`는 task별 최신 snapshot이고, `task_execution_events`는 상태 전이 append-only 로그입니다.

## Operations

API:

```bash
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
curl http://localhost:8000/health
curl http://localhost:8000/health/workers
curl http://localhost:8000/health/app-services
curl http://localhost:8000/checkpoints
curl 'http://localhost:8000/observability/service-runs?limit=20'
curl 'http://localhost:8000/observability/service-requests?limit=20'
curl 'http://localhost:8000/observability/service-events?limit=20'
curl 'http://localhost:8000/observability/task-executions?limit=20'
curl 'http://localhost:8000/observability/task-execution-events?limit=20'
curl http://localhost:8000/dashboard/summary
curl http://localhost:8000/dashboard/service-runs
curl http://localhost:8000/dashboard/apps
```

React dashboard:

```text
http://localhost:8080
```

CLI:

```bash
python3 scripts/ops.py workers
python3 scripts/ops.py task <task_id>
python3 scripts/ops.py checkpoints
python3 scripts/ops.py dlq
python3 scripts/ops.py requeue-dlq ingest:api --count 1
python3 scripts/ops.py queue pause ingest:api --reason maintenance
python3 scripts/ops.py queue status ingest:api
python3 scripts/ops.py queue resume ingest:api
python3 scripts/ops.py prune-observability
python3 scripts/ops.py validate-config
```

수동 enqueue 예:

```bash
python3 scripts/ops.py enqueue ingest.api.fetch \
  --payload '{"request":{"method":"GET","url":"http://runtime-api:8000/health/live"},"target":{"type":"postgres","schema":"public","table":"generic_api_ingest","create_table":true}}' \
  --dedupe-key manual-health-check \
  --correlation-id manual-run-001 \
  --resource-key manual-health-check
```

## Development Rules

- generic runtime에는 앱 로직을 넣지 않습니다.
- 앱 로직은 `apps/<app>` 아래에 둡니다.
- pipeline worker는 task chaining이나 앱 상태 lifecycle을 직접 처리하지 않습니다.
- 새 task는 manifest에 등록합니다.
- blocking I/O는 worker task에서 피하고 가능한 async I/O를 사용합니다.
- worker payload의 `*_env` 필드는 env var 이름만 참조합니다. secret 값 자체를 payload에 넣지 않습니다.
- `services/*`는 앱별 env 이름을 하드코딩하지 않고, 앱이 payload/manifest로 선언한 env-name contract를 처리합니다.

## Paths

```text
core/
  backing/
    postgres/init/
    redis/
  catalog/
  observability/
  runtime/
    app_service/
    runtime_db/
    task_runtime/
    worker/
services/
  runtime_api/
  runtime_dashboard/
  qdrant/
deploy/
  compose/
    docker-compose.yml
    env/
apps/
  g2b/
    api/
    pipeline_scheduler/
    pipeline_worker/
services/
  extract/
  ingest_api/
  ingest_file/
  llm/
```
