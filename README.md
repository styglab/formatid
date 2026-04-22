## Project Overview

이 프로젝트는 Redis 기반 generic worker runtime과 app service runtime을 분리해서 운영합니다.

- Execution Layer: queue consume, retry/DLQ, heartbeat, execution history, catalog, observability
- Pipeline Layer: reusable worker tasks (`ingest`, `extract`, `serve`)
- Application Layer: app-specific orchestration under `apps/*`

앱별 상세 문서는 각 앱 폴더에서 관리합니다.

- [G2B Ingest](apps/g2b_ingest/README.md)
- [G2B Summary](apps/g2b_summary/README.md)

## Run

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml up -d --build
```

종료:

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml down
```

로그 확인:

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f api
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f ingest-api-worker
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f extract-text-worker
```

## Services

Compose 서비스는 manifest에서 생성합니다.

- `redis`: queue, heartbeat, task status, DLQ 저장
- `postgres`: 내부 checkpoint와 runtime observability 저장
- `api`: health/checkpoint/observability/dashboard 조회 API
- `dashboard`: runtime 상태 모니터링 UI
- `ingest-api-worker`: `ingest:api` 큐 소비
- `ingest-file-worker`: `ingest:file` 큐 소비
- `extract-text-worker`: `extract:text` 큐 소비
- `serve-llm-worker`: `serve:llm` 큐 소비
- app services: `apps/*/manifests/services/*.json`에 정의

Manifest 원본:

- `infra/platform_services/*.json`
- `apps/*/manifests/app.json`
- `apps/*/manifests/services/*.json`
- `apps/*/manifests/tasks.json`
- `services/*/manifests/*worker.json`
- `services/*/manifests/tasks.json`

`infra/docker-compose.yml`은 생성 파일입니다. manifest를 수정한 뒤 compose를 다시 만들려면:

```bash
python3 scripts/generate_compose.py
```

## Environment

공통 env 파일은 `infra/env` 아래에 있습니다.

- `compose.env`: host port와 로컬 bind mount 경로
- `postgres.env`: 내부 compose Postgres 접속 정보
- `worker.common.env`: worker 공통 설정
- `infra/env/workers/*.env`: worker별 queue/env 설정
- `api.env`: API 설정

앱별 env는 `apps/<app>/infra/env`에서 관리합니다.

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
python3 scripts/ops.py enqueue ingest:api ingest.api.fetch --payload '{
  "request": {
    "method": "GET",
    "url": "http://api:8000/health/live"
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
python3 scripts/ops.py enqueue ingest:file ingest.file.download --payload '{
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
- worker manifest: `services/*/manifests/*worker.json`

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

- `apps/<app>/README.md`
- `apps/<app>/service`: 앱 오케스트레이터 또는 API
- `apps/<app>/tasks`: 앱-specific 저장소, 판단, schema, helper
- `apps/<app>/manifests/app.json`: 앱 메타데이터와 dashboard/env 연결
- `apps/<app>/manifests/services/<service>.json`: 앱 서비스 컨테이너 정의
- `apps/<app>/manifests/tasks.json`: 앱 전용 worker task가 필요할 때만 사용
- `apps/<app>/infra/env`: 앱별 env
- `apps/<app>/infra/images`: 앱 서비스 이미지

앱 서비스 manifest 예:

```json
{
  "service_name": "example-api",
  "service_type": "api",
  "dockerfile": "apps/example/infra/images/api/Dockerfile",
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
python3 scripts/ops.py enqueue ingest:api ingest.api.fetch \
  --payload '{"request":{"method":"GET","url":"http://api:8000/health/live"},"target":{"type":"postgres","schema":"public","table":"generic_api_ingest","create_table":true}}' \
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

## Paths

```text
infra/
  docker-compose.yml
  env/
  platform_services/
  postgres/init/
apps/
  g2b_ingest/
  g2b_summary/
services/
  api/
  app_service/
  catalog/
  dashboard/
  extract/
  ingest/
  llm/
  observability/
  runtime_db/
  task_runtime/
  worker/
shared/
  checkpoints/
  queue/
  tasking/
  postgres_url.py
  time.py
```
