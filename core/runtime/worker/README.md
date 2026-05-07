# Worker

`core/runtime/worker`는 앱 서비스들이 공유하는 generic worker runtime입니다.

운영 조회 API는 별도 [services/runtime_api](../../../services/runtime_api) 에서 제공합니다. health 조회는 worker queue task가 아니라 Redis heartbeat를 직접 읽는 방식입니다.
API 이미지와 의존성 정의는 [services/runtime_api/infra/image](../../../services/runtime_api/infra/image) 에 둡니다.
주기 enqueue와 앱 판단은 각 앱 서비스가 맡습니다.

## Structure
```
worker/
├── runtime/
│   ├── config.py               # 환경설정
│   ├── logger.py               # 로깅 설정
│   ├── queue.py                # Redis 큐 구현 연결
│   ├── task_loader.py          # app task 모듈 로딩
│   ├── dispatcher.py           # task → handler mapping
│   ├── executor.py             # 실행 orchestration
│   └── worker.py               # main loop / CLI entrypoint
└── README.md
```

task 메타데이터 원본은 `apps/*/manifests/tasks.json` 과 `services/*/manifests/tasks.json` 입니다.
[catalog.py](../task_runtime/catalog.py) 는 manifest를 읽는 로더입니다.
현재 catalog에는 queue 이름과 task별 실행 정책이 들어 있습니다.
payload validation도 여기서 선언합니다.

worker service 메타데이터 원본은 `services/*/manifests/workers/*.json` 입니다.
health/ops에서 사용하는 기본 queue 목록과 expected worker 수는 [service_catalog.py](../catalog/service_catalog.py) 가 이 manifest를 읽어서 제공합니다.

이미지/의존성은 runnable별 `infra/image` 아래에 둡니다.

- [Dockerfile](../../../services/ingest_api/infra/image/Dockerfile): `ingest:api`
- [Dockerfile](../../../services/ingest_file/infra/image/Dockerfile): `ingest:file`

task 정의는 워커 내부가 아니라 `apps/<app>/tasks/` 아래에 둡니다.
각 worker는 startup 시점에도 자기 queue에 속한 task 모듈만 import합니다.

예:
```text
project-root/
├── apps/
│   └── example/
│       ├── tasks/
│       └── manifests/
├── services/
│   ├── ingest_api/
│   ├── ingest_file/
│   └── runtime_api/
├── core/
│   └── runtime/
│       └── worker/
```

## Run
```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up --build
```

worker runtime 환경변수는 `deploy/compose/env/*.env` 에서 관리합니다.
현재 worker replica 기본값은 각 queue worker당 1개입니다.
로그는 기본적으로 stdout JSON과 함께 [logs](../../logs) 아래 날짜별 폴더에도 저장됩니다.

- [worker.common.env](../../deploy/compose/env/worker.common.env): 공통 설정
- [worker.ingest_api.env](../../deploy/compose/env/workers/worker.ingest_api.env): `ingest:api`
- [worker.ingest_file.env](../../deploy/compose/env/workers/worker.ingest_file.env): `ingest:file`

외부 producer에서 task 넣기:
```bash
cd /path/to/project-root
python3 scripts/ops.py enqueue ingest.api.fetch --payload '{"request":{"method":"GET","url":"http://runtime-api:8000/health/live"},"target":{"type":"postgres","schema":"public","table":"generic_api_ingest","create_table":true}}'
```

worker heartbeat 조회:
```bash
python3 scripts/ops.py workers
```

API health 조회:
```bash
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
curl http://localhost:8000/health
curl http://localhost:8000/health/workers
curl http://localhost:8000/checkpoints
```

`check_workers.py`는 worker별 `healthy | stale | down` 과 queue별 `healthy | degraded | down` 상태를 계산합니다.
기본 기준은 `healthy <= HEARTBEAT_INTERVAL * 2`, `stale <= HEARTBEAT_TTL`, 그 초과는 `down` 입니다.
종료 신호가 오면 worker는 새 task 수신을 멈추고, 실행 중 task는 `WORKER_SHUTDOWN_TIMEOUT_SECONDS` 동안만 마무리 대기합니다.

task 상태 조회:
```bash
python3 scripts/ops.py task <task_id>
```

manifest 정합성 검증:
```bash
python3 scripts/ops.py validate-config
```

DLQ 조회:
```bash
python3 scripts/ops.py dlq
```

DLQ 재큐잉:
```bash
python3 scripts/ops.py requeue-dlq ingest:api --count 1
python3 scripts/ops.py requeue-dlq ingest:api --task-id <task_id>
```

## Queue Contract
큐에는 `TaskMessage` JSON이 들어갑니다.

예시:
```json
{
  "queue_name": "ingest:api",
  "task_id": "uuid",
  "task_name": "ingest.api.fetch",
  "payload": {
    "request": {"method": "GET", "url": "https://example.com/items"},
    "target": {"type": "postgres", "schema": "raw", "table": "items"}
  },
  "attempts": 0,
  "enqueued_at": "2026-04-12T17:00:00+09:00"
}
```

Producer는 `task_name`을 enqueue하고, runtime catalog가 task manifest의 `queue` 참조를 queue manifest의 물리 `queue_name`으로 resolve합니다. worker는 env의 `WORKER_QUEUE_NAME`에 해당하는 물리 큐를 `BLPOP`으로 꺼내 실행합니다.

task와 queue 조합은 고정 매핑입니다.

- `ingest.api.fetch -> ingest:api`
- `ingest.file.download -> ingest:file`

잘못된 조합은 enqueue 시점과 worker 실행 시점 모두에서 거부합니다.
매핑과 실행 정책 원본은 `apps/*/manifests/tasks.json` 입니다.
payload도 enqueue 시점과 worker 실행 시점 모두에서 schema 검증합니다.
또한 각 worker는 자신이 담당하는 queue에 속한 task만 실행합니다.
예를 들어 `ingest-api-worker`는 `ingest:api` 큐에 등록된 task만 처리합니다.
현재는 URL/params/target을 payload로 받는 generic API fetch task만 여기에 포함됩니다.

## Task Status
task lifecycle 상태는 Redis에 저장됩니다.

- key pattern: `task:status:<task_id>`
- status flow: `queued -> running -> retrying -> succeeded|dead_lettered`
  종료 중 취소된 task는 `interrupted` 상태로 기록됩니다.

저장 항목에는 기본적으로 `service_name`, `queue_name`, `task_name`, `attempts`, `payload`, `enqueued_at`, `started_at`, `finished_at`, `result` 또는 `error`가 포함됩니다.
추가로 `worker_id`, `retry_count`, `duration_ms`, `policy_snapshot`도 함께 저장됩니다.

`service_name`은 manifest의 app/service 값에서 가져옵니다. 여러 서비스가 같은 runtime을 공유할 때 execution history를 서비스 단위로 구분하기 위한 generic metadata입니다.

공통 추적 필드는 선택적으로 사용할 수 있습니다.

- `dedupe_key`: 같은 service/task/dedupe key 중복 enqueue 방지
- `correlation_id`: 여러 task를 하나의 흐름으로 묶는 trace id
- `resource_key`: 앱 리소스 식별자. 예: external record id, graph run id, tool key

CLI 예:
```bash
python3 scripts/ops.py enqueue ingest.api.fetch \
  --payload '{"request":{"method":"GET","url":"http://runtime-api:8000/health/live"},"target":{"type":"postgres","schema":"public","table":"generic_api_ingest","create_table":true}}' \
  --dedupe-key manual-health-check \
  --correlation-id manual-run-001 \
  --resource-key example
```

runtime은 `task_execution_events`에 상태 전이를 append-only로 기록합니다. 최근 이벤트는 API로 조회할 수 있습니다.

```bash
curl 'http://localhost:8000/task-execution-events?service_name=example&limit=20'
```

긴 작업은 선택적으로 `TaskContext`를 함께 받을 수 있습니다. 기존처럼 `TaskMessage`만 받는 handler도 그대로 지원합니다.

```python
from core.runtime.task_runtime.context import TaskContext
from core.runtime.task_runtime.registry import task
from core.runtime.task_runtime.schemas import TaskMessage, TaskResult


@task("example.jobs.collect")
async def collect_example_jobs(message: TaskMessage, ctx: TaskContext) -> TaskResult:
    await ctx.event("step_started", details={"step": "fetch"})
    await ctx.heartbeat()
    return TaskResult(task_id=message.task_id, task_name=message.task_name, status="succeeded")
```

## Retry And DLQ
기본 재시도 정책은 다음과 같습니다.

- task별 `max_retries`, `retryable`, `backoff_seconds`, `timeout_seconds`, `dlq_enabled`는 `apps/*/manifests/tasks.json` 에서 정의
- `TASK_MAX_RETRIES=3`, `TASK_RETRY_DELAY_SECONDS=0`, `TASK_TIMEOUT_SECONDS=30` 은 fallback 기본값
- retryable 오류는 task별 backoff 뒤 같은 큐로 재큐잉
- 재시도 한도 초과 또는 non-retryable 오류는 DLQ로 이동
- `InvalidTaskRouteError`, `UnknownTaskRoutingError`, `UnknownTaskError`, `WorkerTaskNotAllowedError` 는 non-retryable

task에서 명시적으로 retry 판단을 주고 싶으면 공통 예외를 사용할 수 있습니다.

- `RetryableTaskError`: retry 대상
- `NonRetryableTaskError`: 즉시 terminal 처리 또는 DLQ
- `BlockedTaskError`: retryable 계열이며 `reason`, `blocked_until` 메타데이터를 담을 수 있음

DLQ 이름 규칙:

- `ingest:api:dlq`
- `ingest:file:dlq`

운영 스크립트:

- [check_dlq.py](../../scripts/check_dlq.py): 큐별 DLQ 크기와 메시지 미리보기 조회
- [requeue_dlq.py](../../scripts/requeue_dlq.py): DLQ에서 원래 큐로 재큐잉

`requeue_dlq.py`는 기본적으로 `attempts`를 `0`으로 초기화합니다.
기존 시도 횟수를 유지하려면 `--keep-attempts`를 사용합니다.
또한 task별 `dlq_requeue_limit`를 넘기면 재큐잉을 거부하고, `--force`로만 우회할 수 있습니다.
재큐잉 이력은 task status에 `dlq_requeue_count`, `dlq_requeue_history`로 저장됩니다.

환경 변수:
```bash
export TASK_MAX_RETRIES=3
export TASK_RETRY_DELAY_SECONDS=0
export TASK_TIMEOUT_SECONDS=30
export TASK_DLQ_SUFFIX=dlq
export WORKER_SHUTDOWN_TIMEOUT_SECONDS=30
export WORKER_LOG_TO_FILE=true
export WORKER_LOG_DIR=/app/logs
export APP_TIMEZONE=Asia/Seoul
```

파일 로그 경로 예시:

- `logs/2026-04-12/worker.ingest-api.log`
- `logs/2026-04-12/worker.ingest-file.log`

현재 기본 큐는 다음과 같습니다.

- `ingest:api`: 외부 API 수집 작업
- `ingest:file`: 파일 다운로드 작업

## Worker Health
각 worker는 Redis에 heartbeat를 주기적으로 기록합니다.

- key pattern: `worker:heartbeat:<queue_name>:<worker_id>`
- publish interval: `WORKER_HEARTBEAT_INTERVAL`
- TTL: `WORKER_HEARTBEAT_TTL`

health 조회는 worker heartbeat를 직접 읽는 방식으로 처리합니다.

dashboard queue report는 큐별 `size`, `dlq_size`와 함께 Redis queue head의 `oldest_age_seconds`를 제공합니다. queue size보다 오래 밀린 작업의 나이가 병목 판단에 더 직접적인 신호입니다.

queue 소비를 잠시 멈추거나 재개할 수 있습니다. pause는 worker consumption만 멈추며 enqueue 자체는 허용합니다.
앱 서비스는 queue pause 상태를 감지하면 해당 queue enqueue를 `queue_paused`로 skip할 수 있습니다.

```bash
python3 scripts/ops.py queue pause ingest:api --reason quota
python3 scripts/ops.py queue status ingest:api
python3 scripts/ops.py queue resume ingest:api
```

## Docker Compose
`deploy/compose/docker-compose.yml`은 생성 파일입니다.
worker 서비스 원본은 `services/*/manifests/workers/*.json` 입니다.
platform/runtime 서비스 원본은 `core/**/manifests` 및 `services/*/manifests` 아래 manifest입니다.
app service 원본은 `apps/*/manifests/services/*.json` 입니다.
앱 주기 작업은 각 앱 서비스 내부 loop에서 실행됩니다.

compose 재생성:
```bash
python3 scripts/generate_compose.py
```

현재 compose에는 다음 서비스가 있습니다.

- `runtime-api`: worker heartbeat와 queue 상태를 조회하는 FastAPI 서비스
- `postgres`: runtime checkpoint와 execution history를 저장하는 Postgres 서비스
- app service: 앱 판단 후 generic ingest task를 enqueue하는 producer
- `ingest-api-worker`: `ingest:api` 큐를 소비하는 consumer
- `ingest-file-worker`: `ingest:file` 큐를 소비하는 consumer
- `extract-text-worker`: `extract:text` 큐를 소비하는 consumer
- `llm-worker`: `llm:text` 큐를 소비하는 consumer

## Add A Worker Service
새로운 task service는 새로운 worker 컨테이너에서 실행하는 구조입니다.
즉 queue 하나가 worker service 하나에 대응합니다.

절차:

1. `services/<capability>/manifests/queues.json` 에 queue manifest 추가
2. `services/<capability>/app/tasks/...` 아래에 generic task 구현 추가
3. `services/<capability>/manifests/tasks.json` 에 task manifest 추가
4. `services/<capability>/manifests/workers/<service>_worker.json` 에 worker service manifest 추가
5. 필요 시 `services/*/infra/image` 또는 `core/**/manifests` 와 `deploy/compose/env` 에 service 전용 정의 추가
6. compose 재생성

```bash
python3 scripts/generate_compose.py
```

기본 replica 수는 모두 `1`입니다.
일반 `docker compose up` 기준으로도 현재 서비스 정의는 각 worker 컨테이너를 1개씩만 기동하는 구조입니다.
또한 `runtime-api`, app service, 각 worker는 모두 `restart: unless-stopped` 정책으로 자동 재기동됩니다.

실행:
```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up --build
```

## Current Runtime Capabilities

- manifest 기반 task routing과 payload validation
- Redis queue consume
- worker heartbeat
- task status TTL
- Postgres `task_executions` snapshot 기록
- Postgres `task_execution_events` append-only event log
- task별 retry/DLQ 정책
- DLQ replay limit
- optional `dedupe_key`, `correlation_id`, `resource_key`
- optional `TaskContext` event/heartbeat helper
- queue pause/resume
- app service queue pause skip
- running task lease heartbeat와 만료 정리
- retryable/non-retryable task exception base classes
