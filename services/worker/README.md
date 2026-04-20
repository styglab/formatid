# Worker

`services/worker`는 PPS 도메인 서비스가 올라가는 공통 generic worker runtime입니다.

운영 조회 API는 별도 [services/api](/home/user/projects/formatid/services/api) 에서 제공합니다. health 조회는 worker queue task가 아니라 Redis heartbeat를 직접 읽는 방식입니다.
API 이미지와 의존성 정의는 [infra/images/api](/home/user/projects/formatid/infra/images/api) 에 둡니다.
주기 enqueue는 별도 [services/scheduler](/home/user/projects/formatid/services/scheduler) 가 맡습니다.

## Structure
```
worker/
├── runtime/
│   ├── config.py               # 환경설정
│   ├── logger.py               # 로깅 설정
│   ├── queue.py                # Redis 큐 구현 연결
│   ├── task_loader.py          # 루트 tasks 모듈 로딩
│   ├── dispatcher.py           # task → handler mapping
│   ├── executor.py             # 실행 orchestration
│   └── worker.py               # main loop / CLI entrypoint
└── README.md
```

공용 task 메타데이터 원본은 [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 입니다.
[catalog.py](/home/user/projects/formatid/shared/tasking/catalog.py) 는 이 manifest를 읽는 로더입니다.
현재 catalog에는 queue 이름과 task별 실행 정책이 들어 있습니다.
payload validation도 여기서 선언합니다.

worker service 메타데이터 원본은 [infra/worker_services](/home/user/projects/formatid/infra/worker_services) 아래 JSON manifest입니다.
health/ops에서 사용하는 기본 queue 목록과 expected worker 수는 [service_catalog.py](/home/user/projects/formatid/shared/service_catalog.py) 가 이 manifest를 읽어서 제공합니다.

이미지/의존성은 [infra/images](/home/user/projects/formatid/infra/images) 아래 worker family별로 분리합니다.

- [Dockerfile](/home/user/projects/formatid/infra/images/pps_bid/Dockerfile): `pps:bid`
- [Dockerfile](/home/user/projects/formatid/infra/images/pps_attachment/Dockerfile): `pps:attachment`

task 정의는 워커 내부가 아니라 저장소 루트의 `tasks/` 아래에 둡니다.
각 worker는 startup 시점에도 자기 queue에 속한 task 모듈만 import합니다.

예:
```text
project-root/
├── tasks/
│   └── pps/
│       └── ...
├── services/
│   ├── api/
│   └── worker/
```

## Run
```bash
docker compose -f infra/docker-compose.yml up --build
```

worker runtime 환경변수는 `infra/env/*.env` 에서 관리합니다.
현재 worker replica 기본값은 각 queue worker당 1개입니다.
로그는 기본적으로 stdout JSON과 함께 [logs](/home/user/projects/formatid/logs) 아래 날짜별 폴더에도 저장됩니다.

- [worker.common.env](/home/user/projects/formatid/infra/env/worker.common.env): 공통 설정
- [worker.pps-bid.env](/home/user/projects/formatid/infra/env/worker.pps-bid.env): `pps:bid`
- [worker.pps-attachment.env](/home/user/projects/formatid/infra/env/worker.pps-attachment.env): `pps:attachment`

외부 producer에서 task 넣기:
```bash
cd /path/to/project-root
python3 scripts/ops.py enqueue pps:bid pps.bid.collect --payload '{"source":"cli"}'
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

Docker Compose 통합 스모크 테스트:
```bash
python3 scripts/ops.py smoke
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
python3 scripts/ops.py requeue-dlq pps:bid --count 1
python3 scripts/ops.py requeue-dlq pps:bid --task-id <task_id>
```

## Queue Contract
큐에는 `TaskMessage` JSON이 들어갑니다.

예시:
```json
{
  "queue_name": "pps:bid",
  "task_id": "uuid",
  "task_name": "pps.bid.collect",
  "payload": {
    "source": "cli"
  },
  "attempts": 0,
  "enqueued_at": "2026-04-12T17:00:00+09:00"
}
```

즉 producer는 대상 큐 이름(`pps:bid`, `pps:attachment`)에 이 JSON을 `RPUSH` 하면 되고, worker는 자신이 맡은 큐를 `BLPOP`으로 꺼내 실행합니다.

task와 queue 조합은 고정 매핑입니다.

- `pps.bid.collect -> pps:bid`
- `pps.attachment.download -> pps:attachment`

잘못된 조합은 enqueue 시점과 worker 실행 시점 모두에서 거부합니다.
매핑과 실행 정책 원본은 [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 입니다.
payload도 enqueue 시점과 worker 실행 시점 모두에서 schema 검증합니다.
또한 각 worker는 자신이 담당하는 queue에 속한 task만 실행합니다.
예를 들어 `pps-bid-worker`는 `pps.bid.*` task만 처리합니다.
그리고 import 단계에서도 `tasks.pps.bid.*`만 로드합니다.

## Task Status
task lifecycle 상태는 Redis에 저장됩니다.

- key pattern: `task:status:<task_id>`
- status flow: `queued -> running -> retrying -> succeeded|dead_lettered`
  종료 중 취소된 task는 `interrupted` 상태로 기록됩니다.

저장 항목에는 기본적으로 `queue_name`, `task_name`, `attempts`, `payload`, `enqueued_at`, `started_at`, `finished_at`, `result` 또는 `error`가 포함됩니다.
추가로 `worker_id`, `retry_count`, `duration_ms`, `policy_snapshot`도 함께 저장됩니다.

## Retry And DLQ
기본 재시도 정책은 다음과 같습니다.

- task별 `max_retries`, `retryable`, `backoff_seconds`, `timeout_seconds`, `dlq_enabled`는 [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 에서 정의
- `TASK_MAX_RETRIES=3`, `TASK_RETRY_DELAY_SECONDS=0`, `TASK_TIMEOUT_SECONDS=30` 은 fallback 기본값
- retryable 오류는 task별 backoff 뒤 같은 큐로 재큐잉
- 재시도 한도 초과 또는 non-retryable 오류는 DLQ로 이동
- `InvalidTaskRouteError`, `UnknownTaskRoutingError`, `UnknownTaskError`, `WorkerTaskNotAllowedError` 는 non-retryable

DLQ 이름 규칙:

- `pps:bid:dlq`
- `pps:attachment:dlq`

운영 스크립트:

- [check_dlq.py](/home/user/projects/formatid/scripts/check_dlq.py): 큐별 DLQ 크기와 메시지 미리보기 조회
- [requeue_dlq.py](/home/user/projects/formatid/scripts/requeue_dlq.py): DLQ에서 원래 큐로 재큐잉

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

- `logs/2026-04-12/worker.pps-bid.log`
- `logs/2026-04-12/worker.pps-attachment.log`

현재 기본 큐는 다음과 같습니다.

- `pps:bid`: PPS bid 관련 작업
- `pps:attachment`: PPS attachment 관련 작업

## Worker Health
각 worker는 Redis에 heartbeat를 주기적으로 기록합니다.

- key pattern: `worker:heartbeat:<queue_name>:<worker_id>`
- publish interval: `WORKER_HEARTBEAT_INTERVAL`
- TTL: `WORKER_HEARTBEAT_TTL`

health 조회는 worker heartbeat를 직접 읽는 방식으로 처리합니다.

## Docker Compose
`infra/docker-compose.yml`은 생성 파일입니다.
worker 서비스 원본은 [infra/worker_services](/home/user/projects/formatid/infra/worker_services) 아래 manifest입니다.
platform 서비스 원본은 [infra/platform_services](/home/user/projects/formatid/infra/platform_services) 아래 manifest입니다.
scheduler 주기 작업 원본은 [infra/schedules](/home/user/projects/formatid/infra/schedules) 입니다.

compose 재생성:
```bash
python3 scripts/generate_compose.py
```

현재 compose에는 다음 서비스가 있습니다.

- `api`: worker heartbeat와 queue 상태를 조회하는 FastAPI 서비스
- `postgres`: scheduler checkpoint를 저장하는 Postgres 서비스
- `scheduler`: schedule manifest를 읽어 주기적으로 task를 enqueue하는 producer
- `pps-bid-worker`: `pps:bid` 큐를 소비하는 consumer
- `pps-attachment-worker`: `pps:attachment` 큐를 소비하는 consumer

## Add A Worker Service
새로운 task service는 새로운 worker 컨테이너에서 실행하는 구조입니다.
즉 queue 하나가 worker service 하나에 대응합니다.

절차:

1. `tasks/...` 아래에 task 구현 추가
2. [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 에 task manifest 추가
3. [infra/worker_services](/home/user/projects/formatid/infra/worker_services) 에 worker service manifest 추가
4. 필요 시 [infra/images](/home/user/projects/formatid/infra/images) 와 [infra/env](/home/user/projects/formatid/infra/env) 에 service 전용 정의 추가
5. compose 재생성

```bash
python3 scripts/generate_compose.py
```

기본 replica 수는 모두 `1`입니다.
일반 `docker compose up` 기준으로도 현재 서비스 정의는 각 worker 컨테이너를 1개씩만 기동하는 구조입니다.
또한 `api`, `scheduler`, 각 worker는 모두 `restart: unless-stopped` 정책으로 자동 재기동됩니다.

실행:
```bash
docker compose -f infra/docker-compose.yml up --build
```

## Next
- 실행 결과를 DB 또는 task registry에 기록
- API에서 enqueue 스크립트와 같은 Redis producer 경로 추가
