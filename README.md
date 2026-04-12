# FormatID

현재 저장소는 Redis 기반 generic worker runtime을 먼저 구현한 상태입니다.  
도메인 로직은 최소 골격만 있고, 핵심은 `queue + task_name` 기반 실행 구조입니다.

## Layout
```text
project-root/
├── docs/
│   └── data.md
├── infra/
│   ├── docker-compose.yml
│   ├── env/
│   │   ├── worker.common.env
│   │   ├── worker.system-health.env
│   │   ├── worker.pps-bid.env
│   │   └── worker.pps-attachment.env
│   ├── images/
│   │   ├── system_health/
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   ├── pps_bid/
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   └── pps_attachment/
│   │       ├── Dockerfile
│   │       └── requirements.txt
│   ├── tasks/
│   │   └── catalog.json
│   └── redis/
│       └── redis.conf
├── logs/
│   └── .gitkeep
├── scripts/
│   ├── ops.py
│   ├── enqueue_job.py
│   ├── check_workers.py
│   ├── check_task.py
│   ├── check_dlq.py
│   ├── requeue_dlq.py
│   └── run_compose_smoke_test.py
├── services/
│   └── worker/
│       ├── app/
│       │   ├── config.py
│       │   ├── logger.py
│       │   ├── queue.py
│       │   ├── task_loader.py
│       │   ├── dispatcher.py
│       │   ├── executor.py
│       │   └── worker.py
│       └── README.md
├── shared/
│   ├── queue/
│   │   └── redis.py
│   ├── tasking/
│   │   ├── catalog.py
│   │   ├── errors.py
│   │   ├── registry.py
│   │   ├── routing.py
│   │   ├── schemas.py
│   │   └── status_store.py
│   └── worker_health/
│       ├── health.py
│       └── store.py
└── tasks/
    ├── system/
    │   ├── fail.py
    │   └── health.py
    └── pps/
        ├── bid/
        │   └── collect.py
        └── attachment/
            └── download.py
```

## Ownership

### `services/worker/app`
worker 프로세스 전용 runtime입니다.

- queue consume
- heartbeat publish
- task execution orchestration
- retry / DLQ / graceful shutdown

### `infra/images`
worker family별 이미지와 의존성 정의입니다.

- `system_health`: `system:health` worker 이미지
- `pps_bid`: `pps:bid` worker 이미지
- `pps_attachment`: `pps:attachment` worker 이미지

### `shared/tasking`
producer와 worker가 공유하는 task 계약입니다.

- task catalog
- task registry
- routing validation
- task message schema
- task status 저장

### `shared/queue`
Redis queue 접근 공용 레이어입니다.

### `shared/worker_health`
worker heartbeat 저장, health 판정 공용 레이어입니다.

### `tasks`
실제 task 정의입니다. worker 구현을 직접 import하지 않고 공용 contract만 사용합니다.

## Current Runtime

현재 큐는 다음 3개입니다.

- `system:health`
- `pps:bid`
- `pps:attachment`

현재 고정 매핑은 다음과 같습니다.

- `system.health.check -> system:health`
- `system.test.fail -> system:health`
- `pps.bid.collect -> pps:bid`
- `pps.attachment.download -> pps:attachment`

이 매핑과 실행 정책 원본은 [catalog.json](/home/user/projects/formatid/infra/tasks/catalog.json) 에 있고,
[catalog.py](/home/user/projects/formatid/shared/tasking/catalog.py) 는 그 manifest를 읽는 로더입니다.
현재 task별 정책은 다음을 가집니다.

- `queue_name`
- `max_retries`
- `retryable`
- `backoff_seconds`
- `timeout_seconds`
- `dlq_enabled`
- `dlq_requeue_limit`
- `dlq_requeue_keep_attempts`

또한 각 worker는 자신이 담당하는 queue의 task만 실행합니다.

새 task를 추가할 때 기본 수정 지점은 두 군데입니다.

- `tasks/...` 에 task 구현 추가
- [catalog.json](/home/user/projects/formatid/infra/tasks/catalog.json) 에 task manifest 추가

## Redis Data

현재 Redis에는 다음 데이터가 저장됩니다.

- queue data: `system:health`, `pps:bid`, `pps:attachment`
- DLQ data: `<queue_name>:dlq`
- worker heartbeat: `worker:heartbeat:<queue_name>:<worker_id>`
- task status: `task:status:<task_id>`

## Current Scope

현재까지 구현된 것은 generic runtime입니다.

- Redis queue 기반 worker 실행
- task-to-queue routing validation
- queue별 worker 분리
- worker별 task import 범위 제한
- worker heartbeat와 health 판정
- task lifecycle 상태 저장
- retry / DLQ / DLQ requeue policy
- JSON structured logging
- graceful shutdown
- Docker Compose 기반 smoke test

아직 구현되지 않은 것은 주로 도메인 로직입니다.

- 실제 PPS bid 수집
- 실제 attachment 다운로드/처리
- API endpoint
- 장기 저장용 DB

## Run

로컬 실행:

```bash
export UID=$(id -u)
export GID=$(id -g)
docker compose -f infra/docker-compose.yml up --build
```

worker 컨테이너는 `${UID}:${GID}`로 실행되도록 설정되어 있습니다.
이렇게 해야 `logs/` 아래에 생성되는 파일이 호스트 사용자 소유권으로 기록됩니다.

운영 CLI:

```bash
python3 scripts/ops.py enqueue system:health system.health.check --payload '{"source":"cli"}'
python3 scripts/ops.py workers
python3 scripts/ops.py task <task_id>
python3 scripts/ops.py dlq
python3 scripts/ops.py requeue-dlq system:health --count 1
python3 scripts/ops.py smoke
```

## Operations

- worker runtime 환경변수는 `infra/env/*.env` 에서 관리합니다.
- worker replica 기본값은 queue별 `1`입니다.
- 기본 시간대는 `Asia/Seoul` 입니다.
- 로그는 stdout JSON과 함께 [logs](/home/user/projects/formatid/logs) 아래 `YYYY-MM-DD/worker.<queue>.log` 로도 저장됩니다.
- compose smoke test는 enqueue, heartbeat, task status, DLQ, DLQ requeue 정책까지 실제로 검증합니다.
