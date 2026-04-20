# FormatID

이 저장소는 Redis 기반 generic worker runtime과 그 위에서 동작하는 PPS 도메인 서비스를 함께 구성합니다.  
현재 핵심은 `queue + task_name` 기반 generic worker runtime이고, PPS bid/attachment 작업은 그 위에 올라가는 도메인 레이어입니다.

# 실행
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml down

## Layout
```text
project-root/
├── docs/
│   └── data.md
├── infra/
│   ├── docker-compose.yml
│   ├── env/
│   │   ├── api.env
│   │   ├── postgres.env
│   │   ├── scheduler.env
│   │   ├── worker.common.env
│   │   ├── worker.pps-bid.env
│   │   └── worker.pps-attachment.env
│   ├── images/
│   │   ├── api/
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   ├── pps_bid/
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   ├── pps_attachment/
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   └── scheduler/
│   │       ├── Dockerfile
│   │       └── requirements.txt
│   ├── schedules/
│   │   ├── pps_attachment_download.json
│   │   └── pps_bid_collect.json
│   ├── platform_services/
│   │   ├── api.json
│   │   ├── postgres.json
│   │   ├── redis.json
│   │   └── scheduler.json
│   ├── worker_services/
│   │   ├── pps_bid.json
│   │   └── pps_attachment.json
│   ├── postgres/
│   │   └── init/
│   │       └── 001_create_checkpoints.sql
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
│   ├── api/
│   │   ├── app/
│   │   │   ├── config.py
│   │   │   ├── main.py
│   │   │   ├── routers/
│   │   │   │   └── health.py
│   │   │   ├── schemas/
│   │   │   │   └── health.py
│   │   │   └── services/
│   │   │       └── health_service.py
│   │   ├── README.md
│   │   └── __init__.py
│   ├── scheduler/
│   │   ├── runtime/
│   │   │   ├── config.py
│   │   │   ├── logger.py
│   │   │   └── scheduler.py
│   │   └── __init__.py
│   └── worker/
│       ├── runtime/
│       │   ├── config.py
│       │   ├── logger.py
│       │   ├── queue.py
│       │   ├── task_loader.py
│       │   ├── dispatcher.py
│       │   ├── executor.py
│       │   └── worker.py
│       └── README.md
├── shared/
│   ├── checkpoints/
│   │   ├── postgres.py
│   │   └── store.py
│   ├── queue/
│   │   └── redis.py
│   ├── schedule_catalog.py
│   ├── service_catalog.py
│   ├── tasking/
│   │   ├── catalog.py
│   │   ├── enqueue.py
│   │   ├── errors.py
│   │   ├── registry.py
│   │   ├── routing.py
│   │   ├── schemas.py
│   │   ├── status_store.py
│   │   └── validation.py
│   └── worker_health/
│       ├── health.py
│       └── store.py
└── tasks/
    ├── catalog.json
    └── pps/
        ├── bid/
        │   ├── collect.py
        │   └── schemas.py
        └── attachment/
            ├── download.py
            └── schemas.py
```

## Ownership

### `services/worker/runtime`
worker 프로세스 전용 runtime입니다.

- queue consume
- heartbeat publish
- task execution orchestration
- retry / DLQ / graceful shutdown

### `services/api`
FastAPI 기반 조회 API입니다.

- Redis heartbeat 기반 health 조회
- worker service 상태 요약
- 향후 enqueue / status / DLQ 운영 엔드포인트 확장 지점

API 이미지와 의존성 정의는 [infra/images/api](/home/user/projects/formatid/infra/images/api) 에 둡니다.

### `services/scheduler/runtime`
주기적으로 task를 enqueue하는 scheduler runtime입니다.

- schedule manifest 로드
- Redis 직접 enqueue
- payload validation
- 주기 실행 로그

scheduler 이미지와 의존성 정의는 [infra/images/scheduler](/home/user/projects/formatid/infra/images/scheduler) 에 둡니다.

### `shared/checkpoints`
checkpoint 저장 공용 레이어입니다. 현재 구현체는 Postgres입니다.

### `infra/images`
worker family별 이미지와 의존성 정의입니다.

- `pps_bid`: `pps:bid` worker 이미지
- `pps_attachment`: `pps:attachment` worker 이미지

### `infra/worker_services`
worker service manifest 원본입니다.

- service name
- queue name
- dockerfile
- env files
- replicas

### `infra/platform_services`
platform service manifest 원본입니다.

- service name
- image or dockerfile
- env files
- depends_on / ports / volumes / healthcheck

### `shared/tasking`
producer와 worker가 공유하는 task 계약입니다.

- task catalog
- task registry
- routing validation
- task message schema
- task status 저장

### `shared/queue`
Redis queue 접근 공용 레이어입니다.

### `shared/service_catalog`
worker service manifest 로더입니다. health/ops 기본 queue와 expected worker 수는 여기서 읽습니다.

### `shared/schedule_catalog`
schedule manifest 로더입니다. scheduler와 정합성 검증에서 사용합니다.

### `shared/worker_health`
worker heartbeat 저장, health 판정 공용 레이어입니다.

### `tasks`
실제 task 정의입니다. worker 구현을 직접 import하지 않고 공용 contract만 사용합니다.

## Current Runtime

현재 큐는 다음 2개입니다.

- `pps:bid`
- `pps:attachment`

현재 고정 매핑은 다음과 같습니다.

- `pps.bid.collect -> pps:bid`
- `pps.attachment.download -> pps:attachment`

이 매핑과 실행 정책 원본은 [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 에 있고,
[catalog.py](/home/user/projects/formatid/shared/tasking/catalog.py) 는 그 manifest를 읽는 로더입니다.
현재 task별 정책은 다음을 가집니다.

- `queue_name`
- `payload_schema`
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
- 필요하면 `tasks/.../schemas.py` 에 payload schema 추가
- [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 에 task manifest 추가

새 worker service를 추가할 때 기본 수정 지점은 다음입니다.

- [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 에 task manifest 추가
- `tasks/...` 에 task 구현 추가
- [infra/worker_services](/home/user/projects/formatid/infra/worker_services) 에 worker service manifest 추가

주기 enqueue가 필요하면 schedule manifest를 추가합니다.

- [infra/schedules](/home/user/projects/formatid/infra/schedules) 에 schedule manifest 추가

## Add A Worker Service

현재 구조에서는 새로운 task service가 새로운 worker 컨테이너에서 실행됩니다.
즉 `pps:bid`, `pps:attachment`처럼 queue 하나가 worker service 하나에 대응하는 방식입니다.

새 worker service 추가 절차:

1. `tasks/...` 아래에 task 구현을 추가합니다.
2. [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 에 task manifest를 추가합니다.
   필수 값:
   `task_name`, `queue_name`, `module_path`
3. [infra/worker_services](/home/user/projects/formatid/infra/worker_services) 아래에 worker service manifest를 추가합니다.
   필수 값:
   `service_name`, `queue_name`, `dockerfile`, `env_files`, `replicas`
4. 필요하면 [infra/images](/home/user/projects/formatid/infra/images) 아래에 새 worker image 정의를 추가합니다.
   기존 image를 재사용할 수 있으면 새 Dockerfile은 필요 없습니다.
5. 필요하면 [infra/env](/home/user/projects/formatid/infra/env) 아래에 service 전용 env 파일을 추가합니다.
6. compose를 재생성합니다.

```bash
python3 scripts/generate_compose.py
```

예를 들어 새 queue `pps:notice`를 추가한다면 대략 이런 파일이 생깁니다.

- `tasks/pps/notice/...`
- [catalog.json](/home/user/projects/formatid/tasks/catalog.json) 에 `pps.notice.*`
- `infra/worker_services/pps_notice.json`
- 필요 시 `infra/images/pps_notice/`
- 필요 시 `infra/env/worker.pps-notice.env`

## Redis Data

현재 Redis에는 다음 데이터가 저장됩니다.

- queue data: `pps:bid`, `pps:attachment`
- DLQ data: `<queue_name>:dlq`
- worker heartbeat: `worker:heartbeat:<queue_name>:<worker_id>`
- task status: `task:status:<task_id>`

현재 Postgres에는 다음 영속 데이터가 저장됩니다.

- scheduler checkpoint: `checkpoints`

## Current Scope

현재까지 구현된 것은 generic runtime입니다.

- Redis queue 기반 worker 실행
- task-to-queue routing validation
- queue별 worker 분리
- worker별 task import 범위 제한
- schedule manifest 기반 주기 enqueue
- Postgres 기반 scheduler checkpoint
- worker heartbeat와 health 판정
- task lifecycle 상태 저장
- retry / DLQ / DLQ requeue policy
- JSON structured logging
- graceful shutdown
- Docker Compose 기반 smoke test

아직 구현되지 않은 것은 주로 도메인 로직입니다.

- 실제 PPS bid 수집
- 실제 attachment 다운로드/처리
- 장기 도메인 저장용 DB 확장

## Run

로컬 실행:

```bash
export UID=$(id -u)
export GID=$(id -g)
docker compose -f infra/docker-compose.yml up --build
```

API health 확인:

```bash
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
curl http://localhost:8000/health
curl http://localhost:8000/health/workers
curl http://localhost:8000/checkpoints
curl http://localhost:8000/checkpoints/schedule:pps_bid_collect
```

worker 컨테이너는 `${UID}:${GID}`로 실행되도록 설정되어 있습니다.
이렇게 해야 `logs/` 아래에 생성되는 파일이 호스트 사용자 소유권으로 기록됩니다.

운영 CLI:

```bash
python3 scripts/ops.py enqueue pps:bid pps.bid.collect --payload '{"source":"cli"}'
python3 scripts/ops.py workers
python3 scripts/ops.py task <task_id>
python3 scripts/ops.py checkpoints
python3 scripts/ops.py checkpoints schedule:pps_bid_collect
python3 scripts/ops.py dlq
python3 scripts/ops.py requeue-dlq pps:bid --count 1
python3 scripts/ops.py validate-config
python3 scripts/ops.py smoke
```

## Operations

- worker runtime 환경변수는 `infra/env/*.env` 에서 관리합니다.
- worker replica 기본값은 queue별 `1`입니다.
- `api`, `scheduler`, worker 컨테이너는 모두 `restart: unless-stopped` 정책으로 자동 재기동됩니다.
- scheduler checkpoint는 Postgres `checkpoints` 테이블에 저장됩니다.
- 기본 시간대는 `Asia/Seoul` 입니다.
- 로그는 stdout JSON과 함께 [logs](/home/user/projects/formatid/logs) 아래 `YYYY-MM-DD/worker.<queue>.log` 로도 저장됩니다.
- compose smoke test는 enqueue, heartbeat, task status, DLQ, DLQ requeue 정책까지 실제로 검증합니다.
- [docker-compose.yml](/home/user/projects/formatid/infra/docker-compose.yml) 는 생성 파일입니다.
- worker 서비스 원본은 [infra/worker_services](/home/user/projects/formatid/infra/worker_services) manifest입니다.
- platform 서비스 원본은 [infra/platform_services](/home/user/projects/formatid/infra/platform_services) manifest입니다.
- scheduler 주기 작업 원본은 [infra/schedules](/home/user/projects/formatid/infra/schedules) 이고, Postgres 초기 checkpoint 테이블 생성 SQL은 [infra/postgres/init](/home/user/projects/formatid/infra/postgres/init) 에 있습니다.
- compose 재생성:

```bash
python3 scripts/generate_compose.py
```
