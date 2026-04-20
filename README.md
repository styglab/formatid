# FormatID

Redis 기반 generic worker runtime과 PPS 도메인 수집 서비스를 함께 구성하는 프로젝트입니다.

핵심 구조는 다음처럼 분리합니다.

- Redis: queue, worker heartbeat, task status, DLQ
- 내부 Postgres: scheduler checkpoint
- 외부 PPS Postgres: PPS 수집 데이터와 PPS 작업 상태
- MinIO/S3: PPS 첨부파일 저장
- generic runtime: queue consume, retry, DLQ, heartbeat, scheduler
- PPS domain: 공고 목록, 첨부파일, 참여업체, 낙찰업체 수집

generic runtime에는 PPS 도메인 로직을 넣지 않습니다. PPS 관련 코드는 `domains/pps/tasks` 아래에 둡니다.

## 실행

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml up -d --build
```

종료:

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml down
```

로그 확인:

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f scheduler
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f pps-bid-worker
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f pps-attachment-worker
```

## 서비스 구성

현재 compose 서비스는 manifest에서 관리합니다.

- `redis`: queue, heartbeat, task status, DLQ 저장
- `postgres`: 내부 checkpoint 저장용 Postgres
- `api`: health/checkpoint 조회 API
- `scheduler`: schedule manifest를 읽어 주기적으로 task enqueue
- `pps-bid-worker`: `pps:bid` 큐 소비
- `pps-attachment-worker`: `pps:attachment` 큐 소비

원본 manifest:

- `infra/platform_services/*.json`
- `domains/*/manifests/*worker.json`
- `domains/*/manifests/schedules/*.json`

`infra/docker-compose.yml`은 생성 파일입니다. manifest를 수정한 뒤 compose를 다시 만들려면:

```bash
python3 scripts/generate_compose.py
```

## 환경 변수

공통 env 파일은 `infra/env` 아래에 있습니다.

- `compose.env`: host port와 로컬 bind mount 경로
- `postgres.env`: 내부 compose Postgres 접속 정보
- `scheduler.env`: scheduler generic 설정
- `domains/pps/env/scheduler.pps.env`: PPS scheduler 설정
- `worker.common.env`: worker 공통 설정
- `domains/pps/env/worker.pps-bid.env`: PPS bid worker 설정
- `domains/pps/env/worker.pps-attachment.env`: PPS attachment worker 설정
- `api.env`: API 설정

현재 내부 Postgres 기본값은 `postgres/postgres/postgres`입니다.

```env
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

compose host port와 로컬 데이터 경로는 `infra/env/compose.env`에서 관리합니다.

```env
HOST_UID=1000
HOST_GID=1000
API_HOST_PORT=8000
API_CONTAINER_PORT=8000
POSTGRES_HOST_PORT=5432
POSTGRES_CONTAINER_PORT=5432
POSTGRES_DATA_DIR=./postgres/data
REDIS_HOST_PORT=6379
REDIS_CONTAINER_PORT=6379
```

서비스 내부 checkpoint 접속 URL은 `POSTGRES_*` 값으로 조립합니다. `CHECKPOINT_DATABASE_URL`을 직접 지정하면 그 값을 우선 사용합니다.

PPS 수집 데이터는 `PPS_DATABASE_URL`이 가리키는 외부 Postgres에 저장합니다. 내부 checkpoint DB와 PPS 데이터 DB는 분리합니다.

## PPS 스케줄

현재 scheduler는 두 개의 주기 작업을 실행합니다.

- `pps_bid_list_collect`
  - queue: `pps:bid`
  - task: `pps.bid.list.collect`
  - interval: 60초
  - payload factory: `domains.pps.tasks.schedules.build_bid_list_payload`
- `pps_bid_downstream_enqueue`
  - queue: `pps:bid`
  - task: `pps.bid.downstream.enqueue`
  - interval: 60초
  - payload:
    - `limit=100`
    - `max_failed_retries=3`
    - `retry_failed_after_seconds=86400`

`domains/pps/env/scheduler.pps.env` 기준 수집 설정:

```env
PPS_SCHEDULER_MODE=auto
PPS_BACKFILL_START=202301010000
PPS_WINDOW_MINUTES=1440
PPS_INCREMENTAL_LOOKBACK_MINUTES=120
```

의미:

- `PPS_BACKFILL_START`: checkpoint가 없을 때 시작할 최초 조회 시각입니다.
- `PPS_WINDOW_MINUTES`: 한 번에 조회할 window 크기입니다. 현재는 1440분이라 하루 단위입니다.
- `PPS_INCREMENTAL_LOOKBACK_MINUTES`: backfill이 현재 시각을 따라잡은 뒤 incremental 조회에서 최근 데이터를 다시 확인할 lookback 범위입니다.

scheduler runtime은 APScheduler의 interval job으로 schedule을 실행합니다. Redis lock과 checkpoint 재확인을 함께 사용해 여러 scheduler 인스턴스가 떠도 같은 schedule의 중복 enqueue를 방지합니다.

- 기본 lock key: `scheduler:lock:<schedule_name>`
- 기본 lock TTL: `interval_seconds + SCHEDULER_LOCK_TTL_BUFFER_SECONDS`
- 전역 설정: `SCHEDULER_LOCK_ENABLED=true`
- schedule별 설정: manifest에 `lock_enabled`, `lock_ttl_seconds`를 선택적으로 지정할 수 있습니다.
- process 내부 중복 실행은 APScheduler `max_instances`로 제어합니다.
- 밀린 실행 처리는 `misfire_grace_seconds`, `coalesce`로 제어합니다.

## PPS 파이프라인

현재 PPS 파이프라인은 공고 목록 테이블을 후속 수집의 source of truth로 사용합니다.

1. `pps.bid.list.collect`
   - 날짜 window와 `pageNo` 기준으로 물품 공고 목록을 조회합니다.
   - `raw.pps_bid_notices`에 upsert합니다.
   - page/window checkpoint를 갱신합니다.
   - 첨부파일/참여업체/낙찰업체 task는 직접 enqueue하지 않습니다.

2. `pps.bid.downstream.enqueue`
   - `raw.pps_bid_notices`를 읽습니다.
   - `raw.pps_task_states` 기준으로 미처리 또는 재시도 가능한 실패 건을 찾습니다.
   - 후속 task를 enqueue합니다.
   - 실패 건은 `max_failed_retries`와 `retry_failed_after_seconds` 정책을 따릅니다.

3. 후속 task
   - `pps.bid.attachment.download`
     - 공고 `raw_payload`의 첨부 URL과 파일명을 사용합니다.
     - 첨부파일명 앞의 공고번호/타임스탬프 prefix는 제거하고 실제 파일명만 저장합니다.
     - 파일 메타데이터와 저장 위치를 `raw.pps_bid_attachments`에 저장합니다.
   - `pps.bid_result.participants.collect`
     - `bidNtceNo` 기준 참여업체를 조회합니다.
     - `raw.pps_bid_result_participants`에 저장합니다.
   - `pps.bid_result.winners.collect`
     - `bidNtceNo` 기준 낙찰업체를 조회합니다.
     - `raw.pps_bid_result_winners`에 저장합니다.

## PPS 테이블

PPS 도메인 데이터는 외부 PPS Postgres의 `raw` schema에 저장합니다.

- `raw.pps_bid_notices`
  - 원천 공고 목록
  - 후속 수집 대상 선정의 기준 테이블
- `raw.pps_bid_attachments`
  - 공고 첨부파일 메타데이터와 object storage 위치
- `raw.pps_bid_result_participants`
  - 참여업체 결과
- `raw.pps_bid_result_winners`
  - 낙찰업체 결과
- `raw.pps_task_states`
  - attachment / participants / winners 작업별 상태
  - 미처리, 성공, 실패, 재시도 횟수, 마지막 오류 관리

내부 compose Postgres에는 scheduler checkpoint만 저장합니다.

- `checkpoints`

## Task Catalog

task 정의 원본은 `domains/<domain>/manifests/tasks.json`입니다.

현재 task:

- `pps.bid.list.collect -> pps:bid`
- `pps.bid.downstream.enqueue -> pps:bid`
- `pps.bid.attachment.download -> pps:attachment`
- `pps.bid_result.participants.collect -> pps:bid`
- `pps.bid_result.winners.collect -> pps:bid`

각 task는 catalog에서 다음 정책을 가집니다.

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

## 새 서비스 추가

PPS가 아닌 새 도메인 서비스를 추가할 때도 generic runtime은 그대로 사용합니다. 새 서비스는 기본적으로 다음 단위로 구성합니다.

- domain task 코드: `domains/<domain>/tasks/...`
- task catalog: `domains/<domain>/manifests/tasks.json`
- worker service manifest: `domains/<domain>/manifests/<service>_worker.json`
- worker env: `domains/<domain>/env/worker.<service>.env`
- 필요 시 worker image: `domains/<domain>/images/<service>/`
- 필요 시 schedule manifest: `domains/<domain>/manifests/schedules/<schedule>.json`

서비스 추가 절차:

1. 도메인 task 구현을 추가합니다.

   예시:

   ```text
   domains/example/
     __init__.py
     tasks/
       __init__.py
       jobs/
         __init__.py
         collect.py
         schemas.py
     manifests/
       tasks.json
       example_jobs_worker.json
       schedules/
     env/
     images/
   ```

   task handler는 `@task("<task_name>")`로 등록하고 `TaskMessage`를 받아 `TaskResult`를 반환합니다.

   ```python
   from shared.tasking.registry import task
   from shared.tasking.schemas import TaskMessage, TaskResult


   @task("example.jobs.collect")
   async def collect_example_jobs(message: TaskMessage) -> TaskResult:
       return TaskResult(
           task_id=message.task_id,
           task_name=message.task_name,
           status="succeeded",
           output={"ok": True},
       )
   ```

2. payload schema를 정의합니다.

   schema는 Pydantic model을 사용합니다. schema가 필요 없는 task라면 catalog의 `payload_schema`를 생략할 수 있습니다.

   ```python
   from pydantic import BaseModel, ConfigDict, Field


   class CollectExampleJobsPayload(BaseModel):
       model_config = ConfigDict(extra="forbid")

       target_date: str = Field(pattern=r"^\d{8}$")
   ```

3. `domains/<domain>/manifests/tasks.json`에 task를 등록합니다.

   ```json
   {
     "example.jobs.collect": {
       "task_name": "example.jobs.collect",
       "queue_name": "example:jobs",
       "module_path": "domains.example.tasks.jobs.collect",
       "payload_schema": "domains.example.tasks.jobs.schemas.CollectExampleJobsPayload",
       "max_retries": 3,
       "retryable": true,
       "backoff_seconds": 0,
       "timeout_seconds": 120,
       "dlq_enabled": true,
       "dlq_requeue_limit": 3,
       "dlq_requeue_keep_attempts": false
     }
   }
   ```

   `task_name`과 `queue_name`은 운영 식별자입니다. 같은 queue에 여러 task를 넣을 수 있지만, 하나의 worker service는 하나의 queue를 담당합니다.

4. worker service manifest를 추가합니다.

   예시: `domains/example/manifests/example_jobs_worker.json`

   ```json
   {
     "service_name": "example-jobs-worker",
     "queue_name": "example:jobs",
     "dockerfile": "domains/example/images/example_jobs/Dockerfile",
     "env_files": [
       "infra/env/postgres.env",
       "infra/env/worker.common.env",
       "domains/example/env/worker.example-jobs.env"
     ],
     "replicas": 1
   }
   ```

   `queue_name`은 catalog에 등록한 task의 `queue_name`과 일치해야 합니다. `validate-config`가 이 매핑을 검증합니다.

5. worker env 파일을 추가합니다.

   예시: `domains/example/env/worker.example-jobs.env`

   ```env
   WORKER_QUEUE_NAME=example:jobs
   EXAMPLE_API_TIMEOUT_SECONDS=30
   ```

   공통 worker 설정은 `infra/env/worker.common.env`에 두고, 서비스별 설정만 별도 env 파일에 둡니다.

6. worker image를 추가하거나 기존 이미지를 재사용합니다.

   새 dependency가 필요하면 새 image를 둡니다.

   ```text
   domains/example/images/example_jobs/
     Dockerfile
     requirements.txt
   ```

   기존 이미지로 충분하면 worker manifest의 `dockerfile`만 기존 Dockerfile로 지정해도 됩니다.

7. 주기 실행이 필요하면 schedule manifest를 추가합니다.

   예시: `domains/<domain>/manifests/schedules/example_jobs_collect.json`

   ```json
   {
     "name": "example_jobs_collect",
     "queue_name": "example:jobs",
     "task_name": "example.jobs.collect",
     "interval_seconds": 60,
     "payload": {
       "target_date": "20230101"
     },
     "misfire_grace_seconds": 30,
     "coalesce": true,
     "max_instances": 1,
     "run_immediately": true,
     "enabled": true
   }
   ```

   payload를 동적으로 만들려면 `payload_factory`를 사용합니다.

   ```json
   {
     "name": "example_jobs_collect",
     "queue_name": "example:jobs",
     "task_name": "example.jobs.collect",
     "interval_seconds": 60,
     "payload": {},
     "payload_factory": "domains.example.tasks.schedules.build_example_payload",
     "run_immediately": true,
     "enabled": true
   }
   ```

   같은 schedule이 여러 scheduler 인스턴스에서 동시에 enqueue되지 않도록 기본적으로 Redis lock이 적용됩니다. `max_instances`는 같은 scheduler process 안에서만 적용되므로 Redis lock은 계속 필요합니다. 필요하면 schedule manifest에 다음 값을 추가할 수 있습니다.

   ```json
   {
     "lock_enabled": true,
     "lock_ttl_seconds": 65,
     "misfire_grace_seconds": 30,
     "coalesce": true,
     "max_instances": 1
   }
   ```

   `payload_factory`는 scheduler runtime에서 import하므로 PPS가 아닌 새 도메인은 `domains/<domain>/tasks/schedules.py` 같은 위치에 둡니다.

8. compose를 재생성하고 설정을 검증합니다.

   ```bash
   python3 scripts/generate_compose.py
   python3 scripts/ops.py validate-config
   ```

9. 서비스만 빌드/기동합니다.

   ```bash
   docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml up -d --build example-jobs-worker
   ```

   scheduler schedule까지 추가했다면 scheduler도 재시작합니다.

   ```bash
   docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml restart scheduler
   ```

10. 수동 enqueue로 먼저 확인합니다.

    ```bash
    python3 scripts/ops.py enqueue example:jobs example.jobs.collect --payload '{"target_date":"20230101"}'
    python3 scripts/ops.py workers --format table
    python3 scripts/ops.py task <task_id>
    python3 scripts/ops.py dlq --queues example:jobs
    ```

## 기존 서비스 변경

기존 서비스의 task만 추가하는 경우:

1. `domains/<domain>/tasks/...`에 task 구현을 추가합니다.
2. 필요하면 payload schema를 추가합니다.
3. `domains/<domain>/manifests/tasks.json`에 같은 `queue_name`으로 task를 등록합니다.
4. worker image가 새 module을 포함하도록 필요 시 rebuild합니다.
5. `python3 scripts/ops.py validate-config`를 실행합니다.

기존 서비스에 env만 추가하는 경우:

1. `domains/<domain>/env/worker.<service>.env` 또는 공통이면 `infra/env/worker.common.env`에 추가합니다.
2. 서비스 컨테이너를 recreate합니다.

   ```bash
   docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml up -d --force-recreate <service-name>
   ```

기존 서비스의 queue를 바꾸는 경우:

1. `domains/<domain>/manifests/tasks.json`의 `queue_name`을 바꿉니다.
2. `domains/<domain>/manifests/<service>_worker.json`의 `queue_name`을 같은 값으로 바꿉니다.
3. 해당 worker env의 `WORKER_QUEUE_NAME`도 같은 값으로 바꿉니다.
4. compose를 재생성하고 검증합니다.

   ```bash
   python3 scripts/generate_compose.py
   python3 scripts/ops.py validate-config
   ```

주의사항:

- `services/worker`, `shared/tasking`, `shared/queue`에는 도메인 로직을 넣지 않습니다.
- queue 이름은 Redis key가 되므로 짧고 명확하게 정합니다. 예: `example:jobs`
- task 이름은 도메인 계층이 드러나도록 정합니다. 예: `example.jobs.collect`
- schedule 이름은 checkpoint key에 들어갑니다. 이름을 바꾸면 기존 checkpoint와 분리됩니다.
- task payload schema를 엄격하게 두면 잘못된 enqueue를 worker 실행 전에 막을 수 있습니다.
- `validate-config`는 domain directory와 manifest domain 일치, task/module/schema 위치, worker `WORKER_QUEUE_NAME`, schedule/task queue 매칭, payload factory 함수 존재 여부를 검증합니다.

## Queue와 상태

Redis에 저장되는 데이터:

- queue: `pps:bid`, `pps:attachment`
- DLQ: `pps:bid:dlq`, `pps:attachment:dlq`
- worker heartbeat: `worker:heartbeat:<queue_name>:<worker_id>`
- task status: `task:status:<task_id>`
- PPS quota block: PPS API 일일 트래픽 초과 시 임시 block 정보

PPS OpenAPI가 일일 요청 한도 초과를 반환하면 `resultCode=22`를 quota blocked로 처리합니다.

- 해당 PPS 작업은 `raw.pps_task_states`에 `failed`로 기록합니다.
- 상세 오류는 `last_error`에 저장합니다.
- runtime task는 DLQ로 보내지 않도록 성공 처리 후 skip 결과를 반환합니다.
- scheduler/downstream은 quota block 동안 PPS API 호출 task enqueue를 제한합니다.

## 운영 명령

API:

```bash
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
curl http://localhost:8000/health
curl http://localhost:8000/health/workers
curl http://localhost:8000/health/scheduler
curl http://localhost:8000/checkpoints
curl http://localhost:8000/checkpoints/schedule:pps_bid_list_collect
curl 'http://localhost:8000/schedule-runs?limit=20'
curl 'http://localhost:8000/task-executions?limit=20'
```

CLI:

```bash
python3 scripts/ops.py workers
python3 scripts/ops.py workers --format table
python3 scripts/ops.py task <task_id>
python3 scripts/ops.py checkpoints
python3 scripts/ops.py checkpoints schedule:pps_bid_list_collect
python3 scripts/ops.py dlq
python3 scripts/ops.py requeue-dlq pps:bid --count 1
python3 scripts/ops.py validate-config
```

수동 enqueue 예시:

```bash
python3 scripts/ops.py enqueue pps:bid pps.bid.list.collect --payload '{"inqryBgnDt":"202301010000","inqryEndDt":"202301012359","pageNo":1}'
python3 scripts/ops.py enqueue pps:bid pps.bid.downstream.enqueue --payload '{"limit":100,"max_failed_retries":3,"retry_failed_after_seconds":86400}'
python3 scripts/ops.py enqueue pps:attachment pps.bid.attachment.download --payload '{"bidNtceNo":"20230100001","bidNtceOrd":"000"}'
```

## Checkpoint 초기화

2023년 1월 1일부터 다시 수집하려면 scheduler를 멈춘 뒤 내부 checkpoint DB의 해당 schedule checkpoint를 삭제합니다.

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml stop scheduler
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml exec postgres psql -U postgres -d postgres -c "DELETE FROM checkpoints WHERE name = 'schedule:pps_bid_list_collect';"
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml start scheduler
```

전체 서비스를 멈추려면:

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml down
```

## 개발 규칙

- generic worker runtime에는 PPS 도메인 로직을 넣지 않습니다.
- 도메인 로직은 `domains/<domain>/tasks` 아래에 둡니다. 예: `domains/pps/tasks`, `domains/example/tasks`
- 새 task는 `domains/<domain>/manifests/tasks.json`에 등록합니다.
- 새 주기 enqueue는 `domains/<domain>/manifests/schedules`에 schedule manifest를 추가합니다.
- 새 worker service가 필요하면 `domains/<domain>/manifests`에 manifest를 추가하고 compose를 재생성합니다.
- blocking I/O는 worker task에서 피하고 가능한 async I/O를 사용합니다.

## 주요 경로

```text
infra/
  docker-compose.yml
  env/
  platform_services/
  postgres/init/
domains/
  pps/
    tasks/
    manifests/
    env/
    images/
    sql/
services/
  api/
  scheduler/
  worker/
shared/
  checkpoints/
  queue/
  tasking/
  worker_health/
docs/
  tasks/pps.md
```
