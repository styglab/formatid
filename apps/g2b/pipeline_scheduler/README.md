## G2B Pipeline Scheduler

`g2b-pipeline-scheduler`는 `ingest_graph`를 scheduled trigger로 실행하는 G2B 수집 앱입니다.

담당 범위:

- 수집 window 계산
- checkpoint 관리
- G2B OpenAPI quota block 판단
- generic ingest 결과 normalize
- attachment / participants / winners 후속 수집 대상 선정
- G2B raw table과 task state 관리

## Service

Compose service:

- `g2b-pipeline-scheduler`

Manifest:

- `apps/g2b/pipeline_scheduler/manifests/app.json`
- `apps/g2b/pipeline_scheduler/manifests/services/g2b_pipeline_scheduler.json`

Runtime:

- runner: `apps.g2b.pipeline_scheduler.app.service.run_scheduler`
- trigger: `scheduled`
- scheduler: APScheduler crontab
- graph: `ingest_graph`
- default schedule: `G2B_INGEST_GRAPH_SCHEDULE="* * * * *"`

로그:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f g2b-pipeline-scheduler
```

## Environment

Env files:

- `apps/g2b/pipeline_scheduler/infra/env/pipeline_scheduler.env`
- `apps/g2b/pipeline_scheduler/infra/env/app.env`

주요 설정:

```env
G2B_INGEST_GRAPH_SCHEDULED_ENABLED=true
G2B_INGEST_GRAPH_SCHEDULE=* * * * *
G2B_INGEST_SERVICE_MODE=auto
G2B_INGEST_BACKFILL_START=202301010000
G2B_INGEST_WINDOW_MINUTES=1440
G2B_INGEST_INCREMENTAL_LOOKBACK_MINUTES=120
```

## Graph

Graph와 trigger는 분리합니다. Graph는 `apps/g2b/pipeline_scheduler/app/graph/registry.py`에 등록하고,
trigger 실행 공통 로직은 `core/runtime/graph_runtime`을 사용합니다.

현재 수집 흐름:

1. `ingest_bid_notices`가 날짜 window와 `pageNo`를 계산하고 공고 목록 API task를 enqueue합니다.
2. 같은 node가 이전 공고 목록 API 결과를 `raw.g2b_ingest_bid_notices`로 normalize합니다.
3. 같은 node가 branch별 downstream 후보 공고번호를 계산합니다.
4. `ingest_bid_attachments`, `ingest_bid_result_participants`, `ingest_bid_result_winners`가 병렬 branch로 실행됩니다.
5. 각 branch는 후보만 사용해 이전 generic 결과를 normalize하고 다음 generic task를 enqueue합니다.

Graph 구현:

- `apps/g2b/pipeline_scheduler/app/contracts/ingest.py`
- `apps/g2b/pipeline_scheduler/app/graph/registry.py`
- `apps/g2b/pipeline_scheduler/app/graph/ingest_graph.py`
- `apps/g2b/pipeline_scheduler/app/service/run_scheduler.py`
- `core/runtime/graph_runtime`

Graph 등록은 `core.runtime.graph_runtime.create_graph_definition`을 사용합니다.
앱 graph는 graph builder, step builder, initial state만 제공하고 runtime context adapter는 공통 모듈이 처리합니다.

Generic queue/task:

- bid list API: `ingest:api` / `ingest.api.fetch`
- participants API: `ingest:api` / `ingest.api.fetch`
- winners API: `ingest:api` / `ingest.api.fetch`
- attachment file: `ingest:file` / `ingest.file.download`

## Checkpoint Reset

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml stop g2b-pipeline-scheduler
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml exec postgres psql -U postgres -d postgres -c "DELETE FROM checkpoints WHERE name LIKE 'g2b_ingest:%' OR name = 'service:g2b_ingest_bid_list_collect';"
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml start g2b-pipeline-scheduler
```
