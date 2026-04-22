## G2B Ingest

`g2b-ingest`는 G2B 입찰 공고 수집 앱입니다.

앱이 담당하는 것:

- 수집 window 계산
- checkpoint 관리
- G2B OpenAPI quota block 판단
- generic ingest 결과 normalize
- attachment / participants / winners 후속 수집 대상 선정
- G2B raw table과 task state 관리

generic worker가 담당하는 것:

- 외부 API 호출: `ingest.api.fetch`
- 첨부파일 다운로드: `ingest.file.download`
- Redis queue consume, retry, DLQ, execution history

## Service

Compose service:

- `g2b-ingest`

Manifest:

- `apps/g2b_ingest/manifests/app.json`
- `apps/g2b_ingest/manifests/services/g2b_ingest_service.json`

Runtime:

- `service_type`: `cron`
- runner: `services.app_service.runtime.cron.CronServiceRunner`
- scheduler: APScheduler cron trigger
- orchestration: LangGraph DAG
- default cron: `G2B_INGEST_SERVICE_CRON="* * * * *"`

로그:

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f g2b-ingest
```

## Environment

Env files:

- `apps/g2b_ingest/infra/env/g2b_ingest_service.env`
- `apps/g2b_ingest/infra/env/service.env`
- `apps/g2b_ingest/infra/env/api.env`
- `apps/g2b_ingest/infra/env/ingest.env`

주요 설정:

```env
G2B_INGEST_SERVICE_CRON=* * * * *
G2B_INGEST_SERVICE_MODE=auto
G2B_INGEST_BACKFILL_START=202301010000
G2B_INGEST_WINDOW_MINUTES=1440
G2B_INGEST_INCREMENTAL_LOOKBACK_MINUTES=120
```

의미:

- `G2B_INGEST_SERVICE_CRON`: APScheduler crontab 표현식입니다. 기본값은 매분 실행입니다.
- `G2B_INGEST_BACKFILL_START`: checkpoint가 없을 때 시작할 최초 조회 시각입니다.
- `G2B_INGEST_WINDOW_MINUTES`: 한 번에 조회할 window 크기입니다.
- `G2B_INGEST_INCREMENTAL_LOOKBACK_MINUTES`: backfill 이후 최근 데이터를 다시 확인할 lookback 범위입니다.

수집 데이터는 `G2B_INGEST_DATABASE_URL`이 가리키는 Postgres에 저장합니다. 내부 runtime DB와 수집 데이터 DB는 분리해서 운영합니다.

## Pipeline

현재 수집 흐름은 LangGraph DAG로 표현하고, 공고 목록 테이블을 후속 수집의 source of truth로 사용합니다.

1. `ingest_bid_notices`가 날짜 window와 `pageNo`를 계산하고 공고 목록 API task를 enqueue합니다.
2. 같은 node가 이전 공고 목록 API 결과를 `raw.g2b_ingest_bid_notices`로 normalize합니다.
3. 같은 node가 branch별 downstream 후보 공고번호를 계산해 graph state의 `attachment_candidates`, `participant_candidates`, `winner_candidates`에 담습니다.
4. 공고 목록 normalize 이후 `ingest_bid_attachments`, `ingest_bid_result_participants`, `ingest_bid_result_winners`가 병렬 branch로 실행됩니다.
5. 각 branch는 state로 받은 후보만 사용해 자기 타입의 이전 generic 결과를 normalize하고 다음 generic task를 enqueue합니다.

Graph nodes:

- `ingest_bid_notices`
- `ingest_bid_attachments`
- `ingest_bid_result_participants`
- `ingest_bid_result_winners`

Edges:

```text
ingest_bid_notices -> ingest_bid_attachments
ingest_bid_notices -> ingest_bid_result_participants
ingest_bid_notices -> ingest_bid_result_winners
```

State:

- `notices`: 전체 downstream 후보 공고
- `attachment_candidates`: attachment 다운로드 후보 `bid_ntce_no`, `bid_ntce_ord`
- `participant_candidates`: participants API 후보 `bid_ntce_no`, `bid_ntce_ord`
- `winner_candidates`: winners API 후보 `bid_ntce_no`, `bid_ntce_ord`
- `attachments`: attachment branch enqueue 결과 요약
- `participants`: participants branch enqueue 결과 요약
- `winners`: winners branch enqueue 결과 요약

Graph 구현:

- `apps/g2b_ingest/service/graph.py`
- `apps/g2b_ingest/service/main.py`

Generic queue/task:

- bid list API: `ingest:api` / `ingest.api.fetch`
- participants API: `ingest:api` / `ingest.api.fetch`
- winners API: `ingest:api` / `ingest.api.fetch`
- attachment file: `ingest:file` / `ingest.file.download`

## Tables

G2B 수집 데이터는 `raw` schema에 저장합니다.

- `raw.g2b_ingest_bid_notices`
  - 원천 공고 목록
  - 후속 수집 대상 선정 기준 테이블
- `raw.g2b_ingest_bid_attachments`
  - 공고 첨부파일 메타데이터와 object storage 위치
- `raw.g2b_ingest_bid_result_participants`
  - 참여업체 결과
- `raw.g2b_ingest_bid_result_winners`
  - 낙찰업체 결과
- `raw.g2b_ingest_task_states`
  - attachment / participants / winners 작업별 상태
  - 미처리, 성공, 실패, 재시도 횟수, 마지막 오류 관리

Generic worker 중간 결과 테이블:

- `g2b_ingest_generic_api_ingest`
- `g2b_ingest_generic_file_ingest`

## Quota Block

G2B OpenAPI가 일일 요청 한도 초과를 반환하면 quota block으로 처리합니다.

- G2B 작업은 `raw.g2b_ingest_task_states`에 `failed`로 기록합니다.
- 상세 오류는 `last_error`에 저장합니다.
- runtime task는 DLQ로 보내지 않도록 성공 처리 후 skip 결과를 반환합니다.
- `g2b-ingest`는 quota block 동안 G2B API 호출 task enqueue를 제한합니다.
- persistent quota block은 내부 Postgres `external_api_quota_blocks`에 저장합니다.

Quota block 해제:

```bash
python3 scripts/ops.py g2b-ingest unblock-quota
```

## Checkpoint Reset

2023년 1월 1일부터 다시 수집하려면 `g2b-ingest`를 멈춘 뒤 checkpoint를 삭제합니다.

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml stop g2b-ingest
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml exec postgres psql -U postgres -d postgres -c "DELETE FROM checkpoints WHERE name LIKE 'g2b_ingest:%' OR name = 'service:g2b_ingest_bid_list_collect';"
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml start g2b-ingest
```

CLI helper:

```bash
python3 scripts/ops.py g2b-ingest reset-checkpoint
```

## Dashboard

Platform dashboard:

```text
http://localhost:8080
```

API:

```bash
curl http://localhost:8000/dashboard/apps/g2b_ingest/summary
```

Dashboard에서 보는 주요 항목:

- raw table count
- task state count
- open issues
- retry due
- oldest open tasks
- recent task states
- quota block
- backfill progress

## Development Notes

- G2B 판단 로직은 `apps/g2b_ingest` 아래에 둡니다.
- generic worker payload에는 URL, params, target, metadata만 전달합니다.
- `services/*`나 `shared/*`에는 G2B 전용 필드, schema, branching을 추가하지 않습니다.
