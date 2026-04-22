## G2B Summary

`g2b-summary-api`는 MinIO/S3에 있는 파일을 읽고 텍스트 추출과 mock LLM 생성을 연결하는 LangGraph 샘플 API 서비스입니다.

현재 목적은 실제 LLM 호출이 아니라 구조 검증입니다.

- FastAPI polling API 제공
- LangGraph graph로 실행 계획과 payload 생성
- `extract-text-worker`로 텍스트 추출
- `serve-llm-worker`로 mock generation
- 상태와 결과는 Postgres에 저장
- 공통 Qdrant vector DB 사용 준비

## Service

Compose service:

- `g2b-summary-api`
- `qdrant`

Manifest:

- `apps/g2b_summary/manifests/app.json`
- `apps/g2b_summary/manifests/services/g2b_summary_api.json`

Runtime:

- `service_type`: `api`
- FastAPI middleware: `ServiceRequestMiddleware`
- host port: `${G2B_SUMMARY_API_HOST_PORT:-8010}`
- Qdrant HTTP port: `${QDRANT_HOST_PORT:-6333}`
- Qdrant gRPC port: `${QDRANT_GRPC_HOST_PORT:-6334}`
- Qdrant storage: `${QDRANT_DATA_DIR:-./qdrant/storage}`

로그:

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f g2b-summary-api
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f extract-text-worker
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml logs -f serve-llm-worker
```

## Environment

Env files:

- `apps/g2b_summary/infra/env/api.env`
- `apps/g2b_summary/infra/env/worker.env`

주요 설정:

```env
G2B_SUMMARY_DATABASE_URL=postgresql://postgres:postgres@postgres:5432/postgres
G2B_SUMMARY_S3_ENDPOINT=minio:9000
G2B_SUMMARY_S3_ACCESS_KEY=minioadmin
G2B_SUMMARY_S3_SECRET_KEY=minioadmin
G2B_SUMMARY_S3_BUCKET=g2b-summary
G2B_SUMMARY_S3_SECURE=false
G2B_SUMMARY_QDRANT_URL=http://qdrant:6333
G2B_SUMMARY_QDRANT_GRPC_URL=http://qdrant:6334
G2B_SUMMARY_QDRANT_COLLECTION=g2b_summary
```

## Flow

1. `POST /summary/jobs`가 `bucket`, `object_key`를 받아 summary job을 생성합니다.
2. API 서비스가 LangGraph graph로 실행 계획과 worker payload를 만들고 job 상태를 `extracting`으로 저장합니다.
3. API 서비스가 `extract.text.run`을 `extract:text` 큐에 enqueue합니다.
4. `extract-text-worker`가 MinIO/S3에서 파일을 읽고 텍스트를 추출해 `summary.extracted_texts`에 저장합니다.
5. 클라이언트가 `GET /summary/jobs/{job_id}` 또는 events endpoint를 polling합니다.
6. API 서비스가 추출 완료를 감지하면 상태를 `summarizing`으로 바꾸고 `serve.llm.generate`를 `serve:llm` 큐에 enqueue합니다.
7. `serve-llm-worker`가 추출 텍스트를 읽고 mock generation 결과를 `summary.results`에 저장합니다.
8. polling API가 결과 존재를 감지하면 상태를 `succeeded`로 바꿉니다.

중요한 경계:

- generic workers는 다음 task를 enqueue하지 않습니다.
- generic workers는 `summary.jobs` 상태 lifecycle을 직접 업데이트하지 않습니다.
- `g2b-summary-api`가 orchestration과 상태 전이를 담당합니다.

## API

Health:

```bash
curl http://localhost:8010/health/live
```

Job 생성:

```bash
curl -X POST http://localhost:8010/summary/jobs \
  -H 'content-type: application/json' \
  -d '{"object_key":"samples/notice.txt"}'
```

상태와 결과 polling:

```bash
curl http://localhost:8010/summary/jobs/<job_id>
curl http://localhost:8010/summary/jobs/<job_id>/events
```

`bucket`을 생략하면 `G2B_SUMMARY_S3_BUCKET` 값을 사용합니다.

## LangGraph

Graph 정의는 [service/graph.py](service/graph.py)에 있습니다.

현재 graph nodes:

- `extract_text`
- `serve_llm`
- `load_result`

Graph는 현재 worker task를 직접 실행하지 않고, API가 실행 계획과 payload를 구성하는 데 사용합니다. node 이름과 graph state key는 `snake_case`를 사용합니다.

서비스 파일 구조:

- `service/main.py`: FastAPI route와 middleware
- `service/graph.py`: LangGraph DAG/state 정의
- `service/steps.py`: graph node step과 plan 생성
- `service/payloads.py`: generic worker payload 생성
- `service/orchestration.py`: job lifecycle, polling sync, queue enqueue
- `service/constants.py`: queue/task 상수
- `service/schemas.py`: API request schema

## Worker Tasks

Text extraction:

- queue: `extract:text`
- task: `extract.text.run`
- implementation: `services/extract/tasks/text.py`

Mock LLM generation:

- queue: `serve:llm`
- task: `serve.llm.generate`
- implementation: `services/llm/tasks/llm.py`

## Tables

Schema source:

- `apps/g2b_summary/sql/schema.sql`

Tables:

- `summary.jobs`
  - job 상태, source object, callback URL, error
- `summary.extracted_texts`
  - 추출 텍스트, char count, metadata
- `summary.results`
  - mock generation output, model, prompt version, raw result
- `summary.job_events`
  - job lifecycle event log

## Vector Store

`qdrant`는 공통 platform vector DB입니다. `g2b_summary`는 앱별 collection을 사용합니다.

- image: `qdrant/qdrant:v1.13.4`
- internal HTTP URL: `http://qdrant:6333`
- default collection env: `G2B_SUMMARY_QDRANT_COLLECTION=g2b_summary`
- host dashboard/API: `http://localhost:6333/dashboard`

현재 summary skeleton은 Qdrant를 기동하고 env를 주입하는 단계까지 반영되어 있습니다. embedding 생성, collection 생성, vector upsert/search는 다음 단계에서 `g2b_summary` orchestration 또는 별도 embedding worker에 연결하면 됩니다.

## Current Limitations

- 실제 LLM provider를 호출하지 않습니다.
- callback URL은 metadata로 보관하지만 실제 callback dispatch는 아직 없습니다.
- polling 시점에 다음 task enqueue와 상태 전이를 수행합니다.
- 대용량 파일 처리, OCR, chunking, streaming generation은 아직 skeleton 범위 밖입니다.

## Development Notes

- summary job lifecycle은 `apps/g2b_summary`에서 관리합니다.
- text extraction과 LLM serving worker에는 summary-specific 상태 전이를 넣지 않습니다.
- 실제 LLM 연동 시에도 `serve.llm.generate` payload는 provider/model/prompt/options 중심으로 유지하고, 앱-specific 판단은 API service에 둡니다.
