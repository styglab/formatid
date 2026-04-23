## G2B Pipeline Worker

`g2b-pipeline-worker`는 Redis graph-run request queue를 consume해서 `document_process_graph`를 실행하는 triggered pipeline 앱입니다.

담당 범위:

- triggered graph request consume
- graph run retry / DLQ 위임
- document processing graph 실행

## Service

Compose service:

- `g2b-pipeline-worker`

Manifest:

- `apps/g2b/pipeline_worker/manifests/app.json`
- `apps/g2b/pipeline_worker/manifests/services/g2b_pipeline_worker.json`

Runtime:

- runner: `apps.g2b.pipeline_worker.app.service.run_worker`
- trigger: `triggered`
- queue: `G2B_DOCUMENT_PROCESS_GRAPH_QUEUE`
- graph: `document_process_graph`

로그:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f g2b-pipeline-worker
```

## Environment

Env files:

- `apps/g2b/pipeline_worker/infra/env/pipeline_worker.env`

주요 설정:

```env
G2B_DOCUMENT_PROCESS_GRAPH_TRIGGERED_ENABLED=true
G2B_DOCUMENT_PROCESS_GRAPH_QUEUE=g2b:pipeline:document-process
G2B_DOCUMENT_PROCESS_GRAPH_MAX_ATTEMPTS=3
```

## Graph

Graph와 trigger는 분리합니다. Graph는 `apps/g2b/pipeline_worker/app/graph/registry.py`에 등록하고,
triggered queue consume 공통 로직은 `core/runtime/graph_runtime`을 사용합니다.

Graph 구현:

- `apps/g2b/pipeline_worker/app/contracts/document_process.py`
- `apps/g2b/pipeline_worker/app/graph/registry.py`
- `apps/g2b/pipeline_worker/app/graph/document_process_graph.py`
- `apps/g2b/pipeline_worker/app/service/run_worker.py`
- `core/runtime/graph_runtime`

Graph 등록은 `core.runtime.graph_runtime.create_graph_definition`을 사용합니다.
앱 graph는 graph builder와 initial state만 제공하고 runtime context adapter는 공통 모듈이 처리합니다.
