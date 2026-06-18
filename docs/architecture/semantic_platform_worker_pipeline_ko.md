# Semantic Platform Worker Pipeline

## 목적

이 문서는 `semantic_platform`의 source onboarding worker pipeline을 정의한다.

핵심 목표는 다음과 같다.

- source 업로드 또는 workspace 시작 후 worker가 초안을 끝까지 순차 생성한다
- 사람 검토는 초안 생성 이후에 이뤄진다
- orchestration은 `Prefect`, 필요한 stage 내부 LLM reasoning은 `LangGraph`가 담당한다
- `bundle`은 검토 컨테이너이고, 실제 승인/반영 단위는 proposal item이다

즉 onboarding pipeline은 아래처럼 본다.

```text
Source upload / Start Workspace
  -> Prefect flow start
  -> LangGraph stage draft generation
  -> assets / structures / registry / binding proposals draft
  -> workspace review-ready
  -> human review
  -> publish approved items
```

## 핵심 원칙

### 1. worker는 stage draft를 끝까지 생성한다

사람 검토가 다음 stage draft 생성을 막지 않는다.

잘못된 구조:

```text
stage 1 draft
  -> human review
  -> stage 2 draft
```

권장 구조:

```text
stage 1 draft
  -> stage 2 draft
  -> stage 3 draft
  -> proposal bundle draft
  -> human review
```

### 2. orchestration과 reasoning을 분리한다

- `Prefect`
  - run 시작/재개
  - stage 실행 순서
  - retry / timeout / failure state
  - operator-triggered resume
- `LangGraph`
  - semantic mapping draft 생성
  - proposal draft 생성
  - ambiguity가 큰 discovery / structure extraction 보조

### 3. LLM mode는 stage 경계에서 명시적으로 처리한다

지원 모드:

- `disabled`
  - external LLM 호출 없음
  - heuristic / skipped draft 사용
- `codex_manual`
  - worker가 `manual_llm_request` artifact 생성
  - Codex가 `manual_llm_response` 를 명시적으로 주입
  - worker resume 후 다음 stage 진행
- `openai`
  - OpenAI API 호출 경로
  - 현재는 stage별로 순차 구현

### 4. parser-first, LLM-assist 원칙을 따른다

모든 stage를 LLM 중심으로 설계하지 않는다.

- 가능한 곳은 deterministic parser / extractor 우선
- ambiguity가 큰 곳만 LLM 보조
- semantic meaning 판단은 LLM 중심

즉 source 형태에 따라 실행 전략이 달라진다.

- `structured`
  - OpenAPI
  - Swagger
  - JSON Schema
  - well-formed CSV
- `semi_structured`
  - HTML docs
  - table-heavy docs
  - mixed API references
- `document_heavy`
  - PDF
  - prose-heavy manuals
  - irregular tables / screenshots

### 5. source는 입력물이고 결과물은 workspace 산출물이다

source 자체는 다음만 가진다.

- metadata
- uploaded file / reference URI
- evidence snapshot seed

아래는 worker 산출물이다.

- assets
- access paths
- structures / fields
- mappings
- variants
- operations / bindings
- proposal drafts

### 6. worker는 dependency-aware draft를 생성해야 한다

worker는 끝까지 초안을 생성할 수 있지만, binding draft는 언제나
meaning/model dependency를 명시해야 한다.

핵심 규칙:

- `semantic type`, `canonical model`은 registry authoring 결과물이다
- `field mapping`, `control semantics`, `variant`, `capability binding`은
  source binding 결과물이다
- source binding draft는 approved registry가 없으면 완료될 수 없다
- rejected / renamed / merged / split meaning proposal을 참조하던 binding
  draft는 `blocked` 또는 `needs_rebase` 상태가 된다

draft / proposal payload에 필요한 최소 필드:

- `depends_on_proposal_ids`
- `resolution_basis`
  - `approved`
  - `proposed`
  - `missing`
- `dependency_status`
  - `ready`
  - `blocked`
  - `needs_rebase`

### 7. bundle은 검토 단위이고 atomic approval unit이 아니다

- UI에서는 `proposal bundle` 단위로 검토한다
- 내부 모델은 proposal item 단위 승인 상태를 유지한다
- publish는 승인된 item만 반영한다

## Registry Layers 와 Workflow Phases

worker가 다루는 registry는 아래 세 층으로 구분한다.

### 1. Meaning Registry

- semantic type
- glossary/business meaning
- alias
- representation constraint

### 2. Canonical Model

- entity
- canonical attribute
- relation
- identity system

### 3. Source Binding

- field mapping
- control semantics
- operation variant
- capability binding

binding layer는 meaning / canonical layer에 의존할 수 있고, 이 의존 관계는
명시적으로 저장되어야 한다.

하지만 operator workflow는 아래 두 단계로 묶는 편이 낫다.

### 1. Semantic Model

- meaning registry
- canonical model

이 두 registry는 초안 생성 시 분리될 수 있지만, reviewer는 같은 semantic
model 승인 단계에서 함께 보는 것이 자연스럽다.

### 2. Source Binding

- field mapping
- control semantics
- operation variant
- capability binding

binding은 항상 최신 승인 semantic model snapshot을 입력으로 다시 생성되어야
한다.

## 시스템 경계

### Admin API

역할:

- source upload
- workspace start
- pause / cancel / resume
- worker flow trigger
- review / publish endpoints

### Prefect Worker

역할:

- onboarding run orchestration
- stage 실행 상태 관리
- LangGraph stage runner 호출
- stage result 저장

### LangGraph

역할:

- stage별 reasoning graph 실행
- LLM-assisted draft artifact 생성
- evidence refs / rationale / confidence 부여

### Repository

역할:

- onboarding run
- evidence snapshot
- assets / access paths
- fields / mappings
- work queue tasks
- proposal bundles / proposal items
저장

## 실행 흐름

### 1. source upload 또는 workspace start

API는 다음을 만든다.

- `onboarding_run`
- `evidence_snapshot`
- `proposal_bundle`
- stage task skeleton

그리고 `run_onboarding_pipeline(run_id)` Prefect flow를 시작한다.

### 2. Prefect flow

`run_onboarding_pipeline(run_id)` 는 아래를 순차 실행한다.

1. load run context
2. run `source_evidence_review`
3. run `asset_discovery`
4. run `semantic_model_drafting`
5. wait for `semantic_model_approval`
6. run `binding_drafting`
7. wait for `binding_approval`
8. run `proposal_review`
9. finalize workspace as review-ready

중간에 사람이 끼어들지 않는다.

### 3. review

worker가 끝나면 workspace는 review-ready 상태가 된다.

사람은:

- stage별 draft 확인
- proposal item approve / reject / defer
- publish readiness 확인

을 수행한다.

### 4. codex_manual interruption

`LLM_MODE=codex_manual` 인 stage에서는 worker가 아래처럼 동작한다.

1. stage 입력 context 수집
2. `manual_llm_request` 생성
3. run/task 상태를 `waiting_manual_llm` 로 전환
4. API로 `manual_llm_response` 수신
5. worker resume
6. draft/proposal 저장 후 다음 단계 진행

## 상태 모델

### run 상태는 두 축으로 본다

#### generation status

- `queued`
- `running`
- `drafts_ready`
- `generation_failed`
- `cancelled`

#### review status

- `not_started`
- `in_review`
- `partially_approved`
- `approved`
- `published`

### stage 상태도 두 축으로 본다

#### draft generation status

- `queued`
- `running`
- `ready`
- `failed`

#### review status

- `not_started`
- `in_review`
- `approved`
- `rejected`
- `deferred`

### task 상태

모든 task는 기본적으로 AI draft를 가진다.

- `queued`
- `drafting`
- `draft_ready`
- `needs_review`
- `completed`
- `blocked`
- `cancelled`

task에는 최소 아래가 있어야 한다.

- `draft_payload`
- `draft_rationale`
- `draft_confidence`
- `evidence_refs`
- `recommended_action`

### dependency 상태

- `ready`
- `blocked`
- `needs_rebase`

## stage 정의

### 1. `source_review`

입력:

- source metadata
- uploaded file / reference URI
- seed evidence snapshot

출력:

- source summary draft
- provenance summary
- review notes scaffold
- `ingestion_strategy`
  - `structured`
  - `semi_structured`
  - `document_heavy`

### 2. `asset_discovery`

입력:

- source file/spec
- source type
- evidence snapshot

출력:

- `execution_assets`
- `execution_access_paths`
- asset discovery tasks

예:

- OpenAPI: endpoint groups, paths, methods
- CSV: file/table/sheet asset

실행 전략:

- `structured`
  - parser / deterministic extraction 우선
- `semi_structured`
  - parser 결과 생성 후 LLM 보정 허용
- `document_heavy`
  - LLM assist 적극 사용

즉 이 stage는 무조건 LangGraph가 아니라, source 전략에 따라 LangGraph를
선택적으로 호출한다.

### 3. `structure_review`

입력:

- assets
- access paths
- source document sections

출력:

- extracted fields / paths
- scope classification
  - input
  - output
  - control
- datatype / required hints

실행 전략:

- `structured`
  - parser / schema-based extraction 우선
- `semi_structured`
  - parser + LLM scope/classification 보조
- `document_heavy`
  - LLM이 field/path 후보와 evidence ref 생성 보조

### 4. `registry_gap_detection`

입력:

- extracted fields
- control field candidates
- existing semantic registry
- canonical model

출력:

- registry gap 목록
- existing registry로 바로 binding 가능한 field 목록
- 새 semantic type / canonical attribute가 필요한 field 목록

### 5. `semantic_type_authoring`

입력:

- extracted fields
- existing semantic registry

출력:

- semantic type candidates
- alias / description / constraint draft

### 6. `canonical_model_authoring`

입력:

- semantic type candidates
- existing canonical model
- extracted source evidence

출력:

- canonical entity candidates
- canonical attribute candidates
- relation / identity draft

### 7. `mapping_authoring`

입력:

- extracted fields
- existing mappings
- approved or proposed registry

출력:

- mapping kind candidates
- transform / enum draft
- dependency-aware mapping proposals

### 8. `variant_and_binding_authoring`

입력:

- control fields
- observed values
- operation descriptions

출력:

- control semantic candidates
- variant split candidates
- fixed semantic/raw argument candidates

출력:

- capability / binding candidates
- variant binding candidates

### 9. `proposal_review`

입력:

- all prior stage drafts

출력:

- proposal bundle draft
- proposal item list
- rationale summary
- reviewer checklist

### 10. `publish_readiness`

입력:

- proposal approval state
- unresolved blockers

출력:

- readiness summary
- publish blockers
- final checklist

## Prefect 설계

### flow

권장 flow 이름:

- `run_onboarding_pipeline(run_id)`

### task 구조

권장 Prefect task:

- `load_run_context`
- `run_stage_source_evidence_review`
- `run_stage_asset_discovery`
- `run_stage_structure_review`
- `run_stage_registry_gap_detection`
- `run_stage_semantic_type_authoring`
- `run_stage_canonical_model_authoring`
- `run_stage_mapping_authoring`
- `run_stage_variant_and_binding_authoring`
- `run_stage_proposal_review`
- `finalize_workspace_generation`

### Prefect 책임

- stage 시작/종료 상태 반영
- retry / timeout
- stage failure 시 run 상태 갱신
- cancel / pause / resume 처리
- source strategy 확인 후 parser-only 또는 LangGraph-assisted 경로 선택

## LangGraph 설계

### stage별 graph 파일 권장 위치

```text
services/semantic_platform/internal/ingestion/langgraph/
  source_review.py
  asset_discovery.py
  structure_review.py
  registry_gap_detection.py
  semantic_type_authoring.py
  canonical_model_authoring.py
  mapping_authoring.py
  variant_and_binding_authoring.py
  proposal_review.py
```

주의:

- `asset_discovery.py`
- `structure_review.py`

는 항상 호출되는 graph가 아니다.
structured source에서는 호출하지 않고 parser-only 경로를 탈 수 있다.

### graph 입력 state 공통 필드

- `run_id`
- `source_id`
- `source`
- `evidence_snapshot`
- `existing_assets`
- `existing_fields`
- `existing_mappings`
- `existing_semantic_registry`
- `existing_canonical_model`

### graph 출력 state 공통 필드

- `draft_artifacts`
- `created_assets`
- `created_access_paths`
- `created_fields`
- `created_proposals`
- `task_updates`
- `evidence_refs`
- `confidence_summary`
- `errors`

## 저장 모델

### draft artifacts

task별 draft 저장은 최소 아래를 포함해야 한다.

- `task_id`
- `stage`
- `draft_payload`
- `draft_rationale`
- `draft_confidence`
- `evidence_refs`

### working state

draft 생성 과정에서 다음이 채워질 수 있다.

- `execution_assets`
- `execution_access_paths`
- `execution_operations`
- `operation_fields`

### proposal state

semantic meaning과 publish candidate는 proposal로 간다.

- semantic types
- canonical entities/attributes
- mappings
- variants
- bindings
- capability links

## review / publish 모델

### review 단위

- UI: `proposal bundle`
- 승인 모델: `proposal item`

### publish 규칙

publish는 아래 조건을 만족할 때만 가능하다.

- required proposal items approved
- failed draft 없음
- required mapping coverage 충족
- unresolved blocker 없음

즉 `bundle 전체 승인 = 자동 publish`는 아니다.

## 구현 우선순위

### 1차

- Prefect flow를 full draft generation pipeline으로 개편
- `asset_discovery` parser-first runner 구현
- `structure_review` parser-first runner 구현
- low-confidence / document-heavy source용 LangGraph assist 경로 구현
- workspace preparation progress를 실데이터로 연결

### 2차

- `semantic_mapping` graph 구현
- task draft 자동 생성
- mapping workbench에서 draft review 연결

### 3차

- `controls_and_variants`
- `operation_and_binding_modeling`
- `proposal_review`

## 실제 구현 경로

### 1. 현재 worker scaffold를 orchestration shell로 유지

현재 파일:

- `services/semantic_platform/adapters/worker/deployments.py`

이 파일은 다음 역할만 유지한다.

- Prefect `flow` / `task` 선언
- stage 실행 순서 정의
- stage 시작/종료/실패 상태 반영
- pause / cancel / resume 와 같은 run lifecycle 연계

즉 이 파일 안에 source parsing, field inference, semantic reasoning을 직접
넣지 않는다.

### 2. stage runner를 분리한다

권장 추가 위치:

```text
services/semantic_platform/adapters/worker/flows/
  onboarding_pipeline.py
  stages.py
```

권장 책임:

- `onboarding_pipeline.py`
  - `run_onboarding_pipeline(run_id)` orchestration
- `stages.py`
  - 각 stage runner wrapper
  - repository update
  - LangGraph graph 호출

### 3. LangGraph graph는 ingestion 내부에 둔다

권장 위치:

```text
services/semantic_platform/internal/ingestion/langgraph/
  common.py
  source_review.py
  asset_discovery.py
  structure_review.py
  semantic_mapping.py
  controls_and_variants.py
  operation_and_binding_modeling.py
  proposal_review.py
```

이 계층은 semantic platform 내부 authoring/ingestion concern 이므로
`services/semantic_platform/internal/ingestion` 아래에 두는 것이 맞다.

### 4. Admin API는 trigger/control plane만 담당한다

현재 파일:

- `services/semantic_platform/adapters/admin_api/app/main.py`

이 파일은 다음만 담당한다.

- source upload
- workspace start
- Prefect flow trigger
- run/task 상태 조회
- review / publish endpoint

LLM reasoning이나 graph step 구현을 여기에 넣지 않는다.

## stage별 저장 규칙

### 1. discovery 사실은 working state로 저장한다

아래는 worker가 자동 저장해도 된다.

- `execution_assets`
- `execution_access_paths`
- `execution_operations`
- `operation_fields`
- observed control values
- evidence snapshot refs

이 값들은 source 문서에서 읽어낸 사실 또는 working extraction 결과다.

단, parser 결과와 LLM 보정 결과를 구분할 수 있게 provenance를 남기는 것이
좋다.

예:

- `extraction_method = parser`
- `extraction_method = llm_assist`
- `extraction_confidence`

### 2. semantic meaning은 proposal로 저장한다

아래는 proposal item으로 저장한다.

- semantic type candidate
- canonical attribute link
- field mapping
- transform / enum mapping
- variant candidate
- binding candidate
- capability link

즉 `field -> semantic meaning` 계층은 draft/proposal 없이 곧바로 published
truth가 되면 안 된다.

## generation progress 모델

workspace list 와 workspace detail 이 같은 진행 상태 언어를 쓰려면 worker가
아래 집계를 직접 만들 수 있어야 한다.

### run level

- `preparation_status`
  - `queued`
  - `running`
  - `drafts_ready`
  - `generation_failed`
- `worker_progress_percent`
- `current_worker_step`
- `drafts_ready_count`
- `drafting_count`
- `queued_count`
- `failed_count`

### stage level

- `stage_generation_status`
- `stage_progress_percent`
- `ready_task_count`
- `failed_task_count`

이 값은 dashboard 가 추정하지 말고 worker / repository 집계에서 직접
제공하는 것이 좋다.

또한 progress 집계에는 현재 strategy도 포함할 수 있다.

- `ingestion_strategy`
- `current_execution_mode`
  - `parser_only`
  - `parser_plus_llm`
  - `llm_primary`

## review / approval 모델 상세

### proposal bundle 은 review container 다

reviewer 는 bundle 단위로 진입한다.

하지만 내부 상태는 최소 아래를 유지해야 한다.

- `bundle_status`
- `proposal_item_status`
- `required_for_publish`
- `approval_rationale`
- `approved_by`
- `approved_at`

### publish 는 bundle 통승인이 아니다

publish 시 반영 대상은:

- approved proposal items
- required item coverage 충족 상태
- unresolved blocker 없음

즉 아래 구조를 권장한다.

```text
workspace
  -> proposal bundle
    -> proposal item A approved
    -> proposal item B deferred
    -> proposal item C rejected

publish
  -> approved item만 materialize
```

## 첫 구현 스코프

처음부터 모든 stage를 다 구현하지 않는다.

### milestone 1

- source upload / workspace start
- Prefect flow kick-off
- `asset_discovery` LangGraph
- `structure_review` LangGraph
- progress 집계 저장

성공 기준:

- source 업로드 후 실제 asset / field draft 가 생긴다
- workspace preparation percent 가 실제 worker 상태를 반영한다

### milestone 2

- `semantic_mapping` LangGraph
- mapping draft artifact 생성
- mapping workbench 와 task draft 연결

성공 기준:

- semantic mapping stage에서 unmapped field 별 AI draft 를 볼 수 있다

### milestone 3

- `controls_and_variants`
- `operation_and_binding_modeling`
- `proposal_review`
- publish readiness generation

성공 기준:

- bundle review 와 publish readiness 가 실제 worker 산출물 기반으로 동작한다

## 현재 구현과의 차이

현재 worker는 사실상 stage scaffold 수준이다.

- source를 실제로 읽지 않음
- asset discovery를 하지 않음
- field extraction을 하지 않음
- draft generation을 하지 않음

즉 현재의 `Prefect flow`는 orchestration placeholder이고,
이 문서는 그것을 실제 worker pipeline으로 발전시키기 위한 목표 구조다.

## 관련 문서

- 개요: [semantic_platform_overview_ko.md](/workspace/docs/architecture/semantic_platform_overview_ko.md)
- 대시보드: [semantic_platform_dashboard_ko.md](/workspace/docs/architecture/semantic_platform_dashboard_ko.md)
- 구현 현황: [semantic_platform_implementation_ko.md](/workspace/docs/architecture/semantic_platform_implementation_ko.md)
