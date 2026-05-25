# Semantic Platform Dashboard 설계

## 목적

Semantic Platform Dashboard는 단순 catalog 조회 화면이 아니다.
원천 문서가 semantic catalog로 변환되고, proposal review를 거쳐 planner와
MCP runtime에서 사용할 수 있는 상태가 되는 전체 과정을 운영자가 관리하는
control plane UI다.

핵심 흐름은 다음과 같다.

```text
Source Registry
  -> Ingestion Graph
  -> Evidence Snapshot
  -> Proposal Review
  -> Approved Catalog
  -> Capability Embedding
  -> Planner / MCP Runtime
```

Dashboard는 이 흐름에서 다음 질문에 답해야 한다.

```text
어떤 source가 등록되어 있는가?
어떤 source가 아직 catalog화되지 않았는가?
어떤 source가 stale 상태인가?
ingestion graph는 실행 중인가, 실패했는가?
어떤 proposal이 생겼고 승인되었는가?
승인된 catalog item은 어떤 source에서 왔는가?
planner가 사용할 준비가 되었는가?
```

## 메뉴 구조

추천 메뉴는 다음과 같다.

```text
Sources
Catalog
Proposals
Ingestion Runs
Secrets
Planner Tests
```

### Sources

원천 문서 관리 화면이다. 파일 목록이 아니라 source catalog readiness
상태판으로 동작해야 한다.

주요 기능:

```text
source upload
source metadata edit
source archive
source file delete
catalog status 확인
not cataloged / stale / failed 필터
source 선택 실행
bulk ingestion 실행
evidence / proposal / catalog lineage 보기
purge preview
```

테이블 컬럼:

```text
checkbox
provider
source_key
title
file_name
source_status
catalog_status
last_ingestion_status
proposal_count
pending_proposal_count
approved_capability_count
operation_count
contract_count
variant_count
embedding_status
last_run_at
actions
```

`catalog_status` 추천 값:

```text
not_ingested
ingestion_failed
proposal_pending
proposal_rejected
partially_approved
cataloged
embedded
ready
stale
```

`stale` 판단:

```text
source.sha256 != last_ingested_sha256
```

### Catalog

승인된 semantic source of truth를 보는 화면이다.

표시 대상:

```text
capabilities
semantic_types
entities
resources
operations
operation_contracts
operation_variants
field_mappings
capability_implementations
semantic_join_rules
planning_examples
capability_documents
```

각 catalog item은 lineage를 보여야 한다.

```text
catalog item
  -> source_document_id
  -> proposal_id
  -> evidence_snapshot_id
  -> operation_id / variant_id
```

Catalog 화면의 목적은 수정이 아니라 추적과 검증이다. 직접 편집은 초기에는
최소화하고, 변경은 proposal/apply 흐름을 통하는 것을 기본으로 한다.

### Proposals

Ingestion graph가 생성한 변경 요청을 검토하는 화면이다.

주요 기능:

```text
pending proposal list
proposal detail
evidence 보기
기존 catalog와 비교
apply
reject
affected catalog item 보기
```

Proposal은 capability 단위 review unit이어야 한다.

```text
proposal.<source_document_id>.<capability_id>.review
```

### Ingestion Runs

실행 상태와 로그를 보는 화면이다.

현재 구조에서는 Prefect를 execution tracking의 source of truth로 사용한다.
Semantic Platform DB는 source/catalog/proposal/evidence lineage를 가진다.

역할 분리:

```text
Prefect
  -> flow_run_id
  -> status
  -> started_at / finished_at
  -> task state
  -> logs
  -> retry / failure

Semantic Platform DB
  -> source_id
  -> source sha256
  -> proposal_ids
  -> evidence_snapshot_id
  -> catalog impact summary
  -> embedding result
  -> prefect_flow_run_id link
```

처음에는 source별 flow run을 생성하고, 여러 source 실행은 run group으로
묶는다.

```text
run_group_id
  -> source A flow_run_id
  -> source B flow_run_id
  -> source C flow_run_id
```

이 방식의 장점:

```text
source별 실패 분리
source별 retry 가능
Prefect UI/log와 잘 맞음
Dashboard에서 group으로 묶어 보기 쉬움
```

### Secrets

API key 같은 secret을 관리하는 화면이다.

원칙:

```text
source registry에는 secret 값 저장 금지
source registry에는 secret_ref만 저장
secret 값은 저장/수정 시에만 입력 가능
조회 API는 masked metadata만 반환
evidence/proposal/log에는 secret redaction 필수
```

예:

```json
{
  "auth": {
    "secret_refs": [
      "secret.koreanexim.exchange_rate.auth_key",
      "secret.data_go_kr.service_key"
    ],
    "parameter_names": ["authkey", "serviceKey"]
  }
}
```

Secret resolution 우선순위:

```text
source-specific secret_ref
provider-level secret_ref
global env fallback
```

Env only 방식은 컨테이너 restart가 필요하다. Dashboard에서 운영형 secret
관리를 하려면 API/DB 기반 secret store가 필요하다.

### Planner Tests

질문이 capability retrieval과 planner에서 어떻게 처리되는지 확인하는
화면이다.

표시 항목:

```text
query
retrieved capabilities
selected variant_id / operation_id
semantic arguments
missing arguments
not_found reason
execution readiness
planner output JSON
```

자연어 답변보다 MCP/agent가 쓰기 쉬운 구조화 JSON이 우선이다.

## Source Registry

Source Registry는 운영 관리의 기준이다. `sources/manifest.json`은 초기
bootstrap/import 용도로 남길 수 있지만, 대시보드 운영은 DB 기반 registry가
기준이 되어야 한다.

추천 필드:

```text
source_id
provider
provider_name_ko
source_key
title
file_path
file_name
sha256
status
auth_secret_refs
auth_parameter_names
last_ingested_sha256
last_prefect_flow_run_id
created_at
updated_at
```

상태 값:

```text
draft
active
archived
deprecated
blocked
```

파일 저장 위치:

```text
sources/raw/<provider>/<source_key>/<original_filename>
```

SHA는 사람용 경로에 쓰지 않는다. SHA는 변경 감지와 lineage metadata에만
사용한다.

## Source 삭제 정책

Source 삭제는 파일 삭제와 catalog 삭제가 다르다. 기본은 안전한 soft action
이어야 한다.

### Archive Source

```text
source status를 archived로 변경
catalog는 유지
planner 사용 여부는 catalog item status에 따름
```

### Delete Source File

```text
원본 파일만 삭제
catalog는 유지
evidence 재검증 불가 상태 표시
```

### Purge Source Catalog

```text
해당 source에서만 온 catalog item을 deactivate
다른 source가 참조하는 semantic type/entity는 삭제 금지
approved capability는 기본 hard delete가 아니라 inactive/deprecated
```

Purge는 preview가 먼저 필요하다.

```http
POST /sources/{source_id}/purge-preview
POST /sources/{source_id}/purge
```

Preview 응답 예:

```json
{
  "source_id": "source.nts.business_status",
  "would_deactivate": {
    "capabilities": 2,
    "resources": 1,
    "operations": 2,
    "contracts": 2,
    "variants": 2,
    "field_mappings": 40,
    "proposals": 2,
    "evidence_snapshots": 1
  },
  "blocked": [
    {
      "item_type": "semantic_type",
      "item_id": "business_registration_number",
      "reason": "referenced_by_other_sources"
    }
  ]
}
```

## API 설계

Dashboard/admin API는 `/semantic` prefix를 붙이지 않는다. `/semantic/*`는
외부 runtime/client가 쓰는 semantic catalog/contract API로 유지한다.

### Source API

```http
GET    /sources
POST   /sources
POST   /sources/upload
GET    /sources/{source_id}
PATCH  /sources/{source_id}
DELETE /sources/{source_id}
POST   /sources/{source_id}/archive
DELETE /sources/{source_id}/file
POST   /sources/{source_id}/purge-preview
POST   /sources/{source_id}/purge
```

### Ingestion API

```http
POST /ingestion-runs
GET  /ingestion-runs
GET  /ingestion-runs/{run_group_id}
GET  /ingestion-runs/{flow_run_id}/logs
```

`POST /ingestion-runs` 예:

```json
{
  "source_ids": [
    "source.nts.business_status",
    "source.pps.g2b_contract_info"
  ],
  "apply": false,
  "force": false,
  "llm_mode": "codex_manual"
}
```

응답 예:

```json
{
  "run_group_id": "ingestion_group.20260525.001",
  "runs": [
    {
      "source_id": "source.nts.business_status",
      "prefect_flow_run_id": "..."
    }
  ]
}
```

### Secret API

```http
GET    /secrets
POST   /secrets
PATCH  /secrets/{secret_id}
DELETE /secrets/{secret_id}
```

`GET /secrets`는 원문 값을 반환하지 않는다.

### Catalog/Proposal API

기존 catalog/proposal API는 유지하되 lineage와 pagination을 강화한다.

```http
GET /catalog/sections/{section}?limit=100&offset=0&q=
GET /proposals
GET /proposals/{proposal_id}
POST /proposals/{proposal_id}/apply
POST /proposals/{proposal_id}/reject
```

## 구현 순서

추천 MVP 순서:

```text
1. Source Registry DB/API
2. Source upload API
3. Source catalog_status 계산 API
4. Prefect flow run 생성 wrapper API
5. Ingestion run group link 저장
6. Sources dashboard table/filter/bulk action
7. Evidence/proposal/catalog lineage 연결
8. Secrets API
9. Purge preview
10. Planner Tests 화면
```

초기에는 Prefect task step을 Semantic Platform DB에 복제하지 않는다. Run
state와 logs는 Prefect API에서 조회하고, Semantic Platform DB에는 source와
catalog lineage에 필요한 link만 저장한다.

## 경계 원칙

```text
Source는 원천이다.
Proposal은 변경 요청이다.
Catalog는 승인된 결과다.
Planner는 Catalog만 사용한다.
MCP는 승인된 execution contract만 실행한다.
Prefect는 실행 상태를 추적한다.
Semantic Platform DB는 의미/lineage를 추적한다.
```

Dashboard는 이 경계를 흐리지 않고, 운영자가 source에서 planner-ready
catalog까지의 상태를 한 화면에서 추적하고 제어할 수 있게 하는 것이 목적이다.
