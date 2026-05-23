# Semantic Platform 한글 매뉴얼

## 목적

`services/semantic_platform`는 공공 API 명세를 읽어서 의미 기반 catalog를 만들고, 사용자의 질문을 실행 가능한 semantic execution DAG로 계획하는 서비스다.

이 플랫폼은 단순한 공공데이터 API wrapper가 아니다. 목표는 다음에 가깝다.

```text
Semantic Agentic Data Platform
LLM-native Public Data Orchestration Platform
```

핵심은 `LLM이 API를 직접 호출하게 하는 것`이 아니라, LLM이 안정적으로 의미를 이해하고 결정 가능한 실행 구조를 만들도록 하는 것이다. MCP는 실행 계층이고, 의미 계층은 semantic platform이 담당한다.

전체 흐름은 다음과 같다.

```text
Raw sources
  -> source ingestion graph
  -> Postgres semantic catalog
  -> capability documents
  -> BGE-m3-ko embeddings / pgvector
  -> capability retrieval
  -> planner
  -> semantic execution graph
  -> apps/pubdata_mcp executor
  -> provider API
  -> structured result
```

역할 경계는 명확하게 유지한다.

```text
services/semantic_platform = 무엇을 실행할지 결정하는 declarative semantic intelligence
apps/pubdata_mcp           = 어떻게 provider API를 호출할지 담당하는 imperative execution runtime
```

`semantic_platform`는 provider HTTP 호출, pagination, retry, API key 처리, provider SDK quirks를 소유하지 않는다. 그런 실행 세부 사항은 `apps/pubdata_mcp`가 담당한다.

## 설계 원칙

```text
MCP / Tool Layer  = 호출 계층
Semantic Layer   = 의미 계층
Planner          = 추론 / DAG 생성
Execution Engine = deterministic 실행
Governance       = review / lineage / 품질 관리
```

중요한 원칙:

```text
API spec embedding만으로는 부족하다.
endpoint가 아니라 capability를 retrieval 단위로 삼는다.
planner는 raw API field가 아니라 semantic argument를 만든다.
executor는 LLM이 아니라 approved contract를 해석하는 deterministic interpreter다.
provider별 표현 차이는 코드가 아니라 operation_contract field rule에 저장한다.
```

## 핵심 Catalog

semantic platform의 catalog는 크게 세 가지 관심사로 나뉜다.

### Capability Catalog

retrieval을 위한 catalog다. 사용자의 질문과 가장 먼저 매칭되는 단위다.

예를 들어 사용자가 `최근 공사 계약`이라고 물으면 검색 대상은 물리 endpoint가 아니라 다음 같은 capability다.

```text
search_construction_contracts
search_construction_contract_service_info
search_construction_contract_delete_history
```

Capability Catalog는 다음 정보를 가진다.

```text
capability id
설명
alias
example
use_when
input semantic types
output semantic types
tags
planning hints
```

Capability id는 provider-neutral이어야 한다. 예를 들어 `pps.search_contracts`가 아니라 `search_contracts`, `search_construction_contracts` 같은 형태를 사용한다.

### Execution Catalog

실행 방법을 담는 catalog다. retrieval의 기본 단위는 아니며, 선택된 capability를 실제 API 호출로 바꾸기 위해 사용된다.

주요 객체는 다음과 같다.

```text
resources
operations
operation_contracts
operation_variants
field_mappings
capability_implementations
endpoint_checks
```

`operation_contract`는 물리 provider operation을 설명한다.

```json
{
  "operation_id": "operation.getCntrctInfoListCnstwk",
  "resource_id": "resource.public_procurement_contract_info_service",
  "provider": "public_procurement_contract_info_service",
  "method": "GET",
  "path": "/getCntrctInfoListCnstwk",
  "request": {
    "query": {
      "inqryBgnDt": {
        "semantic_type": "registration_datetime_range",
        "transform": "date_start"
      },
      "inqryEndDt": {
        "semantic_type": "registration_datetime_range",
        "transform": "date_end"
      }
    }
  }
}
```

`operation_variant`는 같은 물리 endpoint 안에서 control value에 따라 의미가 달라지는 경우를 모델링한다.

```json
{
  "variant_id": "variant.search_construction_contracts.registration_datetime",
  "operation_id": "operation.getCntrctInfoListCnstwk",
  "capability_id": "search_construction_contracts",
  "fixed_semantic_arguments": {
    "inquiry_basis": "registration_datetime",
    "contract_domain": "construction"
  },
  "fixed_raw_arguments": {
    "inqryDiv": "1"
  }
}
```

중요한 원칙은 `pubdata_mcp`가 `공사니까 inqryDiv=1` 같은 추론을 하면 안 된다는 것이다. 그런 의미 결정은 catalog의 variant에 저장되어야 한다.

### Governance Context

ingestion과 review 과정에서 품질을 관리하기 위한 정보다.

```text
naming decision
conflict
lineage
proposal status
merge/deprecation decision
recent proposals
```

runtime planner가 직접 쓰는 tool catalog가 아니라, ingestion LLM이 기존 catalog를 참고하고 중복을 줄이기 위한 context다.

## Storage

Postgres가 semantic platform의 source of truth다. YAML 파일은 현재 catalog의 원천이 아니다.

주요 테이블은 다음과 같다.

```text
sp_source_documents
sp_source_chunks
sp_source_evidence_snapshots
sp_resources
sp_operations
sp_operation_fields
sp_semantic_types
sp_entities
sp_entity_identifiers
sp_capabilities
sp_capability_entity_links
sp_capability_dependencies
sp_capability_documents
sp_capability_document_vectors
sp_operation_contracts
sp_operation_variants
sp_field_mappings
sp_capability_implementations
sp_semantic_join_rules
sp_planning_examples
sp_endpoint_checks
sp_execution_graphs
sp_planner_feedback
sp_proposals
sp_proposal_items
sp_catalog_lineage
```

`sp_capability_documents`는 retrieval-facing 문서다. `sp_capability_document_vectors`는 pgvector 기반 embedding index다.

현재 embedding 서비스는 BGE-m3-ko 모델을 사용한다.

```text
host path:      data/models/embeddings/BGE-m3-ko
container path: /data/models/embeddings/BGE-m3-ko
model:          BGE-m3-ko
dimensions:     1024
```

## Semantic Entity Registry

multi-step orchestration을 안정화하려면 `SemanticType`만으로는 부족하다. 다음 단계에서는 lightweight entity registry와 join rule이 필요하다.

예:

```text
Entity: Business
Identifiers:
  business_registration_number
  corporate_registration_number
  supplier_id

Entity: Contract
Identifiers:
  integrated_contract_number
  contract_number

Join Rule:
  Business.business_registration_number
    -> Contract.business_registration_number
```

초기에는 RDF/OWL-first로 가지 않는다. Postgres catalog 위에 `entity`, `identifier semantic type`, `join rule`, `capability dependency` 정도를 관리하고, 필요하면 나중에 RDF export를 붙인다.

## Ingestion Graph

ingestion graph는 공공 API 명세 파일을 읽어서 review 가능한 proposal을 만들고, 사용자가 apply하면 Postgres catalog에 반영한다.

현재 표준 구현 모듈은 다음이다.

```text
services.semantic_platform.ingestion.graph
```

전체 노드 흐름은 다음과 같다.

```text
read_source
  -> extract_text_node
  -> extract_blocks_node
  -> detect_api_sections_node
  -> extract_structured_evidence_node
  -> load_catalog_context
  -> verify_endpoint_candidates
  -> llm_propose_capability_catalog
  -> llm_propose_execution_catalog
  -> verify_capabilities
  -> keep_passed_verified_capabilities
  -> build_review_proposal
```

`run_source_ingestion(..., apply=True)`로 실행하면 graph 이후에 다음 후처리까지 이어진다.

```text
upsert_source_document
replace_chunks
write_evidence_snapshot
create_capability_proposals
record_variant_endpoint_checks
apply_proposal
rebuild_capability_documents
embed_capability_documents
```

즉 apply까지 수행하면 catalog mutation뿐 아니라 capability document rebuild와 vector embedding까지 실행된다.
proposal은 source 전체 단위가 아니라 capability 단위로 생성된다. id는 다음 형태다.

```text
proposal.<source_document_id>.<capability_id>.review
```

각 capability proposal에는 해당 capability를 실행하거나 설명하는 데 필요한 resource, semantic type, operation, operation field, operation contract, operation variant, field mapping, capability implementation이 함께 들어간다. capability payload의 `provenance`에는 추적을 위해 다음 값이 포함된다.

```text
source_document_id
source_file_name
source_path
source_section_ids
operation_ids
variant_ids
endpoints
evidence_snapshot_id
```

source evidence snapshot은 source 단위로 유지하고, review/apply 단위만 capability로 나눈다.

### 노드 설명

`read_source`는 source file을 읽고 sha256 기반 `source_document_id`를 만든다.

`extract_text_node`는 PDF, text 등에서 text를 추출하고 LLM 입력 크기에 맞게 compact 처리한다.

`extract_blocks_node`는 추출된 text를 block 단위로 나눈다.

`detect_api_sections_node`는 API operation으로 보이는 section을 찾는다.

`extract_structured_evidence_node`는 field table, example, control field 후보를 추출한다.

`load_catalog_context`는 기존 capability catalog, execution summary, governance context를 묶어 LLM에게 제공한다. 이 단계는 runtime planner가 아니다.

`verify_endpoint_candidates`는 문서에서 찾은 endpoint 후보가 실제로 호출 가능한지 probe한다.

`llm_propose_capability_catalog`는 capability, semantic type 등 retrieval-facing catalog 후보를 만든다.

`llm_propose_execution_catalog`는 operation contract, operation variant, field mapping, capability implementation 후보를 만든다.

`verify_capabilities`는 variant의 sample semantic arguments를 사용해 endpoint 검증을 수행한다.

`keep_passed_verified_capabilities`는 검증 통과 variant 중심으로 proposal 대상만 남긴다.

`build_review_proposal`은 catalog에 바로 반영하지 않고 capability별 `pending_review` proposal을 만든다.

### Probe Key Env

endpoint 후보 검증은 operation contract가 만들어지기 전 단계라, API key가 필요하면 env에서 읽는다. 문서별 key를 먼저 보고, 없으면 전역 key를 사용한다.

```env
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_<SOURCE_SHA8>=...
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER_<SOURCE_SHA8>=serviceKey

SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY=...
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER=ServiceKey
```

`<SOURCE_SHA8>`은 source id 또는 sha256의 앞 8자리 hex를 대문자로 쓴 값이다. 예를 들어 `source.c08195ad...` 문서는 다음처럼 둔다.

```env
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_C08195AD=...
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER_C08195AD=serviceKey
```

POST API는 body도 필요할 수 있다.

```env
SEMANTIC_PLATFORM_CANDIDATE_PROBE_BODIES={"status":{"b_no":["0000000000"]}}
```

key 값은 env에만 둔다. evidence, proposal, endpoint check에는 redaction된 값만 남겨야 한다.

## LLM Mode

LLM 동작은 API key 값으로 제어하지 않는다. `OPENAI_API_KEY`는 secret일 뿐이다.

모드는 다음 환경변수로 제어한다.

```text
LLM_MODE
SEMANTIC_PLATFORM_LLM_MODE
```

지원 모드는 다음과 같다.

```text
disabled
codex_manual
openai
```

`disabled`는 외부 LLM을 호출하지 않고 명시적인 skipped/not-generated 결과를 반환한다.

`openai`는 OpenAI API를 호출하며 `OPENAI_API_KEY`가 필요하다.

`codex_manual`은 외부 LLM을 호출하지 않는다. 대신 Codex가 LLM 응답 JSON을 명시적으로 만들어 graph/API 경계로 넣는다.

예를 들어 planner에서는 다음처럼 `manual_plan`을 전달한다.

```json
{
  "query": "최근 공사 계약 1건 보여줘",
  "manual_plan": {
    "execution_graph": {
      "type": "dag",
      "status": "planned",
      "nodes": []
    }
  }
}
```

ingestion에서는 `manual_llm_response` 또는 `--manual-llm-response` 형태로 전달한다.

중요한 제한은 다음과 같다.

```text
runtime code가 fixture 파일을 몰래 찾으면 안 된다.
query hash나 document id로 manual artifact를 자동 발견하면 안 된다.
Codex manual 결과는 pending_review 상태로 남아야 한다.
manual reasoning을 코드의 hard-coded provider rule로 바꾸면 안 된다.
```

## 질문 실행 흐름

MCP client가 질문을 하면 제품 entrypoint는 `apps/pubdata_mcp`의 `semantic_query` tool이다.

큰 흐름은 다음과 같다.

```text
MCP Client
  -> pubdata_mcp semantic_query
  -> semantic_platform /planner/plan
  -> capability retrieval
  -> planner context 구성
  -> LLM planner 또는 manual_plan validation
  -> semantic execution graph 반환
  -> pubdata_mcp execute_semantic_plan
  -> approved execution contracts 로딩
  -> semantic arguments를 raw provider arguments로 컴파일
  -> provider API 호출
  -> response normalization
  -> structured result 반환
```

`semantic_platform`가 `pubdata_mcp`에게 주는 핵심 payload는 `execution_graph`다.

Capability가 질문을 충족하는지, 또는 충분한 capability가 없어서 실행하지 말아야 하는지는 LLM planner가 판단한다. Runtime code는 `공사`, `변경 이력` 같은 단어를 deterministic rule로 해석해서 capability 존재 여부를 결정하지 않는다. Retrieval은 후보를 제공하고, validation은 planner가 고른 id가 context에 존재하는지만 확인한다.

충분한 capability가 없다고 판단하면 planner는 근접 capability를 억지로 선택하지 않고 다음처럼 반환해야 한다.

```json
{
  "planner": {
    "status": "not_found",
    "reason": "capability_not_found"
  },
  "execution_graph": {
    "type": "dag",
    "status": "not_found",
    "nodes": []
  },
  "errors": [
    {
      "code": "capability_not_found"
    }
  ]
}
```

```json
{
  "query": "최근 공사 계약 1건 보여줘",
  "planner": {
    "mode": "codex_manual",
    "status": "valid",
    "model": null
  },
  "execution_graph": {
    "type": "dag",
    "status": "planned",
    "nodes": [
      {
        "id": "search_construction_contracts_1",
        "capability": "search_construction_contracts",
        "variant_id": "variant.search_construction_contracts.registration_datetime",
        "operation_id": "operation.getCntrctInfoListCnstwk",
        "call": {
          "semantic_arguments": {
            "registration_datetime_range": {
              "from": "201605010000",
              "to": "201605052359"
            },
            "page_number": 1,
            "page_size": 1,
            "response_format": "json"
          }
        },
        "argument_bindings": {},
        "post_filters": []
      }
    ],
    "joins": []
  },
  "errors": []
}
```

각 node의 의미는 다음과 같다.

```text
id
  DAG node id

capability
  planner-facing 기능 id

variant_id
  실행할 operation variant

operation_id
  실제 provider operation contract id

call.semantic_arguments
  provider raw field가 아니라 semantic type 기준 인자

argument_bindings
  이전 node output을 다음 node input으로 연결하는 선언

post_filters
  실행 후 semantic 결과에 적용할 filter
```

planner validation 후에는 node에 `operation_contract`와 `operation_variant` 정보도 포함된다. 이 정보는 MCP가 plan을 실행 가능하게 컴파일하는 데 사용된다.

## pubdata_mcp 실행 방식

`pubdata_mcp`는 semantic platform에서 approved execution contracts를 읽는다.

```text
GET /semantic/execution/contracts
```

여기에는 다음 정보가 들어 있다.

```text
resources
operation_contracts
operation_variants
operation_field_mappings
capability_implementations
```

MCP executor는 planner node를 다음 순서로 처리한다.

```text
1. variant_id로 operation_id와 fixed arguments 확인
2. operation_contract.request의 semantic_type mapping 확인
3. semantic_arguments를 raw provider arguments로 변환
4. operation_contract request field rule로 transform/validation
5. operation_variant.fixed_raw_arguments 병합
6. auth metadata에 따라 API key 주입
7. HTTP GET/POST 호출
8. operation_contract.response와 field_mappings로 결과 해석
9. structured result와 evidence 반환
```

request field rule 예:

```json
{
  "phone": {
    "semantic_type": "phone_number",
    "transform": {
      "name": "phone_format",
      "style": "kr_mobile_hyphen"
    },
    "pattern": "^01[016789]-[0-9]{3,4}-[0-9]{4}$"
  }
}
```

planner가 `phone_number: "01022223333"`을 넘겨도 executor는 contract에 따라 `010-2222-3333`으로 변환하고 pattern을 검증한다. 검증에 실패하면 provider API를 호출하지 않고 `validation_error`를 반환한다.

예를 들어 다음 semantic arguments가 들어오면:

```json
{
  "registration_datetime_range": {
    "from": "201605010000",
    "to": "201605052359"
  },
  "page_number": 1,
  "page_size": 1,
  "response_format": "json"
}
```

variant와 contract를 적용해 다음 raw arguments로 컴파일된다.

```json
{
  "type": "json",
  "pageNo": 1,
  "inqryDiv": "1",
  "numOfRows": 1,
  "inqryBgnDt": "201605010000",
  "inqryEndDt": "201605052359"
}
```

이후 provider API를 호출하고 구조화된 결과를 반환한다.

## 실제 테스트 예

질문:

```text
최근 공사 계약 1건 보여줘
```

선택된 capability와 variant:

```text
capability:   search_construction_contracts
variant_id:   variant.search_construction_contracts.registration_datetime
operation_id: operation.getCntrctInfoListCnstwk
```

실행 결과 예:

```json
{
  "plan_status": "planned",
  "execution_status": "executed",
  "capability": "search_construction_contracts",
  "variant_id": "variant.search_construction_contracts.registration_datetime",
  "operation_id": "operation.getCntrctInfoListCnstwk",
  "result": {
    "untyCntrctNo": "2016050000051",
    "bsnsDivNm": "공사",
    "cnstwkNm": "관악소방서 채널사인 제작 설치",
    "cntrctInsttNm": "서울특별시 관악소방서",
    "totCntrctAmt": "9359000",
    "cntrctDate": "2016-05-02",
    "totalCount": 5191
  }
}
```

MCP이므로 자연어 답변 생성은 필수 단계가 아니다. 현재 반환 대상은 사람이 읽는 문장이 아니라, tool consumer가 사용할 수 있는 structured result다.

## 주요 API

semantic platform API:

```text
GET  /semantic/catalog
GET  /catalog/sections/{section}
GET  /semantic/execution/contracts
GET  /semantic/capability-documents
POST /semantic/capability-documents/rebuild
POST /semantic/capability-documents/embed
POST /semantic/capabilities/retrieve
POST /planner/plan
POST /planner/execution-plan
POST /runtime/context
GET  /planner/execution-graphs
GET  /semantic/execution/checks
POST /semantic/execution/checks
GET  /proposals
POST /proposals/{proposal_id}/apply
POST /proposals/{proposal_id}/reject
```

pubdata_mcp tools:

```text
semantic_query
semantic_smoke_test_operation
```

## 현재 구현 상태와 주의점

현재 동작하는 것:

```text
source ingestion skeleton
manual/codex ingestion proposal
proposal apply
operation contracts / variants / field mappings 저장
capability document rebuild
BGE-m3-ko embedding service
pgvector hybrid retrieval
planner API
codex_manual manual_plan validation
pubdata_mcp generic HTTP executor
contract 기반 request transform/validation
실제 공공 API 호출
structured result 반환
dashboard catalog pagination
dashboard contract/variant/check review
```

아직 보강이 필요한 것:

```text
Semantic Entity Registry와 Join Rule
Planner DAG schema validator
질문 예시 dataset 기반 retrieval/planner 평가
LLM planner가 ambiguous/missing capability case를 올바르게 not_found 처리하는지 평가하기
feedback 기반 reindex/evaluation set 추가하기
```

`runtime_context`는 retrieval-first planner context와 같은 기준을 사용한다. Planner validation은 retrieved capability 범위 안의 operation/variant를 기준으로 검증한다. `pubdata_mcp` readiness도 variant, operation contract, resource metadata 기반으로 실행 가능성을 계산한다.

API key 같은 secret은 env에만 있어야 하며, response, evidence, proposal artifact, endpoint check payload에 저장되거나 반환되면 안 된다. 현재 executor는 auth 성격의 request key를 반환/저장 전에 redaction한다.

## 운영 명령

compose는 manifest에서 생성한다. `deploy/compose/docker-compose.yml`을 직접 수정하지 않는다.

```bash
python3 scripts/generate_compose.py
python3 scripts/ops.py validate-config
python3 scripts/ops.py lint-boundaries
python3 scripts/ops.py check-all
```

embedding service와 semantic platform 서비스 재기동 예:

```bash
docker compose --env-file /workspace/deploy/compose/env/compose.env \
  -f /workspace/deploy/compose/docker-compose.yml \
  up -d --build embedding-service semantic-platform-api semantic-platform-worker
```

capability document rebuild:

```bash
curl -X POST http://127.0.0.1:8016/semantic/capability-documents/rebuild
```

capability embedding:

```bash
curl -X POST http://127.0.0.1:8016/semantic/capability-documents/embed \
  -H 'Content-Type: application/json' \
  -d '{"limit": 100, "force": true}'
```

capability retrieval:

```bash
curl -X POST http://127.0.0.1:8016/semantic/capabilities/retrieve \
  -H 'Content-Type: application/json' \
  -d '{"query": "최근 공사 계약", "limit": 3}'
```

planner 호출:

```bash
curl -X POST http://127.0.0.1:8016/planner/execution-plan \
  -H 'Content-Type: application/json' \
  -d '{"query": "최근 공사 계약", "limit": 5}'
```

`LLM_MODE=codex_manual`에서는 `manual_plan` 없이 planner가 자동 계획을 만들지 않는다. 이때 `llm_plan_missing`은 정상 동작이다.
