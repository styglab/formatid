---
name: context_platform_ingestion
description: Context Platform에 신규 source/API 문서를 ingestion할 때 사용한다. tmp/sources 같은 지정 폴더 또는 UI에서 queued_for_agent로 접수된 source document를 DB 기준으로 확인하고, agent response artifact를 만든 뒤 scripts/ops.py context-platform ingest-source 또는 ingest-queued-source를 실행한다.
---

# Context Platform Ingestion

이 skill은 지정된 폴더의 신규 source 문서나 UI Source Intake에서
`queued_for_agent`로 접수된 source document를 Context Platform에 ingestion할 때
사용한다. Dashboard는 review/governance surface이며 semantic draft를 직접 만들지
않는다.

## 기본 위치

- 입력 문서 폴더: `tmp/sources`
- 임시 산출물 폴더: `tmp/context_platform`
- 보조 실행 기록: `tmp/context_platform/ingested-manifest.json`
- agent response artifact: `tmp/context_platform/<문서명>.agent-response.json`
- UI 접수 문서: `source_documents.status = queued_for_agent` 또는
  `onboarding_runs.status = queued_for_agent`

## 원샷 실행

사용자가 "끝까지 진행", "한 번에", "ingestion 해줘"라고 요청하면 이 skill을
사용해 Codex가 operator agent로 직접 끝까지 수행한다. 사용자에게 별도 agent
실행법을 길게 설명하지 않는다.

필수 흐름:

```text
1. 중복 여부 확인
2. UI 접수 문서면 source document/run/object URI를 확인하고, 폴더 문서면 파일
   경로를 확인
3. 폴더 문서는 `ingest-source`, UI 접수 문서는 `ingest-queued-source <run_id>`로
   request/evidence 생성
4. request/evidence 기준으로 agent response JSON 작성
5. 같은 명령에 `--agent-response <json>`을 붙여 재실행
6. proposal bundle id와 주요 count 요약
```

터미널에서 비대화형으로 실행해야 할 때만 다음 형태를 안내한다.

```bash
codex exec -C /workspace --sandbox danger-full-access -a never '<원샷 ingestion 요청>'
```

## 핵심 원칙

- 기존에 ingestion된 문서는 다시 처리하지 않는다.
- 중복 판정은 Context Platform DB를 우선한다.
- 같은 문서가 이미 Source Catalog에 있으면 스킵한다.
- manifest는 보조 실행 기록이며, DB 조회가 가능하면 DB보다 우선하지 않는다.
- secret 값은 payload, manifest, 로그, tmp 파일에 쓰지 않는다.
- 이 skill을 사용한 ingestion 작업 중에는 애플리케이션 코드, 마이그레이션, manifest, 문서를 변경하지 않는다.
- 허용되는 쓰기 범위는 기본적으로 `tmp/context_platform/*` 산출물뿐이다.
- 사용자가 명시적으로 코드 또는 skill 수정을 요청한 경우에만 repository 파일을 변경한다.
- UI 업로드는 agent ingestion queue 생성까지만 담당한다. Codex는 queued source를
  처리할 때도 반드시 CLI/API의 agent response boundary를 통과해야 한다.
- Codex나 operator agent가 판단한 내용은 `--agent-mode manual`과 `--agent-response`로 CLI/API 경계를 통과해야 한다.
- Context Platform ingestion runtime은 외부 LLM을 직접 호출하지 않는다. `--llm-mode codex_manual`은 legacy alias로만 허용된다.
- 생성 결과는 proposal/review lifecycle을 따른다. 사용자가 명시하지 않으면 catalog에 바로 적용하거나 승인하지 않는다.
- Context Platform 용어와 경로를 사용한다. `semantic_platform` 이름을 새로 만들지 않는다.
- ingestion 실행과 문서 파싱은 `scripts/ops.py context-platform ingest-source`가
  worker 컨테이너에서 수행하는 active runtime workflow를 따른다.
- PDF/DOCX/HTML 파싱을 위해 host `tmp/*`에 별도 venv, wheel target,
  parser dependency, 변환 스크립트를 만들지 않는다. worker의 ingestion
  pipeline과 worker에 설치된 parser/docling 의존성을 사용한다.
- Codex는 host에서 문서를 임의 파싱해 runtime을 우회하지 않는다. manual
  response가 필요하면 worker workflow가 반환한 `manual_llm_request` 또는
  worker가 추출한 chunk/source term evidence를 기준으로 작성한다.
- Source contract 초안 생성은 machine-readable spec을 먼저 사용한다. Swagger /
  OpenAPI JSON/YAML이면 deterministic OpenAPI parser로 `paths`의 operation,
  parameter, response field만 추출한다. `definitions`/`components.schemas`는
  schema로만 사용하고 operation으로 만들지 않는다.
- Swagger/OpenAPI가 아니거나 table/text 중심 문서이면 LangExtract를 사용한다.
  LangExtract는 chunk text에서 실제 source operation/parameter/field를
  source-grounded extraction으로 뽑는 용도이며, Concept/Binding/Capability를
  자동 승인하거나 runtime 코드에 provider keyword rule을 넣는 용도가 아니다.
- API Source Graph는 반드시 wire contract다. 상세기능 설명문이나 요약 문장의
  한국어 업무 용어만 보고 source parameter/field를 만들지 않는다. request table,
  response table, XML/JSON example, Swagger/OpenAPI 같은 executable evidence가
  없으면 Source Graph 생성은 중단하고 `failed_needs_review` 또는 manual request로
  남긴다.
- API source에서 `wire_name`, `raw_name`, `field_path`는 실제 HTTP/API key만
  허용한다. 한국어 라벨은 `label_ko`, description, aliases, evidence에만 넣는다.
  `raw_name=법인등록번호`, `field_path=request.법인등록번호` 같은 artifact는 실패로
  보아야 한다.
- Evidence 우선순위는 `openapi` > `request/response table` > `message example`
  > `official/public external page` > `narrative detail text`다. narrative detail
  text는 operation description/capability hint로만 사용하고 source field 생성
  evidence로 사용하지 않는다.
- endpoint verification에 필요한 인증키가 env에 있으면 실제 secret 값이 아니라
  env 변수명만 `verification.secret_env`에 넣는다. 필수 샘플값은 문서 또는 공식/공개
  웹 evidence에서 찾을 수 있으며, 비밀이 아닌 값만 `verification.sample_parameters`
  에 넣고 evidence URL/ref를 남긴다.

## 신규 문서 판정

문서를 처리하기 전에 다음 순서로 중복 여부를 확인한다.

1. 후보 문서의 SHA-256 content hash를 계산한다.
2. Context Platform DB의 `source_documents.content_hash`에 같은 값이 있으면 스킵한다.
3. 같은 `source_documents.name`과 같은 source context가 이미 있으면 기존 run 상태를 확인하고, 재처리 요청이 명시되지 않았으면 스킵한다.
4. `sources.name`만 같고 content hash가 다르면 기존 source를 덮어쓰지 않는다. 신규 문서로 확실할 때만 unique `--name`을 붙여 처리한다.
5. DB 조회가 불가능할 때만 `tmp/context_platform/ingested-manifest.json`을 보조 기준으로 사용한다.
6. 판단이 애매하면 임의로 중복 ingestion하지 말고 스킵 사유를 기록한다.

스킵한 문서는 최종 응답에 다음 정보를 남긴다.

```text
skipped:
  source_path: ...
  reason: duplicate_content_hash | duplicate_source_document | duplicate_source_name | already_ingested_manifest | ambiguous_duplicate
```

## Workflow

1. 입력 폴더에서 후보 문서를 찾는다.
   - 기본 확장자: `.pdf`, `.md`, `.json`, `.yaml`, `.yml`, `.csv`, `.txt`
   - `tmp/context_platform` 아래 산출물은 입력 문서로 보지 않는다.
2. UI Source Intake에 `queued_for_agent` source document 또는 onboarding run이
   있는지도 확인한다. UI 접수 문서는 DB의 `source_documents.uri`,
   `source_documents.content_hash`, `onboarding_runs.id`, object metadata를
   source of truth로 사용한다.
3. 각 후보 문서의 content hash를 계산한다. UI 접수 문서는 DB hash를 우선한다.
4. Context Platform DB의 Source Graph와 `source_documents`를 먼저 확인해 기존 문서는 스킵한다.
5. DB 조회가 불가능한 경우에만 manifest를 보조 기준으로 확인한다.
6. 신규 문서는 worker ingestion workflow를 통해 구조화한다. Codex가 직접 판단해야
   하는 내용은 host에서 별도 parser를 붙여 만들지 말고, worker가 생성한
   `manual_llm_request`, chunk 요약, source term evidence를 읽어 다음 정보를
   판단한다.
   - source operation 후보
   - request/control parameter 후보. `wire_name`/`raw_name`은 실제 API key이고,
     `label_ko`는 문서에 표시된 한국어 라벨이다.
   - response/source field 후보. `field_path`는 실제 wire path여야 하며,
     한국어 라벨은 `label_ko`에만 둔다.
   - source evidence tier. `source_evidence_tier`는 `openapi`, `table`,
     `example`, `external` 중 하나를 우선 사용한다. `narrative`만 있는 후보는
     executable source contract로 채택하지 않는다.
   - concept / canonical representation / representation schema 후보
   - field/context/parameter binding 후보. 값 필드, context 필드, request
     parameter를 같은 binding으로 뭉개지 않는다.
   - capability 후보
7. PDF 등 binary 문서는 host에서 별도 parser를 설치하거나 텍스트 변환하지 말고,
   먼저 worker workflow가 `manual_llm_request`를 만들게 한다.
8. Source contract 초안을 먼저 생성할 수 있다. 입력이 Swagger/OpenAPI JSON/YAML이면
   deterministic parser가 사용되고, 그 외 문서는 worker chunk + LangExtract로
   fallback한다.

```bash
python3 scripts/ops.py context-platform draft-source-contract "<source-file>" \
  --output "tmp/context_platform/<문서명>.source-structure.agent-response.json"
```

   이 명령은 worker에서 machine-readable spec을 먼저 감지한다. Swagger/OpenAPI는
   `source_contract_extractor=openapi_parser` artifact를 만들고, 그 외 문서는
   Docling chunk와 LangExtract로 `source_contract_extractor=langextract`
   artifact를 만든다. 생성물은 proposal을 바로 만들지 않으며, 반드시 검토 후
   `--agent-response`로 ingestion boundary를 통과시킨다.
9. Codex나 operator agent가 직접 판단한 결과를 사용할 때는 worker가 반환한
   request/evidence를 기준으로 agent response JSON을 `tmp/context_platform`
   아래에 작성한다.
10. 폴더 문서는 다음 명령으로 ingestion을 실행한다.

```bash
python3 scripts/ops.py context-platform ingest-source "<source-file>" \
  --agent-mode manual \
  --agent-response "<agent-response-json>"
```

   UI Source Intake에서 이미 queued된 문서는 `run_id`로 실행한다.

```bash
python3 scripts/ops.py context-platform ingest-queued-source "<run-id>" \
  --agent-mode manual \
  --agent-response "<agent-response-json>"
```

11. `--agent-response` 없이 `--agent-mode manual`을 실행하면 request/evidence 생성
   지점에서 멈춘다. 그 request에 맞춰 response JSON을 작성한 뒤 같은 명령에
   `--agent-response`를 붙여 재실행한다.
12. `sources_name_key` 중복 등 이름 충돌이 나면 기존 source를 덮어쓰지 말고, 신규 문서일 때만 unique `--name`을 붙여 한 번 재시도한다.
13. 실행 결과를 manifest에 보조 기록으로 남긴다.
14. 최종 응답에는 처리됨/스킵됨/실패 문서를 구분해서 보고한다.

## Manifest 형식

`tmp/context_platform/ingested-manifest.json`은 disposable 보조 실행 기록이다. DB가 중복 판정의 source of truth이며, manifest는 사람이 빠르게 실행 이력을 확인하는 용도로만 사용한다. 없으면 생성한다.

```json
{
  "documents": [
    {
      "source_path": "tmp/sources/example.pdf",
      "content_hash": "sha256...",
      "agent_response_path": "tmp/context_platform/example.agent-response.json",
      "source_id": "src_...",
      "source_document_id": "doc_...",
      "run_id": "run_...",
      "proposal_bundle_id": "bundle_...",
      "verification_summary": {
        "total": 0,
        "verified": 0,
        "failed": 0,
        "skipped": 0,
        "needs_input": 0
      },
      "status": "completed",
      "created_at": "ISO-8601 timestamp"
    }
  ]
}
```

## Agent Response 규칙

agent response는 다음 top-level key를 우선 사용해야 한다. `source_structure`,
`meaning_resolution`,
`resolution_generation`, `capability_generation`은 현재 active graph/API boundary
이름이다. 그 안의 판단은 Meaning Graph, CanonicalRepresentation,
RepresentationSchema, Field/Context/Parameter Binding, Capability를 대상으로
작성한다. `canonical_reconciliation`, `binding_generation`,
`capability_contracting`은 legacy alias로만 허용한다.

Ingestion runtime은 agent response를 proposal bundle로 내리기 전에 schema와 핵심
불변식을 검증한다. 특히 Concept decision에 `datatype`, `regex`, `pattern`,
`enum`, `required`, `minimum`, `maximum`, `examples`, `validation` 같은
RepresentationSchema 제약을 넣으면 실패한다. Capability proposal output은
planner/API 소비자용 `output_key`를 반드시 포함해야 한다. Concept `kind`는
`object_concept`, `metric_concept`, `identifier_concept`, `status_concept`,
`value_concept`, `unit_concept`, `time_concept`, `account_concept`,
`document_concept`, `operation_concept` 중 하나만 사용한다. Capability가
`provides_concepts` 또는 `intent_spec.canonical_outputs`에 선언한 concept는
반드시 `outputs[]`에도 같은 `concept_key`로 존재해야 한다.

Source Structure 단계는 semantic 판단 단계가 아니다. 여기서는 실제 source
contract만 추출한다.

- `wire_name`: 실제 HTTP query/body/response key. 예: `crno`, `bizYear`,
  `enpSaleAmt`.
- `raw_name`: 별도 원천명이 없으면 `wire_name`과 동일하다.
- `label_ko`: 문서에 표시된 한국어 라벨. 예: `법인등록번호`, `사업연도`,
  `기업매출금액`.
- `field_path`: 실제 wire path. 예: `request.query.crno`,
  `response.body.items.item.enpSaleAmt`.
- `source_evidence_tier`: `openapi`, `table`, `example`, `external` 중 하나.
  `narrative`는 operation/capability 설명 근거로만 쓰며 executable field 근거로
  쓰지 않는다.

Resolution 단계에서는 binding을 반드시 세 종류로 분리한다.

- `field_bindings`: source response value field가 CanonicalRepresentation의
  value property를 채우는 연결. 예: `enpSaleAmt ->
  repr.finance.revenue.observation_amount`, `fills_property:
  property.observed_amount`.
- `context_bindings`: source response context field가 이미 선택된
  CanonicalRepresentation의 context를 채우는 연결. 예: `curCd ->
  context_key: currency`, `bizYear -> context_key: fiscal_year`, `fnclDcd ->
  context_key: statement_type`. context field를 `field_bindings`에 넣지 않는다.
- `parameter_bindings`: capability input concept가 source request parameter로
  들어가는 연결. 예: `concept.identifier.kr_corporate_registration_number ->
  request.query.crno`.

Capability 단계에서는 field binding만 `outputs[]`로 노출한다. Context binding은
`operation_link.binding_spec.contexts[]`에 남기며, `revenue_amount` 같은
`output_key`는 consumer/planner-facing 이름일 뿐 canonical property가 아니다.

RepresentationSchema는 datatype/regex/enum/value domain/cardinality/examples를
담는다. 예: 사업자등록번호는 `pattern: "^\\d{10}$"`, 법인등록번호는
`pattern: "^\\d{13}$"`, YYYYMMDD 날짜는 `pattern: "^\\d{8}$"`, `valid`는
`enum_values: ["01", "02"]`, `b_stt_cd`는 `enum_values: ["01", "02", "03"]`,
`utcc_yn`은 `enum_values: ["Y", "N"]`로 표현한다.

실행 가능한 API operation에서 `wire_name`, `raw_name`, `field_path`에
`법인등록번호`, `사업연도`, `기업매출금액` 같은 한국어 라벨을 넣으면 source
contract validation에서 실패한다.

Verification config는 다음 원칙을 지킨다.

- secret 값은 절대 artifact/tmp/log/manifest에 쓰지 않는다.
- 인증키는 `verification.secret_env`에 parameter name -> env var name 형태로만
  쓴다. 예: `"serviceKey": "CONTEXT_PLATFORM_SERVICE_KEY"`.
- 문서/공식 웹에 있는 비밀이 아닌 샘플값은 `verification.sample_parameters`에 쓸 수
  있다. 예: `crno`, `bizYear`, `pageNo`, `numOfRows`, `resultType`.
- `sample_parameters`에 `serviceKey`, `apiKey`, `token`, `authorization` 같은
  secret-like key를 직접 넣으면 실패로 본다.

Swagger/OpenAPI parser를 사용할 때는 다음 규칙을 따른다.

- `paths` 아래 HTTP method만 operation으로 만든다.
- `definitions`와 `components.schemas`는 request/response schema flattening에만
  사용한다.
- security scheme의 apiKey query/header 값은 `scope=control` parameter로 둔다.
- body schema field는 `request.body.<wire_name>` 또는
  `request.body.<array>[]<field>` 형태의 parameter로 둔다.
- response schema field는 `response.body.<wire_path>` 형태의 response field로 둔다.

LangExtract를 사용할 때도 wire-name 규칙은 동일하다. LangExtract output은 다음
class를 우리 artifact로 변환한다.

- `source_operation` -> `source_structure.operations[]`
- `source_parameter` -> `source_structure.operations[].parameters[]`
- `source_response_field` -> `source_structure.operations[].response_fields[]`

LangExtract가 제공하는 character offset/source grounding은 `evidence.kind =
langextract_grounding`으로 보존한다. Grounding이 없는 extraction은 버린다.

```json
{
  "source_structure": {
    "operations": [
      {
        "operation_key": "getSummFinaStat_V2",
        "method": "GET",
        "base_url": "http://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2",
        "path": "/getSummFinaStat_V2",
        "parameters": [
          {
            "wire_name": "crno",
            "raw_name": "crno",
            "label_ko": "법인등록번호",
            "field_path": "request.query.crno",
            "scope": "input"
          }
        ],
        "response_fields": [
          {
            "wire_name": "enpSaleAmt",
            "raw_name": "enpSaleAmt",
            "label_ko": "기업매출금액",
            "field_path": "response.body.items.item.enpSaleAmt",
            "scope": "output"
          }
        ]
      }
    ]
  },
  "meaning_resolution": {
    "concept_decisions": [],
    "representation_decisions": [],
    "representation_schema_decisions": [],
    "value_domain_decisions": [],
    "relation_suggestions": []
  },
  "resolution_generation": {
    "field_bindings": [],
    "context_bindings": [],
    "parameter_bindings": [],
    "transform_rules": []
  },
  "capability_generation": {
    "suggestions": []
  }
}
```

`operation_candidates`, `field_candidates`는 legacy flat source structure alias로만
허용한다. 신규 artifact는 `source_structure.operations`를 우선 사용한다.

`meaning_resolution.decisions`, `resolution_generation.suggestions`,
`canonical_reconciliation`, `binding_generation`, `capability_contracting`은
legacy alias로만 사용한다. 신규 agent artifact는 active key를 우선 사용한다.

Meaning response에서 datatype, regex, enum, pattern, required, cardinality,
minimum/maximum, examples 같은 검증 제약은 Concept에 넣지 않고
`representation_schema_decisions`에 넣는다. Concept에는 meaning만 둔다.

Resolution response는 binding 종류를 구분한다.

- `field_bindings`: source field가 representation value property를 채운다.
- `context_bindings`: source field가 representation context를 채운다. 예:
  `currency`, `fiscal_year`, `statement_type`, `source`, `observed_date`.
- `parameter_bindings`: source parameter가 required input concept를 채운다.
- `transform_rules`: parse/normalize/cast/enum mapping 같은 선언적 rule만 둔다.

Capability response의 input/output은 가능하면 다음 key를 함께 포함한다.

```json
{
  "concept_key": "concept.finance.revenue",
  "representation_key": "repr.finance.revenue.observation_amount",
  "representation_schema_key": "schema.finance.revenue.money_amount",
  "output_key": "revenue_amount"
}
```

`output_key`는 planner/API 소비자용 이름이며 canonical property가 아니다.

계약 회귀 테스트 fixture는 `tests/fixtures/context_platform/revenue_agent_response.json`
에 둔다. prompt나 response contract를 변경하면 이 fixture가
`validate_agent_response_artifact -> normalizer -> proposal shape` 흐름을 계속
통과하는지 확인한다.

선택적으로 endpoint verification에 필요한 비밀이 아닌 샘플과 secret env 이름을
다음 top-level key로 제공할 수 있다.

```json
{
  "verification": {
    "secret_env": {
      "serviceKey": "CONTEXT_PLATFORM_SERVICE_KEY"
    },
    "sample_parameters": {
      "default": {
        "법인등록번호": "1301110006246",
        "사업연도": "2024"
      }
    }
  }
}
```

`verification.sample_parameters`에는 공개 샘플이나 문서 샘플처럼 비밀이 아닌
값만 넣는다. `serviceKey`, token, Authorization, password 같은 secret 값은 절대
agent response, proposal, tmp 파일, 로그에 쓰지 않는다. secret은 `secret_env`에
환경 변수 이름만 남기고 실제 값은 컨테이너/서비스 env에서 읽게 한다.

### Coverage 규칙

- `manual_llm_request.source_terms`의 모든 항목마다 정확히 하나의
  meaning/representation decision과 resolution/binding suggestion을 작성한다.
- 같은 `raw_name` 또는 `field_path`라도 `source_operation_id`,
  `source_parameter_id`, `source_field_id`가 다르면 별도 source term으로 판단한다.
  예를 들어 세 operation에 반복되는 `법인등록번호`, `사업연도`는 총 6개 입력
  term으로 각각 응답해야 한다.
- 가능하면 `source_parameter_id` 또는 `source_field_id`를 반드시 포함한다.
  id가 없는 fallback 응답은 같은 field path가 하나뿐일 때만 안전하다.
- `conflict`, `skip`, `skip_binding` 판단은 후속 단계에서 bind/capability로 다시
  살리지 않는다.
- canonical decision이 `conflict`인 term은 binding suggestion도 `conflict`로 둔다.
- canonical decision이 `skip`인 term은 binding suggestion을 `skip_binding`으로 둔다.

### Public Sample 규칙

- 검증용 샘플은 공식 문서, 공공 API, 공개 기업 공시처럼 출처가 공개된 값만 사용한다.
- 샘플 출처는 agent artifact의 rationale/evidence에 남긴다.
- 공개 식별자를 샘플로 써도 되지만, 해당 API 호출 권한을 증명하지는 않는다.
  실행 검증은 configured secret env와 endpoint response evidence로 별도 확인한다.
- 샘플이 없거나 secret env가 구성되지 않은 경우 proposal을 실행 가능한 capability로
  승인하지 않는다.

API response 구조 차이는 코드 분기가 아니라 `field_candidates.field_path`, `source_fields`, `bindings` 데이터로 표현한다.

예:

```json
{
  "scope": "output",
  "raw_name": "enpSaleAmt",
  "field_path": "response.body.items.item.enpSaleAmt",
  "data_type": "number",
  "is_required": false,
  "description": "기업매출금액"
}
```

## 검증과 보고

실행 후 다음을 확인한다.

- ingestion status
- source operation count
- proposal bundle id
- verification summary
- failed 또는 needs_input endpoint check

최종 응답은 짧게 작성하되 다음 정보를 포함한다.

```text
processed:
  - source_path
  - run_id
  - proposal_bundle_id
  - verification_summary

skipped:
  - source_path
  - reason

failed:
  - source_path
  - error
```
