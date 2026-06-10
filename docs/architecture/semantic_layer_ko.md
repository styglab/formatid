# Semantic Layer 설계안 v1

## 목적

이 플랫폼은 Data Catalog가 아니고 단순 MCP Registry도 아니다.

목표는 AI Agent가 조직 내 Capability를 이해하고, 계획하고, 실행 가능한
컨텍스트를 얻을 수 있도록 하는 Semantic Layer Platform이다.

DataHub의 전체 스택은 도입하지 않는다. 다만 Entity, Aspect,
Relationship, Business Glossary, Context Graph, Change Proposal이라는
메타모델 관점은 참고한다. 우리의 중심 Entity는 Dataset이 아니라
SemanticType, Capability, OperationContract, OperationVariant다.

## 핵심 원칙

API First로 설계하지 않는다.

```text
Semantic Type
  -> Capability
  -> Execution Contract / Variant
  -> Planner Context
  -> App Executor
```

Planner는 provider API 세부사항을 직접 알 필요가 없다. Planner는 승인된
Capability, OperationContract, OperationVariant, semantic argument,
binding, integration plan만 다룬다. 실제 provider 호출, 인증, retry,
pagination, raw response parsing은 app executor가 담당한다.

## Canonical Semantic Model

장기 자산은 API 문서가 아니라 canonical semantic model이다.

예:

```text
BusinessRegistrationNumber
CompanyName
Company
Revenue
OperatingProfit
CreditScore
Contract
BidNotice
ContractAmount
```

Provider field나 tool parameter는 canonical semantic type이 아니다.
외부 필드는 operation contract / variant 안에서 evidence와 함께 semantic
type이나 semantic control로 매핑된다.

## Authoring과 Ingestion

Semantic Layer는 ingestion-only 플랫폼이 아니다.

```text
Manual authoring
  -> validation
  -> context change proposal or direct draft
  -> review/approval
  -> approved context graph
```

```text
API document ingestion
  -> evidence extraction
  -> LLM/manual proposal
  -> pending_review
  -> review/approval
  -> approved context graph
```

Ingestion은 catalog를 직접 변경하지 않는다. proposal을 만든다. 직접 등록과
수정도 review status, provenance, lineage를 남긴다.

## Entity / Aspect / Relationship

MVP 내부 모델은 DataHub식 사고방식을 따른다.

Entity:

```text
semantic_type
capability
provider
resource
operation_contract
operation_variant
context_document
policy
owner
domain
```

Aspect:

```text
properties
ownership
input_schema
output_schema
execution_binding
field_mapping
verification
governance_status
documentation
quality_signal
```

Relationship:

```text
capability_accepts_semantic_type
capability_produces_semantic_type
capability_implemented_by_contract
contract_has_variant
variant_uses_provider
field_maps_to_semantic_type
context_document_describes_entity
policy_governs_entity
```

## Catalog 구분

Capability Catalog:

- provider-neutral capability metadata
- aliases, examples, tags
- inputs and outputs expressed as canonical semantic types

Execution Catalog:

- providers, resources, operations
- operation contracts and variants
- field mappings and semantic controls
- endpoint verification evidence

Governance Context:

- proposals, review status, conflicts
- lineage and evidence references
- merge/deprecation decisions

## Operation Contract / Variant

같은 raw field라도 operation마다 의미가 다를 수 있다. 따라서 field mapping은
global string replacement가 아니라 operation-scoped contract data다.

```yaml
operation_id: pps.search_contracts
field: bizrno
direction: request
semantic_type: BusinessRegistrationNumber
review_status: approved
```

Control parameter도 선언적으로 모델링한다.

```yaml
field: inqryDiv
kind: control
semantic_type: InquiryBasis
planner_selects: true
enum_mapping:
  contract_date: "1"
```

하나의 endpoint가 fixed control 값에 따라 여러 capability를 만들면
OperationVariant로 나눈다.

```text
operation_contract
  -> operation_variant
    -> capability_implementation
```

Runtime code는 provider/domain keyword 규칙을 하드코딩하지 않는다.

## Service Layout

```text
services/semantic_layer/
  adapters/
    admin_api/
    planner_api/
    dashboard/
    worker/
  lib/
    model/
    semantic/
    capability/
    execution/
    authoring/
    governance/
    ingestion/
    planner/
    context/
    storage/
  manifests/
```

문서상 서비스명은 `semantic_layer`다. 현재 구현 경로는
`services/semantic_layer`를 유지한다.

## Runtime Contract

```text
Client question
  -> app executor
  -> semantic-layer-planner-api
  -> semantic execution plan
  -> app executor
  -> provider APIs / MCP tools / internal services
  -> semantic normalization and integration
```

Planner API는 approved context만 읽는다. Admin API는 source upload,
authoring, proposal review, governance mutation을 담당한다.
