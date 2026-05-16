# G2B Pipeline

나라장터(G2B) 공공 API 데이터를 수집해서 raw payload로 보관하고, MCP와 semantic/RAG에서 쓰기 좋은 normalized canonical table로 정리하는 파이프라인입니다.

## 수집 데이터

### 1. 입찰공고

입찰공고는 업무구분별로 수집합니다.

| 카테고리 | 의미 | API |
| --- | --- | --- |
| `GOODS` | 물품 | `BidPublicInfoService/getBidPblancListInfoThngPPSSrch` |
| `SERVICE` | 용역 | `BidPublicInfoService/getBidPblancListInfoServcPPSSrch` |
| `CONSTRUCTION` | 공사 | `BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch` |
| `FOREIGN` | 외자 | `BidPublicInfoService/getBidPblancListInfoFrgcptPPSSrch` |

입찰공고와 같은 시간 구간으로 아래 부가 정보도 함께 수집합니다.

| 데이터 | API | 의미 |
| --- | --- | --- |
| 면허제한 | `BidPublicInfoService/getBidPblancListInfoLicenseLimit` | 참가에 필요한 면허/업종 제한과 대체 허용 업종 |
| 참가가능지역 | `BidPublicInfoService/getBidPblancListInfoPrtcptPsblRgn` | 참가 가능한 지역 제한 |

### 2. 낙찰정보

낙찰정보도 업무구분별로 수집합니다.

| 카테고리 | 의미 | API |
| --- | --- | --- |
| `GOODS` | 물품 | `ScsbidInfoService/getScsbidListSttusThngPPSSrch` |
| `SERVICE` | 용역 | `ScsbidInfoService/getScsbidListSttusServcPPSSrch` |
| `CONSTRUCTION` | 공사 | `ScsbidInfoService/getScsbidListSttusCnstwkPPSSrch` |
| `FOREIGN` | 외자 | `ScsbidInfoService/getScsbidListSttusFrgcptPPSSrch` |

낙찰정보는 `bid_notice_no`, `bid_notice_order` 기준으로 입찰공고와 연결됩니다.

낙찰정보 normalize 이후 해당 window에서 확인된 낙찰업체 사업자번호를 기준으로 조달업체 기본정보와 업종정보를 수집합니다.

| 데이터 | API | 의미 |
| --- | --- | --- |
| 조달업체 기본정보 | `UsrInfoService02/getPrcrmntCorpBasicInfo02` | 낙찰업체명, 대표자, 소재지, 연락처, 변경일시 |
| 조달업체 업종정보 | `UsrInfoService02/getPrcrmntCorpIndstrytyInfo02` | 낙찰업체의 등록 업종 목록 |

### 3. 계약정보

계약정보도 업무구분별로 수집합니다.

| 카테고리 | 의미 | API |
| --- | --- | --- |
| `GOODS` | 물품 | `CntrctInfoService/getCntrctInfoListThngPPSSrch` |
| `SERVICE` | 용역 | `CntrctInfoService/getCntrctInfoListServcPPSSrch` |
| `CONSTRUCTION` | 공사 | `CntrctInfoService/getCntrctInfoListCnstwkPPSSrch` |
| `FOREIGN` | 외자 | `CntrctInfoService/getCntrctInfoListFrgcptPPSSrch` |

계약 본문은 `contract`에 저장하고, API의 목록형 문자열은 관계 테이블로 분리합니다.

| 데이터 | 의미 |
| --- | --- |
| 계약 본문 | 계약번호, 계약명, 계약기관, 계약일자, 계약금액, 계약방법 |
| 계약업체 목록 | `corpList`를 파싱한 계약 참여 업체, 역할, 지분율, 사업자번호 |
| 수요기관 목록 | `dminsttList`를 파싱한 수요기관 코드, 기관명, 부서, 담당자 |

## 수집 정책

모든 G2B API 호출은 기본적으로 다음 정책을 따릅니다.

- `type=json`
- `inqryDiv=1`: 등록일시 기준 조회
- `inqryBgnDt`, `inqryEndDt`: `YYYYMMDDHHMM` 형식
- `pageNo`, `numOfRows` 기반 pagination

계약정보 API는 `*PPSSrch` 엔드포인트를 사용하며, 다른 API와 달리 계약체결일자 기준 파라미터를 사용합니다.

- `inqryDiv=1`: 계약체결일자 기준 조회
- `inqryBgnDate`, `inqryEndDate`: `YYYYMMDD` 형식

HTTP status가 `200`이어도 G2B 응답 payload가 오류이면 실패로 처리합니다.

예:

- `response.header.resultCode != "00"`
- `*.ResponseError.header.resultCode`가 존재하는 경우

이런 응답은 정상 데이터로 저장하지 않고 예외를 발생시킵니다.

## 실행 주기

Prefect deployment는 `apps.g2b.pipeline.app.deployments`에서 등록합니다.

| Flow | Deployment | 실행 방식 |
| --- | --- | --- |
| `g2b-bid-initial-ingest` | `manual` | 수동 backfill |
| `g2b-bid-5min-ingest` | `every-5-minutes` | 5분 주기 |
| `g2b-success-bid-initial-ingest` | `manual` | 수동 backfill |
| `g2b-success-bid-5min-ingest` | `every-5-minutes` | 5분 주기 |
| `g2b-contract-initial-ingest` | `manual` | 수동 backfill |
| `g2b-contract-hourly-ingest` | `every-60-minutes` | 60분 주기 |
| `g2b-contract-daily-reconcile` | `daily-02-kst` | 전일 계약정보 일 단위 보정 |

주기 수집은 마지막 실행 시각만 보는 방식이 아니라 lookback window 방식입니다.

현재 기본값:

- 입찰공고: `G2B_BID_5MIN_LOOKBACK_MINUTES=180`
- 낙찰정보: `G2B_SUCCESS_BID_5MIN_LOOKBACK_MINUTES=180`
- 계약정보: `G2B_CONTRACT_HOURLY_LOOKBACK_MINUTES=360`

입찰공고와 낙찰정보는 5분마다 최근 180분 데이터를 다시 조회하고 upsert합니다. 계약정보는 API 반환량이 많아서 60분마다 최근 360분 데이터를 다시 조회합니다. 이 정책은 API 반영 지연, 짧은 장애, missed run이 있어도 다음 실행에서 누락 구간을 다시 덮기 위한 목적입니다.

Backfill 기본 시작 시점:

- `G2B_INGEST_DEFAULT_START=202605010000`

낙찰업체 정보는 낙찰정보 normalize 이후 해당 window의 `winner_business_no` distinct 전체를 매번 조회합니다.

- 기본정보는 `business_no` 기준 upsert
- 업종정보는 `business_no` 기준 기존 normalized row를 교체한 뒤 현재 API 결과를 insert/upsert
- `last_checked_at`은 API 확인 시각으로 기록

## 저장 구조

raw table과 normalized table은 분리합니다.

| 데이터 | Raw Table | Normalized Table |
| --- | --- | --- |
| 입찰공고 | `g2b.bid_public_notice_raw` | `g2b.bid_public_notice` |
| 면허제한 | `g2b.bid_public_notice_license_limit_raw` | `g2b.bid_public_notice_license_limit` |
| 참가가능지역 | `g2b.bid_public_notice_participation_region_raw` | `g2b.bid_public_notice_participation_region` |
| 낙찰정보 | `g2b.successful_bid_raw` | `g2b.successful_bid` |
| 조달업체 기본정보 | `g2b.procurement_company_raw` | `g2b.procurement_company` |
| 조달업체 업종정보 | `g2b.procurement_company_industry_raw` | `g2b.procurement_company_industry` |
| 계약정보 | `g2b.contract_raw` | `g2b.contract` |
| 계약업체 | - | `g2b.contract_company` |
| 계약 수요기관 | - | `g2b.contract_demand_organization` |

raw table은 API 원문을 `raw_payload`에 저장하고, 조회 window 같은 수집 메타데이터를 함께 보관합니다.

normalized table은 MCP와 semantic layer가 바로 사용할 수 있도록 공통 필드로 정리합니다.

테이블명, entity type, identity key의 기준은 `apps/g2b/schema/canonical.py`에 둡니다. Pipeline repository는 이 기준을 물리 DB 테이블로 구현하고, MCP는 같은 기준으로 조회합니다.

예:

- 입찰공고: 카테고리, 공고번호, 공고명, 수요기관, 예산, 공고일시, 마감일시, 개찰일시
- 면허제한: 제한그룹, 제한순번, 면허명/코드, 허용 업종, 주력 업종 조건
- 참가가능지역: 제한그룹, 제한순번, 지역명/코드
- 낙찰정보: 낙찰업체, 사업자번호, 낙찰금액, 낙찰률, 실개찰일시, 최종낙찰일자
- 조달업체 기본정보: 사업자번호, 업체명, 대표자, 소재지, 연락처, 변경일시, 마지막 확인일시
- 조달업체 업종정보: 사업자번호, 업종명/코드, 유효기간, 대표업종 여부, 상태
- 계약정보: 통합계약번호, 계약명, 계약기관, 계약일자, 체결일자, API 등록일시, 계약금액, 계약업체, 수요기관

컬럼명은 아래 원칙을 따릅니다.

- API 원문 약어를 그대로 쓰지 않고 의미가 드러나는 snake_case 사용
- 조달 도메인 식별자는 `bid_notice_no`, `bid_notice_order`처럼 명시적으로 유지
- 입찰공고 기관은 `notice_agency_*`와 `demand_agency_*`를 구분
- 계약 질문에서 "계약한 건", "오늘 계약"은 `contract_date` 기준으로 해석
- `registered_at`은 API 레코드 등록/조회 기준이며 실제 계약일이 아님
- 낙찰 결과는 `winner_*`, `winning_amount`, `winning_rate`로 표현
- 조건 데이터는 `license_limit_*`, `allowed_industries`, `region_*`처럼 관계 의미가 드러나게 표현

## 중복 처리와 락 정책

수집은 idempotent하게 동작합니다.

- raw row는 안정적인 `resource_key`를 기준으로 `ON CONFLICT ... DO UPDATE`
- normalized row도 동일하게 upsert
- 같은 window를 다시 수집해도 중복 row를 만들지 않음

실행 중복 방지를 위해 두 종류의 락을 사용합니다.

| 대상 | Lock |
| --- | --- |
| 입찰공고 계열 | PostgreSQL advisory lock `g2b_bid_ingest_run` |
| 낙찰정보 계열 | PostgreSQL advisory lock `g2b_success_bid_ingest_run` |
| 계약정보 계열 | PostgreSQL advisory lock `g2b_contract_ingest_run` |

Prefect deployment도 concurrency limit `1`, collision strategy `CANCEL_NEW`를 사용합니다.

## Semantic / MCP 연계

normalized table은 아래 계층에서 사용합니다.

- `apps/g2b/mcp`: MCP tool 조회
- `apps/g2b/semantic`: entity / relationship 정의
- `apps/g2b/semantic`: semantic object/document 생성

현재 주요 semantic relationship:

- `BidNotice -> requires -> LicenseConstraint`
- `BidNotice -> allows_industry -> AllowedIndustry`
- `BidNotice -> restricted_to -> ParticipationRegion`
- `BidNotice -> result_of -> SuccessfulBid`
- `BidNotice -> awarded_to -> Company`
- `Company -> registered_industry -> Industry`
- `Contract -> contracted_by -> Agency`
- `Contract -> contracted_with -> Company`

MCP에서는 다음 조회가 가능합니다.

- `search_bid`: 입찰공고 조회
- `search_bid(..., include_license_limits=True)`: 면허제한 포함
- `search_bid(..., include_participation_regions=True)`: 참가가능지역 포함
- `search_bid(..., include_success_bids=True)`: 연결된 낙찰정보 포함
- `search_success_bid`: 낙찰정보 직접 조회
- `search_contract`: 계약정보 직접 조회

## 운영 명령

루트 compose stack에서 실행합니다.

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml --profile g2b up -d --build
```

전체 검증:

```bash
python3 scripts/ops.py check-all
```

## 주의사항

- `G2B_API_KEY`, DB URL 같은 secret은 env file로 주입합니다.
- secret 값을 task payload나 source code에 직접 넣지 않습니다.
- G2B API가 HTTP 200으로 오류 payload를 반환할 수 있으므로 `resultCode` 검사를 반드시 유지해야 합니다.
- Prefect concurrency slot이 비정상적으로 남으면 이후 5분 주기 실행이 `Deployment concurrency limit reached`로 취소될 수 있습니다. 이 경우 running flow와 global concurrency active slot을 함께 확인해야 합니다.
