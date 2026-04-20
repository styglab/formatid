# pps task workflow

## overview
물품 공고 정보 조회 데이터, 물품 공고 첨부파일, 참여한 업체 목록, 낙찰된 업체 수집

# task info
1. bid list
- inqryBgnDt(조회시작일시), inqryEndDt(조회종료일시) 요청변수를 활용하여 리스트 조회 및 저장

2. bid attachments
- bid list 조회 결과 중, stdNtceDocUrl, ntceSpecDocUrl1, ntceSpecDocUrl2, ntceSpecDocUrl3 형태의 url을 통해 파일을 다운로드 하고 ntceSpecFileNm1, ntceSpecFileNm2, ntceSpecFileNm3 형태로 파일명을 기반으로 저장

3. bid result participants
- bidNtceNo 요청변수를 활용하여 공고별 참여 정보 조회 및 저장

4. bid result winners
- bidNtceNo 요청변수를 활용하여 낙찰 정보 저장

# 수집 데이터
1. bid list - 물품 공고 정보 조회
- 개요: 기본 정보, 첨부파일 URL
- 오픈API 서비스명: 나라장터 입찰공고정보서비스(BidPublicInfoService)
- 상세기능: 나라장터검색조건에 의한 입찰공고물품조회
- 요청주소: http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch
- 요청 예시
PUBLIC_API_KEY = "ZtD9YiUuUVAROJtbeXtGln/75YYiRFlvxRvZ9zZMgPZyh8fae2oIhl9P8SeZcfHJ/8c9xSK4Q1SLVwqrY/nL9w=="
url = "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng"
params = {
    "serviceKey": PUBLIC_API_KEY,
    "pageNo": 1,
    "numOfRows": 1,
    "type": "json",
    "inqryDiv": 1, #1:등록일시 , 2.입찰공고번호 , 3.변경일시
    "inqryBgnDt": "202001010000", #"YYYYMMDDHHMM"
    "inqryEndDt": "202001022359", #"YYYYMMDDHHMM"
}
- 응답 예시
{'response': {'header': {'resultCode': '00', 'resultMsg': '정상'}, 'body': {'items': [{'bidNtceNo': '20200100028', 'bidNtceOrd': '000', 'reNtceYn': 'N', 'rgstTyNm': '조달청 또는 나라장터 자체 공고건', 'ntceKindNm': '등록공고', 'intrbidYn': 'N', 'bidNtceDt': '2020-01-02 08:26:52', 'refNo': '곡성군 공고 2019- 1955호', 'bidNtceNm': '2020년 부산물자원화센터 퇴비혼합용 버섯배지 구매', 'ntceInsttCd': '4860000', 'ntceInsttNm': '전라남도 곡성군', 'dminsttCd': '4860000', 'dminsttNm': '전라남도 곡성군', 'bidMethdNm': '전자입찰', 'cntrctCnclsMthdNm': '제한경쟁', 'ntceInsttOfclNm': '김유신', 'ntceInsttOfclTelNo': '061-360-2813', 'ntceInsttOfclEmailAdrs': 'hgh0947@korea.kr', 'exctvNm': '김유신', 'bidQlfctRgstDt': '2020-01-05 18:00', 'cmmnSpldmdAgrmntRcptdocMethd': '없음', 'cmmnSpldmdAgrmntClseDt': '', 'cmmnSpldmdCorpRgnLmtYn': 'N', 'bidBeginDt': '2020-01-02 09:00:00', 'bidClseDt': '2020-01-06 11:00:00', 'opengDt': '2020-01-06 12:00:00', 'ntceSpecDocUrl1': 'https://www.g2b.go.kr/pn/pnp/pnpe/UntyAtchFile/downloadFile.do?bidPbancNo=20200100028&bidPbancOrd=000&fileType=&fileSeq=11&prcmBsneSeCd=01', 'ntceSpecDocUrl2': 'https://www.g2b.go.kr/pn/pnp/pnpe/UntyAtchFile/downloadFile.do?bidPbancNo=20200100028&bidPbancOrd=000&fileType=&fileSeq=22&prcmBsneSeCd=01', 'ntceSpecDocUrl3': '', 'ntceSpecDocUrl4': '', 'ntceSpecDocUrl5': '', 'ntceSpecDocUrl6': '', 'ntceSpecDocUrl7': '', 'ntceSpecDocUrl8': '', 'ntceSpecDocUrl9': '', 'ntceSpecDocUrl10': '', 'ntceSpecFileNm1': '20200100028-00_1577921176316_공고서(버섯배지 구매).hwp', 'ntceSpecFileNm2': '20200100028-00_1577921176316_과업지시서.hwp', 'ntceSpecFileNm3': '', 'ntceSpecFileNm4': '', 'ntceSpecFileNm5': '', 'ntceSpecFileNm6': '', 'ntceSpecFileNm7': '', 'ntceSpecFileNm8': '', 'ntceSpecFileNm9': '', 'ntceSpecFileNm10': '', 'rbidPermsnYn': 'N', 'prdctClsfcLmtYn': 'Y', 'mnfctYn': 'N', 'prearngPrceDcsnMthdNm': '복수예가', 'totPrdprcNum': '15', 'drwtPrdprcNum': '4', 'asignBdgtAmt': '57750000', 'presmptPrce': '52500000', 'opengPlce': '국가종합전자조달시스템(나라장터)', 'bidNtceDtlUrl': 'https://www.g2b.go.kr/link/PNPE027_01/single/?bidPbancNo=20200100028&bidPbancOrd=000', 'bidNtceUrl': 'https://www.g2b.go.kr/link/PNPE027_01/single/?bidPbancNo=20200100028&bidPbancOrd=000', 'bidPrtcptFeePaymntYn': '', 'bidPrtcptFee': '', 'bidGrntymnyPaymntYn': '', 'crdtrNm': '곡성군재무관', 'dtilPrdctClsfcNo': '1112170101', 'dtilPrdctClsfcNoNm': '톱밥', 'prdctSpecNm': '과업지시서 참조', 'prdctQty': '2100', 'prdctUnit': '㎥', 'prdctUprc': '', 'dlvrTmlmtDt': '2020-12-31 00:00:00', 'dlvrDaynum': '', 'dlvryCndtnNm': '납품장소 하차도', 'purchsObjPrdctList': '[1^1112170101^톱밥]', 'untyNtceNo': '20200100033', 'cmmnSpldmdMethdCd': '', 'cmmnSpldmdMethdNm': '(없음)공동수급불허', 'stdNtceDocUrl': 'https://www.g2b.go.kr/pn/pnp/pnpe/UntyAtchFile/downloadFile.do?bidPbancNo=20200100028&bidPbancOrd=000&fileType=&fileSeq=11&prcmBsneSeCd=01', 'brffcBidprcPermsnYn': 'N', 'dsgntCmptYn': 'N', 'rsrvtnPrceReMkngMthdNm': '', 'arsltApplDocRcptMthdNm': '없음', 'arsltApplDocRcptDt': '', 'orderPlanUntyNo': 'R19DD10453342', 'sucsfbidLwltRate': '84.245', 'rgstDt': '2020-01-02 08:26:52', 'bfSpecRgstNo': '806749', 'infoBizYn': '', 'sucsfbidMthdCd': '낙030001', 'sucsfbidMthdNm': '공고서참조', 'chgDt': '', 'dminsttOfclEmailAdrs': 'hgh0947@korea.kr', 'indstrytyLmtYn': '', 'chgNtceRsn': '', 'rbidOpengDt': '2020-01-06 12:00:00', 'VAT': '', 'indutyVAT': '', 'bidWgrnteeRcptClseDt': '', 'rgnLmtBidLocplcJdgmBssCd': '', 'rgnLmtBidLocplcJdgmBssNm': '', 'techAbltEvlRt': '', 'bidPrceEvlRt': '', 'sucsfbidMthdAppStd': ''}], 'numOfRows': 1, 'pageNo': 1, 'totalCount': 203}}}

2. bid result participants - 참여한 업체 목록 조회
- 개요: 투찰업체 목록
- 오픈API 서비스명: 나라장터 낙찰정보서비스(ScsbidInfoService)
- 상세기능: 개찰결과 개찰완료 목록 조회
- 요청주소: http://apis.data.go.kr/1230000/as/ScsbidInfoService/getOpengResultListInfoOpengCompt
- 요청 예시
PUBLIC_API_KEY = "ZtD9YiUuUVAROJtbeXtGln/75YYiRFlvxRvZ9zZMgPZyh8fae2oIhl9P8SeZcfHJ/8c9xSK4Q1SLVwqrY/nL9w=="
url = "https://apis.data.go.kr/1230000/as/ScsbidInfoService/getOpengResultListInfoOpengCompt"
params = {
    "serviceKey": PUBLIC_API_KEY,
    "type": "json",
    "inqryDiv": 4, #4: 입찰공고번호
    "bidNtceNo": "20200100028",
    "pageNo": 1,
    "numOfRows": 1,
}
- 응답 예시
{'response': {'header': {'resultCode': '00', 'resultMsg': '정상'}, 'body': {'items': [{'opengRsltDivNm': '개찰완료', 'bidNtceNo': '20200100028', 'bidNtceOrd': '001', 'bidClsfcNo': '1', 'rbidNo': '000', 'opengRank': '1', 'prcbdrBizno': '7188100283', 'prcbdrNm': '주식회사 마텍', 'prcbdrCeoNm': '염철호', 'bidprcAmt': '48924000', 'bidprcrt': '84.257', 'rmrk': '정상', 'cnsttyAccotBidAmtUrl': '', 'drwtNo1': ' 01', 'drwtNo2': ' 10', 'bidprcDt': '2020-01-03 15:44:20', 'bidPrceEvlVal': '55.04', 'techEvlVal': '', 'totalEvlAmtVal': '', 'techEvlNaturVal': ''}], 'numOfRows': 1, 'pageNo': 1, 'totalCount': 51}}}

3. 낙찰된 업체 조회
- 개요: 낙찰업체
- 오픈API 서비스명: 나라장터 낙찰정보서비스(ScsbidInfoService)
- 상세기능: 낙찰된 목록 현황 물품조회
- 요청주소: http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThng
- 요청 예시
PUBLIC_API_KEY = "ZtD9YiUuUVAROJtbeXtGln/75YYiRFlvxRvZ9zZMgPZyh8fae2oIhl9P8SeZcfHJ/8c9xSK4Q1SLVwqrY/nL9w=="
url = "https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThng"
params = {
    "serviceKey": PUBLIC_API_KEY,
    "type": "json",
    "inqryDiv": 4, #4: 입찰공고번호
    "bidNtceNo": "20200100028",
    "pageNo": 1,
    "numOfRows": 1,
}
- 응답 예시
{'response': {'header': {'resultCode': '00', 'resultMsg': '정상'}, 'body': {'items': [{'bidNtceNo': '20200100028', 'bidNtceOrd': '001', 'bidClsfcNo': '1', 'rbidNo': '000', 'ntceDivCd': '통050001', 'bidNtceNm': '2020년 부산물자원화센터 퇴비혼합용 버섯배지 구매', 'prtcptCnum': '51', 'bidwinnrNm': '주식회사 마텍', 'bidwinnrBizno': '7188100283', 'bidwinnrCeoNm': '염철호', 'bidwinnrAdrs': '전라남도 광양시 광양읍 익신산단1길 25', 'bidwinnrTelNo': '061-761-9372', 'sucsfbidAmt': '48924000', 'sucsfbidRate': '84.257', 'rlOpengDt': '2020-01-06 18:00:00', 'dminsttCd': '4860000', 'dminsttNm': '전라남도 곡성군', 'rgstDt': '2020-01-15 09:23:03', 'fnlSucsfDate': '2020-01-15', 'fnlSucsfCorpOfcl': ''}], 'numOfRows': 10, 'pageNo': 1, 'totalCount': 1}}}

## 참고사항
API 요청 시, page를 순회해서 데이터를 모두 수집해야 하니, pageNo, numOfRows는 적당히 잡기


## structure
pps/
├── bid/
│   ├── list.py
│   └── attachment.py
│
├── bid_result/
│   ├── participants.py
│   └── winners.py


## workflow
1. pps.bid.list.collect
→ PPS bid list API 조회
→ raw.pps_bid_notices 저장
→ page/window 상태 저장

2. pps.bid.downstream.enqueue
→ raw.pps_bid_notices 조회
→ raw.pps_task_states 기준으로 후속 미처리/failed 대상 선정
→ attachment / participants / winners task enqueue

3. 각 후속 task
→ raw.pps_bid_attachments
→ raw.pps_bid_result_participants
→ raw.pps_bid_result_winners 
에 저장


테이블 역할:

raw.pps_bid_notices
원천 공고 목록. 후속 수집의 source of truth.

raw.pps_bid_attachments
공고 첨부파일 메타/저장 위치.

raw.pps_bid_result_participants
참여업체 결과.

raw.pps_bid_result_winners
낙찰업체 결과.

raw.pps_task_states
공고/후속 작업별 수집 상태.

작업 분리:

pps.bid.list.collect
날짜 window/page 기준으로 공고만 저장.
downstream enqueue 하지 않음.

pps.bid.downstream.enqueue
raw.pps_bid_notices에서 후속 수집이 필요한 공고를 조회.
attachment / participants / winners task enqueue.

pps.bid.attachment.download
공고 raw_payload를 보고 첨부 다운로드.

pps.bid_result.participants.collect
bidNtceNo 기준 참여업체 수집.

pps.bid_result.winners.collect
bidNtceNo 기준 낙찰업체 수집.


