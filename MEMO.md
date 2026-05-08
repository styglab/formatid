# bidgraph
나라장터 데이터 수집, mcp, mcp 활용한 답변

# data pipeline
우선 api raw 데이터 자체를 수집 및 저장
mcp용 데이터 가공

# db가 필요한 이유는
매번 API 호출 안되게
매번 정규화 안되게
추후 분석/추천까지 가려면 과거 데이터 필요
agent 상태 관리 (memory 등)

Step 1 (지금)
FastMCP + API + 매핑
Step 2 (다음)
Postgres 추가
normalize 결과 저장
Step 3
Redis 캐시
Step 4
scoring



