# context platform ingestion 하는 법
1. 간단 버전
$context_platform_ingestion tmp/sources/오픈API 활용자가이드_기업 재무정보.pdf

2. 대화로
$context_platform_ingestion
아래 파일을 끝까지 진행해줘.

대상:
tmp/sources/오픈API 활용자가이드_기업 재무정보.pdf

3. 여러 파일인 경우
Context Platform ingestion skill로 아래 파일을 끝까지 진행해줘.

대상:
tmp/sources/오픈API 활용자가이드_기업 재무정보.pdf

여러 파일이면:

Context Platform ingestion skill로 tmp/sources 안의 신규 문서를 끝
까지 ingestion 해줘.
중복 문서는 스킵하고, 처리 결과와 proposal bundle id를 요약해줘.

4. 터미널에서 직접 실행하려면:

codex exec -C /workspace --sandbox danger-full-access -a never '
$context_platform_ingestion

tmp/sources/오픈API 활용자가이드_기업 재무정보.pdf 를 끝까지
ingestion 해줘.
proposal bundle id와 주요 count를 요약해줘.
'

