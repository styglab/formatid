# run codex yolo mode
codex -a never -s danger-full-access

# codex ingestion
먼저 AGENTS.md를 읽고 그 규칙을 따라.

목표:
Semantic Platform Dashboard/API에 현재 업로드되어 있지만
아직 catalog proposal이 생성되지 않은 신규 source를 찾아
서 codex_manual ingestion을 진행해줘.

중요 규칙:
- 질문하지 마.
- 코드는 변경하지 마.
- tracked file은 수정하지 마. AGENTS.md, docs, services,
apps, deploy, tests 파일은 절대 수정하지 마.
- 작업 산출물은 tmp/semantic_ingestion/ 아래에만 둬.
- tmp/semantic_ingestion/ 아래 비밀이 아닌 임시 파일은
생성/수정/삭제해도 된다.
- secret 값은 출력하지 말고, tmp 파일에도 저장하지 마.
- proposal 생성까지만 진행해. apply/restore/delete는 하
지 마.
- 생성된 proposal은 pending_review 상태로 남겨.
- ingestion은 반드시 semantic platform API/graph 경계를
통해 실행해. 직접 DB에 proposal을 insert하지 마.
- ingestion run이 Ingestion Runs/API에 남는 방식으로 실
행해.
- endpoint check가 가능하면 source에 연결된 secret_ref를
사용해 검증하고, 결과를 evidence/proposal에 연결해.

진행 순서:
1. AGENTS.md를 읽어 semantic_platform, codex_manual, tmp
규칙을 확인해.
2. 실행 중인 semantic platform API/dashboard/postgres 컨
테이너와 포트를 확인해.
3. 현재 등록된 sources, revisions, secrets, ingestion
runs, proposals를 API나 DB로 조회해.
4. 업로드됐지만 아직 proposal이 없거나 최신 revision이
ingest되지 않은 source를 신규 ingestion 대상으로 선택해.
5. 대상 source의 원본 문서를 object storage/API/DB 경로
를 통해 읽어.
6. source 문서를 근거로 codex_manual manual LLM response
JSON을 tmp/semantic_ingestion/ 아래에 작성해.
7. JSON 문법과 schema를 검증해.
8. manual_llm_response를 semantic platform ingestion API
에 전달해서 source별 ingestion run을 시작해.
9. run이 완료될 때까지 상태를 확인해.
10. proposal id, proposal 상태, endpoint check 결과를 확
인해.
11. apply는 하지 말고 종료해.

여러 신규 source가 있으면:
- source별로 ingestion run을 분리해.
- manual LLM payload도 source별로 분리해.
- proposal은 capability-scoped review unit으로 생성해.
- 같은 capability로 보이면 중복 확정하지 말고 merge/
deprecate 후보로 표시해.
- 너무 많으면 최근 업로드/미처리 source부터 처리하고, 처
리 기준을 결과에 적어.

마지막 보고 형식:
- 처리한 source id / revision id
- ingestion run id / status
- 생성된 proposal id 목록 / status
- endpoint check id / passed/failed/skipped
- apply 하지 않았다는 확인
- 실패 또는 주의사항

