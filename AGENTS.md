## Project overview
- redis 기반 generic worker runtime 구현
- pps 도메인 서비스 구현 (collector 등)
- Redis: queue, heartbeat, task status, DLQ
- Postgres: 장기 보관 데이터, 중복 판별, 체크포인트, 조회용 데이터
- docs 폴더 참고

## Rules
- 도메인, 비즈니스 로직은 generic 부분에 절대 포함하지 말 것 (예: services/worker, shared)

## Workflow
- worker runtime 구현
- pps 서비스 구현 
- bid
- attachment
- 

## Commands

