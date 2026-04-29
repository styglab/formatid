# API

`services/runtime_api`는 Redis에 저장된 worker heartbeat와 queue 상태를 읽는 FastAPI 서비스입니다.

빌드 정의는 [services/runtime_api/infra/image](infra/image) 에 두고, 이 디렉터리에는 애플리케이션 코드만 둡니다.

현재 제공하는 엔드포인트:

- `GET /`
- `GET /health/live`
- `GET /health/ready`
- `GET /health`
- `GET /health/workers`
- `GET /checkpoints`
- `GET /checkpoints/{name}`
- `GET /logs/services`
- `GET /logs`

health 조회는 queue task를 태우지 않고 Redis heartbeat를 직접 읽는 방식으로 처리합니다.

## Logs API Contract

`GET /logs`는 runtime dashboard와 운영 도구가 공통으로 쓰는 서비스 로그 조회 엔드포인트입니다.

기본 규약:

- 기본 정렬은 `sort=desc`이며, 최신 로그가 먼저 반환됩니다.
- 로그 행에는 `request_id`, `run_name`, `task_id`, `correlation_id` 같은 상관관계 필드가 포함됩니다.
- 큰 payload 전체를 로그에 넣기보다 요약, 식별자, artifact ref 위주로 남기는 것을 권장합니다.

지원 파라미터:

- `limit`
- `service_name`
- `worker_id`
- `level`
- `event_name`
- `request_id`
- `run_name`
- `task_id`
- `correlation_id`
- `after_id`
- `before_id`
- `sort=asc|desc`

커서 규약:

- `after_id`: 해당 id보다 큰 로그만 반환
- `before_id`: 해당 id보다 작은 로그만 반환
- `sort=desc`: 최신순 조회
- `sort=asc`: 오래된순 조회

## Dashboard Refresh Policy

runtime dashboard는 상단 toolbar의 refresh를 단일 새로고침 진입점으로 사용합니다.

- 각 탭은 자체 refresh 버튼을 두지 않습니다.
- runtime/app 탭 refresh는 dashboard summary 데이터를 갱신합니다.
- log 탭 refresh는 현재 선택된 로그 source와 log source 목록만 갱신합니다.
- auto refresh도 같은 정책을 따릅니다.
