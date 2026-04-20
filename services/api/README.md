# API

`services/api`는 Redis에 저장된 worker heartbeat와 queue 상태를 읽는 FastAPI 서비스입니다.

빌드 정의는 [infra/images/api](/home/user/projects/formatid/infra/images/api) 에 두고, 이 디렉터리에는 애플리케이션 코드만 둡니다.

현재 제공하는 엔드포인트:

- `GET /`
- `GET /health/live`
- `GET /health/ready`
- `GET /health`
- `GET /health/workers`
- `GET /checkpoints`
- `GET /checkpoints/{name}`

health 조회는 queue task를 태우지 않고 Redis heartbeat를 직접 읽는 방식으로 처리합니다.
