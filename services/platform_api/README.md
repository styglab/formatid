# API

`services/platform_api` exposes platform health, checkpoints, service observability, app summaries, and logs.

`/health/ready` checks runtime dependencies such as Redis. App service heartbeat
state is included for visibility, but `not_configured` means no app service is
currently reporting heartbeat and does not make the platform API unready.

Current endpoints:

- `GET /`
- `GET /health/live`
- `GET /health/ready`
- `GET /health`
- `GET /health/app-services`
- `GET /checkpoints`
- `GET /checkpoints/{name}`
- `GET /service-runs`
- `GET /service-requests`
- `GET /service-events`
- `GET /dashboard/summary`
- `GET /dashboard/apps`
- `GET /logs/services`
- `GET /logs`

## Logs

`GET /logs` returns service logs for platform dashboard and ops tooling. Prefer request/run/correlation/resource identifiers over large payloads.
