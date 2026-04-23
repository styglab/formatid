## G2B API

`g2b-api` is the FastAPI container for the G2B app group.

This container is intentionally a minimal API skeleton. It currently keeps only
the FastAPI entrypoint, service middleware, health route, image definition, and
service manifest.

## Service

Compose service:

- `g2b-api`

Manifest files:

- `apps/g2b/api/manifests/app.json`
- `apps/g2b/api/manifests/services/g2b_api.json`

Runtime:

- `service_type`: `api`
- FastAPI middleware: `ServiceRequestMiddleware`
- host port: `${G2B_API_HOST_PORT:-8010}`

## API

Health:

```bash
curl http://localhost:8010/health/live
```

Current routes:

- `GET /health/live`

## Structure

- `app/main.py`: FastAPI application entrypoint and middleware setup
- `apps/g2b/api/infra/image/Dockerfile`: app API image
- `manifests/app.json`: app metadata and optional dependency selection
- `manifests/services/g2b_api.json`: compose service definition

## Development Notes

When the API grows, add HTTP routes under `app/api/v1/routers` and app-owned
business logic under `app/domain`. Do not put pipeline graph steps or worker task
handlers in this container.
