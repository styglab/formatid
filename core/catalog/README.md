# Catalog

`core/catalog` loads manifest-driven platform metadata.

Current catalog groups:

- `app_catalog`: app manifests and app-level requirements
- `queue_catalog`: queue ids, queue names, worker bindings, and queue capabilities
- `service_catalog`: runnable worker service definitions
- `platform_service_catalog`: platform-facing service definitions
- `app_service_catalog`: app-owned API/service definitions
- `capability_catalog`: normalized capability providers and app capability requirements

## Capability Registry

The capability registry is a platform-level view over manifest data.

It normalizes:

- queue capabilities from `queues.json`
- worker service providers from worker manifests
- platform service providers from platform/runtime service manifests
- app service providers from app service manifests
- app requirements from `apps/*/manifests/app.json`

Rules:

- queue capabilities are declared by queue ids such as `ingest.api` or `demo.agent.tool`
- platform-facing capabilities are declared by service names such as `postgres`, `redis`, or `runtime-api`
- app manifests declare required capabilities through `requires.queues`, `requires.platform_services`, and `requires.workers`
- missing capability requirements should be treated as configuration errors
