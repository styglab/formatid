# Catalog

`core/catalog` loads manifest-driven platform metadata.

Current catalog groups:

- `app_catalog`: app manifests and app-level platform requirements
- `platform_service_catalog`: platform service definitions from `services/*/manifests`
- `app_service_catalog`: app-owned API/service definitions from `apps/*/manifests/services`

Apps declare platform dependencies through `requires.platform_services` in `apps/*/manifests/app.json`.
