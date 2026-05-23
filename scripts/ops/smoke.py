from __future__ import annotations

from core.catalog.registry import CatalogRegistry
from scripts.ops.compose import compose


def smoke_services() -> tuple[str, ...]:
    registry = CatalogRegistry.load()
    service_names = [
        *(definition.service_name for definition in registry.active_platform_services),
        *(definition.service_name for definition in registry.app_services),
    ]
    return tuple(dict.fromkeys(service_names))


def run_smoke_test() -> dict:
    services = smoke_services()
    compose("down", "-v", "--remove-orphans", check=False)

    try:
        compose("up", "-d", "--build", *services)
        ps_output = compose("ps", *services)
        return {
            "services": list(services),
            "output": ps_output,
        }
    finally:
        compose("down", "-v", "--remove-orphans", check=False)
