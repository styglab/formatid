from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache

from core.catalog.app_catalog import iter_app_manifest_paths, load_json_file
from core.catalog.app_service_catalog import list_app_service_definitions
from core.catalog.platform_service_catalog import list_platform_service_definitions
from core.catalog.queue_catalog import list_queue_definitions
from core.catalog.service_catalog import list_available_service_definitions


@dataclass(frozen=True)
class CapabilityProvider:
    capability: str
    provider_type: str
    provider_name: str
    capability_family: str | None = None
    queue: str | None = None
    queue_name: str | None = None
    service_name: str | None = None
    app: str | None = None


@dataclass(frozen=True)
class AppCapabilityRequirement:
    app: str
    capability_type: str
    capability_name: str


@dataclass(frozen=True)
class CapabilityRegistry:
    providers: tuple[CapabilityProvider, ...]
    app_requirements: tuple[AppCapabilityRequirement, ...]
    missing_requirements: tuple[AppCapabilityRequirement, ...] = field(default_factory=tuple)

    def providers_for(self, capability: str) -> tuple[CapabilityProvider, ...]:
        return tuple(provider for provider in self.providers if provider.capability == capability)


@lru_cache(maxsize=1)
def load_capability_registry() -> CapabilityRegistry:
    providers = _load_capability_providers()
    requirements = _load_app_capability_requirements()
    provided = {provider.capability for provider in providers}
    missing = tuple(
        requirement
        for requirement in requirements
        if requirement.capability_name not in provided
    )
    return CapabilityRegistry(
        providers=providers,
        app_requirements=requirements,
        missing_requirements=missing,
    )


def _load_capability_providers() -> tuple[CapabilityProvider, ...]:
    providers: list[CapabilityProvider] = []

    for definition in list_queue_definitions():
        providers.append(
            CapabilityProvider(
                capability=definition.queue,
                capability_family=definition.capability,
                provider_type="queue",
                provider_name=definition.queue,
                queue=definition.queue,
                queue_name=definition.queue_name,
                service_name=definition.worker_service,
            )
        )

    for definition in list_platform_service_definitions():
        providers.append(
            CapabilityProvider(
                capability=definition.service_name,
                provider_type="platform_service",
                provider_name=definition.service_name,
                service_name=definition.service_name,
            )
        )

    for definition in list_available_service_definitions():
        providers.append(
            CapabilityProvider(
                capability=definition.service_name,
                capability_family=definition.queue,
                provider_type="worker_service",
                provider_name=definition.service_name,
                queue=definition.queue,
                queue_name=definition.queue_name,
                service_name=definition.service_name,
            )
        )

    for definition in list_app_service_definitions():
        providers.append(
            CapabilityProvider(
                capability=definition.service_name,
                provider_type="app_service",
                provider_name=definition.service_name,
                service_name=definition.service_name,
            )
        )

    return tuple(providers)


def _load_app_capability_requirements() -> tuple[AppCapabilityRequirement, ...]:
    requirements: list[AppCapabilityRequirement] = []
    for path in iter_app_manifest_paths():
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            continue
        app_name = payload.get("app")
        if not isinstance(app_name, str) or not app_name:
            continue
        requires = payload.get("requires", {})
        if not isinstance(requires, dict):
            continue
        for queue in _string_list(requires.get("queues")):
            requirements.append(
                AppCapabilityRequirement(
                    app=app_name,
                    capability_type="queue",
                    capability_name=queue,
                )
            )
        for service_name in _string_list(requires.get("platform_services")):
            requirements.append(
                AppCapabilityRequirement(
                    app=app_name,
                    capability_type="platform_service",
                    capability_name=service_name,
                )
            )
        for worker_name in _string_list(requires.get("workers")):
            requirements.append(
                AppCapabilityRequirement(
                    app=app_name,
                    capability_type="worker_service",
                    capability_name=worker_name,
                )
            )
    return tuple(requirements)


def _string_list(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item)
