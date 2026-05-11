from __future__ import annotations

from dataclasses import dataclass

from core.catalog.app_catalog import list_required_platform_services
from core.catalog.app_service_catalog import list_app_service_definitions
from core.catalog.platform_service_catalog import (
    PlatformServiceDefinition,
    list_active_platform_service_definitions,
    list_platform_service_definitions,
)


@dataclass(frozen=True)
class CatalogRegistry:
    active_platform_services: tuple[PlatformServiceDefinition, ...]
    available_platform_services: tuple[PlatformServiceDefinition, ...]
    app_services: tuple[PlatformServiceDefinition, ...]
    required_platform_services: tuple[str, ...]

    @classmethod
    def load(cls) -> "CatalogRegistry":
        return cls(
            active_platform_services=list_active_platform_service_definitions(),
            available_platform_services=list_platform_service_definitions(),
            app_services=list_app_service_definitions(),
            required_platform_services=list_required_platform_services(),
        )
