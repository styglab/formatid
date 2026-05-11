from __future__ import annotations

from core.catalog.app_catalog import list_required_platform_services
from core.catalog.app_service_catalog import list_app_service_definitions
from core.catalog.platform_service_catalog import list_platform_service_definitions


def inspect_catalog() -> dict:
    active_platform_services = set(list_required_platform_services())
    return {
        "platform_services": [
            {
                "service_name": definition.service_name,
                "service_type": definition.service_type,
                "active": definition.service_name in active_platform_services,
                "source": "services/*/manifests",
            }
            for definition in list_platform_service_definitions()
        ],
        "app_services": [
            {
                "service_name": definition.service_name,
                "service_type": definition.service_type,
                "source": "apps/*/manifests/services",
            }
            for definition in list_app_service_definitions()
        ],
    }
