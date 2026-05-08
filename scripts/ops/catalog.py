from __future__ import annotations

from core.catalog.app_catalog import list_required_platform_services, list_required_workers
from core.catalog.platform_service_catalog import list_platform_service_definitions
from core.catalog.queue_catalog import list_queue_definitions
from core.catalog.service_catalog import list_available_service_definitions


def inspect_catalog() -> dict:
    active_platform_services = set(list_required_platform_services())
    active_workers = set(list_required_workers())
    return {
        "platform_services": [
            {
                "service_name": definition.service_name,
                "service_type": definition.service_type,
                "active": definition.service_name in active_platform_services,
                "source": "core/**/manifests + services/*/manifests",
            }
            for definition in list_platform_service_definitions()
        ],
        "workers": [
            {
                "service_name": definition.service_name,
                "queue": definition.queue,
                "queue_name": definition.queue_name,
                "active": definition.service_name in active_workers,
                "source": "services/*/manifests/workers",
            }
            for definition in list_available_service_definitions()
        ],
        "queues": [
            {
                "queue": definition.queue,
                "queue_name": definition.queue_name,
                "capability": definition.capability,
                "kind": definition.kind,
                "worker_service": definition.worker_service,
                "active": definition.worker_service in active_workers,
                "source": "services/*/manifests/queues.json",
            }
            for definition in list_queue_definitions()
        ],
    }
