from __future__ import annotations

from dataclasses import dataclass

from core.catalog.app_catalog import list_required_platform_services, list_required_workers
from core.catalog.app_service_catalog import list_app_service_definitions
from core.catalog.capability_catalog import CapabilityRegistry, load_capability_registry
from core.catalog.platform_service_catalog import (
    PlatformServiceDefinition,
    list_active_platform_service_definitions,
    list_platform_service_definitions,
)
from core.catalog.queue_catalog import QueueDefinition, list_queue_definitions
from core.catalog.service_catalog import WorkerServiceDefinition, list_available_service_definitions, list_service_definitions
from core.runtime.task_runtime.catalog import TaskDefinition, list_task_definitions


@dataclass(frozen=True)
class CatalogRegistry:
    active_platform_services: tuple[PlatformServiceDefinition, ...]
    available_platform_services: tuple[PlatformServiceDefinition, ...]
    active_worker_services: tuple[WorkerServiceDefinition, ...]
    available_worker_services: tuple[WorkerServiceDefinition, ...]
    app_services: tuple[PlatformServiceDefinition, ...]
    queues: tuple[QueueDefinition, ...]
    tasks: tuple[TaskDefinition, ...]
    required_platform_services: tuple[str, ...]
    required_workers: tuple[str, ...]
    capabilities: CapabilityRegistry

    @classmethod
    def load(cls) -> "CatalogRegistry":
        return cls(
            active_platform_services=list_active_platform_service_definitions(),
            available_platform_services=list_platform_service_definitions(),
            active_worker_services=list_service_definitions(),
            available_worker_services=list_available_service_definitions(),
            app_services=list_app_service_definitions(),
            queues=list_queue_definitions(),
            tasks=list_task_definitions(),
            required_platform_services=list_required_platform_services(),
            required_workers=list_required_workers(),
            capabilities=load_capability_registry(),
        )
