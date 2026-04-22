from __future__ import annotations

from services.app_service.runtime.core import AppServiceRuntime
from services.app_service.runtime.stores import ServiceEventStore, ServiceRequestStore


class ApiServiceRuntime:
    def __init__(self, *, runtime: AppServiceRuntime) -> None:
        self.runtime = runtime
        self.requests = ServiceRequestStore(database_url=runtime.settings.checkpoint_database_url)
        self.events = ServiceEventStore(database_url=runtime.settings.checkpoint_database_url)
        self.runtime.add_close_callback(self.requests.close)
        self.runtime.add_close_callback(self.events.close)

    async def start(self) -> None:
        await self.runtime.start()

    async def close(self) -> None:
        await self.runtime.close()
