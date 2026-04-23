from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from collections.abc import Awaitable, Callable

from core.runtime.app_service.runtime.config import Settings, get_settings
from core.runtime.app_service.runtime.logger import configure_logging, get_logger, log_event
from core.runtime.app_service.runtime.health.store import ServiceHeartbeatStore


CloseCallback = Callable[[], Awaitable[None]]


class AppServiceRuntime:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        logger_name: str = "app_service.runtime",
    ) -> None:
        self.settings = settings or get_settings()
        self.logger = get_logger(logger_name)
        self.shutdown_event = asyncio.Event()
        self.service_id = f"{socket.gethostname()}:{os.getpid()}"
        self._heartbeat_store = ServiceHeartbeatStore(
            redis_url=self.settings.redis_url,
            ttl_seconds=self.settings.service_heartbeat_ttl,
        )
        self._heartbeat_task: asyncio.Task | None = None
        self._close_callbacks: list[CloseCallback] = []

    def add_close_callback(self, callback: CloseCallback) -> None:
        self._close_callbacks.append(callback)

    async def start(self) -> None:
        configure_logging(
            level=self.settings.log_level,
            service_name=self.settings.app_name,
            database_url=self.settings.checkpoint_database_url,
        )
        self._install_signal_handlers()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        log_event(
            self.logger,
            logging.INFO,
            "app_service_started",
            service_name=self.settings.app_name,
            service_id=self.service_id,
        )

    async def wait_for_shutdown(self) -> None:
        await self.shutdown_event.wait()

    async def close(self) -> None:
        self.shutdown_event.set()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        for callback in reversed(self._close_callbacks):
            await callback()
        await self._heartbeat_store.close()
        log_event(
            self.logger,
            logging.INFO,
            "app_service_stopped",
            service_name=self.settings.app_name,
            service_id=self.service_id,
        )

    async def _heartbeat_loop(self) -> None:
        while not self.shutdown_event.is_set():
            await self._heartbeat_store.publish(
                service_id=self.service_id,
                app_name=self.settings.app_name,
            )
            try:
                await asyncio.wait_for(
                    self.shutdown_event.wait(),
                    timeout=self.settings.service_heartbeat_interval,
                )
            except asyncio.TimeoutError:
                continue

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def request_shutdown(signal_name: str) -> None:
            if self.shutdown_event.is_set():
                return
            log_event(
                self.logger,
                logging.INFO,
                "app_service_shutdown_requested",
                service_name=self.settings.app_name,
                signal=signal_name,
            )
            self.shutdown_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, request_shutdown, sig.name)
            except NotImplementedError:
                pass
