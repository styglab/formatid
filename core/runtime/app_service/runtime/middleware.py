from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from core.observability.correlation import build_correlation_details
from core.observability.log_store import ServiceLogStore
from core.runtime.app_service.runtime.stores import ServiceEventStore, ServiceRequestStore


class ServiceRequestMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        *,
        service_name: str,
        database_url: str,
        ignored_paths: set[str] | None = None,
    ) -> None:
        super().__init__(app)
        self._service_name = service_name
        self._requests = ServiceRequestStore(database_url=database_url)
        self._events = ServiceEventStore(database_url=database_url)
        self._logs = ServiceLogStore(database_url=database_url)
        self._ignored_paths = ignored_paths or {"/health/live", "/health/ready"}

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        if request.url.path in self._ignored_paths:
            return await call_next(request)

        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        correlation_id = request.headers.get("x-correlation-id") or request_id
        request.state.request_id = request_id
        request.state.correlation_id = correlation_id
        started_at = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
            await self._safe_record(
                request=request,
                request_id=request_id,
                correlation_id=correlation_id,
                status="failed",
                duration_ms=duration_ms,
                error={"type": type(exc).__name__, "message": str(exc)},
            )
            raise

        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
        status = "succeeded" if response.status_code < 500 else "failed"
        await self._safe_record(
            request=request,
            request_id=request_id,
            correlation_id=correlation_id,
            status=status,
            duration_ms=duration_ms,
            result={"status_code": response.status_code},
        )
        response.headers["x-request-id"] = request_id
        response.headers["x-correlation-id"] = correlation_id
        return response

    async def _safe_record(
        self,
        *,
        request: Request,
        request_id: str,
        correlation_id: str,
        status: str,
        duration_ms: float,
        result: dict | None = None,
        error: dict | None = None,
    ) -> None:
        try:
            await self._requests.record(
                service_name=self._service_name,
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                correlation_id=correlation_id,
                status=status,
                payload={"query": str(request.url.query)},
                result=result,
                error=error,
                duration_ms=duration_ms,
            )
            details = {"method": request.method, "path": request.url.path}
            if result is not None:
                details.update(result)
            if error is not None:
                details["error_type"] = error.get("type")
            correlated_details = build_correlation_details(
                details=details,
                request_id=request_id,
                correlation_id=correlation_id,
            )
            await self._events.record(
                service_name=self._service_name,
                event_name=f"api.request.{status}",
                request_id=request_id,
                correlation_id=correlation_id,
                details=correlated_details,
            )
            await self._logs.record(
                service_name=self._service_name,
                level="info" if status == "succeeded" else "error",
                event_name=f"api.request.{status}",
                message=f"{request.method} {request.url.path} {status}",
                request_id=request_id,
                correlation_id=correlation_id,
                details=build_correlation_details(
                    details={**details, "duration_ms": duration_ms},
                    request_id=request_id,
                    correlation_id=correlation_id,
                ),
            )
        except Exception:
            return
