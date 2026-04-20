from typing import Any, Protocol


class CheckpointStore(Protocol):
    async def get(self, name: str) -> dict[str, Any] | None:
        ...

    async def set(self, name: str, value: dict[str, Any]) -> None:
        ...

    async def close(self) -> None:
        ...
