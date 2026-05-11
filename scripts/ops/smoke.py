from __future__ import annotations

from scripts.ops.compose import compose


PLATFORM_SMOKE_SERVICES = (
    "postgres",
    "redis",
    "minio",
    "qdrant",
    "prefect-postgres",
    "prefect-redis",
    "prefect-server",
    "prefect-services",
    "platform-api",
    "platform-dashboard",
    "nginx",
)


def run_compose_smoke_test() -> dict:
    compose("down", "-v", "--remove-orphans", check=False)

    try:
        compose("up", "-d", "--build", *PLATFORM_SMOKE_SERVICES)
        ps_output = compose("ps", *PLATFORM_SMOKE_SERVICES)
        return {
            "services": list(PLATFORM_SMOKE_SERVICES),
            "output": ps_output,
        }
    finally:
        compose("down", "-v", "--remove-orphans", check=False)
