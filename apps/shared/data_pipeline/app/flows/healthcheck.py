from __future__ import annotations

import json
import os
from urllib.request import Request, urlopen

from prefect import flow, task


@task(retries=2, retry_delay_seconds=5)
def fetch_json(url: str) -> dict:
    request = Request(url, headers={"accept": "application/json"})
    with urlopen(request, timeout=5) as response:
        body = response.read().decode("utf-8")
    return json.loads(body or "{}")


@flow(name="data-pipeline-healthcheck")
def data_pipeline_healthcheck() -> dict[str, dict]:
    runtime_api_url = os.getenv("RUNTIME_API_URL", "http://runtime-api:8000").rstrip("/")
    prefect_api_url = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api").rstrip("/")
    return {
        "runtime_api": fetch_json(f"{runtime_api_url}/health/ready"),
        "prefect_api": fetch_json(f"{prefect_api_url}/health"),
    }


if __name__ == "__main__":
    data_pipeline_healthcheck()
