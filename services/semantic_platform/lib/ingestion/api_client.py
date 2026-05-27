from __future__ import annotations

import json
import mimetypes
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any
from urllib import error, parse, request


DEFAULT_API_URL = "http://127.0.0.1:8016"
TERMINAL_STATUSES = {"succeeded", "failed"}


def api_base_url() -> str:
    return (
        os.getenv("SEMANTIC_PLATFORM_API_URL")
        or os.getenv("SEMANTIC_PLATFORM_API_BASE_URL")
        or DEFAULT_API_URL
    ).rstrip("/")


def upload_and_ingest_source(
    source_path: str | Path,
    *,
    commit_mode: str = "proposal",
    manual_llm_response: dict[str, Any] | None = None,
    llm_secret_ref: str | None = None,
    llm_mode: str | None = None,
    force: bool = False,
    allow_update: bool = True,
    wait: bool = True,
    poll_interval_seconds: float = 2.0,
    timeout_seconds: float = 1800.0,
    api_url: str | None = None,
    requested_by: str = "cli",
) -> dict[str, Any]:
    path = Path(source_path)
    metadata = source_metadata(path)
    upload = upload_source(path, metadata=metadata, api_url=api_url, allow_update=allow_update)
    source = upload.get("source") or {}
    revision = upload.get("revision") or {}
    source_id = str(source.get("id") or metadata.get("source_id") or "")
    if not source_id:
        raise RuntimeError("source upload did not return source id")
    payload: dict[str, Any] = {
        "revision_id": revision.get("id"),
        "commit_mode": commit_mode,
        "force": force,
        "requested_by": requested_by,
    }
    if llm_secret_ref:
        payload["llm_secret_ref"] = llm_secret_ref
    if llm_mode:
        payload["llm_mode"] = llm_mode
    if manual_llm_response is not None:
        payload["llm_mode"] = "codex_manual"
        payload["manual_llm_response"] = manual_llm_response
    started = start_ingestion(source_id, payload, api_url=api_url)
    run = started.get("ingestion_run") or {}
    if wait and run.get("id"):
        run = wait_ingestion_run(
            str(run["id"]),
            api_url=api_url,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        ).get("ingestion_run", run)
    return {
        "source": source,
        "revision": revision,
        "ingestion_run": run,
        "run_id": run.get("id"),
        "status": run.get("status"),
        "result": run.get("result") or {},
        "error_message": run.get("error_message"),
    }


def upload_source(
    path: Path,
    *,
    metadata: dict[str, Any] | None = None,
    api_url: str | None = None,
    allow_update: bool = True,
) -> dict[str, Any]:
    metadata = metadata or {}
    fields = {
        "provider": metadata.get("provider"),
        "provider_name_ko": metadata.get("provider_name_ko"),
        "title": metadata.get("title") or path.stem,
        "source_id": metadata.get("source_id"),
        "auth_secret_refs": _join(metadata.get("auth_secret_refs")),
        "auth_parameter_names": _join(metadata.get("auth_parameter_names")),
        "uploaded_by": "cli",
        "allow_update": "true" if allow_update else "",
    }
    body, content_type = _multipart_body(path, fields)
    try:
        return _request_json(
            f"{api_url or api_base_url()}/sources/upload",
            method="POST",
            body=body,
            headers={"Content-Type": content_type},
        )
    except HttpError as exc:
        if exc.status != 409:
            raise
        source_id = _existing_id_from_detail(exc.detail, prefix="source.")
        if not source_id:
            raise
        sources = _request_json(f"{api_url or api_base_url()}/sources")
        for source in sources.get("sources", []):
            if source.get("id") == source_id:
                return {"source": source, "revision": {"id": source.get("revision_id")}, "created": False}
        raise


def start_ingestion(source_id: str, payload: dict[str, Any], *, api_url: str | None = None) -> dict[str, Any]:
    encoded_source_id = parse.quote(source_id, safe="")
    return _request_json(
        f"{api_url or api_base_url()}/sources/{encoded_source_id}/ingest",
        method="POST",
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )


def wait_ingestion_run(
    run_id: str,
    *,
    api_url: str | None = None,
    poll_interval_seconds: float = 2.0,
    timeout_seconds: float = 1800.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    encoded_run_id = parse.quote(run_id, safe="")
    while True:
        payload = _request_json(f"{api_url or api_base_url()}/ingestion/runs/{encoded_run_id}")
        status = str((payload.get("ingestion_run") or {}).get("status") or "")
        if status in TERMINAL_STATUSES:
            return payload
        if time.monotonic() >= deadline:
            raise TimeoutError(f"ingestion run timed out: {run_id}")
        time.sleep(poll_interval_seconds)


def source_metadata(path: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    metadata.update(_manifest_metadata(path))
    metadata.update(_sidecar_metadata(path))
    metadata["title"] = _normalized_title(metadata.get("title"), path)
    if "source_id" not in metadata:
        provider = str(metadata.get("provider") or "unknown")
        title = str(metadata.get("title") or path.stem)
        metadata["source_id"] = f"source.{_slug(provider)}.{_slug(title)}"
    return metadata


class HttpError(RuntimeError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"{status}: {detail}")
        self.status = status
        self.detail = detail


def _request_json(
    url: str,
    *,
    method: str = "GET",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    req = request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with request.urlopen(req) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        detail = raw
        try:
            detail = json.loads(raw).get("detail") or raw
        except json.JSONDecodeError:
            pass
        raise HttpError(exc.code, str(detail)) from exc
    return json.loads(raw) if raw else {}


def _multipart_body(path: Path, fields: dict[str, Any]) -> tuple[bytes, str]:
    boundary = f"----SemanticPlatform{uuid.uuid4().hex}"
    chunks: list[bytes] = []
    for name, value in fields.items():
        if value is None or value == "":
            continue
        chunks.append(
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f"{value}\r\n"
            ).encode("utf-8")
        )
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    chunks.append(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{path.name}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode("utf-8")
        + path.read_bytes()
        + b"\r\n"
    )
    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


def _manifest_metadata(path: Path) -> dict[str, Any]:
    for manifest in (path.parent / "manifest.json", path.parent / "sources.json"):
        if not manifest.exists():
            continue
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        entries = payload.get("sources") if isinstance(payload, dict) else payload
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict) or not entry.get("path"):
                continue
            candidate = Path(str(entry["path"]))
            if not candidate.is_absolute():
                candidate = manifest.parent / candidate
            try:
                if candidate.resolve() == path.resolve():
                    return {key: value for key, value in entry.items() if key != "path"}
            except OSError:
                continue
    return {}


def _sidecar_metadata(path: Path) -> dict[str, Any]:
    for sidecar in (path.with_suffix(path.suffix + ".source.json"), path.with_suffix(".source.json")):
        if not sidecar.exists():
            continue
        try:
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        return payload if isinstance(payload, dict) else {}
    return {}


def _join(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value if str(item).strip())
    return str(value or "")


def _normalized_title(value: Any, path: Path) -> str:
    title = str(value or path.stem).strip() or path.stem
    suffix = path.suffix
    if suffix and title.lower().endswith(suffix.lower()):
        return title[: -len(suffix)]
    return title


def _existing_id_from_detail(detail: str, *, prefix: str) -> str:
    match = re.search(rf"({re.escape(prefix)}\S+)", detail)
    return match.group(1) if match else ""


def _slug(value: str) -> str:
    slug = re.sub(r"[^\w.-]+", "_", str(value).strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unknown"
