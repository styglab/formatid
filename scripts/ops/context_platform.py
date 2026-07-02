from __future__ import annotations

import hashlib
import json
import mimetypes
from pathlib import Path

from scripts.ops.common import PROJECT_ROOT, run_command
from services.context_platform.internal.storage import ContextPlatformRepository


WORKER_CONTAINER = "infra-context-platform-worker-1"


def reset_context_platform() -> dict:
    try:
        return ContextPlatformRepository().reset_context()
    except ModuleNotFoundError as exc:
        if exc.name != "psycopg":
            raise
        output = run_command(
            "docker",
            "exec",
            "infra-context-platform-api-1",
            "python",
            "-c",
            (
                "import json; "
                "from services.context_platform.internal.storage import ContextPlatformRepository; "
                "print(json.dumps(ContextPlatformRepository().reset_context(), ensure_ascii=False))"
            ),
        )
        return json.loads(output)


def seed_context_platform() -> dict:
    try:
        repo = ContextPlatformRepository()
        repo.ensure_schema()
        return {"status": "seeded", "schema": "context_platform", "counts": _seed_counts(repo)}
    except ModuleNotFoundError as exc:
        if exc.name != "psycopg":
            raise
        output = run_command(
            "docker",
            "exec",
            "infra-context-platform-api-1",
            "python",
            "-c",
            (
                "import json; "
                "from services.context_platform.internal.storage import ContextPlatformRepository; "
                "repo = ContextPlatformRepository(); "
                "repo.ensure_schema(); "
                "print(json.dumps({'status': 'seeded', 'schema': 'context_platform', 'counts': {"
                "'sources': len(repo.list_sources()), "
                "'source_documents': len(repo.list_source_documents()), "
                "'source_operations': len(repo.list_source_operations()), "
                "'source_fields': len(repo.list_source_fields()), "
                "'object_types': len(repo.list_object_types()), "
                "'property_types': len(repo.list_property_types()), "
                "'value_domains': len(repo.list_value_domains()), "
                "'value_domain_values': len(repo.list_value_domain_values()), "
                "'concepts': len(repo.list_concepts()), "
                "'canonical_representations': len(repo.list_canonical_representations()), "
                "'representation_schemas': len(repo.list_representation_schemas()), "
                "'field_bindings': len(repo.list_field_bindings()), "
                "'context_bindings': len(repo.list_context_bindings()), "
                "'parameter_bindings': len(repo.list_parameter_bindings()), "
                "'capabilities': len(repo.list_capabilities()), "
                "'capability_steps': len(repo.list_capability_operations())"
                "}}, ensure_ascii=False))"
            ),
        )
        return json.loads(output)


def ingest_context_platform_source(
    *,
    source_path: str,
    name: str = "",
    provider: str = "",
    source_type: str = "api",
    document_type: str = "api_document",
    description: str = "",
    llm_mode: str = "env",
    manual_llm_response_path: str = "",
) -> dict:
    path = Path(source_path).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"source file not found: {path}")
    manual_path = Path(manual_llm_response_path).expanduser() if manual_llm_response_path else None
    if manual_path and not manual_path.is_absolute():
        manual_path = PROJECT_ROOT / manual_path
    if manual_path and (not manual_path.exists() or not manual_path.is_file()):
        raise FileNotFoundError(f"agent response file not found: {manual_path}")

    resolved_llm_mode = _resolve_agent_mode(llm_mode)

    return _ingest_source_in_worker(
        path=path,
        name=name or path.stem,
        provider=provider,
        source_type=source_type,
        document_type=document_type,
        description=description,
        llm_mode=resolved_llm_mode,
        manual_path=manual_path,
    )


def ingest_queued_context_platform_source(
    *,
    run_id: str,
    llm_mode: str = "env",
    manual_llm_response_path: str = "",
) -> dict:
    if not run_id:
        raise ValueError("run_id is required")
    manual_path = Path(manual_llm_response_path).expanduser() if manual_llm_response_path else None
    if manual_path and not manual_path.is_absolute():
        manual_path = PROJECT_ROOT / manual_path
    if manual_path and (not manual_path.exists() or not manual_path.is_file()):
        raise FileNotFoundError(f"agent response file not found: {manual_path}")

    resolved_llm_mode = _resolve_agent_mode(llm_mode)
    return _ingest_existing_run_in_worker(run_id=run_id, llm_mode=resolved_llm_mode, manual_path=manual_path)


def draft_context_platform_source_contract(
    *,
    source_path: str,
    output_path: str = "",
    model_id: str = "",
    source_name: str = "",
    source_type: str = "api",
) -> dict:
    path = Path(source_path).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"source file not found: {path}")
    upload_dir = f"/tmp/context-platform-langextract-{hashlib.sha256(str(path).encode()).hexdigest()[:12]}"
    container_source = f"{upload_dir}/{path.name}"
    run_command("docker", "exec", WORKER_CONTAINER, "mkdir", "-p", upload_dir)
    run_command("docker", "cp", str(path), f"{WORKER_CONTAINER}:{container_source}")
    payload = {
        "source_path": container_source,
        "model_id": model_id,
        "source_name": source_name or path.stem,
        "source_type": source_type,
    }
    output = run_command(
        "docker",
        "exec",
        "-i",
        WORKER_CONTAINER,
        "python",
        "-c",
        _worker_draft_source_contract_script(payload),
    )
    result = json.loads(output)
    if output_path:
        target = Path(output_path).expanduser()
        if not target.is_absolute():
            target = PROJECT_ROOT / target
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        result = {**result, "_output_path": str(target)}
    return result


def _ingest_source_in_worker(
    *,
    path: Path,
    name: str,
    provider: str,
    source_type: str,
    document_type: str,
    description: str,
    llm_mode: str,
    manual_path: Path | None,
) -> dict:
    upload_dir = f"/tmp/context-platform-ingest-{hashlib.sha256(str(path).encode()).hexdigest()[:12]}"
    container_source = f"{upload_dir}/{path.name}"
    run_command("docker", "exec", WORKER_CONTAINER, "mkdir", "-p", upload_dir)
    run_command("docker", "cp", str(path), f"{WORKER_CONTAINER}:{container_source}")
    container_manual = ""
    if manual_path:
        container_manual = f"{upload_dir}/{manual_path.name}"
        run_command("docker", "cp", str(manual_path), f"{WORKER_CONTAINER}:{container_manual}")

    payload = {
        "source_path": container_source,
        "name": name,
        "provider": provider,
        "source_type": source_type,
        "document_type": document_type,
        "description": description,
        "llm_mode": llm_mode,
        "agent_mode": llm_mode,
        "manual_llm_response_path": container_manual,
        "agent_response_path": container_manual,
    }
    script = _worker_ingest_script(payload)
    output = run_command(
        "docker",
        "exec",
        "-i",
        WORKER_CONTAINER,
        "python",
        "-c",
        script,
    )
    return json.loads(output)


def _ingest_existing_run_in_worker(*, run_id: str, llm_mode: str, manual_path: Path | None) -> dict:
    upload_dir = f"/tmp/context-platform-ingest-run-{hashlib.sha256(run_id.encode()).hexdigest()[:12]}"
    run_command("docker", "exec", WORKER_CONTAINER, "mkdir", "-p", upload_dir)
    container_manual = ""
    if manual_path:
        container_manual = f"{upload_dir}/{manual_path.name}"
        run_command("docker", "cp", str(manual_path), f"{WORKER_CONTAINER}:{container_manual}")

    payload = {
        "run_id": run_id,
        "llm_mode": llm_mode,
        "agent_mode": llm_mode,
        "manual_llm_response_path": container_manual,
        "agent_response_path": container_manual,
    }
    output = run_command(
        "docker",
        "exec",
        "-i",
        WORKER_CONTAINER,
        "python",
        "-c",
        _worker_ingest_existing_run_script(payload),
    )
    return json.loads(output)


def _worker_ingest_script(payload: dict) -> str:
    encoded_payload = json.dumps(payload, ensure_ascii=False)
    return "\n".join(
        [
            "import hashlib, json, mimetypes",
            "from pathlib import Path",
            "from services.context_platform.internal.storage import ContextPlatformRepository",
            "from services.context_platform.internal.storage.object_store import ObjectStore",
            "from services.context_platform.internal.ingestion.api_documents import ingest_source_document",
            f"payload = json.loads({encoded_payload!r})",
            "path = Path(payload['source_path'])",
            "raw = path.read_bytes()",
            "content_hash = hashlib.sha256(raw).hexdigest()",
            "content_type = mimetypes.guess_type(path.name)[0] or 'application/octet-stream'",
            "stored = ObjectStore().put_document(filename=path.name, content_type=content_type, data=raw)",
            "repo = ContextPlatformRepository()",
            "repo.ensure_schema()",
            "source = repo.create_source({",
            "    'name': payload['name'],",
            "    'provider': payload['provider'],",
            "    'source_type': payload['source_type'],",
            "    'description': payload['description'] or f\"Uploaded source document: {path.name}\",",
            "    'status': 'draft',",
            "    'config': {'input_mode': 'ops_ingest_source', 'reference_uri': stored['uri'], 'upload': stored},",
            "})",
            "document = repo.create_source_document({",
            "    'source_id': source['id'],",
            "    'document_type': payload['document_type'],",
            "    'name': path.name,",
            "    'uri': stored['uri'],",
            "    'content_hash': content_hash,",
            "    'content_type': content_type,",
            "    'status': 'draft',",
            "    'metadata': {'object': stored},",
            "})",
            "metadata = {'object_uri': stored['uri'], 'content_hash': content_hash, 'filename': path.name}",
            "if payload.get('agent_mode') or payload.get('llm_mode'):",
            "    metadata['agent_mode'] = payload.get('agent_mode') or payload.get('llm_mode')",
            "    metadata['llm_mode'] = metadata['agent_mode']",
            "manual_path = payload.get('agent_response_path') or payload.get('manual_llm_response_path') or ''",
            "if manual_path:",
            "    metadata['agent_response'] = json.loads(Path(manual_path).read_text(encoding='utf-8'))",
            "    metadata['manual_llm_response'] = metadata['agent_response']",
            "    metadata['agent_response_received'] = True",
            "run = repo.create_onboarding_run({",
            "    'source_id': source['id'],",
            "    'source_document_id': document['id'],",
            "    'status': 'submitted',",
            "    'stage': 'source_uploaded',",
            "    'metadata': metadata,",
            "})",
            "result = ingest_source_document(run['id'], repository=repo)",
            "print(json.dumps({'source': source, 'source_document': document, 'onboarding_run': run, 'ingestion': result}, ensure_ascii=False))",
        ]
    )


def _worker_ingest_existing_run_script(payload: dict) -> str:
    encoded_payload = json.dumps(payload, ensure_ascii=False)
    return "\n".join(
        [
            "import json",
            "from pathlib import Path",
            "from services.context_platform.internal.storage import ContextPlatformRepository",
            "from services.context_platform.internal.ingestion.api_documents import ingest_source_document",
            f"payload = json.loads({encoded_payload!r})",
            "repo = ContextPlatformRepository()",
            "repo.ensure_schema()",
            "run = repo.get_onboarding_run(payload['run_id'])",
            "if run is None:",
            "    print(json.dumps({'run_id': payload['run_id'], 'status': 'not_found'}, ensure_ascii=False))",
            "    raise SystemExit(0)",
            "metadata = run.get('metadata') if isinstance(run.get('metadata'), dict) else {}",
            "if payload.get('agent_mode') or payload.get('llm_mode'):",
            "    metadata['agent_mode'] = payload.get('agent_mode') or payload.get('llm_mode')",
            "    metadata['llm_mode'] = metadata['agent_mode']",
            "manual_path = payload.get('agent_response_path') or payload.get('manual_llm_response_path') or ''",
            "if manual_path:",
            "    metadata['agent_response'] = json.loads(Path(manual_path).read_text(encoding='utf-8'))",
            "    metadata['manual_llm_response'] = metadata['agent_response']",
            "    metadata['agent_response_received'] = True",
            "repo.update_onboarding_run(payload['run_id'], status='submitted', stage='agent_ingestion', metadata=metadata)",
            "result = ingest_source_document(payload['run_id'], repository=repo)",
            "print(json.dumps({'onboarding_run': repo.get_onboarding_run(payload['run_id']), 'ingestion': result}, ensure_ascii=False))",
        ]
    )


def _worker_draft_source_contract_script(payload: dict) -> str:
    encoded_payload = json.dumps(payload, ensure_ascii=False)
    return "\n".join(
        [
            "import json",
            "from services.context_platform.internal.ingestion.langextract_source_contract import draft_agent_response_from_source_path",
            f"payload = json.loads({encoded_payload!r})",
            "result = draft_agent_response_from_source_path(",
            "    payload['source_path'],",
            "    source_name=payload.get('source_name') or '',",
            "    source_type=payload.get('source_type') or 'api',",
            "    model_id=payload.get('model_id') or '',",
            ")",
            "print(json.dumps(result, ensure_ascii=False))",
        ]
    )


def _seed_counts(repo: ContextPlatformRepository) -> dict[str, int]:
    return {
        "sources": len(repo.list_sources()),
        "source_documents": len(repo.list_source_documents()),
        "source_operations": len(repo.list_source_operations()),
        "source_fields": len(repo.list_source_fields()),
        "object_types": len(repo.list_object_types()),
        "property_types": len(repo.list_property_types()),
        "value_domains": len(repo.list_value_domains()),
        "value_domain_values": len(repo.list_value_domain_values()),
        "concepts": len(repo.list_concepts()),
        "canonical_representations": len(repo.list_canonical_representations()),
        "representation_schemas": len(repo.list_representation_schemas()),
        "field_bindings": len(repo.list_field_bindings()),
        "context_bindings": len(repo.list_context_bindings()),
        "parameter_bindings": len(repo.list_parameter_bindings()),
        "capabilities": len(repo.list_capabilities()),
        "capability_steps": len(repo.list_capability_operations()),
    }


def _resolve_agent_mode(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized in {"", "env"}:
        return ""
    if normalized in {"manual", "agent_manual", "codex_manual"}:
        return "agent_manual"
    if normalized == "disabled":
        return "disabled"
    if normalized == "openai":
        raise ValueError("openai is no longer supported for Context Platform ingestion; use --agent-mode manual with --agent-response")
    return "disabled"
