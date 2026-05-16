from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CATALOG_DIR = Path("services/semantic_platform/catalog")
DEFAULT_PROPOSALS_DIR = Path("sources/proposals")


def catalog_dir() -> Path:
    return Path(os.getenv("SEMANTIC_PLATFORM_CATALOG_DIR", str(DEFAULT_CATALOG_DIR)))


def proposals_dir() -> Path:
    return Path(os.getenv("SEMANTIC_PLATFORM_PROPOSALS_DIR", str(DEFAULT_PROPOSALS_DIR)))


def catalog_metadata() -> dict[str, Any]:
    catalog = load_catalog()
    runtime = catalog.get("runtime", {})
    execution = catalog.get("execution", {})
    return {
        "name": "semantic_platform",
        "kind": "semantic_runtime_context_layer",
        "catalog_dir": str(catalog_dir()),
        "domains": sorted(catalog["domains"].keys()),
        "runtime_entity_count": len(runtime.get("entities", {})),
        "semantic_type_count": len(runtime.get("semantic_types", {})),
        "runtime_capability_count": len(runtime.get("capabilities", {})),
        "resource_count": len(catalog.get("resources", {}).get("resources", {})),
        "core_entity_count": len(catalog["core"].get("entities", {})),
        "core_property_count": len(catalog["core"].get("properties", {})),
        "crosswalk_count": len(catalog["mappings"].get("crosswalks", {})),
        "capability_implementation_count": sum(
            len(items)
            for items in execution.get("capability_implementations", {}).values()
            if isinstance(items, list)
        ),
        "operation_field_mapping_count": len(execution.get("operation_field_mappings", {})),
        "operation_contract_count": len(execution.get("operation_contracts", {})),
    }


@lru_cache(maxsize=1)
def load_catalog() -> dict[str, Any]:
    root = catalog_dir()
    return {
        "core": {
            "entities": _load_yaml(root / "core" / "entities.yaml").get("entities", {}),
            "properties": _load_yaml(root / "core" / "properties.yaml").get("properties", {}),
            "identifiers": _load_yaml(root / "core" / "identifiers.yaml").get("identifiers", {}),
        },
        "domains": _load_domains(root / "domains"),
        "runtime": {
            "entities": _load_yaml(root / "core" / "runtime_entities.yaml").get("entities", {}),
            "semantic_types": _load_yaml(root / "core" / "semantic_types.yaml").get("semantic_types", {}),
            "relations": _load_yaml(root / "core" / "runtime_relations.yaml").get("relations", {}),
            "capabilities": _load_yaml(root / "capabilities.yaml").get("capabilities", {}),
        },
        "resources": {
            "resources": _load_yaml(root / "resources" / "resources.yaml").get("resources", {}),
        },
        "mappings": {
            "crosswalks": _load_yaml(root / "mappings" / "crosswalks.yaml").get("crosswalks", {}),
        },
        "execution": load_execution_contracts(),
    }


@lru_cache(maxsize=1)
def load_execution_contracts() -> dict[str, Any]:
    root = catalog_dir() / "execution"
    operation_field_mappings = _load_yaml(root / "operation_field_mappings.yaml").get(
        "operation_field_mappings",
        {},
    )
    if not operation_field_mappings:
        operation_field_mappings = _load_yaml(root / "provider_field_mappings.yaml").get(
            "provider_field_mappings",
            {},
        )
    return {
        "capability_implementations": _load_yaml(root / "capability_implementations.yaml").get(
            "capability_implementations",
            {},
        ),
        "operation_field_mappings": operation_field_mappings,
        "operation_contracts": _load_yaml(root / "operation_contracts.yaml").get("operation_contracts", {}),
    }


def list_domains() -> dict[str, Any]:
    return {"domains": sorted(load_catalog()["domains"].keys())}


def resolve(query: str, limit: int = 10) -> dict[str, Any]:
    text = _normalize(query)
    if not text:
        return {"query": query, "matches": []}
    catalog = load_catalog()
    matches = []
    matches.extend(_match_core_entities(catalog, text))
    matches.extend(_match_core_properties(catalog, text))
    matches.extend(_match_domain_entities(catalog, text))
    matches.extend(_match_domain_properties(catalog, text))
    matches.extend(_match_runtime_entities(catalog, text))
    matches.extend(_match_semantic_types(catalog, text))
    matches.sort(key=lambda item: item["score"], reverse=True)
    return {"query": query, "matches": matches[: max(1, min(limit, 50))]}


def runtime_context(query: str, limit: int = 8) -> dict[str, Any]:
    text = _normalize(query)
    catalog = load_catalog()
    semantic_types = _rank_semantic_types(catalog, text, limit)
    entities = _rank_runtime_entities(catalog, text, semantic_types, limit)
    capabilities = _rank_runtime_capabilities(catalog, text, semantic_types, entities, limit)
    relations = _rank_runtime_relations(catalog, text, capabilities, entities, limit)
    join_keys = _join_keys(semantic_types, capabilities, relations)
    return {
        "query": query,
        "runtime_context": {
            "semantic_types": semantic_types,
            "entities": entities,
            "capabilities": capabilities,
            "relations": relations,
            "join_keys": join_keys,
            "execution_hints": _execution_hints(capabilities, join_keys),
        },
    }


def find_capabilities(entity: str | None = None, properties: list[str] | None = None, limit: int = 20) -> dict[str, Any]:
    entity_text = _normalize(entity or "")
    property_terms = {_normalize(_semantic_type_key(value)) for value in properties or [] if _normalize(value)}
    capabilities = []
    for name, capability in load_catalog().get("runtime", {}).get("capabilities", {}).items():
        terms = _normalize_many(
                [
                    name,
                    capability.get("description_ko", ""),
                    *capability.get("consumes", []),
                    *capability.get("produces", []),
                *capability.get("entities", []),
            ]
        )
        matched_properties = sorted(term for term in property_terms if term in terms)
        entity_matched = bool(entity_text and any(entity_text in term or term in entity_text for term in terms))
        if entity_text and not entity_matched and not matched_properties:
            continue
        if property_terms and not matched_properties and not entity_matched:
            continue
        capabilities.append(
            {
                "name": name,
                "consumes": capability.get("consumes", []),
                "produces": capability.get("produces", []),
                "entities": capability.get("entities", []),
                "relations": capability.get("relations", []),
                "join_keys": capability.get("join_keys", []),
                "description_ko": capability.get("description_ko"),
                "matched_properties": matched_properties,
                "score": len(matched_properties) * 10 + (5 if entity_matched else 0),
            }
        )
    capabilities.sort(key=lambda item: item["score"], reverse=True)
    return {"entity": entity, "properties": properties or [], "capabilities": capabilities[: max(1, min(limit, 50))]}


def plan_join(from_entity: str, to_entity: str) -> dict[str, Any]:
    source = _normalize(from_entity)
    target = _normalize(to_entity)
    candidates = []
    for name, crosswalk in load_catalog()["mappings"].get("crosswalks", {}).items():
        source_name = str(crosswalk.get("source", ""))
        target_name = str(crosswalk.get("target", ""))
        source_hit = source in _normalize(source_name) or source in _normalize(target_name)
        target_hit = target in _normalize(source_name) or target in _normalize(target_name)
        if source_hit and target_hit:
            candidates.append({"name": name, **crosswalk})
    return {
        "from": from_entity,
        "to": to_entity,
        "join_paths": candidates,
        "status": "found" if candidates else "not_found",
    }


def _load_domains(path: Path) -> dict[str, Any]:
    domains: dict[str, Any] = {}
    if not path.exists():
        return domains
    for domain_dir in sorted(item for item in path.iterdir() if item.is_dir()):
        domain = domain_dir.name
        entities_doc = _load_yaml(domain_dir / "entities.yaml")
        properties_doc = _load_yaml(domain_dir / "properties.yaml")
        capabilities_doc = _load_yaml(domain_dir / "capabilities.yaml")
        domains[domain] = {
            "entities": entities_doc.get("entities", {}),
            "properties": properties_doc.get("properties", {}),
            "capabilities": capabilities_doc.get("capabilities", {}),
        }
    return domains


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return data


def list_sources() -> dict[str, Any]:
    root = Path(os.getenv("SEMANTIC_PLATFORM_SOURCES_DIR", "sources"))
    documents = []
    if root.exists():
        for path in sorted(item for item in root.iterdir() if item.is_file()):
            if path.name.startswith("."):
                continue
            documents.append(
                {
                    "document_id": _slug(path.stem),
                    "path": str(path),
                    "status": "available",
                    "provider": "unknown",
                    "size_bytes": path.stat().st_size,
                }
            )
    return {"documents": documents}


def sources_summary() -> dict[str, Any]:
    documents = list_sources()["documents"]
    proposals = list_proposals()["proposals"]
    pending = len([item for item in proposals if item.get("status") == "pending_review"])
    return {
        "documents": {"total": len(documents)},
        "processing": {"pending": pending, "proposal_count": len(proposals)},
    }


def list_proposals() -> dict[str, Any]:
    rows = []
    root = proposals_dir()
    if root.exists():
        for path in sorted(root.glob("*.json")):
            proposal = _load_json(path)
            rows.append(_proposal_summary(path, proposal))
    rows.sort(key=lambda item: (item.get("status") != "pending_review", item["proposal_id"]))
    return {"proposals": rows}


def read_proposal(proposal_id: str) -> dict[str, Any]:
    path = _proposal_path(proposal_id)
    proposal = _load_json(path)
    return {"proposal": _proposal_summary(path, proposal, include_payload=True)}


def apply_proposal(proposal_id: str) -> dict[str, Any]:
    path = _proposal_path(proposal_id)
    proposal = _load_json(path)
    if _proposal_status(proposal) == "applied":
        return {"proposal_id": proposal_id, "status": "applied", "changed": []}
    if _proposal_status(proposal) == "rejected":
        return {"proposal_id": proposal_id, "status": "blocked", "reason": "proposal_rejected"}

    changed = []
    semantic_changes = proposal.get("semantic_platform_proposal", {}).get("changes", {})
    execution_changes = proposal.get("execution_contract_proposal", {}).get("changes", {})

    if semantic_changes.get("semantic_types"):
        changed.append(_merge_yaml_map("core/semantic_types.yaml", "semantic_types", semantic_changes["semantic_types"]))
    if semantic_changes.get("entities"):
        changed.append(_merge_yaml_map("core/runtime_entities.yaml", "entities", semantic_changes["entities"]))
    if semantic_changes.get("relations"):
        changed.append(_merge_yaml_map("core/runtime_relations.yaml", "relations", semantic_changes["relations"]))
    if semantic_changes.get("capabilities"):
        changed.append(_merge_yaml_map("capabilities.yaml", "capabilities", semantic_changes["capabilities"]))
    if semantic_changes.get("resources"):
        changed.append(_merge_yaml_map("resources/resources.yaml", "resources", semantic_changes["resources"]))
    if semantic_changes.get("crosswalks"):
        changed.append(_merge_yaml_map("mappings/crosswalks.yaml", "crosswalks", semantic_changes["crosswalks"]))
    if execution_changes.get("capability_implementations"):
        changed.append(
            _merge_capability_implementations(
                execution_changes["capability_implementations"],
            )
        )
    operation_field_mappings = execution_changes.get("operation_field_mappings") or execution_changes.get(
        "provider_field_mappings"
    )
    if operation_field_mappings:
        changed.append(
            _merge_yaml_map(
                "execution/operation_field_mappings.yaml",
                "operation_field_mappings",
                operation_field_mappings,
            )
        )

    _set_proposal_review(path, proposal, status="applied", action="apply", changed=[item for item in changed if item])
    _clear_caches()
    return {"proposal_id": proposal_id, "status": "applied", "changed": [item for item in changed if item]}


def reject_proposal(proposal_id: str) -> dict[str, Any]:
    path = _proposal_path(proposal_id)
    proposal = _load_json(path)
    _set_proposal_review(path, proposal, status="rejected", action="reject", changed=[])
    return {"proposal_id": proposal_id, "status": "rejected"}


def _proposal_summary(path: Path, proposal: dict[str, Any], include_payload: bool = False) -> dict[str, Any]:
    semantic_changes = proposal.get("semantic_platform_proposal", {}).get("changes", {})
    execution_changes = proposal.get("execution_contract_proposal", {}).get("changes", {})
    operation_count = len(proposal.get("structured_spec", {}).get("operations", []))
    semantic_type_count = len(semantic_changes.get("semantic_types", {}))
    entity_count = len(semantic_changes.get("entities", {}))
    relation_count = len(semantic_changes.get("relations", {}))
    capability_count = len(semantic_changes.get("capabilities", {}))
    resource_count = len(semantic_changes.get("resources", {}))
    crosswalk_count = len(semantic_changes.get("crosswalks", {}))
    implementation_count = len(execution_changes.get("capability_implementations", {}))
    field_mapping_count = len(
        execution_changes.get("operation_field_mappings", {})
        or execution_changes.get("provider_field_mappings", {})
    )
    row = {
        "proposal_id": path.name,
        "path": str(path),
        "status": _proposal_status(proposal),
        "action": "review",
        "section": "proposal",
        "provider": proposal.get("structured_spec", {}).get("provider")
        or proposal.get("semantic_platform_proposal", {}).get("provider")
        or proposal.get("execution_contract_proposal", {}).get("provider")
        or "unknown",
        "proposal_builder": proposal.get("proposal_builder") or proposal.get("llm_mode") or proposal.get("mode"),
        "source_path": proposal.get("source", {}).get("source_path") or proposal.get("structured_spec", {}).get("source_path"),
        "reason": _proposal_reason(
            proposal,
            operation_count,
            semantic_type_count,
            capability_count,
            field_mapping_count,
            entity_count,
            relation_count,
            resource_count,
            crosswalk_count,
        ),
        "counts": {
            "operations": operation_count,
            "semantic_types": semantic_type_count,
            "entities": entity_count,
            "relations": relation_count,
            "capabilities": capability_count,
            "resources": resource_count,
            "crosswalks": crosswalk_count,
            "capability_implementations": implementation_count,
            "operation_field_mappings": field_mapping_count,
        },
        "review": proposal.get("review", {}),
    }
    if include_payload:
        row["payload"] = proposal
    return row


def _proposal_reason(
    proposal: dict[str, Any],
    operation_count: int,
    semantic_type_count: int,
    capability_count: int,
    field_mapping_count: int,
    entity_count: int = 0,
    relation_count: int = 0,
    resource_count: int = 0,
    crosswalk_count: int = 0,
) -> str:
    structured = proposal.get("structured_spec", {})
    if structured.get("reason"):
        return str(structured["reason"])
    return (
        f"{operation_count} operations, {semantic_type_count} semantic types, "
        f"{entity_count} entities, {relation_count} relations, "
        f"{capability_count} capabilities, {resource_count} resources, "
        f"{crosswalk_count} crosswalks, {field_mapping_count} field mappings"
    )


def _proposal_status(proposal: dict[str, Any]) -> str:
    review_status = proposal.get("review", {}).get("status")
    if review_status:
        return str(review_status)
    status = proposal.get("status")
    if status:
        return str(status)
    statuses = [
        proposal.get("semantic_platform_proposal", {}).get("status"),
        proposal.get("execution_contract_proposal", {}).get("status"),
        proposal.get("structured_spec", {}).get("status"),
    ]
    if "pending_review" in statuses:
        return "pending_review"
    if "not_generated" in statuses:
        return "not_generated"
    return "pending_review"


def _proposal_path(proposal_id: str) -> Path:
    if "/" in proposal_id or "\\" in proposal_id:
        raise ValueError("invalid proposal id")
    path = proposals_dir() / proposal_id
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(proposal_id)
    return path


def _load_json(path: Path) -> dict[str, Any]:
    import json

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return data


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    import json

    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _set_proposal_review(path: Path, proposal: dict[str, Any], status: str, action: str, changed: list[dict[str, Any]]) -> None:
    proposal["review"] = {
        "status": status,
        "action": action,
        "changed": changed,
    }
    proposal["status"] = status
    _write_json(path, proposal)


def _merge_yaml_map(relative_path: str, key: str, changes: dict[str, Any]) -> dict[str, Any]:
    path = catalog_dir() / relative_path
    document = _load_yaml(path)
    target = document.setdefault(key, {})
    if not isinstance(target, dict):
        raise ValueError(f"YAML key must be an object: {path}:{key}")
    applied = []
    for name, value in changes.items():
        if not isinstance(value, dict):
            continue
        clean_value = _canonical_value(value)
        if name in target and isinstance(target[name], dict):
            target[name] = {**target[name], **clean_value}
        else:
            target[name] = clean_value
        applied.append(name)
    _write_yaml(path, document)
    return {"path": str(path), "key": key, "applied": applied}


def _merge_capability_implementations(changes: dict[str, Any]) -> dict[str, Any]:
    path = catalog_dir() / "execution" / "capability_implementations.yaml"
    document = _load_yaml(path)
    target = document.setdefault("capability_implementations", {})
    applied = []
    for capability, proposed in changes.items():
        proposed_items = proposed if isinstance(proposed, list) else [proposed]
        current_items = target.setdefault(capability, [])
        if not isinstance(current_items, list):
            current_items = []
            target[capability] = current_items
        for item in proposed_items:
            if not isinstance(item, dict):
                continue
            clean_item = _canonical_value(item)
            matched = _implementation_index(current_items, clean_item)
            if matched is None:
                current_items.append(clean_item)
            else:
                current_items[matched] = {**current_items[matched], **clean_item}
            applied.append(capability)
    _write_yaml(path, document)
    return {"path": str(path), "key": "capability_implementations", "applied": sorted(set(applied))}


def _implementation_index(items: list[dict[str, Any]], candidate: dict[str, Any]) -> int | None:
    for index, item in enumerate(items):
        if item.get("provider") == candidate.get("provider") and item.get("tool") == candidate.get("tool"):
            return index
    return None


def _canonical_value(value: dict[str, Any]) -> dict[str, Any]:
    ignored = {"existing", "evidence"}
    return {key: item for key, item in value.items() if key not in ignored}


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    _clear_caches()


def _clear_caches() -> None:
    load_catalog.cache_clear()
    load_execution_contracts.cache_clear()


def _slug(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", value.strip().lower())
    return normalized.strip("_") or "source"


def _match_core_entities(catalog: dict[str, Any], text: str) -> list[dict[str, Any]]:
    return [
        {"kind": "core_entity", "name": name, "score": score, **value}
        for name, value in catalog["core"].get("entities", {}).items()
        if (score := _score(text, _terms(name, value))) > 0
    ]


def _match_core_properties(catalog: dict[str, Any], text: str) -> list[dict[str, Any]]:
    return [
        {"kind": "core_property", "name": name, "score": score, **value}
        for name, value in catalog["core"].get("properties", {}).items()
        if (score := _score(text, _terms(name, value))) > 0
    ]


def _match_domain_entities(catalog: dict[str, Any], text: str) -> list[dict[str, Any]]:
    matches = []
    for domain, payload in catalog["domains"].items():
        for name, value in payload.get("entities", {}).items():
            score = _score(text, _terms(name, value))
            if score > 0:
                matches.append({"kind": "domain_entity", "domain": domain, "name": name, "score": score, **value})
    return matches


def _match_domain_properties(catalog: dict[str, Any], text: str) -> list[dict[str, Any]]:
    matches = []
    for domain, payload in catalog["domains"].items():
        for name, value in payload.get("properties", {}).items():
            score = _score(text, _terms(name, value))
            if score > 0:
                matches.append({"kind": "domain_property", "domain": domain, "name": name, "score": score, **value})
    return matches


def _match_runtime_entities(catalog: dict[str, Any], text: str) -> list[dict[str, Any]]:
    return [
        {"kind": "runtime_entity", "name": name, "score": score, **value}
        for name, value in catalog.get("runtime", {}).get("entities", {}).items()
        if (score := _score(text, _terms(name, value))) > 0
    ]


def _match_semantic_types(catalog: dict[str, Any], text: str) -> list[dict[str, Any]]:
    return [
        {"kind": "semantic_type", "name": name, "score": score, **value}
        for name, value in catalog.get("runtime", {}).get("semantic_types", {}).items()
        if (score := _score(text, _terms(name, value))) > 0
    ]


def _rank_semantic_types(catalog: dict[str, Any], text: str, limit: int) -> list[dict[str, Any]]:
    matches = []
    for name, value in catalog.get("runtime", {}).get("semantic_types", {}).items():
        score = _score(text, _terms(name, value))
        if score > 0:
            matches.append({"name": name, "score": score, **value})
    matches.sort(key=lambda item: item["score"], reverse=True)
    return matches[: max(1, min(limit, 20))]


def _rank_runtime_entities(
    catalog: dict[str, Any],
    text: str,
    semantic_types: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    type_entities = {item.get("entity") for item in semantic_types if item.get("entity")}
    matches = []
    for name, value in catalog.get("runtime", {}).get("entities", {}).items():
        score = _score(text, _terms(name, value))
        if name in type_entities:
            score = max(score, 80)
        if score > 0:
            matches.append({"name": name, "score": score, **value})
    matches.sort(key=lambda item: item["score"], reverse=True)
    return matches[: max(1, min(limit, 20))]


def _rank_runtime_capabilities(
    catalog: dict[str, Any],
    text: str,
    semantic_types: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    type_names = {item["name"] for item in semantic_types}
    entity_names = {item["name"] for item in entities}
    matches = []
    for name, capability in catalog.get("runtime", {}).get("capabilities", {}).items():
        consumes = set(capability.get("consumes", []))
        produces = set(capability.get("produces", []))
        capability_entities = set(capability.get("entities", []))
        matched_types = sorted(type_names & (consumes | produces))
        matched_entities = sorted(entity_names & capability_entities)
        score = _score(text, _terms(name, capability))
        score = max(score, len(matched_types) * 20 + len(matched_entities) * 10)
        if score > 0:
            matches.append(
                {
                    "name": name,
                    "score": score,
                    "matched_semantic_types": matched_types,
                    "matched_entities": matched_entities,
                    **capability,
                }
            )
    matches.sort(key=lambda item: item["score"], reverse=True)
    return matches[: max(1, min(limit, 20))]


def _rank_runtime_relations(
    catalog: dict[str, Any],
    text: str,
    capabilities: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    relation_names = {relation for capability in capabilities for relation in capability.get("relations", [])}
    entity_names = {item["name"] for item in entities}
    matches = []
    for name, relation in catalog.get("runtime", {}).get("relations", {}).items():
        score = _score(text, _terms(name, relation))
        if name in relation_names:
            score = max(score, 80)
        if relation.get("source") in entity_names or relation.get("target") in entity_names:
            score = max(score, 50)
        if score > 0:
            matches.append({"name": name, "score": score, **relation})
    matches.sort(key=lambda item: item["score"], reverse=True)
    return matches[: max(1, min(limit, 20))]


def _join_keys(
    semantic_types: list[dict[str, Any]],
    capabilities: list[dict[str, Any]],
    relations: list[dict[str, Any]],
) -> list[str]:
    values = []
    for item in semantic_types:
        if item.get("join_priority") == "high":
            values.append(item["name"])
    for item in capabilities:
        values.extend(item.get("join_keys", []))
    for item in relations:
        values.extend(item.get("join_keys", []))
    return list(dict.fromkeys(values))


def _semantic_type_key(value: str) -> str:
    return str(value or "").rsplit(".", 1)[-1]


def _execution_hints(capabilities: list[dict[str, Any]], join_keys: list[str]) -> list[str]:
    hints = []
    if capabilities:
        hints.append(
            "Candidate semantic capabilities: "
            + ", ".join(dict.fromkeys(item["name"] for item in capabilities))
            + "."
        )
    if join_keys:
        hints.append(f"Preferred join keys: {', '.join(join_keys)}.")
    hints.append("Provider/tool implementation is resolved by pubdata_mcp, not semantic_platform.")
    return hints


def _terms(name: str, value: dict[str, Any]) -> list[str]:
    return [
        name,
        value.get("description", ""),
        value.get("description_ko", ""),
        value.get("maps_to", ""),
        value.get("entity", ""),
        *value.get("aliases", []),
    ]


def _score(text: str, terms: list[str]) -> int:
    score = 0
    for term in _normalize_many(terms):
        if term == text:
            score = max(score, 100)
        elif text in term:
            score = max(score, 70)
        elif term in text:
            score = max(score, 55)
    return score


def _normalize_many(values: list[str]) -> set[str]:
    return {_normalize(value) for value in values if _normalize(value)}


def _normalize(value: str) -> str:
    return "".join(str(value or "").lower().split())
