from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class Entity:
    entity_type: str
    urn: str
    name: str
    status: str = "draft"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Aspect:
    entity_urn: str
    aspect_name: str
    payload: dict[str, Any]
    version: int = 1
    status: str = "draft"


@dataclass(frozen=True)
class Relationship:
    source_urn: str
    target_urn: str
    relationship_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    status: str = "draft"


@dataclass(frozen=True)
class ContextChangeProposal:
    proposal_id: str
    source_type: str
    status: str = "pending_review"
    items: tuple[dict[str, Any], ...] = ()
    evidence_refs: tuple[str, ...] = ()
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
