from .common import (
    DiscoveredAsset,
    DiscoveredField,
    LoadedSource,
    StructureOperationDraft,
    determine_ingestion_strategy,
    load_source_payload,
)


def discover_assets(*args, **kwargs):
    from .asset_discovery import discover_assets as _discover_assets

    return _discover_assets(*args, **kwargs)


def discover_structures(*args, **kwargs):
    from .structure_review import discover_structures as _discover_structures

    return _discover_structures(*args, **kwargs)

__all__ = [
    "DiscoveredAsset",
    "DiscoveredField",
    "LoadedSource",
    "StructureOperationDraft",
    "determine_ingestion_strategy",
    "discover_assets",
    "discover_structures",
    "load_source_payload",
]
