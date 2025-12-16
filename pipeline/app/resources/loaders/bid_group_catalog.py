from pathlib import Path
import json
from functools import lru_cache
from typing import TypedDict, List
from typing_extensions import NotRequired

APP_DIR = Path(__file__).resolve().parents[2]  # app/
CATALOG_DIR = APP_DIR / "resources" / "catalogs"

class BidGroup(TypedDict):
    group_id: str
    group_name: str
    description: str
    keywords: NotRequired[List[str]]

@lru_cache
def load_bid_group_catalog() -> list[BidGroup]:
    """
    Bid group 기준 분류 catalog 로드
    """
    path = CATALOG_DIR / "bid_group_catalog.json"

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("bid_group_catalog.json must be a list")

    for item in data:
        if "group_id" not in item or "group_name" not in item:
            raise ValueError(f"invalid bid group entry: {item}")

    return data

