from apps.g2b.mcp.app.mapping.bid_mapping import FIELD_MAP
from datetime import datetime


def parse_g2b_datetime(value: str):
    """
    G2B 날짜 포맷: YYYYMMDDHHMM 또는 YYYY-MM-DD HH:MM:SS
    """
    for fmt in ("%Y%m%d%H%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except (TypeError, ValueError):
            continue
    return None


def transform(raw: dict):
    result = {}

    for raw_key, (new_key, dtype) in FIELD_MAP.items():
        value = raw.get(raw_key)

        if value is None:
            continue

        try:
            if dtype == int:
                value = int(value)
            elif dtype == "datetime":
                value = parse_g2b_datetime(value)
        except (TypeError, ValueError):
            continue

        if isinstance(value, datetime):
            value = value.isoformat()

        result[new_key] = value

    return result

def enrich(data):
    # budget range
    b = data.get("budget", 0)

    if b > 1_000_000_000:
        data["budget_range"] = "HIGH"
    elif b > 100_000_000:
        data["budget_range"] = "MID"
    else:
        data["budget_range"] = "LOW"

    # deadline_days
    if data.get("deadline"):
        deadline = datetime.fromisoformat(data["deadline"])
        data["deadline_days"] = (deadline - datetime.now()).days

    return data


def normalize_bid(raw: dict):
    mapped = transform(raw)
    return enrich(mapped)
