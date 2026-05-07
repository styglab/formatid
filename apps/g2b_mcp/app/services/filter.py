from datetime import datetime, timedelta


def filter_deadline(bids: list[dict], deadline_dt: str) -> list[dict]:
    limit = _parse_deadline(deadline_dt)

    return [
        b for b in bids
        if b.get("deadline") and datetime.fromisoformat(b["deadline"]) <= limit
    ]


def _parse_deadline(value: str) -> datetime:
    normalized = value.strip()
    if normalized.isdigit() and len(normalized) <= 3:
        return datetime.now() + timedelta(days=int(normalized))
    for fmt in ("%Y%m%d%H%M", "%Y%m%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(normalized)
