

def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def parse_amount(value):
    if value in (None, ""):
        return None
    try:
        return int(str(value).replace(",", ""))
    except ValueError:
        return None

