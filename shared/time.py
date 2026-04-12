import os
from datetime import datetime
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE = "Asia/Seoul"


def get_timezone_name() -> str:
    return os.getenv("APP_TIMEZONE", DEFAULT_TIMEZONE)


def get_timezone() -> ZoneInfo:
    return ZoneInfo(get_timezone_name())


def now() -> datetime:
    return datetime.now(get_timezone())


def iso_now() -> str:
    return now().isoformat()
