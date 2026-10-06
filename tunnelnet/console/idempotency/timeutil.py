"""UTC helpers used by idempotency records."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def stamp(value: datetime | None = None) -> str:
    return (value or utc_now()).strftime(FORMAT)


def parse(value: str) -> datetime:
    return datetime.strptime(value, FORMAT).replace(tzinfo=timezone.utc)


def after(value: str, seconds: int) -> str:
    return stamp(parse(value) + timedelta(seconds=seconds))


def expired(value: str, now: datetime | None = None) -> bool:
    if not value:
        return False
    return parse(value) <= (now or utc_now())
