"""Errors returned by the idempotency layer."""

from __future__ import annotations

from ..http import RequestError


class IdempotencyConflict(RequestError):
    def __init__(self, message: str, status: int = 409) -> None:
        super().__init__(status, message)


class ExpiredRequest(RequestError):
    def __init__(self, message: str = "request id has expired") -> None:
        super().__init__(410, message)
