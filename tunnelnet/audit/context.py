"""Request context shared by audit entries written for one write request."""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .model import Record

CURRENT_REQUEST_ID: ContextVar[str] = ContextVar("current_request_id", default="")
CURRENT_AUDIT_BATCH: ContextVar["list[Record] | None"] = ContextVar("current_audit_batch", default=None)


def current_request_id() -> str:
    return CURRENT_REQUEST_ID.get()


def set_current_request_id(request_id: str) -> Token[str]:
    return CURRENT_REQUEST_ID.set(request_id)


def reset_current_request_id(token: Token[str]) -> None:
    CURRENT_REQUEST_ID.reset(token)


def current_audit_batch() -> "list[Record] | None":
    return CURRENT_AUDIT_BATCH.get()


def set_current_audit_batch(batch: "list[Record]") -> Token["list[Record] | None"]:
    return CURRENT_AUDIT_BATCH.set(batch)


def reset_current_audit_batch(token: Token["list[Record] | None"]) -> None:
    CURRENT_AUDIT_BATCH.reset(token)
