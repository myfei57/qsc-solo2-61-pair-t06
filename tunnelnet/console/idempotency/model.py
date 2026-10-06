"""Persistent shape of a console write request."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any

PENDING = "pending"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
INTERRUPTED = "interrupted"
UNCERTAIN = "uncertain"
EXPIRED = "expired"

TERMINAL = frozenset({SUCCEEDED, FAILED, INTERRUPTED, UNCERTAIN, EXPIRED})
SETTLED = frozenset({SUCCEEDED, FAILED, EXPIRED})
RECOVERABLE = frozenset({INTERRUPTED, UNCERTAIN})


def stable_payload(payload: dict[str, object]) -> dict[str, object]:
    """Canonical payload without the client supplied idempotency key."""

    return {key: value for key, value in payload.items() if key != "idempotency_key"}


def _stable_headers(headers: dict[str, str] | None) -> dict[str, str]:
    excluded = {"idempotency-key", "x-request-id"}
    return {
        key: value
        for key, value in sorted((headers or {}).items())
        if key.lower() not in excluded
    }


def request_fingerprint(
    method: str,
    path: str,
    query: dict[str, list[str]],
    payload: dict[str, object],
    headers: dict[str, str] | None = None,
) -> str:
    material = {
        "method": method.upper(),
        "path": path,
        "query": {key: list(values) for key, values in sorted(query.items())},
        "headers": _stable_headers(headers),
        "payload": stable_payload(payload),
    }
    encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def key_file_id(request_id: str) -> str:
    return hashlib.sha256(request_id.encode("utf-8")).hexdigest()


@dataclass
class IdempotencyRecord:
    """The request, execution result, audit events and lifecycle in one document."""

    request_id: str
    method: str
    path: str
    fingerprint: str
    created_at: str
    updated_at: str
    status: str = PENDING
    attempts: int = 0
    completed_at: str = ""
    settled_at: str = ""
    expires_at: str = ""
    failure: dict[str, object] = field(default_factory=dict)
    response: dict[str, object] = field(default_factory=dict)
    audit_events: list[dict[str, str]] = field(default_factory=list)
    recovery: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "IdempotencyRecord":
        return cls(
            request_id=str(payload.get("request_id", "")),
            method=str(payload.get("method", "")),
            path=str(payload.get("path", "")),
            fingerprint=str(payload.get("fingerprint", "")),
            created_at=str(payload.get("created_at", "")),
            updated_at=str(payload.get("updated_at", "")),
            status=str(payload.get("status", PENDING)),
            attempts=int(payload.get("attempts", 0)),
            completed_at=str(payload.get("completed_at", "")),
            settled_at=str(payload.get("settled_at", "")),
            expires_at=str(payload.get("expires_at", "")),
            failure=dict(payload.get("failure") or {}),
            response=dict(payload.get("response") or {}),
            audit_events=list(payload.get("audit_events") or []),
            recovery=dict(payload.get("recovery") or {}),
        )

    @property
    def file_id(self) -> str:
        return key_file_id(self.request_id)

    def has_audit(self, audit_id: str) -> bool:
        return any(item.get("id") == audit_id for item in self.audit_events)
