"""File backed storage for idempotency records."""

from __future__ import annotations

from tunnelnet.store import Store

from .model import IdempotencyRecord, key_file_id

KIND = "idempotency-request"


class IdempotencyStore:
    def __init__(self, store: Store) -> None:
        self._store = store

    def save(self, record: IdempotencyRecord) -> None:
        self._store.save(KIND, record.file_id, record.to_dict())

    def load(self, request_id: str) -> IdempotencyRecord | None:
        payload = self._store.load(KIND, key_file_id(request_id))
        if payload is None:
            return None
        return IdempotencyRecord.from_dict(payload)

    def list(self) -> list[IdempotencyRecord]:
        records: list[IdempotencyRecord] = []
        for file_id in self._store.list(KIND):
            payload = self._store.load(KIND, file_id)
            if payload is None:
                continue
            try:
                records.append(IdempotencyRecord.from_dict(payload))
            except (TypeError, ValueError, KeyError):
                continue
        return sorted(records, key=lambda item: item.created_at)
