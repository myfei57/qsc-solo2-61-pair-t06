"""Durable idempotency guard for console write requests."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from tunnelnet.audit import (
    Record as AuditRecord,
    reset_current_audit_batch,
    reset_current_request_id,
    set_current_audit_batch,
    set_current_request_id,
)

from ..http import Request, RequestError, Response, json_response
from .errors import ExpiredRequest, IdempotencyConflict
from .locks import KeyLock
from .model import (
    EXPIRED,
    FAILED,
    INTERRUPTED,
    PENDING,
    RECOVERABLE,
    RUNNING,
    SETTLED,
    SUCCEEDED,
    UNCERTAIN,
    IdempotencyRecord,
    request_fingerprint,
)
from .persistence import IdempotencyStore
from .timeutil import after, stamp, utc_now

DEFAULT_TTL_SECONDS = 24 * 60 * 60

Handler = Callable[[Any, Request], Response]

if TYPE_CHECKING:  # pragma: no cover
    from ..runtime import Runtime
    from ..server import Server


class IdempotencyManager:
    """Serializes one request id and stores its terminal result durably."""

    def __init__(
        self,
        store: IdempotencyStore,
        runtime: "Runtime",
        lock: KeyLock | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        self._store = store
        self._runtime = runtime
        self._lock = lock or KeyLock(runtime.store)
        self._ttl_seconds = ttl_seconds
        self.recover_startup()

    def request_id(self, request: Request) -> str:
        request_id = request.idempotency_key()
        if not request_id:
            raise IdempotencyConflict("idempotency_key is required for write requests", status=400)
        if len(request_id) > 128:
            raise IdempotencyConflict(
                "idempotency_key must be no longer than 128 characters", status=400
            )
        return request_id

    def execute(self, server: "Server", request: Request, handler: Handler) -> Response:
        request_id = self.request_id(request)
        fingerprint = request_fingerprint(
            request.method,
            request.path,
            request.query,
            request.payload,
            request.headers,
        )
        now = stamp()
        try:
            lock_context = self._lock.acquire(request_id)
        except OSError as exc:
            raise IdempotencyConflict("idempotency lock is unavailable", status=503) from exc
        except TimeoutError as exc:
            raise IdempotencyConflict("request is still being processed") from exc
        with lock_context:
            existing = self._store.load(request_id)
            if existing is None:
                record = IdempotencyRecord(
                    request_id=request_id,
                    method=request.method.upper(),
                    path=request.path,
                    fingerprint=fingerprint,
                    created_at=now,
                    updated_at=now,
                    status=PENDING,
                )
                self._add_event(record, "idempotency_reserved")
                self._save(record, 503, "could not reserve request id")
                self._publish_events(record)
            else:
                record = existing
                replay = self._check(record, fingerprint, now)
                if replay is not None:
                    return replay

            record.status = RUNNING
            record.attempts += 1
            record.updated_at = now
            self._add_event(record, "idempotency_execution_started")
            self._save(record, 503, "could not mark request as running")
            self._publish_events(record)

            batch: list[AuditRecord] = []
            id_token = set_current_request_id(request_id)
            audit_token = set_current_audit_batch(batch)
            try:
                try:
                    response = handler(server, request)
                except RequestError as exc:
                    response = json_response({"error": exc.message}, status=exc.status)
                except ValueError as exc:
                    response = json_response({"error": str(exc)}, status=400)
                except Exception as exc:  # noqa: BLE001 - side effects may have started
                    try:
                        response = self._mark_execution_uncertain(record, exc)
                    except OSError:
                        response = self._mark_uncertain(record)
                    self._publish_events(record)
                    return response

                self._attach_business_events(record, batch)
                completed_at = stamp()
                succeeded = response.status < 400
                record.status = SUCCEEDED if succeeded else FAILED
                record.completed_at = completed_at
                record.updated_at = completed_at
                record.settled_at = completed_at
                record.expires_at = after(completed_at, self._ttl_seconds)
                record.response = self._response_data(response)
                if not succeeded:
                    record.failure = {
                        "type": "HTTPError",
                        "status": response.status,
                        "message": self._response_message(response),
                    }
                try:
                    self._store.save(record)
                except OSError:
                    response = self._mark_uncertain(record)
                    self._publish_events(record)
                    return response
                terminal_event = (
                    "idempotency_execution_succeeded"
                    if succeeded
                    else "idempotency_execution_failed"
                )
                self._add_event(record, terminal_event)
                try:
                    self._store.save(record)
                except OSError:
                    # The terminal response is already durable; only the audit
                    # annotation could not be attached. Do not turn this into a
                    # business retry.
                    pass
                self._publish_events(record)
                return self._replay_header(response, replayed=False)
            finally:
                reset_current_request_id(id_token)
                reset_current_audit_batch(audit_token)

    def recover_startup(self) -> int:
        changed = 0
        for record in self._store.list():
            if record.status in {PENDING, RUNNING}:
                record.status = INTERRUPTED
                record.updated_at = stamp()
                record.recovery = {"reason": "process restarted before terminal result was durable"}
                self._add_event(record, "idempotency_interrupted")
                self._store.save(record)
                self._publish_events(record)
                changed += 1
            else:
                self._replay_audit(record)
        return changed

    def recover(
        self,
        request_id: str,
        note: str,
        status_code: int,
        message: str,
        outcome: str = FAILED,
    ) -> Response:
        if outcome not in {SUCCEEDED, FAILED}:
            raise IdempotencyConflict("outcome must be succeeded or failed", status=400)
        with self._lock.acquire(request_id):
            record = self._require(request_id)
            if record.status not in RECOVERABLE:
                raise IdempotencyConflict(f"request status {record.status} is not recoverable")
            now = stamp()
            record.status = outcome
            record.completed_at = now
            record.updated_at = now
            record.settled_at = now
            record.expires_at = after(now, self._ttl_seconds)
            record.recovery = {"resolved_at": now, "note": note, "status": str(status_code)}
            if outcome == SUCCEEDED:
                record.failure = {}
                record.response = {
                    "status": status_code,
                    "body": {
                        "recovered": True,
                        "request_id": request_id,
                        "note": note,
                    },
                    "content_type": "application/json; charset=utf-8",
                }
            else:
                record.failure = {"type": "ManualRecovery", "message": message}
                record.response = {
                    "status": status_code,
                    "body": {"error": message},
                    "content_type": "application/json; charset=utf-8",
                }
            self._add_event(record, "idempotency_recovered")
            self._store.save(record)
            self._publish_events(record)
            return json_response(
                {"request_id": request_id, "status": record.status, "replayed": False}
            )

    def sweep_expired(self) -> dict[str, object]:
        now = utc_now()
        marked: list[str] = []
        protected: list[str] = []
        for record in self._store.list():
            is_due = record.status in SETTLED and record.expires_at
            if is_due and self._is_due(record.expires_at, now):
                record.status = EXPIRED
                record.updated_at = stamp(now)
                self._add_event(record, "idempotency_expired", at=record.updated_at)
                self._store.save(record)
                self._publish_events(record)
                marked.append(record.request_id)
            elif record.status in {PENDING, RUNNING, INTERRUPTED, UNCERTAIN}:
                protected.append(record.request_id)
        return {"expired": marked, "protected": protected, "count": len(marked)}

    def describe(self, request_id: str) -> dict[str, object]:
        record = self._require(request_id)
        return record.to_dict()

    def list(self) -> list[dict[str, object]]:
        return [record.to_dict() for record in self._store.list()]

    def _check(
        self, record: IdempotencyRecord, fingerprint: str, now: str
    ) -> Response | None:
        if record.status == EXPIRED:
            raise ExpiredRequest()
        if record.fingerprint != fingerprint:
            raise IdempotencyConflict(
                "idempotency key was already used with a different request"
            )
        if record.status in {PENDING, RUNNING}:
            raise IdempotencyConflict("request is still being processed")
        if record.status in RECOVERABLE:
            raise IdempotencyConflict(
                "request has an uncertain outcome; use the recovery endpoint before "
                "issuing a new command"
            )
        if record.status in {SUCCEEDED, FAILED}:
            self._add_event(record, "idempotency_replayed", at=now)
            self._store.save(record)
            self._publish_events(record)
            cached = self._response(record.response)
            return self._replay_header(cached, replayed=True)
        raise IdempotencyConflict(f"unsupported request status {record.status}")

    def _save(self, record: IdempotencyRecord, status: int, message: str) -> None:
        try:
            self._store.save(record)
        except OSError as exc:
            raise IdempotencyConflict(message, status=status) from exc

    def _mark_execution_uncertain(self, record: IdempotencyRecord, exc: Exception) -> Response:
        completed_at = stamp()
        record.status = UNCERTAIN
        record.completed_at = completed_at
        record.updated_at = completed_at
        record.settled_at = ""
        record.expires_at = ""
        record.response = {}
        record.failure = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        self._add_event(record, "idempotency_execution_uncertain", at=completed_at)
        self._store.save(record)
        return json_response(
            {"error": "request outcome is uncertain and must be recovered manually"},
            status=503,
        )

    def _mark_uncertain(self, record: IdempotencyRecord) -> Response:
        record.status = UNCERTAIN
        record.updated_at = stamp()
        record.settled_at = ""
        record.expires_at = ""
        record.response = {}
        record.failure = {
            "type": "ResultWriteFailed",
            "message": "business action ran but terminal result did not persist",
        }
        self._add_event(record, "idempotency_result_write_failed")
        try:
            self._store.save(record)
        except OSError as exc:
            raise IdempotencyConflict(
                "could not persist terminal result", status=503
            ) from exc
        self._publish_events(record)
        return json_response(
            {"error": "request outcome is uncertain and must be recovered manually"},
            status=503,
        )

    def _add_event(
        self,
        record: IdempotencyRecord,
        event: str,
        at: str | None = None,
        audit_id: str | None = None,
    ) -> AuditRecord:
        audit = AuditRecord(
            id=audit_id or uuid.uuid4().hex,
            event=event,
            at=at or stamp(),
            request_id=record.request_id,
        )
        if not record.has_audit(audit.id):
            record.audit_events.append(audit.as_dict())
        return audit

    def _attach_business_events(
        self, record: IdempotencyRecord, events: list[AuditRecord]
    ) -> None:
        for audit in events:
            audit = AuditRecord(
                id=audit.id,
                event=audit.event,
                at=audit.at,
                request_id=record.request_id,
            )
            if not record.has_audit(audit.id):
                record.audit_events.append(audit.as_dict())

    def _publish_events(self, record: IdempotencyRecord) -> None:
        self._replay_audit(record)

    def _replay_audit(self, record: IdempotencyRecord) -> None:
        self._runtime.audit.replay(
            [
                AuditRecord(
                    id=str(item.get("id", "")),
                    event=str(item.get("event", "")),
                    at=str(item.get("at", "")),
                    request_id=str(item.get("request_id", "")),
                )
                for item in record.audit_events
            ]
        )

    def _require(self, request_id: str) -> IdempotencyRecord:
        record = self._store.load(request_id)
        if record is None:
            raise IdempotencyConflict("request id not found", status=404)
        return record

    @staticmethod
    def _response_data(response: Response) -> dict[str, object]:
        body: object
        if response.content_type.startswith("application/json"):
            try:
                body = json.loads(response.body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                body = response.body.decode("utf-8", errors="replace")
        else:
            body = response.body.decode("utf-8", errors="replace")
        return {"status": response.status, "body": body, "content_type": response.content_type}

    @staticmethod
    def _response(payload: dict[str, object]) -> Response:
        status = int(payload.get("status", 200))
        content_type = str(payload.get("content_type", "application/json; charset=utf-8"))
        body = payload.get("body", {})
        if content_type.startswith("application/json"):
            raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        else:
            raw = str(body).encode("utf-8")
        return Response(status=status, body=raw, content_type=content_type)

    @staticmethod
    def _replay_header(response: Response, replayed: bool) -> Response:
        return Response(
            status=response.status,
            body=response.body,
            content_type=response.content_type,
            headers=(*response.headers, ("Idempotency-Replay", "true" if replayed else "false")),
        )

    @staticmethod
    def _response_message(response: Response) -> str:
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return response.body.decode("utf-8", errors="replace")
        if isinstance(payload, dict) and "error" in payload:
            return str(payload["error"])
        return response.body.decode("utf-8", errors="replace")

    @staticmethod
    def _is_due(value: str, now) -> bool:
        from .timeutil import parse

        try:
            return parse(value) <= now
        except ValueError:
            return False
