"""Tests for durable write-request idempotency and replay protection."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest

from tunnelnet.console.idempotency.model import RUNNING, IdempotencyRecord, request_fingerprint
from tunnelnet.console.idempotency.persistence import KIND, IdempotencyStore
from tunnelnet.console.idempotency.timeutil import after, stamp
from tunnelnet.console.server import Server
from tunnelnet.store import Store


class IdempotencyCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/data")
        self.server = Server(self.store, web_dir="web")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def call(self, method: str, path: str, payload: dict | None = None, headers: dict | None = None):
        response = self.server.dispatch(method, path, payload, headers=headers)
        body = json.loads(response.body.decode("utf-8"))
        return response, response.status, body

    def test_duplicate_request_executes_once_and_restart_replays_result(self) -> None:
        payload = {"cabin_id": "cabin-1", "requester": "ops", "idempotency_key": "approve-once"}
        first, status, body = self.call("POST", "/api/access/approve", payload)
        self.assertEqual(status, 200)
        self.assertEqual(first.headers[-1], ("Idempotency-Replay", "false"))
        approval_id = body["id"]

        second, status, body = self.call("POST", "/api/access/approve", payload)
        self.assertEqual(status, 200)
        self.assertEqual(second.headers[-1], ("Idempotency-Replay", "true"))
        self.assertEqual(body["id"], approval_id)
        self.assertEqual(len(self.store.list("approval")), 1)

        restarted = Server(self.store, web_dir="web")
        response = restarted.dispatch("POST", "/api/access/approve", payload)
        replay = json.loads(response.body)
        self.assertEqual(response.status, 200)
        self.assertEqual(replay["id"], approval_id)
        self.assertEqual(len(self.store.list("approval")), 1)

    def test_failed_result_is_replayed_and_new_key_can_be_used(self) -> None:
        payload = {"request_id": "ghost", "idempotency_key": "grant-failed"}
        first, status, first_body = self.call("POST", "/api/access/grant", payload)
        self.assertEqual(status, 400)
        self.assertIn("not found", first_body["error"])

        second, status, second_body = self.call("POST", "/api/access/grant", payload)
        self.assertEqual(status, 400)
        self.assertEqual(second_body, first_body)

        retry, status, _ = self.call(
            "POST", "/api/access/grant", {"request_id": "ghost", "idempotency_key": "manual-retry"}
        )
        self.assertEqual(status, 400)
        self.assertEqual(dict(retry.headers).get("Idempotency-Replay"), "false")

    def test_same_key_with_different_request_is_conflict(self) -> None:
        self.call(
            "POST",
            "/api/cabins/valve",
            {"idempotency_key": "same-key", "id": "cabin-1", "open": True},
        )
        _, status, body = self.call(
            "POST",
            "/api/cabins/valve",
            {"idempotency_key": "same-key", "id": "cabin-2", "open": True},
        )
        self.assertEqual(status, 409)
        self.assertIn("different request", body["error"])

    def test_header_key_and_missing_key_are_supported(self) -> None:
        _, status, _ = self.call("POST", "/api/cabins/valve", {"id": "cabin-1", "open": True})
        self.assertEqual(status, 400)

        response = self.server.dispatch(
            "POST",
            "/api/cabins/valve",
            {"id": "cabin-1", "open": False},
            headers={"Idempotency-Key": "header-key"},
        )
        self.assertEqual(response.status, 200)

    def test_lifecycle_and_business_audit_share_request_id(self) -> None:
        payload = {"cabin_id": "cabin-1", "requester": "ops", "idempotency_key": "audit-key"}
        self.call("POST", "/api/access/approve", payload)
        self.call("POST", "/api/access/approve", payload)

        records = self.server.runtime.audit.records()
        related = [item for item in records if item.request_id == "audit-key"]
        self.assertIn("idempotency_reserved", [item.event for item in related])
        self.assertIn("idempotency_replayed", [item.event for item in related])
        self.assertEqual(
            [item.event for item in related if item.event == "access_approved"],
            ["access_approved"],
        )

    def test_terminal_persist_failure_becomes_recoverable_uncertain(self) -> None:
        records = IdempotencyStore(self.store)
        original_save = records.save

        def flaky_save(record):
            if record.status == "succeeded":
                raise OSError("disk full")
            return original_save(record)

        records.save = flaky_save
        self.server.idempotency._store = records

        _, status, body = self.call(
            "POST",
            "/api/cabins/valve",
            {"idempotency_key": "uncertain-key", "id": "cabin-1", "open": True},
        )
        self.assertEqual(status, 503)
        self.assertIn("uncertain", body["error"])

        _, status, body = self.call(
            "POST",
            "/api/cabins/valve",
            {"idempotency_key": "uncertain-key", "id": "cabin-1", "open": True},
        )
        self.assertEqual(status, 409)
        self.assertIn("uncertain", body["error"])

        _, status, _ = self.call(
            "POST",
            "/api/idempotency/recover",
            {"request_id": "uncertain-key", "note": "valve confirmed open"},
        )
        self.assertEqual(status, 200)

        _, status, body = self.call(
            "POST",
            "/api/cabins/valve",
            {"idempotency_key": "uncertain-key", "id": "cabin-1", "open": True},
        )
        self.assertEqual(status, 503)
        self.assertIn("manual recovery", body["error"])

    def test_restarted_inflight_request_is_interrupted_and_recoverable(self) -> None:
        request = {"id": "cabin-1", "open": True, "idempotency_key": "inflight-key"}
        now = stamp()
        record = IdempotencyRecord(
            request_id="inflight-key",
            method="POST",
            path="/api/cabins/valve",
            fingerprint=request_fingerprint("POST", "/api/cabins/valve", {}, request),
            created_at=now,
            updated_at=now,
            status=RUNNING,
            attempts=1,
        )
        IdempotencyStore(self.store).save(record)

        restarted_server = Server(self.store, web_dir="web")
        self.server = restarted_server
        response, status, body = self.call("POST", "/api/cabins/valve", request)
        self.assertEqual(status, 409)
        self.assertIn("uncertain outcome", body["error"])

        response = restarted_server.dispatch(
            "POST",
            "/api/idempotency/recover",
            {"request_id": "inflight-key", "note": "operator checked valve"},
        )
        self.assertEqual(response.status, 200)

    def test_expiry_marks_record_expired_and_keeps_unsettled_records(self) -> None:
        server = Server(self.store, web_dir="web", idempotency_ttl_seconds=-1)
        server.dispatch(
            "POST",
            "/api/cabins/valve",
            {"idempotency_key": "expiring-key", "id": "cabin-1", "open": True},
        )
        request = {"id": "cabin-1", "open": True}
        now = stamp()
        pending = IdempotencyRecord(
            request_id="settled-later",
            method="POST",
            path="/api/cabins/valve",
            fingerprint=request_fingerprint("POST", "/api/cabins/valve", {}, request),
            created_at=now,
            updated_at=now,
            status=RUNNING,
            expires_at=after(now, -10),
        )
        IdempotencyStore(self.store).save(pending)

        response = server.dispatch("POST", "/api/idempotency/expire", {})
        result = json.loads(response.body)
        self.assertIn("expiring-key", result["expired"])
        self.assertIn("settled-later", result["protected"])
        self.assertEqual(len(self.store.list(KIND)), 2)

        response = server.dispatch(
            "POST",
            "/api/cabins/valve",
            {"idempotency_key": "expiring-key", "id": "cabin-1", "open": True},
        )
        body = json.loads(response.body)
        self.assertEqual(response.status, 410)
        self.assertIn("expired", body["error"])

    def test_concurrent_duplicates_wait_instead_of_running_action_twice(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        original = type(self.server.runtime.cabins).open_valve

        def blocked_open(registry, cabin_id):
            entered.set()
            self.assertTrue(release.wait(2))
            return original(registry, cabin_id)

        type(self.server.runtime.cabins).open_valve = blocked_open
        payload = {"idempotency_key": "concurrent-key", "id": "cabin-2", "open": True}
        results: list[tuple[int, object]] = []

        def invoke() -> None:
            response = self.server.dispatch("POST", "/api/cabins/valve", payload)
            results.append((response.status, json.loads(response.body)))

        first = threading.Thread(target=invoke)
        second = threading.Thread(target=invoke)
        first.start()
        self.assertTrue(entered.wait(2))
        second.start()
        release.set()
        first.join(2)
        second.join(2)
        type(self.server.runtime.cabins).open_valve = original

        self.assertEqual(len(results), 2)
        self.assertEqual([item[0] for item in results], [200, 200])
        self.assertEqual([item[1] for item in results], [{"id": "cabin-2", "open": True}] * 2)


if __name__ == "__main__":
    unittest.main()
