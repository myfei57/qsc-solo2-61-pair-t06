"""Tests for the write-request idempotency and replay protection layer."""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest

from tunnelnet.console.idempotency import (
    EXPIRED,
    FAILED,
    IN_FLIGHT,
    SUCCEEDED,
    IdempotencyGuard,
    request_hash,
    scope_of,
)
from tunnelnet.console.http import Request, json_response
from tunnelnet.console.server import Server
from tunnelnet.store import Store


def make_request(key: str, payload: dict | None = None, path: str = "/api/x") -> Request:
    body = dict(payload or {})
    body["idempotency_key"] = key
    return Request(method="POST", path=path, query={}, payload=body)


class GuardCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/data")
        self.now = [1000.0]
        self.guard = IdempotencyGuard(
            self.store, ttl_seconds=10.0, wait_timeout=0.3, clock=lambda: self.now[0]
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def run_once(self, calls: list, body: object = None, status: int = 200):
        def run():
            calls.append(1)
            return json_response(body if body is not None else {"n": len(calls)}, status=status)

        return run

    def test_same_key_executes_once_then_replays(self) -> None:
        calls: list = []
        first = self.guard.guarded(make_request("idem-key-1"), self.run_once(calls))
        second = self.guard.guarded(make_request("idem-key-1"), self.run_once(calls))
        self.assertEqual(len(calls), 1)
        self.assertEqual(first.status, 200)
        self.assertEqual(second.status, 200)
        self.assertEqual(json.loads(second.body), {"n": 1})
        self.assertIn(("Idempotent-Replay", "true"), second.headers)
        self.assertNotIn(("Idempotent-Replay", "true"), first.headers)

    def test_concurrent_duplicates_execute_exactly_once(self) -> None:
        guard = IdempotencyGuard(self.store, wait_timeout=5.0, clock=lambda: self.now[0])
        calls: list = []
        barrier = threading.Barrier(8)

        def run():
            calls.append(1)
            time.sleep(0.1)
            return json_response({"n": len(calls)})

        responses = []

        def worker():
            barrier.wait()
            responses.append(guard.guarded(make_request("idem-key-race"), run))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(calls), 1)
        self.assertTrue(all(response.status == 200 for response in responses))
        bodies = {response.body for response in responses}
        self.assertEqual(len(bodies), 1)
        replays = [r for r in responses if ("Idempotent-Replay", "true") in r.headers]
        self.assertEqual(len(replays), 7)

    def test_result_survives_restart(self) -> None:
        calls: list = []
        self.guard.guarded(make_request("idem-key-2"), self.run_once(calls))
        reopened = IdempotencyGuard(self.store, clock=lambda: self.now[0])
        replay = reopened.guarded(make_request("idem-key-2"), self.run_once(calls))
        self.assertEqual(len(calls), 1)
        self.assertIn(("Idempotent-Replay", "true"), replay.headers)
        self.assertEqual(json.loads(replay.body), {"n": 1})

    def test_failure_is_settled_and_replayed_verbatim(self) -> None:
        calls: list = []
        failing = self.run_once(calls, body={"error": "pump offline"}, status=500)
        first = self.guard.guarded(make_request("idem-key-3"), failing)
        self.assertEqual(first.status, 500)
        replay = self.guard.guarded(make_request("idem-key-3"), failing)
        self.assertEqual(len(calls), 1)
        self.assertEqual(replay.status, 500)
        self.assertEqual(json.loads(replay.body), {"error": "pump offline"})
        self.assertIn(("Idempotent-Replay", "true"), replay.headers)

    def test_manual_resend_uses_new_key_with_retry_of(self) -> None:
        calls: list = []
        self.guard.guarded(make_request("idem-key-4"), self.run_once(calls, status=500))
        resent = self.guard.guarded(
            make_request("idem-key-5", {"retry_of": "idem-key-4"}), self.run_once(calls)
        )
        self.assertEqual(resent.status, 200)
        self.assertEqual(len(calls), 2)
        record = self.guard.status("POST /api/x", "idem-key-5")
        self.assertEqual(record["retry_of"], "idem-key-4")
        self.assertEqual(record["events"][0]["detail"], "retry_of=idem-key-4")

    def test_4xx_releases_the_key_without_settling(self) -> None:
        calls: list = []
        bad = self.run_once(calls, body={"error": "bad request"}, status=400)
        first = self.guard.guarded(make_request("idem-key-6"), bad)
        self.assertEqual(first.status, 400)
        second = self.guard.guarded(make_request("idem-key-6"), self.run_once(calls))
        self.assertEqual(second.status, 200)
        self.assertEqual(len(calls), 2)
        self.assertNotIn(("Idempotent-Replay", "true"), second.headers)

    def test_same_key_with_different_payload_is_rejected(self) -> None:
        calls: list = []
        self.guard.guarded(make_request("idem-key-7", {"a": 1}), self.run_once(calls))
        conflict = self.guard.guarded(make_request("idem-key-7", {"a": 2}), self.run_once(calls))
        self.assertEqual(conflict.status, 422)
        self.assertEqual(len(calls), 1)

    def test_invalid_key_is_rejected(self) -> None:
        response = self.guard.guarded(make_request("bad key!"), self.run_once([]))
        self.assertEqual(response.status, 400)

    def test_expiry_marks_tombstone_and_never_reexecutes(self) -> None:
        calls: list = []
        self.guard.guarded(make_request("idem-key-8"), self.run_once(calls))
        self.now[0] += 11.0  # 超过 ttl=10s
        self.assertEqual(self.guard.sweep_expired(), 1)
        record = self.guard.status("POST /api/x", "idem-key-8")
        self.assertEqual(record["state"], EXPIRED)  # 墓碑：记录仍在
        self.assertIn("expired", [event["event"] for event in record["events"]])
        replay = self.guard.guarded(make_request("idem-key-8"), self.run_once(calls))
        self.assertEqual(replay.status, 410)
        self.assertEqual(len(calls), 1)

    def test_sweep_never_touches_in_flight(self) -> None:
        claimed = self.guard._claim("POST /api/x", "idem-key-9", "digest")
        self.assertIsNotNone(claimed)
        self.now[0] += 10_000.0
        self.assertEqual(self.guard.sweep_expired(), 0)
        record = self.guard.status("POST /api/x", "idem-key-9")
        self.assertEqual(record["state"], IN_FLIGHT)

    def test_recovery_settles_interrupted_claims(self) -> None:
        request = make_request("idem-key-10")
        self.guard._claim(scope_of(request), "idem-key-10", request_hash(request))
        reopened = IdempotencyGuard(self.store, clock=lambda: self.now[0])
        self.assertEqual(reopened.recover_interrupted(), 1)
        record = reopened.status("POST /api/x", "idem-key-10")
        self.assertEqual(record["state"], FAILED)
        self.assertEqual(record["error"], "interrupted")
        self.assertIn("recovered", [event["event"] for event in record["events"]])
        calls: list = []
        replay = reopened.guarded(request, self.run_once(calls))
        self.assertEqual(replay.status, 500)
        self.assertIn("outcome unknown", json.loads(replay.body)["error"])
        self.assertEqual(len(calls), 0)  # 恢复后绝不重新执行

    def test_settle_failure_is_a_recoverable_partial_failure(self) -> None:
        calls: list = []
        original_save = self.store.save
        self.store.save = lambda *args, **kwargs: (_ for _ in ()).throw(OSError("disk full"))
        try:
            response = self.guard.guarded(make_request("idem-key-11"), self.run_once(calls))
        finally:
            self.store.save = original_save
        self.assertEqual(response.status, 500)
        self.assertIn("outcome unknown", json.loads(response.body)["error"])
        record = self.guard.status("POST /api/x", "idem-key-11")
        self.assertEqual(record["state"], IN_FLIGHT)  # 结果没存下，记录未结算
        self.assertEqual(self.guard.recover_interrupted(), 1)
        self.assertEqual(self.guard.status("POST /api/x", "idem-key-11")["state"], FAILED)

    def test_in_flight_duplicate_gets_409_after_wait(self) -> None:
        guard = IdempotencyGuard(self.store, wait_timeout=0.3, clock=lambda: self.now[0])
        release = threading.Event()
        done = []

        def slow_run():
            release.wait(2.0)
            done.append(1)
            return json_response({"slow": True})

        leader = threading.Thread(
            target=lambda: done.append(guard.guarded(make_request("idem-key-12"), slow_run))
        )
        leader.start()
        time.sleep(0.1)
        duplicate = guard.guarded(make_request("idem-key-12"), slow_run)
        self.assertEqual(duplicate.status, 409)
        release.set()
        leader.join(2.0)
        self.assertEqual(len(done), 2)

    def test_try_save_is_an_atomic_claim(self) -> None:
        self.assertTrue(self.store.try_save("kind", "rid", {"a": 1}))
        self.assertFalse(self.store.try_save("kind", "rid", {"a": 2}))
        self.assertEqual(self.store.load("kind", "rid"), {"a": 1})


class ServerIntegrationCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/data")
        self.server = Server(self.store, web_dir="web")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def post(self, path: str, payload: dict):
        return self.server.dispatch("POST", path, payload)

    def test_enforced_route_requires_a_key(self) -> None:
        response = self.post("/api/drain/start", {"cabin_id": "cabin-1"})
        self.assertEqual(response.status, 400)
        self.assertIn("idempotency key required", json.loads(response.body)["error"])

    def test_drain_start_replays_across_restart(self) -> None:
        payload = {"cabin_id": "cabin-1", "idempotency_key": "idem-drain-restart"}
        first = self.post("/api/drain/start", payload)
        self.assertEqual(first.status, 200)
        self.assertEqual(json.loads(first.body)["runs"], 1)

        restarted = Server(self.store, web_dir="web")
        replay = restarted.dispatch("POST", "/api/drain/start", payload)
        self.assertEqual(replay.status, 200)
        self.assertIn(("Idempotent-Replay", "true"), replay.headers)
        self.assertEqual(json.loads(replay.body)["runs"], 1)  # 缓存的上次结果
        self.assertEqual(restarted.runtime.drain.run_count(), 0)  # 没有再次执行

    def test_idempotency_events_reach_the_audit_feed(self) -> None:
        payload = {"cabin_id": "cabin-1", "idempotency_key": "idem-drain-audit"}
        self.post("/api/drain/start", payload)
        self.post("/api/drain/start", payload)
        events = [record.event for record in self.server.runtime.audit.records()]
        self.assertIn("idempotency_claimed", events)
        self.assertIn("idempotency_settled", events)
        self.assertIn("idempotency_replayed", events)

    def test_status_endpoint_reports_the_record(self) -> None:
        payload = {"cabin_id": "cabin-1", "idempotency_key": "idem-drain-status"}
        self.post("/api/drain/start", payload)
        found = self.server.dispatch(
            "GET", "/api/idempotency", query={"path": ["/api/drain/start"], "key": ["idem-drain-status"]}
        )
        self.assertEqual(found.status, 200)
        body = json.loads(found.body)
        self.assertEqual(body["state"], SUCCEEDED)
        self.assertEqual(body["status"], 200)
        self.assertTrue(body["events"])
        missing = self.server.dispatch(
            "GET", "/api/idempotency", query={"path": ["/api/drain/start"], "key": ["idem-nope-1"]}
        )
        self.assertEqual(missing.status, 404)

    def test_4xx_response_does_not_consume_the_key(self) -> None:
        payload = {"request_id": "ghost", "idempotency_key": "idem-grant-retry"}
        first = self.post("/api/access/grant", payload)
        self.assertEqual(first.status, 400)
        second = self.post("/api/access/grant", payload)
        self.assertEqual(second.status, 400)
        self.assertNotIn(("Idempotent-Replay", "true"), second.headers)


if __name__ == "__main__":
    unittest.main()
