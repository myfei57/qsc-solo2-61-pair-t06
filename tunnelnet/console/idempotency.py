"""Idempotency and replay protection for console write requests.

每个写请求携带一个幂等标识（``Idempotency-Key`` 头或 ``idempotency_key``
字段），同一标识重复到达只生效一次。标识、执行结果与审计事件存在同一
份记录里（``Store`` 的 ``idempotency`` kind），重启后仍然认得。

两条既定决策（完整论证见 DESIGN.md）：

* **失败也结算。** 进入执行阶段后（本层以响应状态码划线：2xx/5xx 进入
  执行，4xx 视为未进入执行），无论成败都结算并缓存结果；同标识重放
  原样返回上次结果，绝不重复执行。人工补发必须换新标识，并以
  ``retry_of`` 关联上一次，审计链可追溯。
* **过期只标记失效。** 过期记录转为 ``EXPIRED`` 墓碑，不删除；重放过期
  标识返回 410，绝不会被当作新请求再执行。未结算（``IN_FLIGHT``）的
  记录永不参与过期清理。
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from tunnelnet.store import Store

from .http import Request, Response, error_response

IN_FLIGHT = "IN_FLIGHT"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
EXPIRED = "EXPIRED"

KIND = "idempotency"
KEY_HEADER = "idempotency-key"
REPLAY_HEADER = ("Idempotent-Replay", "true")
DEFAULT_TTL_SECONDS = 7 * 24 * 3600
DEFAULT_WAIT_TIMEOUT = 10.0
_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_\-]{7,127}")

Observer = Callable[[str, str], None]


def scope_of(request: Request) -> str:
    """标识的作用域：同一方法 + 同一路径。不同接口之间标识互不影响。"""

    return f"{request.method} {request.path}"


def key_of(request: Request) -> str:
    """请求携带的幂等标识：``Idempotency-Key`` 头优先，其次请求体字段。"""

    header = request.headers.get(KEY_HEADER, "")
    if header.strip():
        return header.strip()
    value = request.payload.get("idempotency_key", "")
    return str(value).strip() if value else ""


def request_hash(request: Request) -> str:
    """请求指纹：方法 + 路径 + 规范化请求体。同标识不同指纹即客户端误用。"""

    canonical = json.dumps(
        {"method": request.method, "path": request.path, "payload": request.payload},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class IdempotencyGuard:
    """认领 → 执行 → 结算的写请求守卫；重放只读缓存结果。

    不变量：

    1. 同一 ``(scope, key)`` 最多进入一次执行（``Store.try_save`` 原子认领）。
    2. 每个认领最终必然结算：运行时结算失败会重试，崩溃由
       :meth:`recover_interrupted` 在启动时按"结果未知"兜底结算。
    3. 状态迁移与审计事件写入同一份记录、同一次落盘，口径一致。
    4. 重放只读，绝不触发执行；过期只标记，且只作用于已结算记录。
    """

    def __init__(
        self,
        store: Store,
        *,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        wait_timeout: float = DEFAULT_WAIT_TIMEOUT,
        observer: Observer | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._store = store
        self._ttl = float(ttl_seconds)
        self._wait_timeout = float(wait_timeout)
        self._observer = observer
        self._clock = clock
        self._io_lock = threading.RLock()
        self._signals: dict[tuple[str, str], threading.Event] = {}
        self._signals_lock = threading.Lock()

    # ---------------------------------------------------------------- 主入口

    def guarded(self, request: Request, run: Callable[[], Response]) -> Response:
        """执行一个写请求；同标识重放返回上次的结果，绝不重复执行。"""

        scope = scope_of(request)
        key = key_of(request)
        if not _KEY_PATTERN.fullmatch(key):
            return error_response(
                400, "idempotency key must be 8-128 chars of letters, digits, '_' or '-'"
            )
        digest = request_hash(request)
        retry_of = str(request.payload.get("retry_of", "") or "")
        for _ in range(2):  # 认领冲突或认领被释放（4xx）时至多重认一轮
            record = self._load(scope, key)
            if record is None:
                claimed = self._claim(scope, key, digest, retry_of)
                if claimed is not None:
                    return self._execute(claimed, run)
                record = self._load(scope, key)
                if record is None:
                    # 文件存在但不可读：崩溃留下的残损记录，按中断清理后重认
                    self._store.delete(KIND, self._record_id(scope, key))
                    continue
            outcome = self._serve_existing(scope, key, digest, record)
            if outcome is not None:
                return outcome
            # outcome 为 None：上一个认领因 4xx 被释放，回到循环重认
        return error_response(409, "request is being processed; retry with the same idempotency key")

    # ---------------------------------------------------------------- 认领与执行

    def _claim(self, scope: str, key: str, digest: str, retry_of: str = "") -> dict[str, Any] | None:
        now = self._clock()
        record: dict[str, Any] = {
            "key": key,
            "scope": scope,
            "request_hash": digest,
            "state": IN_FLIGHT,
            "claim_token": uuid.uuid4().hex,
            "status": 0,
            "content_type": "",
            "body": None,
            "error": "",
            "retry_of": retry_of,
            "created_at": now,
            "settled_at": None,
            "expires_at": None,
            "events": [self._event("claimed", now, f"retry_of={retry_of}" if retry_of else "")],
        }
        with self._io_lock:
            won = self._store.try_save(KIND, self._record_id(scope, key), record)
        if won:
            self._observe("idempotency_claimed", key)
            return record
        return None

    def _execute(self, record: dict[str, Any], run: Callable[[], Response]) -> Response:
        try:
            response = run()
        except Exception:
            self._signal(record)
            raise
        if 400 <= response.status < 500:
            # 未进入执行（参数或前置状态被拒绝）：回滚认领，不占用标识，
            # 客户端修正后可用同一标识重发。
            self._release(record)
            return response
        state = SUCCEEDED if response.status < 400 else FAILED
        try:
            self._settle(record, state, response)
        except Exception:
            try:
                self._settle(record, state, response)
            except Exception:
                # 落盘失败是部分失败：副作用可能已发生但结果没存下。
                # 保留 IN_FLIGHT 交给启动恢复按"结果未知"结算，绝不重执行。
                self._signal(record)
                return error_response(
                    500,
                    "idempotency result could not be persisted; outcome unknown, "
                    "reconcile via GET /api/idempotency before resubmitting with a new key",
                )
        self._signal(record)
        return replace(
            response, headers=(*response.headers, ("Idempotency-Key", record["key"]))
        )

    def _settle(self, record: dict[str, Any], state: str, response: Response) -> None:
        now = self._clock()
        record.update(
            state=state,
            status=response.status,
            content_type=response.content_type,
            body=self._decode_body(response),
            error="" if state == SUCCEEDED else self._body_text(response)[:200],
            settled_at=now,
            expires_at=now + self._ttl,
        )
        record["events"].append(
            self._event("settled_ok" if state == SUCCEEDED else "settled_fail", now, f"status={response.status}")
        )
        with self._io_lock:
            self._store.save(KIND, self._record_id(record["scope"], record["key"]), record)
        self._observe("idempotency_settled", record["key"])

    def _release(self, record: dict[str, Any]) -> None:
        with self._io_lock:
            self._store.delete(KIND, self._record_id(record["scope"], record["key"]))
        self._signal(record)
        self._observe("idempotency_released", record["key"])

    # ---------------------------------------------------------------- 重放与等待

    def _serve_existing(
        self, scope: str, key: str, digest: str, record: dict[str, Any]
    ) -> Response | None:
        if record.get("request_hash") != digest:
            return error_response(422, "idempotency key was already used with a different request")
        state = str(record.get("state", ""))
        if state == EXPIRED:
            return error_response(410, "idempotency key expired; resubmit with a new key")
        if state in (SUCCEEDED, FAILED) and self._expired(record):
            self._expire(record)
            return error_response(410, "idempotency key expired; resubmit with a new key")
        if state == IN_FLIGHT:
            record = self._await_settled(scope, key)
            if record is None:
                return None  # 认领被 4xx 释放，调用方重认
            if record.get("state") == IN_FLIGHT:
                return error_response(
                    409, "request is still in flight; retry with the same idempotency key"
                )
        self._note_replay(record)
        return self._replay_response(record)

    def _await_settled(self, scope: str, key: str) -> dict[str, Any] | None:
        deadline = time.monotonic() + self._wait_timeout
        while True:
            record = self._load(scope, key)
            if record is None or record.get("state") != IN_FLIGHT:
                return record
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return record
            self._signal_for(scope, key).wait(timeout=min(0.1, remaining))

    def _note_replay(self, record: dict[str, Any]) -> None:
        record["events"].append(self._event("replayed", self._clock(), ""))
        with self._io_lock:
            self._store.save(KIND, self._record_id(record["scope"], record["key"]), record)
        self._observe("idempotency_replayed", record["key"])

    def _replay_response(self, record: dict[str, Any]) -> Response:
        body = record.get("body")
        if isinstance(body, (dict, list)):
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        else:
            payload = str(body or "").encode("utf-8")
        return Response(
            status=int(record.get("status") or 200),
            body=payload,
            content_type=str(record.get("content_type") or "application/json; charset=utf-8"),
            headers=(REPLAY_HEADER, ("Idempotency-Key", str(record.get("key", "")))),
        )

    # ---------------------------------------------------------------- 过期与恢复

    def sweep_expired(self) -> int:
        """把已结算且过期的记录标记为失效（墓碑），返回处理数量。

        只标记、不删除；``IN_FLIGHT`` 记录不在谓词内，永远不会被清理。
        """

        count = 0
        for record_id in self._store.list(KIND):
            record = self._store.load(KIND, record_id)
            if record is None:
                continue
            if record.get("state") in (SUCCEEDED, FAILED) and self._expired(record):
                self._expire(record)
                count += 1
        return count

    def recover_interrupted(self) -> int:
        """启动恢复：残留的 IN_FLIGHT 一律按"结果未知"结算为 FAILED。

        崩溃发生在执行与结算之间时，副作用是否发生已不可知，因此绝不
        重新执行，只能结算留痕；人工确认后用新标识（``retry_of``）补发。
        """

        now = self._clock()
        count = 0
        for record_id in self._store.list(KIND):
            record = self._store.load(KIND, record_id)
            if record is None or record.get("state") != IN_FLIGHT:
                continue
            record.update(
                state=FAILED,
                status=500,
                content_type="application/json; charset=utf-8",
                body={"error": "interrupted before the result was persisted; outcome unknown"},
                error="interrupted",
                settled_at=now,
                expires_at=now + self._ttl,
            )
            record["events"].append(self._event("recovered", now, "in-flight at startup"))
            with self._io_lock:
                self._store.save(KIND, record_id, record)
            self._observe("idempotency_recovered", str(record.get("key", "")))
            count += 1
        return count

    # ---------------------------------------------------------------- 查询

    def status(self, scope: str, key: str) -> dict[str, Any] | None:
        """单条记录的完整视图（含审计事件），供人工补发前核查。"""

        record = self._load(scope, key)
        if record is None:
            return None
        return {
            "key": record.get("key"),
            "scope": record.get("scope"),
            "state": record.get("state"),
            "status": record.get("status"),
            "error": record.get("error"),
            "retry_of": record.get("retry_of"),
            "created_at": record.get("created_at"),
            "settled_at": record.get("settled_at"),
            "expires_at": record.get("expires_at"),
            "events": list(record.get("events", [])),
        }

    # ---------------------------------------------------------------- 内部工具

    @staticmethod
    def _record_id(scope: str, key: str) -> str:
        return hashlib.sha256(f"{scope}\n{key}".encode("utf-8")).hexdigest()

    def _load(self, scope: str, key: str) -> dict[str, Any] | None:
        return self._store.load(KIND, self._record_id(scope, key))

    def _expired(self, record: dict[str, Any]) -> bool:
        expires_at = record.get("expires_at")
        return expires_at is not None and float(expires_at) <= self._clock()

    def _expire(self, record: dict[str, Any]) -> None:
        record["state"] = EXPIRED
        record["events"].append(self._event("expired", self._clock(), ""))
        with self._io_lock:
            self._store.save(KIND, self._record_id(record["scope"], record["key"]), record)
        self._observe("idempotency_expired", str(record.get("key", "")))

    def _signal_for(self, scope: str, key: str) -> threading.Event:
        with self._signals_lock:
            return self._signals.setdefault((scope, key), threading.Event())

    def _signal(self, record: dict[str, Any]) -> None:
        ident = (str(record.get("scope", "")), str(record.get("key", "")))
        with self._signals_lock:
            event = self._signals.pop(ident, None)
        if event is not None:
            event.set()

    @staticmethod
    def _event(event: str, at: float, detail: str) -> dict[str, object]:
        return {"event": event, "at": at, "detail": detail}

    @staticmethod
    def _decode_body(response: Response) -> object:
        try:
            return json.loads(response.body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return response.body.decode("utf-8", errors="replace")

    @staticmethod
    def _body_text(response: Response) -> str:
        return response.body.decode("utf-8", errors="replace")

    def _observe(self, event: str, key: str) -> None:
        if self._observer is None:
            return
        try:
            self._observer(event, key)
        except Exception:  # 审计观察者不允许影响主流程
            pass
