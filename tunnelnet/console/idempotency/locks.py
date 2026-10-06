"""Cross-thread and cross-process locks keyed by request id."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Iterator

import fcntl

from tunnelnet.store import Store

from .model import key_file_id


class KeyLock:
    def __init__(self, store: Store) -> None:
        self._directory = os.path.join(store.root, ".idempotency-locks")
        os.makedirs(self._directory, exist_ok=True)

    @contextmanager
    def acquire(self, request_id: str, wait_seconds: float = 30.0) -> Iterator[None]:
        path = os.path.join(self._directory, f"{key_file_id(request_id)}.lock")
        handle = open(path, "a+b")
        try:
            if wait_seconds <= 0:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                deadline = time.monotonic() + wait_seconds
                while True:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError(request_id)
                        time.sleep(0.01)
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
