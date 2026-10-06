"""WSGI application that serves the control console."""

from __future__ import annotations

from typing import Callable, Iterable

from tunnelnet.store import Store

from . import handlers
from .http import Request, RequestError, Response, Router, build_request, error_response
from .idempotency import key_of, scope_of
from .metrics import Metrics
from .pages import load_page, page_file, root_redirect
from .routes import register_routes
from .runtime import Runtime
from .seed import seed_defaults

WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

# 强制携带幂等标识的写路由：放行与排水类操作，重复执行的代价最高。
# 名单之外的写路由：带标识即受保护，不带则保持 legacy 行为。
ENFORCED_ROUTES = frozenset(
    {
        "POST /api/access/approve",
        "POST /api/access/grant",
        "POST /api/access/revoke",
        "POST /api/cabins/door",
        "POST /api/cabins/valve",
        "POST /api/drain/start",
        "POST /api/drain/execute",
    }
)


class Server:
    """Owns the runtime state and dispatches console requests."""

    def __init__(self, store: Store, web_dir: str = "web") -> None:
        self.runtime = Runtime(store)
        seed_defaults(self.runtime)
        self.runtime.idempotency.recover_interrupted()
        self.runtime.idempotency.sweep_expired()
        self.web_dir = web_dir
        self.router = Router()
        self.metrics = Metrics()
        register_routes(self.router, handlers)

    def respond(self, request: Request) -> Response:
        response = self._resolve(request)
        self.metrics.record(request.method, request.path, response.status)
        return response

    def _resolve(self, request: Request) -> Response:
        if request.method == "GET":
            filename = page_file(request.path)
            if filename is not None:
                return load_page(self.web_dir, filename)
            if request.path == "/":
                return root_redirect()
        return self._route(request)

    def _route(self, request: Request) -> Response:
        handler = self.router.match(request.method, request.path)
        if handler is None:
            if self.router.allows(request.path):
                return error_response(
                    405, f"method {request.method} not allowed for {request.path}"
                )
            return error_response(404, f"no route for {request.path}")
        if request.method in WRITE_METHODS:
            return self._guarded_write(request, handler)
        return self._invoke(handler, request)

    def _guarded_write(self, request: Request, handler: Callable[..., Response]) -> Response:
        if not key_of(request):
            if scope_of(request) in ENFORCED_ROUTES:
                return error_response(
                    400,
                    "idempotency key required for this route "
                    "(Idempotency-Key header or idempotency_key field)",
                )
            return self._invoke(handler, request)
        return self.runtime.idempotency.guarded(request, lambda: self._invoke(handler, request))

    def _invoke(self, handler: Callable[..., Response], request: Request) -> Response:
        try:
            return handler(self, request)
        except RequestError as exc:
            return error_response(exc.status, exc.message)
        except ValueError as exc:
            return error_response(400, str(exc))
        except Exception as exc:  # noqa: BLE001 - last resort for a live console
            return error_response(500, str(exc))

    def dispatch(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        query: dict[str, list[str]] | None = None,
    ) -> Response:
        """Call a route directly, used by the test suite and CLI probes."""

        request = Request(
            method=method.upper(),
            path=path,
            query=dict(query or {}),
            payload=dict(payload or {}),
        )
        return self.respond(request)

    def __call__(
        self, environ: dict[str, object], start_response: Callable[..., object]
    ) -> Iterable[bytes]:
        try:
            response = self.respond(build_request(environ))
        except RequestError as exc:
            response = error_response(exc.status, exc.message)
        status, headers = response.wsgi()
        start_response(status, headers)
        return [response.body]


def create_application(store: Store, web_dir: str = "web") -> Server:
    """Build a ready to serve console for the supplied store."""

    return Server(store, web_dir)
