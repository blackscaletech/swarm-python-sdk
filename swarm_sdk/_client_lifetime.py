from __future__ import annotations

import errno
import math
import socket
import ssl
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from http.client import HTTPException
from urllib.error import URLError

from ._client_types import (SwarmCancelledError, SwarmConnectionError,
                            SwarmTimeoutError, SwarmTransportError, SwarmTLSError)


@contextmanager
def _owned_response(response):
    try:
        yield response
    finally:
        try:
            response.close()
        except Exception:
            # Cleanup cannot replace the primary failure or disclose close diagnostics.
            pass


def _seconds(value: float | None, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite positive number or None")
    return float(value)


class _Lifetime:
    def __init__(self, timeout: float | None = None, *, kind: str = "operation",
                 stop_event: threading.Event | None = None, parent: _Lifetime | None = None) -> None:
        self.deadline = None if timeout is None else time.monotonic() + timeout
        self.kind = kind
        self.stop_event = stop_event
        self.parent = parent

    def check(self) -> None:
        # Cancellation wins over an expired budget; neither is retryable.
        current: _Lifetime | None = self
        while current is not None:
            if current.stop_event is not None and current.stop_event.is_set():
                raise SwarmCancelledError()
            current = current.parent
        deadlines = []
        current = self
        while current is not None:
            if current.deadline is not None:
                deadlines.append((current.deadline, current.kind))
            current = current.parent
        if deadlines:
            deadline, kind = min(deadlines)
            if time.monotonic() >= deadline:
                raise SwarmTimeoutError(kind)

    def socket_timeout(self, timeout: float | None) -> float | None:
        self.check()
        current: _Lifetime | None = self
        while current is not None:
            if current.deadline is not None:
                remaining = max(0.001, current.deadline - time.monotonic())
                timeout = remaining if timeout is None else min(timeout, remaining)
            current = current.parent
        return timeout

    def wait(self, delay: float) -> None:
        until = time.monotonic() + max(0.0, delay)
        while True:
            self.check()
            remaining = until - time.monotonic()
            if remaining <= 0:
                return
            # Poll all parent events as well as the local one.
            time.sleep(min(0.02, remaining, self.socket_timeout(remaining)))


_NATIVE_GUARD: ContextVar[_SocketGuard | None] = ContextVar("swarm_native_guard", default=None)


class _SocketGuard:
    """Shutdown owned sockets, never close a response concurrently with read()."""

    def __init__(self, lifetime: _Lifetime) -> None:
        self.lifetime = lifetime
        self.socket: socket.socket | None = None
        self.failure: SwarmTransportError | None = None
        self._done = threading.Event()
        self._thread: threading.Thread | None = None
        self._token = None

    def __enter__(self):
        self.lifetime.check()
        self._token = _NATIVE_GUARD.set(self)
        current: _Lifetime | None = self.lifetime
        while current is not None:
            if current.deadline is not None or current.stop_event is not None:
                self._thread = threading.Thread(target=self._watch, name="swarm-transport-lifetime", daemon=True)
                self._thread.start()
                break
            current = current.parent
        return self

    def bind(self, sock: socket.socket) -> None:
        self.socket = sock
        try:
            self.check()
        except SwarmTransportError:
            self._interrupt()
            raise

    def check(self) -> None:
        if self.failure is not None:
            raise self.failure
        self.lifetime.check()

    def _interrupt(self) -> None:
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _watch(self) -> None:
        while not self._done.wait(0.01):
            try:
                self.lifetime.check()
            except SwarmTransportError as failure:
                self.failure = failure
                self._interrupt()
                return

    def __exit__(self, *_):
        self._done.set()
        if self._thread is not None:
            self._thread.join()
        _NATIVE_GUARD.reset(self._token)
        self.socket = None


def _transport_failure(exc: Exception, lifetime: _Lifetime | None = None,
                       *, timeout_kind: str = "socket") -> SwarmTransportError:
    if lifetime is not None:
        try:
            lifetime.check()
        except SwarmTransportError as failure:
            failure.__context__ = None
            return failure
    if isinstance(exc, SwarmTransportError):
        return exc
    reason = exc.reason if isinstance(exc, URLError) else exc
    if isinstance(reason, TimeoutError):
        return SwarmTimeoutError(timeout_kind)
    if isinstance(reason, ssl.SSLError):
        if isinstance(reason, (ssl.SSLEOFError, ssl.SSLZeroReturnError)):
            return SwarmConnectionError()
        return SwarmTLSError()
    network_errnos = {errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
                      errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EPIPE, errno.ETIMEDOUT}
    if isinstance(reason, (ConnectionError, socket.gaierror, HTTPException)) or (
        isinstance(reason, OSError) and reason.errno in network_errnos
    ):
        return SwarmConnectionError()
    return SwarmTransportError()
