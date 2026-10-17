from __future__ import annotations

import errno
import io
import socket
import ssl
import threading
import time
import traceback
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from swarm_sdk import (SwarmAPIError, SwarmCancelledError, SwarmClient, SwarmConnectionError,
                       SwarmResponseLimitError, SwarmStreamError, SwarmTimeoutError, SwarmTransportError,
                       SwarmTLSError,
                       run_connected_core_loop, run_connected_core_once)
from swarm_sdk.agent_runtime import _call
from swarm_sdk._client_retry import _retry_after, _retryable
from swarm_sdk.client import UrllibTransport
from swarm_sdk.connected_core_runtime import _LeaseRenewal
from sdk_test_support import FakeTransport, json_response


class CountedBody(io.BytesIO):
    def __init__(self, body=b"{}", *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or {"content-type": "application/json"}
        self.close_count = 0

    def close(self):
        self.close_count += 1
        super().close()


class StreamTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def stream(self, method, url, headers, timeout):
        self.calls.append((method, url, headers, timeout))
        return self.response


@contextmanager
def loopback(mode="stall", status=200):
    started, peer_closed = threading.Event(), threading.Event()
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            self.do_GET()

        def do_GET(self):
            calls.append((self.path, self.headers.get("Idempotency-Key")))
            if mode == "headers":
                started.set()
            else:
                self.send_response(status)
                self.send_header("Content-Type", "text/event-stream" if "stream" in self.path else "application/json")
                self.send_header("Retry-After", "0")
                self.end_headers()
                if mode == "error":
                    self.wfile.write(b'{"code":"synthetic_rejection"}')
                    return
                if mode in {"frame", "healthy"}:
                    self.wfile.write(b'id: a\nevent: audit_event\ndata: {"n":1}\n\n')
                    self.wfile.flush()
                started.set()
                if mode == "healthy":
                    time.sleep(0.12)
                    self.wfile.write(b'id: b\nevent: audit_event\ndata: {"n":2}\n\n')
                    return
                if mode == "drip":
                    try:
                        for _ in range(100):
                            self.wfile.write(b" ")
                            self.wfile.flush()
                            time.sleep(0.015)
                    except (BrokenPipeError, ConnectionResetError):
                        peer_closed.set()
                    return
            self.connection.settimeout(2)
            try:
                if self.connection.recv(1) == b"":
                    peer_closed.set()
            except (ConnectionResetError, socket.timeout):
                peer_closed.set()

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.daemon_threads = True
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", started, peer_closed, calls
        finally:
            server.shutdown()
            thread.join(2)


class TransportLifetimeTests(unittest.TestCase):
    def assert_safe(self, failure):
        self.assertEqual(failure.status, 0)
        self.assertIsNone(failure.body)
        self.assertEqual(failure.headers, {})
        self.assertIsNone(failure.__cause__)
        self.assertIsNone(failure.__context__)
        self.assertNotIn("private-secret", "".join(traceback.format_exception(failure)))

    def test_typed_transport_errors_do_not_expose_raw_diagnostics_or_retry_unknowns(self):
        vectors = [(TimeoutError("private-secret"), SwarmTimeoutError, True),
                   (URLError(TimeoutError("private-secret")), SwarmTimeoutError, True),
                   (URLError(ConnectionResetError("private-secret")), SwarmConnectionError, True),
                   (socket.gaierror("private-secret"), SwarmConnectionError, True),
                   (ValueError("private-secret network timeout"), SwarmTransportError, False),
                   (OSError(errno.ENOENT, "private-secret"), SwarmTransportError, False)]
        for original, kind, retryable in vectors:
            class FailingTransport:
                def request(self, method, url, headers, body, timeout):
                    raise original

            client = SwarmClient(base_url="https://local.invalid", transport=FailingTransport())
            with self.subTest(kind=kind), self.assertRaises(kind) as caught:
                client.get("/")
            self.assert_safe(caught.exception)
            self.assertEqual(caught.exception.retryable, retryable)
            self.assertEqual(_retryable(caught.exception), retryable)
        self.assertFalse(_retryable(RuntimeError("network timeout")))
        self.assertFalse(_retryable(SwarmCancelledError()))
        self.assertFalse(_retryable(SwarmTimeoutError("retry")))

    def test_invalid_deadlines_are_rejected(self):
        for name in ("timeout", "header_timeout", "operation_timeout", "stream_timeout", "stream_idle_timeout", "retry_timeout"):
            for value in (0, -1, True, float("inf"), float("nan"), "1"):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    SwarmClient(base_url="https://local.invalid", **{name: value})

    def test_default_urllib_certificate_and_ssl_configuration_errors_are_terminal_and_safe(self):
        errors = [ssl.SSLCertVerificationError(ssl.SSL_ERROR_SSL, "private-secret certificate hostname"),
                  ssl.SSLError(ssl.SSL_ERROR_SSL, "private-secret wrong protocol configuration")]
        for original in errors:
            client = SwarmClient(base_url="https://local.invalid", transport=UrllibTransport())
            # The real urllib handler wraps this connect failure as URLError;
            # no DNS, socket or external TLS connection is performed.
            with self.subTest(error=type(original)), patch(
                "swarm_sdk._client_transport._HTTPSConnection.connect", side_effect=original
            ) as connect:
                with self.assertRaises(SwarmTLSError) as caught:
                    run_connected_core_loop(client)
                connect.assert_called_once()
            self.assertFalse(caught.exception.retryable)
            self.assertFalse(_retryable(caught.exception))
            self.assertEqual(caught.exception.code, "tls_failed")
            self.assert_safe(caught.exception)

    def test_default_urllib_ssl_eof_remains_a_safe_retryable_connection_interruption(self):
        original = ssl.SSLEOFError(ssl.SSL_ERROR_EOF, "private-secret interrupted socket")
        client = SwarmClient(base_url="https://local.invalid", transport=UrllibTransport())
        with patch("swarm_sdk._client_transport._HTTPSConnection.connect", side_effect=original) as connect:
            with self.assertRaises(SwarmConnectionError) as caught:
                client.get("/")
            connect.assert_called_once()
        self.assertTrue(caught.exception.retryable)
        self.assert_safe(caught.exception)

    def test_precancelled_request_and_both_streams_do_not_open_transport(self):
        stop = threading.Event()
        stop.set()
        transport = FakeTransport()
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        for call in (lambda: client.get("/", stop_event=stop),
                     lambda: client.stream_audit_events(lambda *_: None, stop_event=stop),
                     lambda: client.stream_runtime_hints("agent", lambda *_: None, stop_event=stop)):
            with self.assertRaises(SwarmCancelledError):
                call()
        self.assertEqual(transport.requests, [])

    def test_five_argument_transport_deadline_is_checked_after_return_without_threads(self):
        class SlowTransport:
            def request(self, method, url, headers, body, timeout):
                time.sleep(0.04)
                return json_response({"private-secret": "not returned"})

        before = set(threading.enumerate())
        client = SwarmClient(base_url="https://local.invalid", transport=SlowTransport(), operation_timeout=0.01)
        with self.assertRaises(SwarmTimeoutError) as caught:
            client.get("/")
        self.assertEqual(caught.exception.kind, "operation")
        self.assert_safe(caught.exception)
        self.assertEqual(set(threading.enumerate()), before)

    def test_retry_after_cannot_outlive_one_intent_budget_or_start_another_attempt(self):
        transport = FakeTransport()
        transport.responses = [json_response({"retry_after_ms": 999999}, status=429)]
        client = SwarmClient(base_url="https://local.invalid", transport=transport, retry_timeout=0.04)
        began = time.monotonic()
        with self.assertRaises(SwarmTimeoutError) as caught:
            _call(lambda: client.post("/command", {"body": "same"}, idempotency_key="one-intent"), client=client)
        self.assertEqual(caught.exception.kind, "retry")
        self.assertLess(time.monotonic() - began, 0.5)
        self.assertEqual(len(transport.requests), 1)
        self.assertIsNone(client._operation.get())

    def test_retry_attempts_share_budget_stable_key_and_five_argument_transport(self):
        class SlowRetry(FakeTransport):
            def request(self, method, url, headers, body, timeout):
                response = super().request(method, url, headers, body, timeout)
                time.sleep(0.03)
                return response

        transport = SlowRetry()
        transport.responses = [json_response({}, status=503), json_response({}), json_response({})]
        client = SwarmClient(base_url="https://local.invalid", transport=transport, retry_timeout=0.05)
        with patch("swarm_sdk.agent_runtime._retry_delay", return_value=0), self.assertRaises(SwarmTimeoutError):
            _call(lambda: client.post("/command", {"body": "same"}, idempotency_key="one-intent"), client=client)
        self.assertEqual(len(transport.requests), 2)
        self.assertEqual(transport.requests[0]["body"], transport.requests[1]["body"])
        self.assertEqual(transport.requests[0]["headers"], transport.requests[1]["headers"])
        self.assertLess(transport.requests[1]["timeout"], transport.requests[0]["timeout"])

    def test_retry_wait_remains_cancelable_and_http_date_hints_are_bounded(self):
        stop = threading.Event()
        transport = FakeTransport()
        transport.responses = [json_response({}, status=503)]
        client = SwarmClient(base_url="https://local.invalid", transport=transport, retry_timeout=1)
        timer = threading.Timer(0.04, stop.set)
        timer.start()
        try:
            with patch("swarm_sdk.agent_runtime._retry_delay", return_value=30), self.assertRaises(SwarmCancelledError):
                _call(lambda: client.get("/"), client=client, stop_event=stop)
        finally:
            timer.join()
        self.assertEqual(len(transport.requests), 1)
        date = format_datetime(datetime.now(timezone.utc) + timedelta(days=1), usegmt=True)
        self.assertEqual(_retry_after(SwarmAPIError("busy", status=503, headers={"Retry-After": date})), 30)

    def test_nested_operation_scopes_retain_earlier_budget_and_restore_context(self):
        client = SwarmClient(base_url="https://local.invalid", transport=FakeTransport())
        with self.assertRaises(SwarmTimeoutError):
            with client.operation(timeout=0.01):
                with client.operation(timeout=10):
                    time.sleep(0.02)
                    client.get("/")
        self.assertIsNone(client._operation.get())
        self.assertEqual(client.transport.requests, [])

    def test_stalled_headers_and_bodies_and_dripping_body_obey_total_budget(self):
        for mode in ("headers", "stall", "drip"):
            with self.subTest(mode=mode), loopback(mode) as (url, _, closed, _):
                client = SwarmClient(base_url=url, timeout=1, operation_timeout=0.08)
                began = time.monotonic()
                with self.assertRaises(SwarmTimeoutError) as caught:
                    client.get("/")
                self.assertEqual(caught.exception.kind, "operation")
                self.assertLess(time.monotonic() - began, 0.6)
                self.assertTrue(closed.wait(0.5))
        self.assertFalse(any(t.name == "swarm-transport-lifetime" for t in threading.enumerate()))

    def test_cancel_during_headers_and_body_interrupts_native_socket(self):
        for mode in ("headers", "stall"):
            with self.subTest(mode=mode), loopback(mode) as (url, started, closed, _):
                stop = threading.Event()
                timer = threading.Thread(target=lambda: (started.wait(1), stop.set()))
                timer.start()
                try:
                    client = SwarmClient(base_url=url, timeout=2)
                    with self.assertRaises(SwarmCancelledError):
                        client.get("/", stop_event=stop)
                finally:
                    timer.join(2)
                self.assertTrue(closed.wait(0.5))

    def test_header_deadline_ends_at_headers_not_at_healthy_stream_completion(self):
        with loopback("headers") as (url, _, closed, _):
            client = SwarmClient(base_url=url, timeout=1, header_timeout=0.05)
            with self.assertRaises(SwarmTimeoutError) as caught:
                client.stream_audit_events(lambda *_: self.fail("no headers"), last_event_id="prior")
            self.assertEqual(caught.exception.kind, "headers")
            self.assertEqual(caught.exception.last_event_id, "prior")
            self.assertTrue(closed.wait(0.5))
        with loopback("healthy") as (url, _, _, _):
            client = SwarmClient(base_url=url, timeout=0.05, header_timeout=0.05)
            self.assertEqual(client.stream_audit_events(lambda *_: None, strict=True), "b")

    def test_default_transport_truncation_is_a_typed_connection_failure(self):
        class TruncatedOpener:
            def open(self, request, timeout=None):
                return body

        body = CountedBody(b"{}", headers={"content-length": "10"})
        client = SwarmClient(base_url="https://local.invalid", transport=UrllibTransport(TruncatedOpener()))
        with self.assertRaises(SwarmConnectionError) as caught:
            client.get("/")
        self.assert_safe(caught.exception)
        self.assertEqual(body.close_count, 1)

    def test_stream_read_connection_failure_keeps_cursor_and_closes_once(self):
        class InterruptedBody(CountedBody):
            def read1(self, size):
                if self.tell():
                    raise ConnectionResetError("private-secret")
                return super().read1(size)

        body = InterruptedBody(b"id: a\ndata: applied\n\n")
        client = SwarmClient(base_url="https://local.invalid", transport=StreamTransport(body))
        with self.assertRaises(SwarmConnectionError) as caught:
            client.stream_audit_events(lambda *_: None, strict=True)
        self.assertEqual(caught.exception.last_event_id, "a")
        self.assertEqual(body.close_count, 1)
        self.assert_safe(caught.exception)

    def test_stream_error_callback_failure_does_not_disclose_callback_exception(self):
        body = CountedBody(b"id: b\nevent: audit_event\ndata: bad\n\n")
        client = SwarmClient(base_url="https://local.invalid", transport=StreamTransport(body))

        def error_callback(_):
            raise ValueError("private-secret")

        with self.assertRaises(SwarmStreamError) as caught:
            client.stream_audit_events(lambda *_: None, strict=True, on_stream_error=error_callback, last_event_id="a")
        self.assertEqual(caught.exception.code, "error_callback_failed")
        self.assertEqual(caught.exception.last_event_id, "a")
        self.assertEqual(body.close_count, 1)
        self.assertNotIn("private-secret", "".join(traceback.format_exception(caught.exception)))

    def test_native_stream_idle_and_total_are_separate_from_header_timeout(self):
        for name, value, kind in (("stream_idle_timeout", 0.06, "stream_idle"), ("stream_timeout", 0.06, "stream")):
            with self.subTest(name=name), loopback() as (url, _, closed, _):
                client = SwarmClient(base_url=url, timeout=1, **{name: value})
                with self.assertRaises(SwarmTimeoutError) as caught:
                    client.stream_audit_events(lambda *_: self.fail("no frame"), strict=True, last_event_id="prior")
                self.assertEqual(caught.exception.kind, kind)
                self.assertEqual(caught.exception.last_event_id, "prior")
                self.assertTrue(closed.wait(0.5))
        with loopback("healthy") as (url, _, _, _):
            client = SwarmClient(base_url=url, timeout=0.05)
            seen = []
            self.assertEqual(client.stream_audit_events(lambda data, _: seen.append(data["n"]), strict=True), "b")
            self.assertEqual(seen, [1, 2])

    def test_native_stream_cancellation_while_awaiting_next_frame_keeps_applied_cursor(self):
        with loopback("frame") as (url, _, closed, _):
            stop = threading.Event()
            applied = threading.Event()
            timer = threading.Thread(target=lambda: (applied.wait(1), time.sleep(0.03), stop.set()))
            timer.start()
            try:
                client = SwarmClient(base_url=url, timeout=1)
                with self.assertRaises(SwarmCancelledError) as caught:
                    client.stream_audit_events(lambda *_: applied.set(), strict=True, stop_event=stop)
                self.assertEqual(caught.exception.last_event_id, "a")
                self.assertEqual(caught.exception.last_received_event_id, "a")
                self.assertTrue(closed.wait(0.5))
            finally:
                timer.join(2)

    def test_callback_cancellation_and_timeout_never_advance_applied_cursor_and_close_once(self):
        for callback_kind in ("cancel", "timeout", "failure"):
            for stream_kind in ("audit", "hint"):
                with self.subTest(callback=callback_kind, stream=stream_kind):
                    event = "audit_event" if stream_kind == "audit" else "runtime_hint"
                    body = CountedBody(f'id: a\nevent: {event}\ndata: {{}}\n\nid: b\nevent: {event}\ndata: {{}}\n\n'.encode())
                    stop = threading.Event()
                    client = SwarmClient(base_url="https://local.invalid", transport=StreamTransport(body))

                    def callback(_, meta):
                        if meta["id"] != "b":
                            return
                        if callback_kind == "cancel":
                            stop.set()
                        elif callback_kind == "timeout":
                            time.sleep(0.04)
                        else:
                            raise ValueError("private-secret")

                    kind = {"cancel": SwarmCancelledError, "timeout": SwarmTimeoutError, "failure": SwarmStreamError}[callback_kind]
                    with self.assertRaises(kind) as caught:
                        options = dict(strict=True, stop_event=stop, stream_timeout=0.02 if callback_kind == "timeout" else 1)
                        if stream_kind == "audit":
                            client.stream_audit_events(callback, **options)
                        else:
                            client.stream_runtime_hints("agent", callback, **options)
                    self.assertEqual(caught.exception.last_event_id, "a")
                    self.assertEqual(caught.exception.last_received_event_id, "b")
                    self.assertEqual(body.close_count, 1)
                    self.assertNotIn("private-secret", "".join(traceback.format_exception(caught.exception)))

    def test_slow_custom_stream_checks_deadline_after_return_and_closes_once(self):
        class SlowBody(CountedBody):
            def read1(self, size):
                time.sleep(0.04)
                return super().read1(size)

        body = SlowBody(b"data: not-delivered\n\n")
        client = SwarmClient(base_url="https://local.invalid", transport=StreamTransport(body))
        with self.assertRaises(SwarmTimeoutError):
            client.stream_audit_events(lambda *_: self.fail("expired frame"), stream_timeout=0.01)
        self.assertEqual(body.close_count, 1)

    def test_error_callback_cancellation_has_applied_cursor_and_cleanup(self):
        body = CountedBody(b"id: b\nevent: audit_event\ndata: {bad}\n\n")
        stop = threading.Event()
        client = SwarmClient(base_url="https://local.invalid", transport=StreamTransport(body))
        with self.assertRaises(SwarmCancelledError) as caught:
            client.stream_audit_events(lambda *_: self.fail("bad frame"), strict=True, last_event_id="a",
                                       stop_event=stop, on_stream_error=lambda _: stop.set())
        self.assertEqual(caught.exception.last_event_id, "a")
        self.assertEqual(body.close_count, 1)

    def test_real_urllib_stream_errors_preserve_terminal_status_and_retry_headers(self):
        for status in (401, 403, 408, 429, 503, 302):
            with self.subTest(status=status), loopback("error", status) as (url, _, _, _):
                client = SwarmClient(base_url=url)
                with self.assertRaises(SwarmAPIError) as caught:
                    client.stream_audit_events(lambda *_: self.fail("rejection"))
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(caught.exception.headers["retry-after"], "0")
                self.assertEqual(_retryable(caught.exception), status in {408, 429, 503})

    def test_malformed_http_errors_keep_status_and_close_once(self):
        for status in (401, 503):
            body = CountedBody(b'{"private-secret":', status=status)

            class Opener:
                def open(self, request, timeout=None):
                    raise HTTPError(request.full_url, status, "private-secret", body.headers, body)

            client = SwarmClient(base_url="https://local.invalid", transport=UrllibTransport(Opener()))
            with self.assertRaises(SwarmAPIError) as caught:
                client.get("/")
            self.assertEqual(caught.exception.status, status)
            self.assertEqual(body.close_count, 1)
            self.assertNotIn("private-secret", "".join(traceback.format_exception(caught.exception)))

    def test_connected_core_poll_recovers_with_same_key_but_callbacks_are_never_retried(self):
        class RecoveryTransport(FakeTransport):
            def request(self, method, url, headers, body, timeout):
                response = super().request(method, url, headers, body, timeout)
                if response.status == 599:
                    raise ConnectionResetError("private-secret")
                return response

        transport = RecoveryTransport()
        transport.responses = [json_response({}, status=599), json_response({}), json_response(None)]
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        with patch("swarm_sdk.connected_core_runtime._poll_retry_delay", return_value=0):
            self.assertIsNone(run_connected_core_once(client))
        self.assertEqual(transport.requests[0]["headers"], transport.requests[1]["headers"])
        transport.responses = [json_response({}), json_response(None)]
        callback = unittest.mock.Mock(side_effect=SwarmTimeoutError())
        with self.assertRaises(SwarmTimeoutError):
            run_connected_core_loop(client, on_idle=callback)
        callback.assert_called_once()

    def test_real_urllib_connected_core_poll_recovery_keeps_each_intent_key(self):
        replies = [503, 200, 408, 429, 503, 204]
        calls = []
        stop = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                calls.append((self.path, self.headers.get("Idempotency-Key")))
                status = replies.pop(0)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "0")
                self.end_headers()
                if status != 204:
                    self.wfile.write(b"{}")

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
            thread.start()
            try:
                client = SwarmClient(base_url=f"http://127.0.0.1:{server.server_port}", retry_timeout=1)
                with patch("swarm_sdk.connected_core_runtime._poll_retry_delay", return_value=0):
                    run_connected_core_loop(client, stop_event=stop, on_idle=stop.set)
            finally:
                server.shutdown()
                thread.join(2)
        self.assertEqual(len(calls), 6)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(len(set(calls[2:])), 1)
        self.assertNotEqual(calls[0][1], calls[2][1])
        self.assertEqual(replies, [])

    def test_unknown_transport_failure_does_not_retry_or_restart_connected_core_poll(self):
        class BrokenTransport:
            def __init__(self):
                self.calls = 0

            def request(self, method, url, headers, body, timeout):
                self.calls += 1
                raise RuntimeError("private-secret network timeout")

        transport = BrokenTransport()
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        with self.assertRaises(SwarmTransportError) as caught:
            run_connected_core_loop(client)
        self.assert_safe(caught.exception)
        self.assertEqual(transport.calls, 1)

    def test_connected_core_renewal_cleanup_interrupts_inflight_native_io_and_joins(self):
        with loopback("stall") as (url, started, closed, _):
            client = SwarmClient(base_url=url, timeout=2)
            renewal = _LeaseRenewal(client, "run", "lease", 1, None)
            renewal.start()
            try:
                self.assertTrue(started.wait(2))
                began = time.monotonic()
                renewal.close()
                self.assertLess(time.monotonic() - began, 0.5)
                self.assertTrue(closed.wait(0.5))
                self.assertIsNone(renewal.error)
                self.assertFalse(renewal._thread.is_alive())
            finally:
                renewal.close()


if __name__ == "__main__":
    unittest.main()
