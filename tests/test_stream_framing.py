from __future__ import annotations

import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Event, Thread
from urllib.parse import parse_qs, urlparse

from swarm_sdk import SwarmClient, SwarmStreamError


class ChunkStream:
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    def __iter__(self):
        yield from self.chunks

    def close(self):
        self.closed = True


class StreamTransport:
    def __init__(self, chunks):
        self.body = ChunkStream(chunks)
        self.requests = []

    def stream(self, method, url, headers, timeout):
        self.requests.append((url, headers))
        return self.body


def fixture(chunks, **options):
    transport = StreamTransport(chunks)
    client = SwarmClient(base_url="https://swarm.example", transport=transport, **options)
    return client, transport


class StreamFramingTests(unittest.TestCase):
    def test_split_crlf_utf8_whitespace_and_inherited_reset_ids(self):
        payload = ("\ufeffid: a\r\nevent: message\r\ndata:  alpha\t \r\ndata:\r\n\r\n"
                   ": comment\r\n\r\ndata: \u03b2\r\rid:\ndata: reset\n\n"
                   "id: bad\0id\ndata: unchanged\n\nid: incomplete\ndata: discarded").encode()
        for split in range(len(payload) + 1):
            with self.subTest(split=split):
                client, transport = fixture([payload[:split], payload[split:]])
                seen = []
                cursor = client.stream_audit_events(lambda data, meta: seen.append((data, meta["id"])))
                self.assertEqual(seen, [(" alpha\t \n", "a"), ("\u03b2", "a"), ("reset", ""), ("unchanged", "")])
                self.assertEqual(cursor, "")
                self.assertTrue(transport.body.closed)

    def test_callback_failure_closes_response(self):
        client, transport = fixture([b"id: a\ndata: value\n\n"])
        failure = ValueError("apply failed")

        def apply(data, meta):
            raise failure

        with self.assertRaises(ValueError) as caught:
            client.stream_audit_events(apply)
        self.assertIs(caught.exception, failure)
        self.assertTrue(transport.body.closed)

    def test_oversized_tail_is_not_delivered(self):
        client, _ = fixture([b"id: dropped\ndata: " + b"x" * 400,
                             b'\ndata: {"tail":true}\n\nid: good\nevent: audit_event\ndata: {"ok":true}\n\n'],
                            sse_max_event_bytes=120, sse_max_buffer_bytes=80)
        seen = []
        client.stream_audit_events(lambda data, meta: seen.append(data))
        self.assertEqual(seen, [{"ok": True}])

    def test_strict_failures_and_reconnect_use_applied_cursor(self):
        for kind in ("audit_event", "runtime_hint"):
            for failure in ("invalid_json", "callback_failed", "truncated_frame", "invalid_utf8", "event_too_large", "buffer_limit_exceeded"):
                with self.subTest(kind=kind, failure=failure):
                    def call(client, callback, **options):
                        if kind == "audit_event":
                            return client.stream_audit_events(callback, **options)
                        return client.stream_runtime_hints("agent", callback, **options)

                    bad = f"id: b\nevent: {kind}\ndata: "
                    if failure == "invalid_json":
                        bad += "{bad}\n\n"
                    elif failure == "truncated_frame":
                        bad += '{"n":2}'
                    elif failure in ("event_too_large", "buffer_limit_exceeded"):
                        bad += "x" * 250 + "\n\n"
                    else:
                        bad += '{"n":2}\n\n'
                    chunks = [f'id: a\nevent: {kind}\ndata: {{"n":1}}\n\n'.encode(),
                              b"\xff" if failure == "invalid_utf8" else bad.encode()]
                    if failure != "truncated_frame":
                        chunks.append(f'id: c\nevent: {kind}\ndata: {{"n":3}}\n\n'.encode())
                    limits = {}
                    if failure == "event_too_large":
                        limits["sse_max_event_bytes"] = 80
                    if failure == "buffer_limit_exceeded":
                        limits["sse_max_buffer_bytes"] = 80
                    client, transport = fixture(chunks, **limits)
                    seen = []

                    def apply(data, meta):
                        if failure == "callback_failed" and data["n"] == 2:
                            raise ValueError("not committed")
                        seen.append(data["n"])

                    with self.assertRaises(SwarmStreamError) as caught:
                        call(client, apply, strict=True, last_event_id="prior")
                    self.assertEqual(caught.exception.code, failure)
                    self.assertEqual(caught.exception.last_event_id, "a")
                    self.assertEqual(seen, [1])
                    self.assertTrue(transport.body.closed)
                    retry, transport = fixture([f'id: b\nevent: {kind}\ndata: {{"n":2}}\n\n'.encode()])
                    cursor = call(retry, lambda data, meta: seen.append(data["n"]), strict=True, last_event_id=caught.exception.last_event_id)
                    self.assertEqual(transport.requests[0][1]["Last-Event-ID"], "a")
                    self.assertNotIn("strict", parse_qs(urlparse(transport.requests[0][0]).query))
                    self.assertEqual(seen, [1, 2])
                    self.assertEqual(cursor, "b")

    def test_id_only_frames_are_not_acknowledged_in_strict_mode(self):
        client, _ = fixture([b"id: future\n\n: heartbeat\n\n"])
        cursor = client.stream_audit_events(lambda *_: self.fail("no data"), strict=True, last_event_id="applied")
        self.assertEqual(cursor, "applied")
        client, _ = fixture([b"id:\ndata: reset\n\n"])
        cursor = client.stream_audit_events(lambda data, meta: self.assertEqual(meta["id"], ""), strict=True, last_event_id="applied")
        self.assertEqual(cursor, "")

    def test_empty_and_nonfinite_json_are_reported_not_acknowledged(self):
        for data in (b"", b'{"n":NaN}'):
            client, _ = fixture([b"id: bad\nevent: audit_event\ndata: " + data + b"\n\n"])
            errors = []
            cursor = client.stream_audit_events(lambda *_: self.fail("invalid JSON"), last_event_id="prior", on_stream_error=errors.append)
            self.assertEqual(cursor, "bad")
            self.assertEqual(errors[0]["code"], "invalid_json")
            self.assertEqual(errors[0]["last_event_id"], "prior")
            self.assertEqual(errors[0]["last_received_event_id"], "bad")

    def test_native_http_dispatches_before_eof_and_forwards_resume_cursor(self):
        applied = Event()
        headers = []
        timed_out = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                headers.append(self.headers.get("Last-Event-ID"))
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(b'id: a\r\nevent: audit_event\r\ndata: {"n":1}\r\n\r\n')
                self.wfile.flush()
                if not applied.wait(2):
                    timed_out.append(True)
                self.wfile.write(b'id: b\nevent: audit_event\ndata: {"n":2}\n\n')

        with HTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                client = SwarmClient(base_url=f"http://127.0.0.1:{server.server_port}", timeout=3)
                seen = []

                def apply(data, meta):
                    seen.append(data["n"])
                    applied.set()

                cursor = client.stream_audit_events(apply, strict=True, last_event_id="prior")
                self.assertEqual(seen, [1, 2])
                self.assertEqual(cursor, "b")
                self.assertEqual(headers, ["prior"])
                self.assertEqual(timed_out, [])
            finally:
                applied.set()
                server.shutdown()
                thread.join(timeout=3)
