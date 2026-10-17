from __future__ import annotations

import hashlib
import io
import json
import tempfile
import threading
import time
import traceback
import unittest
from contextlib import contextmanager
from email.message import Message
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from swarm_sdk import (SwarmAPIError, SwarmArtifactIntegrityError, SwarmCancelledError,
                       SwarmClient, SwarmConnectionError, SwarmResponseLimitError,
                       SwarmTimeoutError, SwarmTransportError, VerifiedArtifactDownload)
from swarm_sdk.client import UrllibTransport


def metadata(body, artifact_id="artifact"):
    digest = hashlib.sha256(body).hexdigest()
    return {"Content-Type": "application/octet-stream", "X-Swarm-Artifact-Id": artifact_id,
            "X-Swarm-Content-Id": "sha256:" + digest, "X-Swarm-Content-Sha256": digest,
            "X-Swarm-Content-Length": str(len(body)), "Content-Length": str(len(body)),
            "ETag": '"sha256:' + digest + '"', "Cache-Control": "no-store, no-transform",
            "X-Swarm-Content-Media-Type": "application/octet-stream"}


class RawResponse(io.BytesIO):
    def __init__(self, body=b"body", *, headers=None, status=200, on_read=None):
        super().__init__(body)
        self.headers = metadata(body) if headers is None else headers
        self.status = status
        self.on_read = on_read
        self.read_sizes = []
        self.close_count = 0

    def read1(self, size):
        self.read_sizes.append(size)
        if self.on_read:
            self.on_read(self)
        return super().read(size)

    def close(self):
        self.close_count += 1
        super().close()


class RawTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def stream(self, method, url, headers, timeout):
        self.calls.append(dict(method=method, url=url, headers=headers, timeout=timeout))
        return self.response


def fixture(response=None, **options):
    transport = RawTransport(response or RawResponse())
    return SwarmClient(base_url="https://local.invalid/prefix/", token="synthetic-secret-token",
                       transport=transport, **options), transport


@contextmanager
def raw_loopback(mode="healthy", status=200):
    calls, started, closed = [], threading.Event(), threading.Event()
    body = b"\x00\xffraw-binary\x80"

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            calls.append((self.path, dict(self.headers)))
            started.set()
            try:
                if mode == "headers":
                    time.sleep(0.25)
                self.send_response(status)
                if status != 200:
                    rejection = b'{"error":"gone","connection_token":"synthetic-secret-token"}'
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(rejection)))
                    self.send_header("Location", "/redirect?secret=synthetic-secret-token")
                    self.end_headers()
                    self.wfile.write(rejection)
                    return
                for key, value in metadata(body).items():
                    self.send_header(key, value)
                self.end_headers()
                if mode == "stall":
                    time.sleep(0.25)
                if mode == "drip":
                    for value in body:
                        self.wfile.write(bytes([value]))
                        self.wfile.flush()
                        time.sleep(0.015)
                else:
                    self.wfile.write(body)
            except (ConnectionError, OSError):
                pass
            finally:
                closed.set()

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/prefix/", calls, started, closed, body
        finally:
            server.shutdown()
            thread.join(2)


class ArtifactDownloadTests(unittest.TestCase):
    def assert_safe(self, failure):
        rendered = "".join(traceback.format_exception(failure))
        for text in ("synthetic-secret-token", "private-chunk", "storage.invalid"):
            self.assertNotIn(text, rendered)
        self.assertIsNone(failure.__context__)

    def track_spools(self):
        spools = []
        original = tempfile.SpooledTemporaryFile

        def create(*args, **kwargs):
            spool = original(*args, **kwargs)
            spools.append(spool)
            return spool

        return spools, patch("swarm_sdk._artifact_download.tempfile.SpooledTemporaryFile", side_effect=create)

    def test_export_empty_binary_and_multimegabyte_verified_seekable_files(self):
        import swarm_sdk
        self.assertIn("VerifiedArtifactDownload", swarm_sdk.__all__)
        for body in (b"", b"\x00\xff\x80\r\n", b"\x00\xff" * (1024 * 1024 + 9)):
            response = RawResponse(body)
            client, transport = fixture(response)
            with client.download_artifact("artifact", expected_sha256=hashlib.sha256(body).hexdigest().upper()) as result:
                self.assertIsInstance(result, VerifiedArtifactDownload)
                self.assertEqual(result.size_bytes, len(body))
                self.assertEqual(result.sha256, hashlib.sha256(body).hexdigest())
                self.assertEqual(result.content_id, "sha256:" + result.sha256)
                self.assertEqual(result.etag, '"' + result.content_id + '"')
                self.assertEqual(result.file.tell(), 0)
                self.assertEqual(result.read(), body)
                result.file.seek(0)
                self.assertEqual(result.file.read(), body)
                self.assertEqual(result.file._rolled, len(body) > 1024 * 1024)
                self.assertTrue(all(0 < size <= 65536 for size in response.read_sizes))
                self.assertEqual(len(transport.calls), 1)
            self.assertTrue(result.closed)
            result.close()
            self.assertEqual(response.close_count, 1)

    def test_result_is_not_constructed_until_all_chunks_verified(self):
        response = RawResponse(b"private-chunk" * 10000)
        client, _ = fixture(response)
        with patch("swarm_sdk._artifact_download.VerifiedArtifactDownload", wraps=VerifiedArtifactDownload) as constructor:
            response.on_read = lambda _: constructor.assert_not_called()
            result = client.download_artifact("artifact")
            constructor.assert_called_once()
            result.close()

    def test_spooled_file_cleanup_on_context_exception_and_explicit_close(self):
        client, _ = fixture(RawResponse(b"x" * (1024 * 1024 + 1)))
        with self.assertRaisesRegex(RuntimeError, "caller"):
            with client.download_artifact("artifact") as result:
                raise RuntimeError("caller")
        self.assertTrue(result.closed)
        with self.assertRaises(ValueError):
            result.read()

    def test_authenticated_prefix_escaped_id_identity_encoding_and_no_conditions(self):
        identifier = "artifact/other?query=#fragment"
        response = RawResponse(headers=metadata(b"body", identifier))
        client, transport = fixture(response, headers={"accept": "application/json", "ACCEPT-ENCODING": "gzip",
            "range": "bytes=0-1", "If-None-Match": "etag", "If-Match": "etag", "If-Range": "etag",
            "If-Modified-Since": "date", "If-Unmodified-Since": "date", "Idempotency-Key": "unused",
            "authorization": "wrong", "X-Caller": "retained"})
        client.download_artifact(identifier).close()
        call = transport.calls[0]
        self.assertEqual(call["url"], "https://local.invalid/prefix/v1/artifacts/artifact%2Fother%3Fquery%3D%23fragment/content")
        self.assertEqual(call["method"], "GET")
        self.assertEqual(call["headers"], {"Accept": "application/octet-stream", "Accept-Encoding": "identity",
                                          "Authorization": "Bearer synthetic-secret-token", "X-Caller": "retained"})
        self.assertNotIn("synthetic-secret-token", call["url"])

    def test_invalid_inputs_fail_before_transport_open(self):
        client, transport = fixture()
        for identifier in (None, 1, "", ".", "..", "\n", "\ud800"):
            with self.subTest(identifier=repr(identifier)), self.assertRaises(ValueError):
                client.download_artifact(identifier)
        for digest in ("", 1, "g" * 64, "0" * 63):
            with self.assertRaises(ValueError):
                client.download_artifact("artifact", expected_sha256=digest)
        for key in ("max_artifact_bytes", "max_response_bytes", "max_error_response_bytes"):
            for value in (0, -1, True, 1.5, 2**53):
                with self.assertRaises(ValueError):
                    client.download_artifact("artifact", **{key: value})
        for key in ("operation_timeout", "header_timeout", "idle_timeout"):
            for value in (0, -1, True, float("nan"), float("inf"), "1"):
                with self.assertRaises(ValueError):
                    client.download_artifact("artifact", **{key: value})
        with self.assertRaises(ValueError):
            client.download_artifact("artifact", stop_event=True)
        self.assertEqual(transport.calls, [])

    def test_mandatory_header_identity_digest_size_etag_cache_and_status_rejection(self):
        original = metadata(b"private-chunk")
        invalid = [{key: value for key, value in original.items() if key != missing}
                   for missing in ("Content-Type", "X-Swarm-Artifact-Id", "X-Swarm-Content-Id",
                                   "X-Swarm-Content-Sha256", "X-Swarm-Content-Length", "ETag", "Cache-Control")]
        for key, value in (("Content-Type", "application/json"), ("X-Swarm-Artifact-Id", "other"),
                ("X-Swarm-Content-Id", "sha256:" + "0" * 64), ("X-Swarm-Content-Sha256", "G" * 64),
                ("X-Swarm-Content-Sha256", original["X-Swarm-Content-Sha256"].upper()),
                ("X-Swarm-Content-Length", "-1"), ("X-Swarm-Content-Length", "+1"),
                ("X-Swarm-Content-Length", "1.0"), ("X-Swarm-Content-Length", "1" * 500),
                ("X-Swarm-Content-Length", "\u0661"), ("Content-Length", "1"),
                ("ETag", 'W/' + original["ETag"]), ("ETag", original["ETag"].strip('"')),
                ("Cache-Control", "no-store"), ("Cache-Control", "no-transform"),
                ("Content-Encoding", "gzip"), ("Content-Encoding", "br"),
                ("Transfer-Encoding", "chunked"), ("X-Swarm-Content-Media-Type", "text/plain\nsecret")):
            invalid.append({**original, key: value})
        for headers in invalid:
            response = RawResponse(b"private-chunk", headers=headers)
            client, _ = fixture(response)
            spools, tracking = self.track_spools()
            with tracking, self.assertRaises(SwarmArtifactIntegrityError) as caught:
                client.download_artifact("artifact")
            self.assert_safe(caught.exception)
            self.assertEqual(response.close_count, 1)
            self.assertEqual(spools, [])
            self.assertEqual(response.read_sizes, [])
        for status in (201, 204, 206):
            response = RawResponse(status=status)
            client, _ = fixture(response)
            with self.assertRaises(SwarmArtifactIntegrityError):
                client.download_artifact("artifact")
            self.assertEqual(response.close_count, 1)

    def test_case_insensitive_headers_no_content_length_and_binary_media_metadata(self):
        headers = {key.lower(): value for key, value in metadata(b"\x1f\x8bgzip-payload").items() if key != "Content-Length"}
        headers["x-swarm-content-media-type"] = "application/gzip"
        headers["content-encoding"] = "identity"
        headers["transfer-encoding"] = "chunked"
        response = RawResponse(b"\x1f\x8bgzip-payload", headers=headers)
        client, _ = fixture(response)
        with client.download_artifact("artifact") as result:
            self.assertEqual(result.media_type, "application/gzip")
            self.assertEqual(result.read(), b"\x1f\x8bgzip-payload")
        headers.pop("x-swarm-content-media-type")
        client, _ = fixture(RawResponse(b"\x1f\x8bgzip-payload", headers=headers))
        with client.download_artifact("artifact") as result:
            self.assertIsNone(result.media_type)

    def test_duplicate_and_missing_header_structures_are_refused(self):
        headers = Message()
        for key, value in metadata(b"body").items():
            headers[key] = value
        headers["content-length"] = "4"
        for value in (headers, []):
            response = RawResponse(headers=value)
            client, _ = fixture(response)
            with self.assertRaises(SwarmArtifactIntegrityError):
                client.download_artifact("artifact")
            self.assertEqual(response.close_count, 1)

    def test_expected_digest_mismatch_refused_before_read_or_spool(self):
        response = RawResponse(b"private-chunk")
        client, _ = fixture(response)
        spools, tracking = self.track_spools()
        with tracking, self.assertRaises(SwarmArtifactIntegrityError) as caught:
            client.download_artifact("artifact", expected_sha256="0" * 64)
        self.assert_safe(caught.exception)
        self.assertEqual(spools, [])
        self.assertEqual(response.read_sizes, [])

    def test_minimum_of_caller_server_and_route_wire_limits(self):
        for options, call_options, limit in (({}, {"max_artifact_bytes": 3}, 3),
                ({"max_response_bytes": 3}, {"max_response_bytes": 100}, 3),
                ({"response_byte_limits": [{"method": "GET", "path": "/v1/artifacts/{id}/content", "max_response_bytes": 2}]},
                 {"max_artifact_bytes": 100, "max_response_bytes": 100}, 2),
                ({}, {"max_response_bytes": 1}, 1)):
            response = RawResponse()
            client, _ = fixture(response, **options)
            with self.assertRaises(SwarmResponseLimitError) as caught:
                client.download_artifact("artifact", **call_options)
            self.assertEqual(caught.exception.max_bytes, limit)
            self.assertEqual(response.read_sizes, [])
            self.assertEqual(response.close_count, 1)
        headers = metadata(b"body")
        for key in ("Content-Length", "X-Swarm-Content-Length"):
            headers[key] = str(64 * 1024 * 1024 + 1)
        client, _ = fixture(RawResponse(headers=headers))
        with self.assertRaises(SwarmResponseLimitError) as caught:
            client.download_artifact("artifact", max_artifact_bytes=100 * 1024 * 1024, max_response_bytes=100 * 1024 * 1024)
        self.assertEqual(caught.exception.max_bytes, 64 * 1024 * 1024)

    def test_corrupt_truncated_overrun_and_read_errors_clean_spool_and_response(self):
        class Interrupted(RawResponse):
            def read1(self, size):
                if self.tell():
                    raise IncompleteRead(b"private-chunk")
                return super().read1(1)

        responses = [RawResponse(b"changed", headers=metadata(b"private-chunk")),
                     RawResponse(b"abcd", headers=metadata(b"body")),
                     RawResponse(b"", headers=metadata(b"body")),
                     RawResponse(b"extra", headers=metadata(b"body")), Interrupted()]
        for response in responses:
            client, _ = fixture(response)
            spools, tracking = self.track_spools()
            with tracking, self.assertRaises((SwarmArtifactIntegrityError, SwarmConnectionError)) as caught:
                client.download_artifact("artifact")
            self.assert_safe(caught.exception)
            self.assertEqual(response.close_count, 1)
            self.assertEqual(len(spools), 1)
            self.assertTrue(spools[0].closed)

    def test_actual_wire_overrun_is_bounded_even_when_metadata_lies(self):
        response = RawResponse(b"longer", headers=metadata(b"abc"))
        client, _ = fixture(response)
        spools, tracking = self.track_spools()
        with tracking, self.assertRaises(SwarmResponseLimitError) as caught:
            client.download_artifact("artifact", max_artifact_bytes=3)
        self.assertEqual(caught.exception.max_bytes, 3)
        self.assertTrue(spools[0].closed)

    def test_json_rejections_including_gone_are_bounded_typed_and_not_retried(self):
        for status in (302, 401, 403, 410, 429, 503):
            response = RawResponse(b'{"error":"gone","connection_token":"synthetic-secret-token"}', status=status,
                                   headers={"Content-Type": "application/json", "Retry-After": "1"})
            client, transport = fixture(response)
            with self.assertRaises(SwarmAPIError) as caught:
                client.download_artifact("artifact")
            self.assertEqual(caught.exception.status, status)
            self.assert_safe(caught.exception)
            self.assertNotIn("synthetic-secret-token", json.dumps(caught.exception.body))
            self.assertEqual(len(transport.calls), 1)
            self.assertEqual(response.close_count, 1)
        client, _ = fixture(RawResponse(b"private-chunk", status=410, headers={}))
        with self.assertRaises(SwarmAPIError) as caught:
            client.download_artifact("artifact")
        self.assertIsNone(caught.exception.body)
        self.assert_safe(caught.exception)

    def test_error_byte_ceiling_is_not_relaxed_by_per_call_or_route_rules(self):
        for headers in ({}, {"Content-Length": "100"}):
            response = RawResponse(b"private-chunk", status=410, headers=headers)
            client, _ = fixture(response, max_error_response_bytes=3)
            with self.assertRaises(SwarmResponseLimitError) as caught:
                client.download_artifact("artifact", max_error_response_bytes=100)
            self.assertEqual(caught.exception.max_bytes, 3)
            self.assertEqual(response.close_count, 1)

    def test_cancellation_before_and_during_download_never_returns_partial_file(self):
        stop = threading.Event()
        stop.set()
        client, transport = fixture()
        with self.assertRaises(SwarmCancelledError):
            client.download_artifact("artifact", stop_event=stop)
        self.assertEqual(transport.calls, [])
        stop.clear()
        response = RawResponse(b"private-chunk" * 10000, on_read=lambda r: stop.set() if r.tell() else None)
        client, _ = fixture(response)
        spools, tracking = self.track_spools()
        with tracking, self.assertRaises(SwarmCancelledError) as caught:
            client.download_artifact("artifact", stop_event=stop)
        self.assert_safe(caught.exception)
        self.assertTrue(spools[0].closed)
        self.assertEqual(response.close_count, 1)

    def test_deadline_covers_hashing_and_spooling_not_only_socket_reads(self):
        client, _ = fixture()
        original = hashlib.sha256

        class SlowDigest:
            def __init__(self):
                self.digest = original()

            def update(self, chunk):
                time.sleep(0.03)
                self.digest.update(chunk)

            def hexdigest(self):
                return self.digest.hexdigest()

        spools, tracking = self.track_spools()
        with tracking, patch("swarm_sdk._artifact_download.hashlib.sha256", side_effect=SlowDigest):
            with self.assertRaises(SwarmTimeoutError) as caught:
                client.download_artifact("artifact", operation_timeout=0.01)
        self.assertEqual(caught.exception.kind, "operation")
        self.assertTrue(spools[0].closed)

    def test_custom_header_and_idle_overruns_checked_after_cooperative_return(self):
        class SlowOpen(RawTransport):
            def stream(self, *args):
                time.sleep(0.03)
                return super().stream(*args)

        for phase in ("headers", "download_idle"):
            response = RawResponse(on_read=(lambda _: time.sleep(0.03)) if phase == "download_idle" else None)
            transport = SlowOpen(response) if phase == "headers" else RawTransport(response)
            client = SwarmClient(base_url="https://local.invalid", transport=transport)
            with self.assertRaises(SwarmTimeoutError) as caught:
                client.download_artifact("artifact", header_timeout=0.01 if phase == "headers" else 1,
                                         idle_timeout=0.01 if phase == "download_idle" else 1)
            self.assertEqual(caught.exception.kind, phase)
            self.assertEqual(response.close_count, 1)

    def test_unknown_open_or_read_failures_do_not_leak_diagnostics(self):
        class BrokenTransport:
            def stream(self, *_):
                raise RuntimeError("synthetic-secret-token https://storage.invalid")

        client = SwarmClient(base_url="https://local.invalid", transport=BrokenTransport())
        with self.assertRaises(SwarmTransportError) as caught:
            client.download_artifact("artifact")
        self.assert_safe(caught.exception)
        response = RawResponse(on_read=lambda _: (_ for _ in ()).throw(RuntimeError("private-chunk")))
        client, _ = fixture(response)
        with self.assertRaises(SwarmTransportError) as caught:
            client.download_artifact("artifact")
        self.assert_safe(caught.exception)
        self.assertEqual(response.close_count, 1)

    def test_outer_operation_and_default_120_second_bound_are_respected(self):
        seen = []
        original = __import__("swarm_sdk._artifact_download", fromlist=["_Lifetime"])._Lifetime

        def lifetime(timeout=None, **options):
            seen.append((timeout, options.get("kind", "operation")))
            return original(timeout, **options)

        client, _ = fixture()
        with patch("swarm_sdk._artifact_download._Lifetime", side_effect=lifetime):
            client.download_artifact("artifact", operation_timeout=1000).close()
        self.assertEqual(seen[0], (120.0, "operation"))
        client, _ = fixture(RawResponse(on_read=lambda _: time.sleep(0.03)))
        with self.assertRaises(SwarmTimeoutError), client.operation(timeout=0.01):
            client.download_artifact("artifact", operation_timeout=100)
        self.assertIsNone(client._operation.get())

    def test_native_urllib_reads_authenticates_and_never_follows_redirect(self):
        with raw_loopback() as (url, calls, _, _, body):
            client = SwarmClient(base_url=url, token="synthetic-secret-token")
            with client.download_artifact("artifact") as result:
                self.assertEqual(result.read(), body)
            self.assertEqual(calls[0][0], "/prefix/v1/artifacts/artifact/content")
            self.assertEqual(calls[0][1]["Authorization"], "Bearer synthetic-secret-token")
            self.assertEqual(calls[0][1]["Accept"], "application/octet-stream")
            self.assertEqual(calls[0][1]["Accept-Encoding"], "identity")
        for status in (302, 410):
            with raw_loopback(status=status) as (url, calls, _, _, _):
                client = SwarmClient(base_url=url, token="synthetic-secret-token")
                with self.assertRaises(SwarmAPIError) as caught:
                    client.download_artifact("artifact")
                self.assertEqual(caught.exception.status, status)
                self.assert_safe(caught.exception)
                self.assertEqual(len(calls), 1)

    def test_native_header_idle_and_dripping_total_deadlines_interrupt_sockets(self):
        for mode, options, kind in (("headers", {"header_timeout": 0.05}, "headers"),
                                    ("stall", {"idle_timeout": 0.05}, "download_idle"),
                                    ("drip", {"idle_timeout": 0.08, "operation_timeout": 0.08}, "operation")):
            with raw_loopback(mode) as (url, _, _, closed, _):
                began = time.monotonic()
                client = SwarmClient(base_url=url, timeout=1)
                with self.assertRaises(SwarmTimeoutError) as caught:
                    client.download_artifact("artifact", **options)
                self.assertEqual(caught.exception.kind, kind)
                self.assertLess(time.monotonic() - began, 0.6)
                self.assertTrue(closed.wait(0.5))
        self.assertFalse(any(t.name == "swarm-transport-lifetime" for t in threading.enumerate()))

    def test_native_stop_interrupts_header_and_body_io_and_cleans_spool(self):
        for mode in ("headers", "stall"):
            with raw_loopback(mode) as (url, _, started, closed, _):
                stop = threading.Event()
                timer = threading.Thread(target=lambda: (started.wait(1), time.sleep(0.02), stop.set()))
                timer.start()
                spools, tracking = self.track_spools()
                try:
                    with tracking, self.assertRaises(SwarmCancelledError):
                        SwarmClient(base_url=url, timeout=1).download_artifact("artifact", stop_event=stop)
                finally:
                    timer.join(2)
                self.assertTrue(all(spool.closed for spool in spools))
                self.assertTrue(closed.wait(0.5))


if __name__ == "__main__":
    unittest.main()
