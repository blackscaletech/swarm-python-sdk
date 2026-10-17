import io
import unittest
from urllib.error import HTTPError

from swarm_sdk import SwarmClient, SwarmResponseLimitError
from swarm_sdk.client import HTTPResponse, UrllibTransport
from sdk_test_support import FakeTransport


class Body(io.BytesIO):
    def __init__(self, payload, status=200, headers=None):
        super().__init__(payload)
        self.status = status
        self.headers = headers or {"content-type": "text/plain"}
        self.read_sizes = []

    def read(self, amount=-1):
        self.read_sizes.append(amount)
        return super().read(amount)


class Opener:
    def __init__(self, body):
        self.body = body

    def open(self, request, timeout=None):
        return self.body


class ResponseLimitTests(unittest.TestCase):
    def test_endpoint_templates_ignore_base_prefix_and_explicit_overrides_win(self):
        transport = FakeTransport()
        transport.responses = [HTTPResponse(200, {}, b"x" * 20) for _ in range(5)]
        client = SwarmClient(base_url="https://audit.example/api/", transport=transport,
                             max_response_bytes=10, response_byte_limits=[
                                 {"method": "get", "path": "/v1/artifacts/{artifactId}/content", "max_response_bytes": 25}])
        self.assertEqual(len(client.get("/v1/artifacts/artifact-1/content", query={"inline": True})), 20)
        self.assertEqual(len(client.get("https://AUDIT.example:443/api/v1/artifacts/a/content")), 20)
        with self.assertRaises(SwarmResponseLimitError) as failure:
            client.get("/v1/artifacts/a/content", max_response_bytes=15)
        self.assertEqual(failure.exception.max_bytes, 15)
        with self.assertRaises(SwarmResponseLimitError):
            client.get("/v1/artifacts/a")
        with self.assertRaises(SwarmResponseLimitError):
            client.post("/v1/artifacts/a/content", idempotency_policy="durable-content")

    def test_endpoint_error_limit_overrides_global_and_preserves_legacy_bounded_transport(self):
        class BoundedTransport:
            def __init__(self):
                self.limits = []

            def request_bounded(self, method, url, headers, body, timeout, *, max_response_bytes, max_error_response_bytes):
                self.limits.append((max_response_bytes, max_error_response_bytes))
                return HTTPResponse(503, {}, b"x" * 10)

        transport = BoundedTransport()
        client = SwarmClient(base_url="https://audit.example", transport=transport,
                             max_error_response_bytes=100, response_byte_limits=[
                                 {"method": "GET", "path": "/v1/artifacts/{id}/content", "max_error_response_bytes": 5}])
        with self.assertRaises(SwarmResponseLimitError):
            client.get("/v1/artifacts/a/content")
        with self.assertRaisesRegex(RuntimeError, "failed"):
            client.get("/v1/artifacts/a/content", max_error_response_bytes=20)
        self.assertEqual(transport.limits, [(64 * 1024 * 1024, 5), (64 * 1024 * 1024, 20)])

    def test_route_limits_are_validated_without_patterns_or_ambiguous_duplicates(self):
        invalid = [True, {}, [None], [{"method": "*", "path": "/v1/a", "max_response_bytes": 1}],
                   [{"method": "GET", "path": "/v1/*", "max_response_bytes": 1}],
                   [{"method": "GET", "path": "https://other.invalid/v1/a", "max_response_bytes": 1}],
                   [{"method": "GET", "path": "/v1/a?secret=yes", "max_response_bytes": 1}],
                   [{"method": "GET", "path": "/v1/{id}/../a", "max_response_bytes": 1}],
                   [{"method": "GET", "path": "/v1/a"}],
                   [{"method": "GET", "path": "/v1/a", "max_response_bytes": None}],
                   [{"method": "GET", "path": "/v1/a", "max_response_bytes": True}],
                   [{"method": "GET", "path": "/v1/{id}", "max_response_bytes": 1},
                    {"method": "GET", "path": "/v1/{other}", "max_response_bytes": 2}]]
        for rules in invalid:
            with self.subTest(rules=rules), self.assertRaises(ValueError):
                SwarmClient(base_url="https://audit.example", response_byte_limits=rules)

    def test_default_transport_bounds_success_and_error_reads_and_closes(self):
        for status in [200, 503]:
            with self.subTest(status=status):
                body = Body(b"API_KEY=synthetic_secret_123456789" * 100, status)
                client = SwarmClient(base_url="https://audit.example", transport=UrllibTransport(Opener(body)),
                                     max_response_bytes=64, max_error_response_bytes=64)
                with self.assertRaises(SwarmResponseLimitError) as caught:
                    client.get("v1/test")
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(caught.exception.max_bytes, 64)
                self.assertNotIn("synthetic_secret", str(caught.exception))
                self.assertTrue(body.closed)
                self.assertTrue(all(0 < size <= 65 for size in body.read_sizes))

    def test_urllib_http_error_also_uses_bounded_reader_and_closes(self):
        body = Body(b"x" * 100)
        error = HTTPError("https://audit.example", 503, "synthetic", {"content-type": "text/plain"}, body)

        class ErrorOpener:
            def open(self, request, timeout=None):
                raise error

        client = SwarmClient(base_url="https://audit.example", transport=UrllibTransport(ErrorOpener()), max_error_response_bytes=64)
        with self.assertRaises(SwarmResponseLimitError):
            client.get("v1/test")
        self.assertTrue(body.closed)

    def test_multi_megabyte_body_and_custom_transport_overrides(self):
        payload = b"x" * (2 * 1024 * 1024)
        body = Body(payload)
        client = SwarmClient(base_url="https://audit.example", transport=UrllibTransport(Opener(body)))
        self.assertEqual(len(client.get("v1/test")), len(payload))
        transport = FakeTransport()
        transport.responses = [HTTPResponse(200, {}, payload)]
        client = SwarmClient(base_url="https://audit.example", transport=transport)
        with self.assertRaises(SwarmResponseLimitError):
            client.get("v1/test", max_response_bytes=64)

    def test_declared_length_and_encoded_bytes(self):
        for body in [Body(b"tiny", headers={"content-length": "1000"}), Body("\u00e9\u00e9\u00e9".encode())]:
            client = SwarmClient(base_url="https://audit.example", transport=UrllibTransport(Opener(body)), max_response_bytes=5)
            with self.assertRaises(SwarmResponseLimitError):
                client.get("v1/test")
            self.assertTrue(body.closed)

    def test_invalid_limits_and_private_malformed_json_diagnostics(self):
        for value in [0, -1, True, 1.5, float("inf")]:
            with self.assertRaises(ValueError):
                SwarmClient(base_url="https://audit.example", max_response_bytes=value)
        body = Body(b'{"API_KEY":"synthetic_secret_123456789"', headers={"content-type": "application/json"})
        client = SwarmClient(base_url="https://audit.example", transport=UrllibTransport(Opener(body)))
        with self.assertRaises(ValueError) as caught:
            client.get("v1/test")
        self.assertNotIn("synthetic_secret", str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
