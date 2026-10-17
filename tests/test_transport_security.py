import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from swarm_sdk import SwarmClient, SwarmAPIError
from sdk_test_support import FakeTransport, json_response


class TransportSecurityTests(unittest.TestCase):
    def test_urllib_preserves_real_api_errors_without_following_redirects(self):
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append(self.path)
                status = int(self.path.strip("/"))
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "2")
                if status == 302:
                    self.send_header("Location", "https://outside.invalid/never-follow")
                self.end_headers()
                self.wfile.write(json.dumps({"code": "task_changed", "next_action": "Read the latest Task", "retryable": False}).encode())

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                client = SwarmClient(base_url=f"http://127.0.0.1:{server.server_port}", token="synthetic")
                for status in (401, 403, 409, 422, 429, 503, 302):
                    with self.subTest(status=status), self.assertRaises(SwarmAPIError) as failure:
                        client.get(f"/{status}")
                    self.assertEqual(failure.exception.status, status)
                    self.assertEqual(failure.exception.body["code"], "task_changed")
                    self.assertEqual(failure.exception.body["next_action"], "Read the latest Task")
                    self.assertFalse(failure.exception.body["retryable"])
                    self.assertEqual(failure.exception.headers["retry-after"], "2")
                    self.assertIn(f"({status})", str(failure.exception))
                self.assertEqual(calls, [f"/{status}" for status in (401, 403, 409, 422, 429, 503, 302)])
            finally:
                server.shutdown()
                thread.join(timeout=5)

    def test_cross_origin_and_url_credentials_never_reach_transport(self):
        transport = FakeTransport()
        client = SwarmClient(base_url="https://swarm.example/api/", token="test-token", transport=transport)
        for path in [
            "https://other.example/v1/spaces",
            "http://swarm.example/v1/spaces",
            "https://swarm.example:444/v1/spaces",
            "https://swarm.example:0/v1/spaces",
            "https://user:password@swarm.example/v1/spaces",
        ]:
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "origin|credentials"):
                client.get(path)
        self.assertEqual(transport.requests, [])

    def test_equivalent_origins_and_relative_paths_keep_auth(self):
        transport = FakeTransport()
        transport.responses = [json_response({"ok": True}), json_response({"ok": True})]
        client = SwarmClient(base_url="https://swarm.example/api/", token="test-token", transport=transport)
        client.get("/v1/spaces", query={"limit": 1})
        client.get("https://SWARM.example:443/v1/spaces")
        self.assertEqual(transport.requests[0]["url"], "https://swarm.example/api/v1/spaces?limit=1")
        self.assertEqual(len(transport.requests), 2)
        self.assertTrue(all(row["headers"]["Authorization"] == "Bearer test-token" for row in transport.requests))

    def test_base_url_disallows_credentials_even_with_insecure_override(self):
        with self.assertRaisesRegex(ValueError, "credentials"):
            SwarmClient(base_url="https://user:password@swarm.example", allow_insecure_transport=True)


if __name__ == "__main__":
    unittest.main()
