import json
import unittest
from urllib.parse import parse_qs, urlsplit

from swarm_sdk import SwarmClient
from swarm_sdk.client import HTTPResponse
from sdk_test_support import FakeTransport


class RuntimeCredentialListTests(unittest.TestCase):
    def test_one_page_preserves_scope_filters_cursor_and_order(self):
        transport = FakeTransport()
        transport.responses.extend([
            HTTPResponse(status=200, headers={"content-type": "application/json", "x-next-cursor": "cursor /?&=", "x-order": "asc"}, body=json.dumps([{"id": "cred_first"}]).encode()),
            HTTPResponse(status=200, headers={"content-type": "application/json", "x-order": "asc"}, body=json.dumps([{"id": "cred_last"}]).encode()),
        ])
        client = SwarmClient(base_url="https://swarm.example", transport=transport)
        query = {"active_only": True, "limit": 2, "order": "asc"}
        first = client.list_agent_runtime_credentials("agt /?&=", **query)
        self.assertEqual(first.items, [{"id": "cred_first"}])
        self.assertEqual(first.next_cursor, "cursor /?&=")
        self.assertEqual(first.order, "asc")
        self.assertEqual(len(transport.requests), 1)
        last = client.list_agent_runtime_credentials("agt /?&=", **query, cursor=first.next_cursor)
        self.assertEqual(last.items, [{"id": "cred_last"}])
        self.assertIsNone(last.next_cursor)
        first_url = urlsplit(transport.requests[0]["url"])
        self.assertEqual(first_url.path, "/v1/agents/agt%20%2F%3F%26%3D/runtime-credentials")
        self.assertEqual(parse_qs(first_url.query)["active_only"], ["true"])
        self.assertEqual(parse_qs(urlsplit(transport.requests[1]["url"]).query)["cursor"], [first.next_cursor])
        self.assertNotIn("cursor", query)
        for request in transport.requests:
            self.assertEqual(request["method"], "GET")
            self.assertIsNone(request["body"])

    def test_older_array_without_continuation_headers_remains_readable(self):
        transport = FakeTransport()
        transport.responses.append(HTTPResponse(status=200, headers={"content-type": "application/json"}, body=b"[]"))
        page = SwarmClient(base_url="https://swarm.example", transport=transport).list_agent_runtime_credentials("agt_legacy", active_only=False)
        self.assertEqual(page.items, [])
        self.assertIsNone(page.next_cursor)
