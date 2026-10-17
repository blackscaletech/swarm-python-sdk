import json
import unittest
from urllib.parse import parse_qs, urlsplit

from swarm_sdk import ExecutionParticipant, SwarmClient
from swarm_sdk.client import HTTPResponse
from sdk_test_support import FakeTransport


class ExecutionParticipantTests(unittest.TestCase):
    def test_discovery_preserves_addresses_false_fields_and_empty_page_cursor(self):
        participant: ExecutionParticipant = {"id": "executor_peer", "executor_identity_id": "executor_peer", "name": "Viewer", "kind": "client",
                                             "is_self": False, "can_execute": False, "created_at": "2026-10-12T12:00:00Z"}
        transport = FakeTransport()
        transport.responses.extend([
            HTTPResponse(status=200, headers={"content-type": "application/json", "x-order": "desc", "x-next-cursor": "candidate /?&="}, body=b"[]"),
            HTTPResponse(status=200, headers={"content-type": "application/json", "x-order": "desc"}, body=json.dumps([participant]).encode("utf-8")),
        ])
        client = SwarmClient(base_url="https://swarm.example", token="synthetic", transport=transport)
        first = client.list_space_agent_participants("space /?&=", limit=1, order="desc")
        self.assertEqual(first.items, [])
        self.assertEqual(first.next_cursor, "candidate /?&=")
        second = client.list_space_agent_participants("space /?&=", limit=1, order="desc", cursor=first.next_cursor)
        self.assertEqual(second.items, [participant])
        self.assertIsNone(second.next_cursor)
        for request in transport.requests:
            url = urlsplit(request["url"])
            self.assertEqual(url.path, "/v1/spaces/space%20%2F%3F%26%3D/agent-participants")
            query = parse_qs(url.query)
            self.assertEqual(query["limit"], ["1"])
            self.assertEqual(query["order"], ["desc"])
            self.assertEqual(request["method"], "GET")
            self.assertIsNone(request["body"])
        self.assertEqual(parse_qs(urlsplit(transport.requests[1]["url"]).query)["cursor"], [first.next_cursor])
