import json
import unittest
from urllib.parse import parse_qs, urlsplit

from swarm_sdk import SwarmClient
from swarm_sdk.client import HTTPResponse
from sdk_test_support import FakeTransport


class RunGroupListTests(unittest.TestCase):
    def test_run_group_list_preserves_filters_static_route_and_cursor(self):
        transport = FakeTransport()
        response = HTTPResponse(status=200, headers={"content-type": "application/json", "x-next-cursor": "cursor /?&=", "x-order": "desc"},
                                body=json.dumps([{"id": "run_1"}]).encode("utf-8"))
        transport.responses.extend([response, response, response])
        client = SwarmClient(base_url="https://swarm.example", token="synthetic", transport=transport)
        query = {"space_id": "space_1", "run_group_id": "group /?&=", "task_id": "task_1",
                 "automation_profile_id": "profile_1", "status": "queued", "limit": 2, "order": "desc"}
        page = client.list_runs(**query)
        self.assertEqual(page.items, [{"id": "run_1"}])
        self.assertEqual(page.next_cursor, "cursor /?&=")
        self.assertEqual(page.order, "desc")
        client.list_runs(**query, cursor=page.next_cursor)
        for request in transport.requests:
            url = urlsplit(request["url"])
            self.assertEqual(url.path, "/v1/runs")
            self.assertEqual(url.fragment, "")
            self.assertEqual(request["method"], "GET")
            self.assertIsNone(request["body"])
            parameters = parse_qs(url.query)
            for key, value in query.items():
                self.assertEqual(parameters[key], [str(value)])
        self.assertEqual(parse_qs(urlsplit(transport.requests[1]["url"]).query)["cursor"], [page.next_cursor])
        client.list_runs(run_group_id="group_1", limit=2)
        parameters = parse_qs(urlsplit(transport.requests[2]["url"]).query)
        self.assertEqual(parameters["run_group_id"], ["group_1"])
        self.assertNotIn("space_id", parameters)
        self.assertNotIn("cursor", query)
