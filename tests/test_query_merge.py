import unittest
from unittest.mock import patch

from swarm_sdk import SwarmClient
from swarm_sdk._client_inputs import _build_stable_mutation_idempotency_key
from swarm_sdk.agent_runtime import _call
from sdk_test_support import FakeTransport, json_response


class QueryMergeTests(unittest.TestCase):
    def client(self, count=1, **options):
        transport = FakeTransport()
        transport.responses = [json_response({}) for _ in range(count)]
        return SwarmClient(base_url="https://audit.example/api/", transport=transport, **options), transport

    def test_existing_query_is_merged_before_fragment_for_relative_and_absolute_urls(self):
        cases = [
            ("/v1/spaces?limit=1", {"cursor": "page 2"}, "limit=1&cursor=page+2"),
            ("/v1/spaces?limit=1#view", {"cursor": "page 2"}, "limit=1&cursor=page+2#view"),
            ("https://audit.example/api/v1/spaces?limit=1#view", {"cursor": "page 2"}, "limit=1&cursor=page+2#view"),
            ("/v1/spaces#view", {"limit": 1}, "limit=1#view"),
        ]
        client, transport = self.client(len(cases), token="synthetic")
        for path, query, expected in cases:
            with self.subTest(path=path):
                client.get(path, query=query)
                request = transport.requests[-1]
                self.assertEqual(request["url"], f"https://audit.example/api/v1/spaces?{expected}")
                self.assertEqual(request["headers"]["Authorization"], "Bearer synthetic")

    def test_scalar_replaces_duplicates_in_place_and_arrays_append_like_typescript(self):
        cases = [
            ({"tag": "new", "cursor": "next"}, "tag=new&keep=a&keep=b&cursor=next"),
            ({"tag": ["new", None, "", False, 0]}, "tag=old&keep=a&tag=older&keep=b&tag=new&tag=false&tag=0"),
            ({"tag": ("one", "two")}, "tag=old&keep=a&tag=older&keep=b&tag=one&tag=two"),
        ]
        client, transport = self.client(len(cases))
        for query, expected in cases:
            with self.subTest(query=query):
                client.get("/v1/spaces?tag=old&keep=a&tag=older&keep=b#view", query=query)
                self.assertEqual(transport.requests[-1]["url"], f"https://audit.example/api/v1/spaces?{expected}#view")

    def test_absent_and_empty_values_do_not_reencode_or_remove_existing_query(self):
        cases = [None, {}, {"tag": None, "ignored": "", "empty": []}, {"tag": [None, ""]}]
        client, transport = self.client(len(cases))
        for query in cases:
            with self.subTest(query=query):
                client.get("/v1/spaces?tag=a%20b&tag=c#view", query=query)
                self.assertEqual(transport.requests[-1]["url"], "https://audit.example/api/v1/spaces?tag=a%20b&tag=c#view")

    def test_durable_retry_reuses_merged_query_and_excludes_fragment_from_identity(self):
        class LostResponseTransport(FakeTransport):
            def request(self, method, url, headers, body, timeout):
                response = super().request(method, url, headers, body, timeout)
                if response.status == 599:
                    raise TimeoutError("synthetic uncertain response")
                return response

        transport = LostResponseTransport()
        transport.responses = [json_response({}, status=599), json_response({"id": "artifact"})]
        client = SwarmClient(base_url="https://audit.example/api/", transport=transport)
        body = {"body": " exact evidence "}
        with patch("swarm_sdk.agent_runtime._retry_delay", return_value=0):
            result = _call(lambda: client.post("/v1/artifacts?tag=old&tag=older#view", body,
                                              query={"tag": "new", "cursor": "next"}), client=client)
        self.assertEqual(result, {"id": "artifact"})
        self.assertEqual(len(transport.requests), 2)
        self.assertEqual(transport.requests[0], transport.requests[1])
        self.assertEqual(transport.requests[0]["url"], "https://audit.example/api/v1/artifacts?tag=new&cursor=next#view")
        expected = _build_stable_mutation_idempotency_key(
            "swarm-sdk-py-v2", "POST", "/v1/artifacts?tag=new&cursor=next", body, preserve_strings=True)
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], expected)

    def test_identity_tracks_effective_query_not_overridden_values_or_fragment(self):
        cases = [
            ("/v1/artifacts?tag=old&tag=older#one", {"tag": "new"}),
            ("/v1/artifacts?tag=different#two", {"tag": "new"}),
            ("/v1/artifacts?tag=new#three", None),
            ("/v1/artifacts?tag=new#four", {"tag": "changed"}),
        ]
        client, transport = self.client(len(cases))
        for path, query in cases:
            client.post(path, {"body": "same"}, query=query)
        keys = [request["headers"]["Idempotency-Key"] for request in transport.requests]
        self.assertEqual(keys[0], keys[1])
        self.assertEqual(keys[0], keys[2])
        self.assertNotEqual(keys[0], keys[3])

    def test_unaffected_default_and_explicit_retry_keys_remain_unchanged(self):
        client, transport = self.client(3)
        body = {"body": "same"}
        for path, query, expected_path in [
            ("/v1/artifacts", None, "/v1/artifacts"),
            ("/v1/artifacts", {"tag": ["a", "b"], "enabled": False}, "/v1/artifacts?tag=a&tag=b&enabled=false"),
        ]:
            client.post(path, body, query=query)
            expected = _build_stable_mutation_idempotency_key(
                "swarm-sdk-py-v2", "POST", expected_path, body, preserve_strings=True)
            self.assertEqual(transport.requests[-1]["headers"]["Idempotency-Key"], expected)
        client.post("/v1/artifacts?tag=old#view", body, query={"tag": "new"}, idempotency_key=" original-key ")
        self.assertEqual(transport.requests[-1]["headers"]["Idempotency-Key"], " original-key ")
