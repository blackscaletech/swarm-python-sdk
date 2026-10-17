import hashlib
import json
import unittest
from unittest.mock import patch
from swarm_sdk import SwarmClient, build_launch_run_idempotency_key
from swarm_sdk.agent_runtime import _call
from swarm_sdk._client_inputs import _build_stable_mutation_idempotency_key
from sdk_test_support import FakeTransport, json_response


class AttemptIdempotency(unittest.TestCase):
    def test_generic_v2_preserves_leading_trailing_and_nested_evidence_strings(self):
        transport = FakeTransport()
        bodies = [{"body": value, "source_prompt": value, "evidence": [{"source": value}]}
                  for value in ("value", " value", "value ", " value ", "\nvalue\n")]
        transport.responses = [json_response({}) for _ in bodies]
        client = SwarmClient(base_url="https://audit.example", transport=transport)
        for body in bodies:
            client.post("/v1/artifacts", body)
            self.assertEqual(transport.requests[-1]["body"], body)
            expected = {"method": "POST", "path": "/v1/artifacts", "body": body, "intent_key": ""}
            digest = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            self.assertEqual(transport.requests[-1]["headers"]["Idempotency-Key"], f"swarm-sdk-py-v2-{digest[:32]}")
        self.assertEqual(len({row["headers"]["Idempotency-Key"] for row in transport.requests}), len(bodies))

    def test_generic_v2_matches_json_wire_values_sorts_objects_and_preserves_array_order(self):
        bodies = [{"z": {2: " x ", "1": None}, "a": ("first", "second")},
                  {"a": ["first", "second"], "z": {"1": None, "2": " x "}},
                  {"a": ["second", "first"], "z": {"1": None, "2": " x "}},
                  {"a": ["first", "second"], "z": {"2": " x "}}]
        transport = FakeTransport()
        transport.responses = [json_response({}) for _ in bodies]
        client = SwarmClient(base_url="https://audit.example", transport=transport)
        for body in bodies:
            client.post("/v1/artifacts", body)
        keys = [row["headers"]["Idempotency-Key"] for row in transport.requests]
        self.assertEqual(transport.requests[0]["body"], transport.requests[1]["body"])
        self.assertEqual(keys[0], keys[1])
        self.assertNotEqual(keys[0], keys[2])
        self.assertNotEqual(keys[0], keys[3])

    def test_generic_v2_default_remains_stable_after_an_uncertain_transport_response(self):
        class LostResponseTransport(FakeTransport):
            def request(self, method, url, headers, body, timeout):
                response = super().request(method, url, headers, body, timeout)
                if response.status == 599:
                    raise TimeoutError("synthetic uncertain response")
                return response

        transport = LostResponseTransport()
        transport.responses = [json_response({}, status=599), json_response({"id": "artifact"})]
        client = SwarmClient(base_url="https://audit.example", transport=transport)
        body = {"body": " exact source bytes\n", "source": [" keep ", " order "]}
        with patch("swarm_sdk.agent_runtime._retry_delay", return_value=0):
            result = _call(lambda: client.post("/v1/artifacts", body), client=client)
        self.assertEqual(result["id"], "artifact")
        self.assertEqual(len(transport.requests), 2)
        self.assertEqual(transport.requests[0], transport.requests[1])
        self.assertTrue(transport.requests[0]["headers"]["Idempotency-Key"].startswith("swarm-sdk-py-v2-"))

    def test_explicit_keys_fresh_attempt_keys_and_named_recipes_are_unchanged(self):
        transport = FakeTransport()
        transport.responses = [json_response({}) for _ in range(4)]
        client = SwarmClient(base_url="https://audit.example", transport=transport)
        for _ in range(2):
            client.post("/v1/artifacts", {"body": " exact "}, idempotency_key=" original-caller-key ")
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], " original-caller-key ")
        self.assertEqual(transport.requests[0], transport.requests[1])
        for _ in range(2):
            client.claim_next_run("space")
        keys = [row["headers"]["Idempotency-Key"] for row in transport.requests[2:]]
        self.assertTrue(all(key.startswith("swarm-sdk-py-op-") for key in keys))
        self.assertNotEqual(keys[0], keys[1])
        named = build_launch_run_idempotency_key("space", {"title": " normalized ", "source_prompt": " evidence "})
        self.assertTrue(named.startswith("swarm-launch-"))
        self.assertEqual(named, build_launch_run_idempotency_key("space", {"title": "normalized", "source_prompt": "evidence"}))

    def test_preserving_wire_strings_is_opt_in_for_the_private_legacy_recipe(self):
        padded = {"body": " padded "}
        plain = {"body": "padded"}
        self.assertEqual(_build_stable_mutation_idempotency_key("legacy", "POST", "/v1/artifacts", padded),
                         _build_stable_mutation_idempotency_key("legacy", "POST", "/v1/artifacts", plain))
        self.assertNotEqual(_build_stable_mutation_idempotency_key("swarm-sdk-py-v2", "POST", "/v1/artifacts", padded, preserve_strings=True),
                            _build_stable_mutation_idempotency_key("swarm-sdk-py-v2", "POST", "/v1/artifacts", plain, preserve_strings=True))

    def test_new_attempts_explicit_retries_and_durable_commands(self):
        for base_url in ["https://audit.example", "https://audit.example/api/"]:
            transport = FakeTransport()
            transport.responses = [json_response({}) for _ in range(20)]
            client = SwarmClient(base_url=base_url, transport=transport)
            for path in ["/v1/spaces/s/runs:claim-next", "/v1/runs/r/acquire", "/v1/spaces/s/learning-queue/claim"]:
                client.post(path, {"ttl_seconds": 300})
                client.post(path, {"ttl_seconds": 300})
                self.assertNotEqual(transport.requests[-1]["headers"]["Idempotency-Key"], transport.requests[-2]["headers"]["Idempotency-Key"])
                client.post(path, {}, idempotency_key="same-uncertain-attempt")
                client.post(path, {}, idempotency_key="same-uncertain-attempt")
                self.assertEqual(transport.requests[-1]["headers"]["Idempotency-Key"], transport.requests[-2]["headers"]["Idempotency-Key"])
            client.claim_space_learning_queue_item("s", idempotency_key="learning-retry")
            self.assertEqual(transport.requests[-1]["headers"]["Idempotency-Key"], "learning-retry")
            client.post("/v1/artifacts", {"body": "durable note"})
            client.post("/v1/artifacts", {"body": "durable note"})
            self.assertEqual(transport.requests[-1]["headers"]["Idempotency-Key"], transport.requests[-2]["headers"]["Idempotency-Key"])
