from __future__ import annotations

import json
import unittest

from swarm_sdk import SwarmClient
from swarm_sdk.client import HTTPResponse

from sdk_test_support import FakeTransport


class SwarmSDKValueLoopTests(unittest.TestCase):
    def test_value_loop_reads_use_canonical_post_routes_without_idempotency(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                HTTPResponse(
                    status=200,
                    headers={"content-type": "application/json"},
                    body=json.dumps(
                        {
                            "state_key": "ready",
                            "source_resource_ref": "artifact:art_1",
                            "source_revision": 4,
                            "markdown": "# Swarm handoff",
                            "citations": [{"resource_ref": "artifact:art_1", "rank": 1}],
                        }
                    ).encode("utf-8"),
                ),
                HTTPResponse(
                    status=200,
                    headers={"content-type": "application/json"},
                    body=json.dumps(
                        {
                            "state_key": "ready",
                            "plan_revision": 4,
                            "included_refs": [],
                            "exclusion_aggregates": [],
                            "policy_summary": {
                                "token_budget": 4000,
                                "max_refs": 32,
                                "graph_depth": 1,
                                "model_call": False,
                            },
                        }
                    ).encode("utf-8"),
                ),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="client-token", transport=transport)

        handoff = client.preview_handoff("sp_1", {"source_resource_ref": "artifact:art_1"})
        plan = client.get_context_plan("sp_1", {"composer_text": "Continue the work."})

        self.assertEqual(handoff["source_resource_ref"], "artifact:art_1")
        self.assertFalse(plan["policy_summary"]["model_call"])
        self.assertEqual(transport.requests[0]["url"], "https://swarm.example/v1/spaces/sp_1/handoff-preview/view")
        self.assertEqual(transport.requests[1]["url"], "https://swarm.example/v1/spaces/sp_1/context-plan/view")
        self.assertNotIn("Idempotency-Key", transport.requests[0]["headers"])
        self.assertNotIn("Idempotency-Key", transport.requests[1]["headers"])

    def test_request_effect_conflicts_fail_closed(self) -> None:
        client = SwarmClient(base_url="https://swarm.example", transport=FakeTransport())

        with self.assertRaisesRegex(ValueError, "GET requests cannot declare mutation effect"):
            client.request("GET", "/v1/spaces", effect="mutation")
        with self.assertRaisesRegex(ValueError, "Read requests cannot declare an idempotency key"):
            client.request(
                "POST",
                "/v1/spaces/sp_1/context-plan/view",
                body={},
                effect="read",
                idempotency_key="must-not-be-present",
            )

    def test_learning_correction_methods_use_canonical_routes(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                HTTPResponse(status=200, headers={"content-type": "application/json"}, body=b'{"candidate":{"id":"lcand_2"}}'),
                HTTPResponse(status=200, headers={"content-type": "application/json"}, body=b'{"candidate":{"id":"lcand_1"}}'),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="client-token", transport=transport)

        client.correct_space_learning_candidate(
            "sp_1",
            "lcand_1",
            {"summary": "Corrected knowledge.", "reason": "Reviewed evidence."},
            idempotency_key="correct-learning-1",
        )
        client.supersede_space_learning_candidate(
            "sp_1",
            "lcand_1",
            {"reason_key": "learning.supersession.stale", "reason": "The source is no longer current."},
            idempotency_key="stale-learning-1",
        )

        self.assertEqual(
            transport.requests[0]["url"],
            "https://swarm.example/v1/spaces/sp_1/learning-candidates/lcand_1/corrections",
        )
        self.assertEqual(
            transport.requests[1]["url"],
            "https://swarm.example/v1/spaces/sp_1/learning-candidates/lcand_1/supersede",
        )
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], "correct-learning-1")
        self.assertEqual(transport.requests[1]["headers"]["Idempotency-Key"], "stale-learning-1")

    def test_evaluation_exposes_caller_idempotency_key(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=b'{"id":"eval_1","space_id":"sp_1","outcome":"pass"}',
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="client-token", transport=transport)

        evaluation = client.create_evaluation(
            {
                "space_id": "sp_1",
                "artifact_id": "art_1",
                "outcome": "pass",
                "score": 95,
                "summary": "Verified output.",
            },
            idempotency_key="evaluation-1",
        )

        self.assertEqual(evaluation["id"], "eval_1")
        self.assertEqual(transport.requests[0]["url"], "https://swarm.example/v1/evaluations")
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], "evaluation-1")


if __name__ == "__main__":
    unittest.main()
