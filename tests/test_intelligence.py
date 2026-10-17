from __future__ import annotations

import io
import json
import unittest
from urllib import request

from swarm_sdk import (
    SwarmAPIError,
    SwarmClient,
    compute_connected_core_loop_delay_seconds,
    compute_webhook_signature,
    run_connected_core_loop,
    run_connected_core_once,
    parse_webhook_headers,
    verify_webhook_signature,
)
from swarm_sdk.client import (
    HTTPResponse,
    UrllibTransport,
    _NoRedirectHandler,
    build_bootstrap_current_user_entity_idempotency_key,
    build_bootstrap_space_idempotency_key,
    build_connect_space_client_idempotency_key,
    build_connect_current_user_and_launch_idempotency_key,
    build_connect_current_user_client_idempotency_key,
    build_bootstrap_space_idempotency_key,
    build_launch_run_idempotency_key,
)

from sdk_test_support import FakeOpener, FakeTransport, json_list_response, json_response, on_run


class SwarmSDKIntelligenceTests(unittest.TestCase):
    def test_space_intelligence_helpers_target_canonical_endpoints(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "answer_id": "ans_1",
                        "space_id": "sp_1",
                        "question": "What should we do next?",
                        "answer": "Use grounded Space memory.",
                        "confidence": "low",
                        "trust_boundary": "Based only on visible Space memory.",
                        "supporting_artifacts": ["art_1"],
                        "used_projections": [],
                        "used_strategies": [],
                        "gaps": [],
                        "follow_up_learning_candidates": [],
                        "created_at": "2026-05-08T16:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "answer_id": "ans_1",
                        "space_id": "sp_1",
                        "question": "What should we do next?",
                        "answer": "Use grounded Space memory.",
                        "confidence": "low",
                        "trust_boundary": "Based only on visible Space memory.",
                        "supporting_artifacts": ["art_1"],
                        "used_projections": [],
                        "used_strategies": [],
                        "gaps": [],
                        "follow_up_learning_candidates": [],
                        "created_at": "2026-05-08T16:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "space_id": "sp_1",
                        "generated_at": "2026-05-08T16:00:00Z",
                        "memory_nodes": [],
                        "memory_edges": [],
                        "projections": [],
                        "trust_boundary": "Based only on visible Space memory.",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "space_id": "sp_1",
                        "schema": "swarm.space.answer_contract.v1",
                        "version": 1,
                        "trust_boundary": "Based only on visible Space memory.",
                        "default_constraints": ["use_space_memory_only"],
                        "supported_output_shapes": ["brief_with_rationale"],
                        "required_response_fields": ["answer_id", "answer"],
                        "supported_learning_queue_kinds": ["outside_question", "real_user_question"],
                        "promotion_statuses": ["candidate", "validated", "promoted", "superseded", "rejected"],
                        "connector_candidate_artifact_kind_suffixes": [
                            "research_note",
                            "answer_draft",
                            "validation_report",
                            "learning_candidate",
                        ],
                        "connector_diagnostic_artifact_kind_suffixes": ["stdout", "stderr", "execution_report"],
                        "max_question_bytes": 1000,
                        "max_objective_bytes": 1000,
                        "max_constraint_bytes": 240,
                        "max_constraints": 12,
                        "allows_learning_update": True,
                        "requires_visible_evidence": True,
                        "generated_at": "2026-05-08T16:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "id": "lcand_1",
                        "space_id": "sp_1",
                        "family": "travel",
                        "summary": "Promote this candidate.",
                        "status": "candidate",
                        "source_artifact_ids": ["art_1"],
                        "evaluation_ids": ["eval_1"],
                        "created_at": "2026-05-08T16:00:00Z",
                        "updated_at": "2026-05-08T16:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "id": "lcand_2",
                        "space_id": "sp_1",
                        "family": "travel",
                        "summary": "Swiss hiking route shortlist",
                        "status": "candidate",
                        "source_artifact_ids": ["art_1"],
                        "evaluation_ids": ["eval_1"],
                        "created_at": "2026-05-08T16:00:00Z",
                        "updated_at": "2026-05-08T16:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    [
                        {
                            "id": "lcand_1",
                            "space_id": "sp_1",
                            "family": "travel",
                            "summary": "Promote this candidate.",
                            "status": "promoted",
                            "source_artifact_ids": ["art_1"],
                            "evaluation_ids": ["eval_1"],
                            "created_at": "2026-05-08T16:00:00Z",
                            "updated_at": "2026-05-08T16:00:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "projection_id": "proj_1",
                        "space_id": "sp_1",
                        "family": "travel",
                        "summary": "Promoted projection.",
                        "source_artifacts": ["art_1"],
                        "status": "promoted",
                        "version": 1,
                        "validation_score": 0.91,
                        "trust_boundary": "Based only on visible Space memory.",
                        "updated_at": "2026-05-08T16:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "id": "lcand_2",
                        "space_id": "sp_1",
                        "family": "travel",
                        "summary": "Swiss hiking route shortlist",
                        "status": "rejected",
                        "source_artifact_ids": ["art_1"],
                        "evaluation_ids": ["eval_1"],
                        "created_at": "2026-05-08T16:00:00Z",
                        "updated_at": "2026-05-08T16:05:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    [
                        {
                            "id": "pgate_1",
                            "space_id": "sp_1",
                            "candidate_id": "lcand_1",
                            "projection_id": "proj_1",
                            "decision": "promoted",
                            "criteria": {"source_lineage": True, "evaluation_score_threshold": True},
                            "reasons": ["source lineage present"],
                            "created_at": "2026-05-08T16:00:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    [
                        {
                            "projection_id": "proj_1",
                            "space_id": "sp_1",
                            "family": "travel",
                            "summary": "Promoted projection.",
                            "source_artifacts": ["art_1"],
                            "status": "promoted",
                            "version": 1,
                            "validation_score": 0.91,
                            "trust_boundary": "Based only on visible Space memory.",
                            "updated_at": "2026-05-08T16:00:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        answer = client.ask_space("sp_1", {"question": "What should we do next?"})
        read_back = client.get_space_answer("sp_1", answer["answer_id"])
        graph = client.get_space_decision_graph("sp_1")
        answer_contract = client.get_space_answer_contract("sp_1")
        candidate = client.create_space_learning_candidate(
            "sp_1",
            {
                "family": "travel",
                "summary": "Promote this candidate.",
                "source_artifact_ids": ["art_1"],
                "evaluation_ids": ["eval_1"],
            },
        )
        artifact_candidate = client.create_space_learning_candidate_from_artifact(
            "sp_1",
            {
                "artifact_id": "art_1",
                "family": "travel",
                "evaluation_ids": ["eval_1"],
            },
        )
        candidates = client.list_space_learning_candidates("sp_1", status="promoted")
        projection = client.promote_space_learning_candidate("sp_1", candidate["id"])
        rejected = client.reject_space_learning_candidate("sp_1", artifact_candidate["id"], {"reason": "Not useful."})
        promotion_gates = client.list_space_promotion_gates("sp_1", decision="promoted")
        projections = client.list_space_intelligence_projections("sp_1", status="promoted")
        for status, payload in [
            (
                201,
                {
                    "id": "lq_1",
                    "space_id": "sp_1",
                    "kind": "outside_question",
                    "family": "travel",
                    "prompt": "Ask the next grounded question.",
                    "priority": 75,
                    "status": "queued",
                    "created_at": "2026-05-08T16:00:00Z",
                    "updated_at": "2026-05-08T16:00:00Z",
                },
            ),
            (
                200,
                [
                    {
                        "id": "lq_1",
                        "space_id": "sp_1",
                        "kind": "outside_question",
                        "family": "travel",
                        "prompt": "Ask the next grounded question.",
                        "priority": 75,
                        "status": "queued",
                        "created_at": "2026-05-08T16:00:00Z",
                        "updated_at": "2026-05-08T16:00:00Z",
                    }
                ],
            ),
            (
                200,
                {
                    "id": "lq_1",
                    "space_id": "sp_1",
                    "kind": "outside_question",
                    "family": "travel",
                    "prompt": "Ask the next grounded question.",
                    "priority": 75,
                    "status": "claimed",
                    "claimed_by_agent_id": "agt_1",
                    "claim_generation": "9007199254740993",
                    "claimed_until": "2026-05-08T16:05:00Z",
                    "created_at": "2026-05-08T16:00:00Z",
                    "updated_at": "2026-05-08T16:00:00Z",
                },
            ),
            (
                200,
                {
                    "id": "lq_1",
                    "space_id": "sp_1",
                    "kind": "outside_question",
                    "family": "travel",
                    "prompt": "Ask the next grounded question.",
                    "priority": 75,
                    "status": "completed",
                    "claimed_by_agent_id": "agt_1",
                    "claim_generation": "9007199254740993",
                    "completed_at": "2026-05-08T16:05:00Z",
                    "created_at": "2026-05-08T16:00:00Z",
                    "updated_at": "2026-05-08T16:05:00Z",
                },
            ),
            (
                201,
                {
                    "item": {
                        "id": "lq_1",
                        "space_id": "sp_1",
                        "kind": "outside_question",
                        "family": "travel",
                        "prompt": "Ask the next grounded question.",
                        "priority": 75,
                        "status": "completed",
                        "claimed_by_agent_id": "agt_1",
                    "claim_generation": "9007199254740993",
                        "completed_at": "2026-05-08T16:05:00Z",
                        "created_at": "2026-05-08T16:00:00Z",
                        "updated_at": "2026-05-08T16:05:00Z",
                    },
                    "artifact": {
                        "id": "art_learning",
                        "space_id": "sp_1",
                        "kind": "neutral_learning_candidate",
                        "title": "Neutral learning candidate",
                        "media_type": "text/markdown",
                        "sha256": "hash",
                        "size_bytes": 42,
                        "moderation_status": "active",
                        "created_at": "2026-05-08T16:05:00Z",
                    },
                    "candidate": {
                        "id": "lcand_result",
                        "space_id": "sp_1",
                        "family": "travel",
                        "summary": "Neutral learning candidate",
                        "status": "candidate",
                        "source_artifact_ids": ["art_learning"],
                        "evaluation_ids": ["eval_learning"],
                        "created_at": "2026-05-08T16:05:00Z",
                        "updated_at": "2026-05-08T16:05:00Z",
                    },
                    "evaluation": {
                        "id": "eval_learning",
                        "space_id": "sp_1",
                        "artifact_id": "art_learning",
                        "evaluator_agent_id": "agt_1",
                        "moderation_status": "active",
                        "outcome": "pass",
                        "score": 82,
                        "rubric_key": "swarm.learning_queue.generation.v1",
                        "summary": "Connector generated a learning candidate.",
                        "created_at": "2026-05-08T16:05:00Z",
                    },
                },
            ),
            (
                200,
                {
                    "space_id": "sp_1",
                    "generated_at": "2026-05-08T16:00:00Z",
                    "queue_counts": {"queued": 1},
                    "candidate_counts": {"promoted": 1},
                    "projection_counts": {"promoted": 1},
                    "promotion_gate_counts": {"promoted": 1},
                    "promoted_projection_count": 1,
                    "average_validation_score": 0.91,
                    "queued_learning_work_count": 1,
                    "active_learning_work_count": 1,
                    "failed_promotion_candidate_count": 0,
                },
            ),
            (
                200,
                {
                    "space_id": "sp_1",
                    "query": "Swiss hiking routes",
                    "objective": "Prepare compact context.",
                    "token_budget": 1200,
                    "estimated_tokens": 128,
                    "estimator_version": "swarm.token_estimator.v1",
                    "trust_boundary": "Based only on visible Space memory.",
                    "context_text": "Trust boundary: Based only on visible Space memory.",
                    "items": [
                        {
                            "source_type": "artifact",
                            "source_id": "art_1",
                            "title": "Swiss route note",
                            "excerpt": "A compact route shortlist.",
                            "estimated_tokens": 24,
                            "score": 90,
                        }
                    ],
                    "omitted_item_count": 0,
                    "created_at": "2026-05-08T16:00:00Z",
                },
            ),
        ]:
            transport.responses.append(
                HTTPResponse(
                    status=status,
                    headers={"content-type": "application/json"},
                    body=json.dumps(payload).encode("utf-8"),
                )
            )
        queue_item = client.create_space_learning_queue_item(
            "sp_1",
            {
                "kind": "outside_question",
                "family": "travel",
                "prompt": "Ask the next grounded question.",
                "priority": 75,
            },
        )
        queue = client.list_space_learning_queue_items("sp_1", status="queued")
        claimed = client.claim_space_learning_queue_item(
            "sp_1", {"agent_id": "agt_operator", "kind": "outside_question", "claim_ttl_seconds": 300}
        )
        completed = client.complete_space_learning_queue_item("sp_1", claimed["id"], {"agent_id": "agt_operator", "claim_generation": claimed["claim_generation"]})
        submitted = client.submit_space_learning_queue_result(
            "sp_1",
            claimed["id"],
            {
                "agent_id": "agt_operator",
                "claim_generation": claimed["claim_generation"],
                "artifact": {
                    "kind": "neutral_learning_candidate",
                    "media_type": "text/markdown",
                    "title": "Neutral learning candidate",
                    "body": "Promote only after validation.",
                },
                "candidate": {"family": "travel"},
                "evaluation": {
                    "outcome": "pass",
                    "score": 82,
                    "rubric_key": "swarm.learning_queue.generation.v1",
                    "summary": "Connector generated a learning candidate.",
                },
            },
        )
        frontier = client.get_space_learning_frontier("sp_1")
        context_pack = client.build_space_context_pack(
            "sp_1",
            {
                "query": "Swiss hiking routes",
                "objective": "Prepare compact context.",
                "token_budget": 1200,
            },
        )

        self.assertEqual(answer["answer_id"], "ans_1")
        self.assertEqual(read_back["answer_id"], "ans_1")
        self.assertEqual(graph["space_id"], "sp_1")
        self.assertEqual(answer_contract["schema"], "swarm.space.answer_contract.v1")
        self.assertIn("real_user_question", answer_contract["supported_learning_queue_kinds"])
        self.assertIn("promoted", answer_contract["promotion_statuses"])
        self.assertIn("learning_candidate", answer_contract["connector_candidate_artifact_kind_suffixes"])
        self.assertIn("stdout", answer_contract["connector_diagnostic_artifact_kind_suffixes"])
        self.assertEqual(candidate["id"], "lcand_1")
        self.assertEqual(artifact_candidate["id"], "lcand_2")
        self.assertEqual(candidates.items[0]["id"], "lcand_1")
        self.assertEqual(projection["projection_id"], "proj_1")
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(promotion_gates.items[0]["candidate_id"], "lcand_1")
        self.assertEqual(projections.items[0]["projection_id"], "proj_1")
        self.assertEqual(queue_item["id"], "lq_1")
        self.assertEqual(queue.items[0]["id"], "lq_1")
        self.assertEqual(claimed["status"], "claimed")
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(submitted["candidate"]["id"], "lcand_result")
        self.assertEqual(frontier["queued_learning_work_count"], 1)
        self.assertEqual(context_pack["items"][0]["source_id"], "art_1")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/spaces/sp_1/ask"))
        self.assertTrue(transport.requests[1]["url"].endswith("/v1/spaces/sp_1/answers/ans_1"))
        self.assertTrue(transport.requests[2]["url"].endswith("/v1/spaces/sp_1/decision-graph"))
        self.assertTrue(transport.requests[3]["url"].endswith("/v1/spaces/sp_1/answer-contract"))
        self.assertTrue(transport.requests[4]["url"].endswith("/v1/spaces/sp_1/learning-candidates"))
        self.assertTrue(transport.requests[5]["url"].endswith("/v1/spaces/sp_1/learning-candidates/from-artifact"))
        self.assertTrue(transport.requests[6]["url"].endswith("/v1/spaces/sp_1/learning-candidates?status=promoted"))
        self.assertTrue(transport.requests[7]["url"].endswith("/v1/spaces/sp_1/learning-candidates/lcand_1/promote"))
        self.assertTrue(transport.requests[8]["url"].endswith("/v1/spaces/sp_1/learning-candidates/lcand_2/reject"))
        self.assertTrue(transport.requests[9]["url"].endswith("/v1/spaces/sp_1/promotion-gates?decision=promoted"))
        self.assertTrue(transport.requests[10]["url"].endswith("/v1/spaces/sp_1/projections?status=promoted"))
        self.assertTrue(transport.requests[11]["url"].endswith("/v1/spaces/sp_1/learning-queue"))
        self.assertTrue(transport.requests[12]["url"].endswith("/v1/spaces/sp_1/learning-queue?status=queued"))
        self.assertTrue(transport.requests[13]["url"].endswith("/v1/spaces/sp_1/learning-queue/claim"))
        self.assertTrue(transport.requests[14]["url"].endswith("/v1/spaces/sp_1/learning-queue/lq_1/complete"))
        self.assertTrue(transport.requests[15]["url"].endswith("/v1/spaces/sp_1/learning-queue/lq_1/result"))
        self.assertTrue(transport.requests[16]["url"].endswith("/v1/spaces/sp_1/learning-frontier"))
        self.assertTrue(transport.requests[17]["url"].endswith("/v1/spaces/sp_1/context-pack"))
        self.assertEqual(transport.requests[0]["body"]["question"], "What should we do next?")
        self.assertEqual(transport.requests[5]["body"]["artifact_id"], "art_1")
        self.assertEqual(transport.requests[8]["body"]["reason"], "Not useful.")
        self.assertEqual(transport.requests[13]["body"]["agent_id"], "agt_operator")
        self.assertEqual(transport.requests[14]["body"]["agent_id"], "agt_operator")
        self.assertEqual(transport.requests[14]["body"]["claim_generation"], "9007199254740993")
        self.assertEqual(transport.requests[15]["body"]["claim_generation"], "9007199254740993")
        self.assertEqual(transport.requests[15]["body"]["agent_id"], "agt_operator")
        self.assertEqual(transport.requests[15]["body"]["artifact"]["kind"], "neutral_learning_candidate")
        self.assertEqual(transport.requests[17]["body"]["token_budget"], 1200)

    def test_connected_chat_memory_methods_target_chat_scoped_routes(self) -> None:
        transport = FakeTransport()
        client = SwarmClient(base_url="https://api.example.test", token="token", transport=transport)
        for payload in (
            {
                "items": [{"source_id": "proj_1"}],
                "bundle_id": "",
                "cache_eligible": False,
                "context_identity": "context_capture:example",
                "coverage": {"version": "context_coverage_v1", "scope": "bounded_space_retrieval", "status": "partial", "excerpt_only_count": 1, "budget_omitted_count": 0, "required_omitted_count": 0, "rendered_bytes": 17},
            },
            {"answer_id": "ans_1", "confidence": "moderate"},
            {"artifact_id": "art_digest", "candidate_ids": ["lcand_1"]},
        ):
            transport.responses.append(
                HTTPResponse(
                    status=200,
                    headers={"content-type": "application/json"},
                    body=json.dumps(payload).encode("utf-8"),
                )
            )

        context_pack = client.build_connected_chat_context_pack({"query": "resume", "token_budget": 1200})
        answer = client.ask_connected_chat_space(
            {"question": "What changed?", "allow_learning_update": False},
            idempotency_key="ask-chat-2026-05-22",
        )
        digest = client.create_connected_chat_session_digest(
            {
                "summary": "Resolved the deployment issue.",
                "durable_decisions": ["Use the chat-scoped memory flow."],
                "create_memory_candidates": True,
            }
        )

        self.assertEqual(context_pack["items"][0]["source_id"], "proj_1")
        self.assertEqual(context_pack["bundle_id"], "")
        self.assertFalse(context_pack["cache_eligible"])
        self.assertEqual(context_pack["context_identity"], "context_capture:example")
        self.assertEqual(context_pack["coverage"]["status"], "partial")
        self.assertEqual(answer["answer_id"], "ans_1")
        self.assertEqual(digest["artifact_id"], "art_digest")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/chat/context-pack"))
        self.assertTrue(transport.requests[1]["url"].endswith("/v1/chat/ask"))
        self.assertTrue(transport.requests[2]["url"].endswith("/v1/chat/session-digest"))
        self.assertEqual(transport.requests[0]["body"]["token_budget"], 1200)
        self.assertEqual(transport.requests[1]["body"]["allow_learning_update"], False)
        self.assertEqual(transport.requests[1]["headers"]["Idempotency-Key"], "ask-chat-2026-05-22")
        self.assertEqual(transport.requests[2]["body"]["durable_decisions"][0], "Use the chat-scoped memory flow.")

    def test_list_space_answers_targets_paginated_answer_history_with_encoded_path_segments(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={
                    "content-type": "application/json",
                    "x-next-cursor": "cur_2",
                    "x-order": "created_at_desc",
                },
                body=json.dumps(
                    [
                        {
                            "answer_id": "ans_1",
                            "space_id": "sp/a",
                            "question": "What changed?",
                            "answer": "Answer history is available.",
                            "confidence": "moderate",
                            "trust_boundary": "Based only on visible Space memory.",
                            "supporting_artifacts": [],
                            "used_projections": [],
                            "used_strategies": [],
                            "gaps": [],
                            "follow_up_learning_candidates": [],
                            "created_at": "2026-05-08T16:00:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        answers = client.list_space_answers("sp/a", limit=1, cursor="cur 1")

        self.assertEqual(answers.items[0]["answer_id"], "ans_1")
        self.assertEqual(answers.next_cursor, "cur_2")
        self.assertEqual(answers.order, "created_at_desc")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/spaces/sp%2Fa/answers?limit=1&cursor=cur+1"))

    def test_webhook_helpers_target_operator_webhook_endpoints(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps([{"action": "run.succeeded", "label": "Run succeeded", "description": "Run completed successfully"}]).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    [
                        {
                            "key": "chat_follow_up",
                            "label": "Chat Follow-Up",
                            "description": "Push follow-up for chat clients",
                            "included_actions": ["run.succeeded", "artifact.created"],
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    [
                        {
                            "id": "wht_1",
                            "space_id": "ws_1",
                            "name": "Ops Hook",
                            "url": "https://hooks.example/swarm",
                            "included_actions": ["run.succeeded"],
                            "has_signing_secret": True,
                            "max_attempts": 5,
                            "initial_backoff_seconds": 10,
                            "max_backoff_seconds": 300,
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        target_body = {
            "id": "wht_1",
            "space_id": "ws_1",
            "name": "Ops Hook",
            "url": "https://hooks.example/swarm",
            "included_actions": ["run.succeeded"],
            "has_signing_secret": True,
            "max_attempts": 5,
            "initial_backoff_seconds": 10,
            "max_backoff_seconds": 300,
            "created_at": "2026-03-12T00:00:00Z",
            "updated_at": "2026-03-12T00:00:00Z",
        }
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(target_body).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    [
                        {
                            "key": "chat_follow_up",
                            "label": "Chat Follow-Up",
                            "description": "Push follow-up for chat clients",
                            "included_actions": ["run.succeeded", "artifact.created"],
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(target_body).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(target_body).encode("utf-8"),
            )
        )
        updated_target_body = dict(target_body)
        updated_target_body["name"] = "Ops Hook Updated"
        updated_target_body["included_actions"] = ["run.succeeded", "artifact.created"]
        updated_target_body["max_attempts"] = 6
        updated_target_body["updated_at"] = "2026-03-12T01:00:00Z"
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(updated_target_body).encode("utf-8"),
            )
        )
        disabled_target_body = dict(target_body)
        disabled_target_body["disabled_at"] = "2026-03-12T02:00:00Z"
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(disabled_target_body).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "target": target_body,
                        "state": {
                            "target_id": "wht_1",
                            "last_audit_sequence": 42,
                            "dead_letter_count": 0,
                            "updated_at": "2026-03-12T00:10:00Z",
                        },
                        "queued_deliveries": [],
                        "processed_count": 1,
                    }
                ).encode("utf-8"),
            )
        )
        delivery_body = {
            "id": "whd_1",
            "target_id": "wht_1",
            "space_id": "ws_1",
            "audit_event_id": "evt_1",
            "target_url": "https://hooks.example/swarm",
            "payload": "{}",
            "signature": "abc",
            "status": "failed",
            "attempt_count": 3,
            "failure_count": 3,
            "created_at": "2026-03-12T00:00:00Z",
            "updated_at": "2026-03-12T00:10:00Z",
        }
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps([delivery_body]).encode("utf-8"),
            )
        )
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(delivery_body).encode("utf-8"),
            )
        )
        redriven_delivery_body = dict(delivery_body)
        redriven_delivery_body["status"] = "pending"
        redriven_delivery_body["updated_at"] = "2026-03-12T00:11:00Z"
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(redriven_delivery_body).encode("utf-8"),
            )
        )

        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        actions = client.list_webhook_actions()
        presets = client.list_webhook_action_presets()
        targets = client.list_webhook_targets(space_id="ws_1", active_only=True)
        created = client.create_webhook_target(
            {
                "space_id": "ws_1",
                "name": "Ops Hook",
                "url": "https://hooks.example/swarm",
                "included_actions": ["run.succeeded"],
            }
        )
        created_from_preset = client.create_webhook_target_from_preset(
            "chat_follow_up",
            {
                "space_id": "ws_1",
                "name": "Chat Hook",
                "url": "https://hooks.example/swarm/chat",
            },
        )
        target = client.get_webhook_target("wht_1")
        updated = client.update_webhook_target(
            "wht_1",
            {
                "name": "Ops Hook Updated",
                "url": "https://hooks.example/swarm",
                "included_actions": ["run.succeeded", "artifact.created"],
                "max_attempts": 6,
            },
        )
        disabled = client.disable_webhook_target("wht_1")
        reconciled = client.reconcile_webhook_target("wht_1", limit=5)
        deliveries = client.list_webhook_deliveries(space_id="ws_1", status="failed")
        delivery = client.get_webhook_delivery("whd_1")
        redriven = client.redrive_webhook_delivery("whd_1")

        self.assertEqual(actions[0]["action"], "run.succeeded")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/webhook-actions"))
        self.assertEqual(presets[0]["key"], "chat_follow_up")
        self.assertTrue(transport.requests[1]["url"].endswith("/v1/webhook-action-presets"))
        self.assertIn("/v1/webhook-targets?", transport.requests[2]["url"])
        self.assertIn("space_id=ws_1", transport.requests[2]["url"])
        self.assertIn("active_only=true", transport.requests[2]["url"])
        self.assertEqual(targets.items[0]["id"], "wht_1")
        self.assertEqual(created["id"], "wht_1")
        self.assertTrue(transport.requests[3]["url"].endswith("/v1/webhook-targets"))
        self.assertEqual(created_from_preset["id"], "wht_1")
        self.assertEqual(transport.requests[5]["body"]["included_actions"], ["run.succeeded", "artifact.created"])
        self.assertEqual(target["id"], "wht_1")
        self.assertEqual(updated["name"], "Ops Hook Updated")
        self.assertTrue(transport.requests[7]["url"].endswith("/v1/webhook-targets/wht_1/update"))
        self.assertEqual(disabled["disabled_at"], "2026-03-12T02:00:00Z")
        self.assertEqual(reconciled["processed_count"], 1)
        self.assertTrue(transport.requests[9]["url"].endswith("/v1/webhook-targets/wht_1/reconcile"))
        self.assertEqual(deliveries.items[0]["id"], "whd_1")
        self.assertEqual(delivery["id"], "whd_1")
        self.assertEqual(redriven["status"], "pending")

    def test_search_targets_authorized_search_endpoint(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={
                    "content-type": "application/json",
                    "x-next-cursor": "search_cursor_2",
                    "x-order": "desc",
                },
                body=json.dumps(
                    [
                        {
                            "resource_type": "run",
                            "resource_id": "run_1",
                            "entity_id": "ent_1",
                            "space_id": "ws_1",
                            "visibility_scope": "authorized",
                            "title": "run_1",
                            "snippet": "Task routing failed once and recovered.",
                            "score": 42,
                            "created_at": "2026-03-11T00:00:00Z",
                            "updated_at": "2026-03-11T00:05:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        page = client.search(q="routing failed", space_id="ws_1", resource_type="run", limit=5)

        self.assertEqual(len(page.items), 1)
        self.assertEqual(page.items[0]["resource_id"], "run_1")
        self.assertEqual(page.next_cursor, "search_cursor_2")
        self.assertEqual(page.order, "desc")
        self.assertTrue(
            transport.requests[0]["url"].endswith("/v1/search?q=routing+failed&space_id=ws_1&resource_type=run&limit=5")
        )

    def test_search_public_targets_public_search_endpoint(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={
                    "content-type": "application/json",
                    "x-next-cursor": "",
                    "x-order": "desc",
                },
                body=json.dumps(
                    [
                        {
                            "resource_type": "space",
                            "resource_id": "ws_1",
                            "entity_id": "ent_1",
                            "space_id": "ws_1",
                            "visibility_scope": "public",
                            "title": "Open Research",
                            "snippet": "Public space for research collaboration.",
                            "score": 87,
                            "created_at": "2026-03-11T00:00:00Z",
                            "updated_at": "2026-03-11T00:05:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        page = client.search_public(q="open research", resource_type="space", limit=5)

        self.assertEqual(len(page.items), 1)
        self.assertEqual(page.items[0]["visibility_scope"], "public")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/search/public?q=open+research&resource_type=space&limit=5"))
