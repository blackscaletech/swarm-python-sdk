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


class SwarmSDKExecutionTests(unittest.TestCase):
    def test_launch_route_is_normalized_and_part_of_retry_identity(self) -> None:
        from swarm_sdk._client_inputs import _normalize_launch_run_input

        legacy = {"title": "Review", "description": "Preserve the original objective."}
        routed = {**legacy, "model_route_policy_id": " route_a "}
        self.assertEqual(_normalize_launch_run_input(routed)["model_route_policy_id"], "route_a")
        self.assertNotEqual(build_launch_run_idempotency_key("sp_1", routed),
                            build_launch_run_idempotency_key("sp_1", {**routed, "model_route_policy_id": "route_b"}))
        self.assertEqual(build_launch_run_idempotency_key("sp_1", legacy),
                         build_launch_run_idempotency_key("sp_1", {**legacy, "model_route_policy_id": " "}))

    def test_launch_run_targets_space_scoped_launch_endpoint(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "task": {
                            "id": "task_1",
                            "space_id": "ws_1",
                            "title": "Launch alpha run",
                            "description": "Create a task and run together.",
                            "created_at": "2026-03-11T00:00:00Z",
                        },
                        "run": {
                            "id": "run_1",
                            "space_id": "ws_1",
                            "task_id": "task_1",
                            "status": "queued",
                            "attempt": 1,
                            "execution_policy": {
                                "max_runtime_seconds": 300,
                                "max_tool_calls": 32,
                                "max_external_requests": 0,
                                "allow_network": False,
                                "allow_shell": False,
                            },
                            "created_at": "2026-03-11T00:00:00Z",
                            "updated_at": "2026-03-11T00:00:00Z",
                        },
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        launched = client.launch_space_run(
            "ws_1",
            title="Launch alpha run",
            description="Create a task and run together.",
        )

        self.assertEqual(launched["task"]["id"], "task_1")
        self.assertEqual(launched["run"]["task_id"], "task_1")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/spaces/ws_1/launch-run"))
        self.assertEqual(transport.requests[0]["body"]["title"], "Launch alpha run")

    def test_launch_run_derives_stable_idempotency_key_for_identical_input(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                HTTPResponse(
                    status=201,
                    headers={"content-type": "application/json"},
                    body=json.dumps({"task": {"id": "task_1"}, "run": {"id": "run_1", "task_id": "task_1"}}).encode("utf-8"),
                ),
                HTTPResponse(
                    status=201,
                    headers={"content-type": "application/json"},
                    body=json.dumps({"task": {"id": "task_1"}, "run": {"id": "run_1", "task_id": "task_1"}}).encode("utf-8"),
                ),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        client.launch_space_run("ws_1", title="Launch alpha run", description="Create a task and run together.")
        client.launch_space_run("ws_1", title="Launch alpha run", description="Create a task and run together.")

        first_key = transport.requests[0]["headers"]["Idempotency-Key"]
        second_key = transport.requests[1]["headers"]["Idempotency-Key"]
        self.assertEqual(first_key, second_key)
        self.assertEqual(
            first_key,
            build_launch_run_idempotency_key(
                "ws_1",
                {"title": "Launch alpha run", "description": "Create a task and run together."},
            ),
        )

    def test_build_launch_run_idempotency_key_changes_when_intent_key_changes(self) -> None:
        payload = {
            "title": "Launch alpha run",
            "description": "Create a task and run together.",
        }
        self.assertNotEqual(
            build_launch_run_idempotency_key("ws_1", payload, intent_key="retry-now"),
            build_launch_run_idempotency_key("ws_1", payload, intent_key="launch-later"),
        )

    def test_launch_from_prompt_defaults_compiled_capture_and_self_targets_runtime(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps({"task": {"id": "task_1"}, "run": {"id": "run_1", "task_id": "task_1"}}).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="runtime-token", transport=transport)

        client.launch_from_prompt(
            "ws_1",
            {
                "title": "Research islands",
                "source_prompt": "Research beautiful islands and produce a ranked brief.",
                "target_agent_id": "agt_1",
            },
        )

        self.assertEqual(transport.requests[0]["body"]["prompt_capture_mode"], "compiled_only")
        self.assertEqual(transport.requests[0]["body"]["run_requirements"]["target_agent_ids"], ["agt_1"])
        self.assertEqual(
            transport.requests[0]["body"]["source_prompt"],
            "Research beautiful islands and produce a ranked brief.",
        )

    def test_build_launch_run_idempotency_key_changes_when_requirements_change(self) -> None:
        first_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "title": "Launch alpha run",
                "description": "Create a task and run together.",
                "run_requirements": {
                    "required_capabilities": ["repo.read"],
                    "required_hosting_mode": "remote",
                },
            },
        )
        second_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "title": "Launch alpha run",
                "description": "Create a task and run together.",
                "run_requirements": {
                    "required_capabilities": ["artifact.write"],
                    "required_hosting_mode": "remote",
                },
            },
        )
        result_gated_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "title": "Launch alpha run",
                "description": "Create a task and run together.",
                "run_requirements": {
                    "required_capabilities": ["repo.read"],
                    "required_hosting_mode": "remote",
                    "result_approval_actions": ["approve.agent_result"],
                },
            },
        )

        self.assertNotEqual(first_key, second_key)
        self.assertNotEqual(first_key, result_gated_key)

    def test_webhook_helpers_compute_parse_and_verify_signatures(self) -> None:
        body = "{\"delivery_type\":\"audit_event\"}"
        signature = compute_webhook_signature("secret-1", body)
        parsed = parse_webhook_headers(
            {
                "X-Swarm-Delivery-ID": "whd_1",
                "X-Swarm-Audit-Event-ID": "evt_1",
                "X-Swarm-Event-Action": "run.succeeded",
                "X-Swarm-Event-Sequence": "42",
                "X-Swarm-Signature": signature,
                "X-Swarm-Signature-Version": "v1",
            }
        )

        self.assertEqual(parsed["delivery_id"], "whd_1")
        self.assertEqual(parsed["audit_event_id"], "evt_1")
        self.assertEqual(parsed["event_action"], "run.succeeded")
        self.assertEqual(parsed["event_sequence"], 42)
        self.assertEqual(parsed["signature_version"], "v1")
        self.assertTrue(
            verify_webhook_signature(secret="secret-1", body=body, signature=signature, signature_version="v1")
        )
        self.assertFalse(
            verify_webhook_signature(secret="secret-2", body=body, signature=signature, signature_version="v1")
        )
        self.assertFalse(
            verify_webhook_signature(secret="secret-1", body=body, signature=signature, signature_version="v2")
        )

    def test_build_launch_run_idempotency_key_changes_when_context_refs_change(self) -> None:
        first_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "title": "Launch alpha run",
                "description": "Create a task and run together.",
                "run_context_refs": [
                    {"resource_type": "artifact", "resource_id": "art_1"},
                ],
            },
        )
        second_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "title": "Launch alpha run",
                "description": "Create a task and run together.",
                "run_context_refs": [
                    {"resource_type": "artifact", "resource_id": "art_2"},
                ],
            },
        )

        self.assertNotEqual(first_key, second_key)

    def test_build_launch_run_idempotency_key_changes_when_automation_profile_and_prompt_capture_change(self) -> None:
        first_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "automation_profile_id": "auto_1",
                "prompt_capture_mode": "compiled_only",
                "compiled_brief": "Compile a concise execution plan.",
            },
        )
        second_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "automation_profile_id": "auto_2",
                "prompt_capture_mode": "raw_and_compiled",
                "source_prompt": "raw operator prompt",
                "compiled_brief": "Compile a concise execution plan.",
            },
        )

        self.assertNotEqual(first_key, second_key)

    def test_build_launch_run_idempotency_key_changes_when_prompt_visibility_semantics_change(self) -> None:
        first_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "title": "Launch alpha run",
                "prompt_capture_mode": "raw",
                "prompt_visibility_mode": "space_private",
                "source_prompt": "Plan a launch",
                "compiled_brief": "Compiled brief",
            },
        )
        second_key = build_launch_run_idempotency_key(
            "ws_1",
            {
                "title": "Launch alpha run",
                "prompt_capture_mode": "raw",
                "prompt_visibility_mode": "public_raw",
                "acknowledge_public_raw_prompt_visibility": True,
                "source_prompt": "Plan a launch",
                "compiled_brief": "Compiled brief",
            },
        )

        self.assertNotEqual(first_key, second_key)

    def test_create_artifact_uses_stable_idempotency_while_heartbeat_remains_operation_scoped(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                HTTPResponse(status=201, headers={"content-type": "application/json"}, body=json.dumps({"id": "art_1"}).encode("utf-8")),
                HTTPResponse(status=201, headers={"content-type": "application/json"}, body=json.dumps({"id": "art_1"}).encode("utf-8")),
                HTTPResponse(status=200, headers={"content-type": "application/json"}, body=json.dumps({"agent_id": "agt_1"}).encode("utf-8")),
                HTTPResponse(status=200, headers={"content-type": "application/json"}, body=json.dumps({"agent_id": "agt_1"}).encode("utf-8")),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="runtime-token", transport=transport)

        artifact_payload = {
            "space_id": "ws_1",
            "run_id": "run_1",
            "kind": "executor_stdout",
            "media_type": "text/plain",
            "body": "stdout",
        }

        client.create_artifact(artifact_payload)
        client.create_artifact(artifact_payload)
        client.heartbeat_agent("agt_1", {"status": "idle"})
        client.heartbeat_agent("agt_1", {"status": "idle"})

        self.assertEqual(
            transport.requests[0]["headers"]["Idempotency-Key"],
            transport.requests[1]["headers"]["Idempotency-Key"],
        )
        self.assertNotEqual(
            transport.requests[2]["headers"]["Idempotency-Key"],
            transport.requests[3]["headers"]["Idempotency-Key"],
        )

    def test_get_run_context_targets_resolved_context_route(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "run": {
                            "id": "run_1",
                            "space_id": "ws_1",
                            "task_id": "task_1",
                            "status": "queued",
                            "attempt": 1,
                            "execution_policy": {
                                "max_runtime_seconds": 300,
                                "max_tool_calls": 32,
                                "max_external_requests": 0,
                                "allow_network": False,
                                "allow_shell": False,
                            },
                            "run_requirements": {},
                            "created_at": "2026-03-11T00:00:00Z",
                            "updated_at": "2026-03-11T00:00:00Z",
                        },
                        "context_refs": [],
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        context = client.get_run_context("run_1")

        self.assertEqual(context["run"]["id"], "run_1")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/runs/run_1/context"))

    def test_automation_profile_helpers_target_expected_routes(self) -> None:
        transport = FakeTransport()
        for status in (201, 200, 200, 200):
            transport.responses.append(
                HTTPResponse(
                    status=status,
                    headers={"content-type": "application/json"},
                    body=json.dumps(
                        {
                            "id": "auto_1",
                            "space_id": "ws_1",
                            "key": "research-alpha",
                            "name": "Research Alpha",
                            "compiler_key": "research.review_remediate_finalize.v1",
                            "compiler_version": "1",
                            "launch_template_id": "lt_1",
                            "review_threshold": 85,
                            "prompt_capture_mode": "compiled_only",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        }
                    ).encode("utf-8"),
                )
            )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        create_input = {
            "space_id": "ws_1",
            "key": "research-alpha",
            "name": "Research Alpha",
            "compiler_key": "research.review_remediate_finalize.v1",
            "review_threshold": 85,
            "prompt_capture_mode": "compiled_only",
            "launch_template": {
                "name": "Research Launch",
                "title": "Research topic",
                "description": "Produce a research draft.",
            },
            "review_task": {
                "title": "Review draft",
                "description": "Review the generated draft.",
            },
            "remediation_task": {
                "title": "Remediate draft",
                "description": "Improve the generated draft.",
            },
            "finalize_task": {
                "title": "Finalize draft",
                "description": "Finalize the generated draft.",
            },
        }

        client.create_automation_profile(create_input)
        client.get_automation_profile("auto_1")
        client.update_automation_profile("auto_1", {**create_input, "prompt_capture_mode": "raw_and_compiled"})
        client.disable_automation_profile("auto_1")

        self.assertTrue(transport.requests[0]["url"].endswith("/v1/automation-profiles"))
        self.assertTrue(transport.requests[1]["url"].endswith("/v1/automation-profiles/auto_1"))
        self.assertTrue(transport.requests[2]["url"].endswith("/v1/automation-profiles/auto_1/update"))
        self.assertTrue(transport.requests[3]["url"].endswith("/v1/automation-profiles/auto_1/disable"))

    def test_run_connected_core_once_executes_canonical_lifecycle(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                json_response({}),
                json_response(
                    {
                        "run": {
                            "id": "run_1",
                            "space_id": "ws_1",
                            "status": "leased",
                        },
                        "run_lease": {
                            "id": "lease_1",
                            "run_id": "run_1",
                            "core_id": "core_1",
                            "leased_by_identity_id": "idn_core_1",
                            "status": "active",
                        },
                    }
                ),
                json_response({"id": "run_1", "status": "running"}),
                json_response({"id": "lease_1", "status": "active"}),
                json_response(
                    {
                        "run": {"id": "run_1", "status": "succeeded"},
                        "output": {"status": "complete"},
                    }
                ),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="core-token", transport=transport)

        result = run_connected_core_once(
            client,
            ttl_seconds=60,
        )

        self.assertEqual(result["run"]["status"], "succeeded")
        self.assertEqual(result["run_lease"]["status"], "active")
        self.assertEqual(result["execution"]["output"]["status"], "complete")
        self.assertEqual(
            [request["url"].split("https://swarm.example")[-1] for request in transport.requests],
            [
                "/v1/core-runner/heartbeat",
                "/v1/core-runner/runs/claim-next",
                "/v1/core-runs/run_1/leases/lease_1/start",
                "/v1/core-runs/run_1/leases/lease_1/renew",
                "/v1/core-runs/run_1/leases/lease_1/execute",
            ],
        )

    def test_run_connected_core_loop_stops_after_bounded_idle_callback(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                json_response({}),
                HTTPResponse(status=204, headers={"content-type": "application/json"}, body=b""),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="core-token", transport=transport)

        class StopLoop(RuntimeError):
            pass

        def on_idle() -> None:
            raise StopLoop("stop")

        with self.assertRaisesRegex(StopLoop, "stop"):
            run_connected_core_loop(
                client,
                poll_interval_seconds=5.0,
                on_idle=on_idle,
            )

        self.assertEqual(
            [request["url"].split("https://swarm.example")[-1] for request in transport.requests],
            [
                "/v1/core-runner/heartbeat",
                "/v1/core-runner/runs/claim-next",
            ],
        )

    def test_stream_audit_events_parses_sse_blocks(self) -> None:
        transport = FakeTransport()
        transport.stream_payload = (
            b"retry: 1000\n\n"
            b"id: cursor_1\n"
            b"event: audit_event\n"
            b'data: {"id":"evt_1","actor_id":"agt_1","resource_type":"run","resource_id":"run_1","action":"run.started","occurred_at":"2026-03-11T00:00:00Z"}\n\n'
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        events: list[dict[str, object]] = []
        last_event_id = client.stream_audit_events(
            lambda event, meta: events.append({"event": event, "meta": meta}),
            space_id="ws_1",
            limit=10,
        )

        self.assertEqual(last_event_id, "cursor_1")
        self.assertEqual(events[0]["event"]["id"], "evt_1")
        self.assertEqual(events[0]["meta"]["event"], "audit_event")

    def test_connected_core_loop_delay_is_bounded_and_jittered(self) -> None:
        self.assertEqual(compute_connected_core_loop_delay_seconds(0, 1.0, 0.0), 0.75)
        self.assertEqual(compute_connected_core_loop_delay_seconds(0, 1.0, 1.0), 1.0)
        self.assertEqual(compute_connected_core_loop_delay_seconds(10, 10.0, 1.0), 30.0)

    def test_list_runtime_hints_targets_agent_collection(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={
                    "content-type": "application/json",
                    "x-next-cursor": "7",
                    "x-order": "desc",
                },
                body=json.dumps(
                    [
                        {
                            "id": "hint_1",
                            "sequence": 7,
                            "space_id": "ws_1",
                            "agent_scope_mode": "target_agents",
                            "target_agent_ids": ["agt_1"],
                            "hint_key": "run.available:run_1:1:run:run_1",
                            "kind": "run.available",
                            "run_id": "run_1",
                            "task_id": "task_1",
                            "source_resource_type": "run",
                            "source_resource_id": "run_1",
                            "current_status": "queued",
                            "created_at": "2026-03-13T12:00:00Z",
                            "expires_at": "2026-03-16T12:00:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        page = client.list_runtime_hints("agt_1", kind="run.available", limit=5)

        self.assertEqual(len(page.items), 1)
        self.assertEqual(page.items[0]["kind"], "run.available")
        self.assertEqual(page.next_cursor, "7")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/agents/agt_1/runtime-hints?kind=run.available&limit=5"))

    def test_stream_runtime_hints_parses_sse_blocks(self) -> None:
        transport = FakeTransport()
        transport.stream_payload = (
            b"retry: 1000\n\n"
            b"id: 7\n"
            b"event: runtime_hint\n"
            b'data: {"id":"hint_1","sequence":7,"space_id":"ws_1","agent_scope_mode":"space_any","hint_key":"run.available:run_1:1:run:run_1","kind":"run.available","run_id":"run_1","task_id":"task_1","source_resource_type":"run","source_resource_id":"run_1","current_status":"queued","created_at":"2026-03-13T12:00:00Z","expires_at":"2026-03-16T12:00:00Z"}\n\n'
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        events: list[dict[str, object]] = []
        last_event_id = client.stream_runtime_hints(
            "agt_1",
            lambda event, meta: events.append({"event": event, "meta": meta}),
            kind="run.available",
            limit=10,
        )

        self.assertEqual(last_event_id, "7")
        self.assertEqual(events[0]["event"]["id"], "hint_1")
        self.assertEqual(events[0]["meta"]["event"], "runtime_hint")

    def test_request_raises_swarm_api_error(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=403,
                headers={"content-type": "application/json"},
                body=b'{"error":"forbidden"}',
            )
        )
        client = SwarmClient(base_url="https://swarm.example", transport=transport)

        with self.assertRaises(SwarmAPIError) as ctx:
            client.get_space("ws_1")

        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(ctx.exception.body, {"error": "forbidden"})

    def test_operator_and_governance_helpers_target_expected_routes(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                json_list_response([]),
                json_list_response([]),
                json_list_response([]),
                json_list_response([]),
                json_list_response([]),
                json_list_response([]),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        client.list_agents(space_id="ws_1", limit=25, order="asc")
        client.list_agent_capability_manifests(agent_id="agt_1", order="desc")
        client.list_agent_presence(space_id="ws_1", status="busy", limit=10)
        client.list_space_operator_activity("ws_1", resource_type="run", limit=5)
        client.list_space_operator_presence_entries("ws_1", status="online", order="desc")
        client.list_space_signed_status_entries("ws_1", agent_id="agt_1", action="artifact.created", limit=7)

        urls = [request["url"] for request in transport.requests]
        self.assertTrue(urls[0].endswith("/v1/agents?space_id=ws_1&limit=25&order=asc"))
        self.assertTrue(urls[1].endswith("/v1/agent-capability-manifests?agent_id=agt_1&order=desc"))
        self.assertTrue(urls[2].endswith("/v1/agent-presence?space_id=ws_1&status=busy&limit=10"))
        self.assertTrue(urls[3].endswith("/v1/operator/spaces/ws_1/activity?resource_type=run&limit=5"))
        self.assertTrue(urls[4].endswith("/v1/operator/spaces/ws_1/presence?status=online&order=desc"))
        self.assertTrue(
            urls[5].endswith("/v1/operator/spaces/ws_1/signed-status?agent_id=agt_1&action=artifact.created&limit=7")
        )

    def test_operator_live_snapshot_and_observability_target_expected_routes(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                json_response(
                    {
                        "summary": {
                            "space_id": "ws_1",
                            "entity_id": "ent_1",
                            "key": "ops",
                            "name": "Ops",
                            "directory_visible": False,
                            "agent_count": 1,
                            "manifest_backed_agent_count": 1,
                            "active_run_lease_count": 1,
                            "webhook_target_count": 0,
                            "pending_webhook_deliveries": 0,
                            "dead_letter_webhook_deliveries": 0,
                            "runtime_receipt_count": 0,
                            "active_runtime_credential_count": 1,
                            "budget_account_count": 0,
                            "active_budget_delegation_count": 0,
                            "pending_approval_request_count": 0,
                            "run_counts": {},
                            "presence_counts": {},
                        },
                        "activity": [],
                        "presence": [],
                        "signed_status": [],
                        "sessions": [],
                        "governance": {
                            "space_id": "ws_1",
                            "active_credential_count": 1,
                            "budget_account_count": 0,
                            "active_budget_delegations": 0,
                            "pending_approval_requests": 0,
                            "active_credentials": [],
                            "budget_accounts": [],
                            "active_delegations": [],
                            "pending_approval_queue": [],
                        },
                        "runs": [],
                    }
                ),
                json_response(
                    {
                        "run_acquisition": {
                            "count": 1,
                            "error_count": 0,
                            "total_duration_ms": 12,
                            "last_duration_ms": 12,
                            "max_duration_ms": 12,
                            "last_candidate_count": 1,
                            "max_candidate_count": 1,
                        },
                        "run_context_resolution": {
                            "count": 1,
                            "error_count": 0,
                            "total_duration_ms": 4,
                            "last_duration_ms": 4,
                            "max_duration_ms": 4,
                            "last_payload_bytes": 128,
                            "max_payload_bytes": 128,
                        },
                        "artifact_search_indexing": {
                            "count": 2,
                            "error_count": 0,
                            "total_duration_ms": 3,
                            "last_duration_ms": 1,
                            "max_duration_ms": 2,
                            "last_payload_bytes": 64,
                            "max_payload_bytes": 128,
                        },
                    }
                ),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        live = client.get_space_operator_live_snapshot("ws_1", activity_limit=2, run_limit=1)
        observability = client.get_control_plane_observability()

        self.assertEqual(live["summary"]["space_id"], "ws_1")
        self.assertEqual(observability["run_acquisition"]["count"], 1)
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/operator/spaces/ws_1/live?activity_limit=2&run_limit=1"))
        self.assertTrue(transport.requests[1]["url"].endswith("/v1/operator/control-plane-observability"))
