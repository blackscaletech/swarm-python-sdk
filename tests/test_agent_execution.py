import unittest

from swarm_sdk import SwarmClient
from sdk_test_support import FakeTransport, json_response


class AgentExecutionTests(unittest.TestCase):
    def test_agent_execution_routes_preserve_scope_lease_and_retry_identity(self):
        transport = FakeTransport()
        client = SwarmClient(base_url="https://swarm.example", token="synthetic", transport=transport)
        key = "explicit-retry-key"
        version = "2026-10-12T12:00:00.000001Z"
        cases = [
            (lambda: client.launch_run("sp/1", goal_summary="Compare the sources", run_requirements={"execution_mode": "agent", "assignment_mode": "others"}, idempotency_key=key), "/v1/foundation/spaces/sp%2F1/runs:launch", {"goal_summary": "Compare the sources", "run_requirements": {"execution_mode": "agent", "assignment_mode": "others"}}),
            (lambda: client.create_run_group("sp/1", {"primary_task_id": "task_1", "goal_summary": "Review"}, idempotency_key=key), "/v1/foundation/spaces/sp%2F1/run-groups", {"primary_task_id": "task_1", "goal_summary": "Review"}),
            (lambda: client.create_group_run("rg/1", {"goal_summary": "Verify the evidence", "run_requirements": {"execution_mode": "agent"}}, idempotency_key=key), "/v1/foundation/run-groups/rg%2F1/runs", {"goal_summary": "Verify the evidence", "run_requirements": {"execution_mode": "agent"}}),
            (lambda: client.pause_run_group("rg/1", idempotency_key=key), "/v1/foundation/run-groups/rg%2F1:pause", {}),
            (lambda: client.resume_run_group("rg/1", idempotency_key=key), "/v1/foundation/run-groups/rg%2F1:resume", {}),
            (lambda: client.cancel_run_group("rg/1", idempotency_key=key), "/v1/foundation/run-groups/rg%2F1:cancel", {}),
            (lambda: client.create_task({"space_id": "sp_1", "title": "Brief", "description": "Exact evidence"}, idempotency_key=key), "/v1/tasks", {"space_id": "sp_1", "title": "Brief", "description": "Exact evidence"}),
            (lambda: client.update_task("task/1", {"title": "Edited brief", "description": "Exact evidence", "expected_updated_at": version}, idempotency_key=key), "/v1/tasks/task%2F1", {"title": "Edited brief", "description": "Exact evidence", "expected_updated_at": version}),
            (lambda: client.cancel_task("task/1", {"reason": "Replaced"}, idempotency_key=key), "/v1/tasks/task%2F1/cancel", {"reason": "Replaced"}),
            (lambda: client.launch_task_run("task/1", {"expected_task_updated_at": version}, idempotency_key=key), "/v1/foundation/tasks/task%2F1/runs:launch", {"expected_task_updated_at": version}),
            (lambda: client.acquire_run("run/1", ttl_seconds=120, idempotency_key=key), "/v1/runs/run%2F1/acquire", {"ttl_seconds": 120}),
            (lambda: client.claim_next_run("sp/1", cursor="bounded-scan", ttl_seconds=120, idempotency_key=key), "/v1/spaces/sp%2F1/runs:claim-next", {"cursor": "bounded-scan", "ttl_seconds": 120}),
            (lambda: client.declare_executor_capabilities("sp/1", {"tools": ["evidence.read"]}, idempotency_key=key), "/v1/spaces/sp%2F1/executor-capabilities", {"tools": ["evidence.read"]}),
            (lambda: client.start_run("run/1", "lease_1", idempotency_key=key), "/v1/runs/run%2F1/start", {"lease_id": "lease_1"}),
            (lambda: client.request_run_stop("run/1", "Replaced", idempotency_key=key), "/v1/runs/run%2F1/stop", {"reason": "Replaced"}),
            (lambda: client.complete_run("run/1", {"lease_id": "lease_1", "outcome": "failed", "body": "Source unavailable"}, idempotency_key=key), "/v1/runs/run%2F1/complete", {"lease_id": "lease_1", "outcome": "failed", "body": "Source unavailable"}),
            (lambda: client.renew_run_lease("lease/1", 120, idempotency_key=key), "/v1/run-leases/lease%2F1/renew", {"ttl_seconds": 120}),
            (lambda: client.release_run_lease("lease/1", idempotency_key=key), "/v1/run-leases/lease%2F1/release", {}),
            (lambda: client.launch_task_run("task/1", {"run_requirements": {"assignment_mode": "self"}}, idempotency_key=key), "/v1/foundation/tasks/task%2F1/runs:launch", {"run_requirements": {"assignment_mode": "self"}}),
            (lambda: client.spawn_subrun("run/1", {"spawn_reason_key": "review"}, idempotency_key=key), "/v1/foundation/runs/run%2F1/subruns", {"spawn_reason_key": "review"}),
        ]
        for invoke, path, body in cases:
            with self.subTest(path=path):
                transport.responses.append(json_response({"id": "run_synthetic"}))
                invoke()
                request = transport.requests[-1]
                self.assertEqual(request["url"], "https://swarm.example" + path)
                self.assertEqual(request["body"], body)
                self.assertEqual(request["headers"]["Idempotency-Key"], key)

    def test_freeform_run_retry_identity_covers_goal_and_execution_boundary(self):
        transport = FakeTransport()
        transport.responses.extend(json_response({"id": "run_synthetic"}) for _ in range(4))
        client = SwarmClient(base_url="https://swarm.example", transport=transport)
        payload = {"goal_summary": "Review the rollout", "run_requirements": {"execution_mode": "agent", "assignment_mode": "self"}}
        client.launch_run("sp_1", **payload)
        client.launch_run("sp_1", **payload)
        client.launch_run("sp_1", **{**payload, "goal_summary": "Review the pilot"})
        client.launch_run("sp_1", **{**payload, "run_requirements": {**payload["run_requirements"], "assignment_mode": "any"}})
        keys = [request["headers"]["Idempotency-Key"] for request in transport.requests]
        self.assertEqual(keys[0], keys[1])
        self.assertNotEqual(keys[0], keys[2])
        self.assertNotEqual(keys[0], keys[3])
        self.assertTrue(all(request["url"].endswith("/foundation/spaces/sp_1/runs:launch") for request in transport.requests))

    def test_run_reads_and_hosted_launch_encode_identifiers(self):
        transport = FakeTransport()
        transport.responses.extend(json_response({}) for _ in range(4))
        client = SwarmClient(base_url="https://swarm.example", transport=transport)
        client.get_run("run/1?other=#fragment")
        client.get_run_context("run/1?other=#fragment")
        client.get_run_lease("lease/1?other=#fragment")
        client.launch_space_run("sp/1?other=#fragment", title="Review", description="Verify the sources")
        self.assertEqual([request["url"] for request in transport.requests], [
            "https://swarm.example/v1/runs/run%2F1%3Fother%3D%23fragment",
            "https://swarm.example/v1/runs/run%2F1%3Fother%3D%23fragment/context",
            "https://swarm.example/v1/run-leases/lease%2F1%3Fother%3D%23fragment",
            "https://swarm.example/v1/spaces/sp%2F1%3Fother%3D%23fragment/launch-run",
        ])

    def test_execution_identifiers_cannot_change_the_url_route(self):
        transport = FakeTransport()
        client = SwarmClient(base_url="https://swarm.example", transport=transport)
        for identifier in ("", ".", ".."):
            with self.subTest(identifier=identifier):
                for invoke in (lambda: client.get_run(identifier), lambda: client.launch_run(identifier), lambda: client.create_group_run(identifier)):
                    with self.assertRaisesRegex(ValueError, "identifier"):
                        invoke()
        self.assertEqual(transport.requests, [])
