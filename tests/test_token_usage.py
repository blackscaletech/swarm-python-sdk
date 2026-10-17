from __future__ import annotations

import json
import unittest

from swarm_sdk import SwarmClient
from swarm_sdk.client import HTTPResponse


class FakeTransport:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []
        self.responses: list[HTTPResponse] = []

    def request(self, method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float | None) -> HTTPResponse:
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "body": json.loads(body.decode("utf-8")) if body else None,
                "timeout": timeout,
            }
        )
        if not self.responses:
            raise AssertionError("unexpected request without queued response")
        return self.responses.pop(0)


def json_response(payload: object, *, status: int = 200, headers: dict[str, str] | None = None) -> HTTPResponse:
    return HTTPResponse(
        status=status,
        headers={"content-type": "application/json", **(headers or {})},
        body=json.dumps(payload).encode("utf-8"),
    )


class SwarmTokenUsageSDKTests(unittest.TestCase):
    def test_token_usage_helpers_target_canonical_routes(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                json_response({"id": "tok_1", "total_tokens": 42}, status=201),
                json_response([{"id": "tok_1"}], headers={"x-next-cursor": "tok_2"}),
                json_response({"id": "tok_1"}),
                json_response([{"total_tokens": 42}]),
                json_response({"traces": [{"id": "aat_1", "action_descriptor_key": "trace.action.tool_call"}], "last_sequence": 0, "last_event_hash": "a" * 64, "replay_count": 0}, status=201),
                json_response([{"id": "aat_1"}], headers={"x-next-cursor": "aat_2"}),
                json_response({"trace": {"id": "aat_1", "action_descriptor_key": "trace.action.tool_call"}, "links": []}),
                json_response({"space_id": "sp_1", "trace_sample_count": 1, "token_usage": {"total_tokens": 42}}),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        created = client.create_token_usage_event(
            {
                "space_id": "sp_1",
                "run_id": "run_1",
                "agent_id": "agt_1",
                "source_resource_type": "run",
                "source_resource_id": "run_1",
                "operation": "connector.run",
                "count_source": "provider_reported",
                "total_tokens": 42,
            }
        )
        page = client.list_token_usage_events(space_id="sp_1", count_source="provider_reported")
        loaded = client.get_token_usage_event("tok_1")
        rollups = client.list_token_usage_rollups(space_id="sp_1", bucket="total")
        trace_batch = client.append_agent_action_trace_batch(
            "sp_1",
            {
                "trace_root_id": "trace_root_1",
                "events": [{
                    "sequence": 0,
                    "action_descriptor_key": "trace.action.tool_call",
                    "operation_key": "filesystem.read",
                    "status": "succeeded",
                    "tool_descriptor_key": "trace.tool.filesystem.read",
                    "tool_descriptor_hash": "b" * 64,
                    "started_at": "2026-08-25T00:00:00Z",
                }],
            }
        )
        trace_page = client.list_agent_action_traces(space_id="sp_1", action_descriptor_key="trace.action.tool_call")
        loaded_trace = client.get_agent_action_trace("aat_1")
        action_intelligence = client.get_space_action_intelligence("sp_1", trace_limit=50)

        self.assertEqual(created["id"], "tok_1")
        self.assertEqual(page.next_cursor, "tok_2")
        self.assertEqual(loaded["id"], "tok_1")
        self.assertEqual(rollups[0]["total_tokens"], 42)
        self.assertEqual(trace_batch["traces"][0]["id"], "aat_1")
        self.assertEqual(trace_page.next_cursor, "aat_2")
        self.assertEqual(loaded_trace["trace"]["id"], "aat_1")
        self.assertEqual(action_intelligence["token_usage"]["total_tokens"], 42)
        self.assertEqual(transport.requests[0]["method"], "POST")
        self.assertEqual(transport.requests[0]["url"], "https://swarm.example/v1/token-usage-events")
        self.assertIn("Idempotency-Key", transport.requests[0]["headers"])
        self.assertEqual(transport.requests[1]["url"], "https://swarm.example/v1/token-usage-events?space_id=sp_1&count_source=provider_reported")
        self.assertEqual(transport.requests[2]["url"], "https://swarm.example/v1/token-usage-events/tok_1")
        self.assertEqual(transport.requests[3]["url"], "https://swarm.example/v1/token-usage-rollups?space_id=sp_1&bucket=total")
        self.assertEqual(transport.requests[4]["url"], "https://swarm.example/v1/spaces/sp_1/agent-action-traces/batches")
        self.assertEqual(transport.requests[4]["body"]["events"][0]["operation_key"], "filesystem.read")
        self.assertEqual(transport.requests[5]["url"], "https://swarm.example/v1/agent-action-traces?space_id=sp_1&action_descriptor_key=trace.action.tool_call")
        self.assertEqual(transport.requests[6]["url"], "https://swarm.example/v1/agent-action-traces/aat_1")
        self.assertEqual(transport.requests[7]["url"], "https://swarm.example/v1/spaces/sp_1/action-intelligence?trace_limit=50")

    def test_checkpoint_helper_targets_canonical_route(self) -> None:
        transport = FakeTransport()
        transport.responses.append(json_response({
            "artifact": {"id": "art_checkpoint", "kind": "checkpoint.decision"},
            "trace": {"id": "aat_checkpoint", "action_descriptor_key": "trace.action.checkpoint"},
            "links": [],
            "replayed": False,
        }, status=201))
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        result = client.publish_agent_checkpoint(
            "sp_1",
            {
                "checkpoint_kind_key": "checkpoint.kind.decision",
                "title": "Durable decision",
                "body_text": "Use the canonical execution path.",
            },
            idempotency_key="checkpoint-sdk-1",
        )

        self.assertEqual(result["artifact"]["id"], "art_checkpoint")
        self.assertEqual(transport.requests[0]["url"], "https://swarm.example/v1/spaces/sp_1/checkpoints")
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], "checkpoint-sdk-1")

if __name__ == "__main__":
    unittest.main()
