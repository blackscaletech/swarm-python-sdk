import unittest
from urllib.parse import quote, urljoin, urlparse

from swarm_sdk import SwarmClient
from swarm_sdk.client import HTTPResponse


INPUT = {"description": "unchanged synthetic input"}
CASES = [
    ("get_user_session", "GET", "/v1/user-sessions/{id}", (), {}),
    ("revoke_user_session", "POST", "/v1/user-sessions/{id}/revoke", (), {"reason": "synthetic"}),
    ("bootstrap_space", "POST", "/v1/entities/{id}/bootstrap-space", ({},), {}),
    ("update_space", "PATCH", "/v1/spaces/{id}", (), INPUT),
    ("connect_space_client", "POST", "/v1/spaces/{id}/connect-client", ({},), {}),
    ("connect_client", "POST", "/v1/spaces/{id}/connect-client", ({},), {}),
    ("get_agent", "GET", "/v1/agents/{id}", (), {}),
    ("get_space", "GET", "/v1/spaces/{id}", (), {}),
    ("get_launch_template", "GET", "/v1/setups/{id}", (), {}),
    ("update_launch_template", "POST", "/v1/setups/{id}/update", (INPUT,), {}),
    ("disable_launch_template", "POST", "/v1/setups/{id}/disable", (), {}),
    ("get_automation_profile", "GET", "/v1/automation-profiles/{id}", (), {}),
    ("update_automation_profile", "POST", "/v1/automation-profiles/{id}/update", (INPUT,), {}),
    ("disable_automation_profile", "POST", "/v1/automation-profiles/{id}/disable", (), {}),
    ("list_runtime_hints", "GET", "/v1/agents/{id}/runtime-hints", (), {}),
    ("get_agent_capability_manifest", "GET", "/v1/agent-capability-manifests/{id}", (), {}),
    ("update_agent_capability_manifest", "POST", "/v1/agent-capability-manifests/{id}/update", (INPUT,), {}),
    ("heartbeat_agent", "POST", "/v1/agents/{id}/heartbeat", (INPUT,), {}),
    ("stream_runtime_hints", "GET", "/v1/agents/{id}/runtime-hints/stream", (lambda *_: None,), {}),
    ("get_agent_presence", "GET", "/v1/agents/{id}/presence", (), {}),
    ("get_budget_account", "GET", "/v1/budget-accounts/{id}", (), {}),
    ("fund_budget_account", "POST", "/v1/budget-accounts/{id}/fund", (1,), {"reason": "synthetic"}),
    ("get_budget_ledger_entry", "GET", "/v1/budget-ledger-entries/{id}", (), {}),
    ("get_budget_delegation", "GET", "/v1/budget-delegations/{id}", (), {}),
    ("consume_budget_delegation", "POST", "/v1/budget-delegations/{id}/consume", (1,), {"reason": "synthetic"}),
    ("revoke_budget_delegation", "POST", "/v1/budget-delegations/{id}/revoke", (), {"reason": "synthetic"}),
    ("get_approval_request", "GET", "/v1/approval-requests/{id}", (), {}),
    ("approve_approval_request", "POST", "/v1/approval-requests/{id}/approve", (), {"resolution_note": "synthetic"}),
    ("reject_approval_request", "POST", "/v1/approval-requests/{id}/reject", (), {"resolution_note": "synthetic"}),
    ("get_webhook_target", "GET", "/v1/webhook-targets/{id}", (), {}),
    ("update_webhook_target", "POST", "/v1/webhook-targets/{id}/update", (INPUT,), {}),
    ("disable_webhook_target", "POST", "/v1/webhook-targets/{id}/disable", (), {}),
    ("reconcile_webhook_target", "POST", "/v1/webhook-targets/{id}/reconcile", (), {"limit": 1}),
    ("get_webhook_delivery", "GET", "/v1/webhook-deliveries/{id}", (), {}),
    ("redrive_webhook_delivery", "POST", "/v1/webhook-deliveries/{id}/redrive", (), {}),
    ("get_space_operator_summary", "GET", "/v1/operator/spaces/{id}/summary", (), {}),
    ("get_space_operator_live_snapshot", "GET", "/v1/operator/spaces/{id}/live", (), {}),
    ("list_space_operator_activity", "GET", "/v1/operator/spaces/{id}/activity", (), {}),
    ("list_space_operator_presence_entries", "GET", "/v1/operator/spaces/{id}/presence", (), {}),
    ("list_space_signed_status_entries", "GET", "/v1/operator/spaces/{id}/signed-status", (), {}),
    ("get_run", "GET", "/v1/runs/{id}", (), {}),
]
IDENTIFIERS = ("sp_CaseSensitive-09", "../tasks/tsk_synthetic", "a/b", "sp?injected=1", "sp#fragment",
               "sp%2f..%2fme", "sp\\..\\me", "\u00e9\u6f22 space")


class Stream:
    def __init__(self):
        self.closed = False

    def __iter__(self):
        yield b'id: applied\nevent: runtime_hint\ndata: {"ok":true}\n\n'

    def close(self):
        self.closed = True


class Transport:
    def __init__(self):
        self.calls = []
        self.response = None

    def request(self, method, url, headers, body, timeout):
        self.calls.append((method, url, headers, body))
        return HTTPResponse(200, {"content-type": "application/json", "x-next-cursor": "next", "x-order": "desc"}, b"[]")

    def stream(self, method, url, headers, timeout):
        self.calls.append((method, url, headers, None))
        self.response = Stream()
        return self.response


class IdentifierTests(unittest.TestCase):
    def fixture(self, base_url="https://audit.invalid/"):
        transport = Transport()
        return SwarmClient(base_url=base_url, token="synthetic-token", transport=transport), transport

    def test_empty_and_exact_dot_ids_fail_before_dispatch(self):
        for name, _, _, args, kwargs in CASES:
            for identifier in ("", ".", ".."):
                with self.subTest(method=name, identifier=identifier):
                    client, transport = self.fixture()
                    with self.assertRaisesRegex(ValueError, "identifier|traversal"):
                        getattr(client, name)(identifier, *args, **kwargs)
                    self.assertEqual(transport.calls, [])

    def test_retry_keys_bodies_pagination_and_stream_resume_are_preserved(self):
        import json
        client, transport = self.fixture()
        client.update_space("sp_Normal", **INPUT)
        self.assertEqual(json.loads(transport.calls[0][3]), INPUT)
        first_key = transport.calls[0][2]["Idempotency-Key"]
        client.update_space("sp_Normal", **INPUT)
        self.assertEqual(transport.calls[1][2]["Idempotency-Key"], first_key)
        page = client.list_runtime_hints("a/b", limit=2, cursor="old")
        self.assertEqual(transport.calls[2][1], "https://audit.invalid/v1/agents/a%2Fb/runtime-hints?limit=2&cursor=old")
        self.assertEqual((page.next_cursor, page.order), ("next", "desc"))
        seen = []
        cursor = client.stream_runtime_hints("a/b", lambda data, _: seen.append(data), last_event_id="prior", kind="ready", limit=2)
        self.assertEqual(cursor, "applied")
        self.assertEqual(transport.calls[3][2]["Last-Event-ID"], "prior")
        self.assertEqual(urlparse(transport.calls[3][1]).query, "kind=ready&limit=2")
        self.assertEqual(seen, [{"ok": True}])
        self.assertTrue(transport.response.closed)
        client.post("/v1/spaces/sp_Normal", INPUT, idempotency_key="original-retry-key")
        self.assertEqual(transport.calls[4][2]["Idempotency-Key"], "original-retry-key")

    def test_raw_same_origin_absolute_urls_remain_supported(self):
        client, transport = self.fixture("https://audit.invalid/prefix/")
        client.get("https://audit.invalid/v1/me")
        self.assertEqual(transport.calls[0][1], "https://audit.invalid/v1/me")


def identifier_test(name, method, template, args, kwargs):
    def check(self):
        for base_url in ("https://audit.invalid/", "https://audit.invalid/prefix/"):
            for identifier in IDENTIFIERS:
                with self.subTest(base_url=base_url, identifier=identifier):
                    client, transport = self.fixture(base_url)
                    getattr(client, name)(identifier, *args, **kwargs)
                    self.assertEqual(len(transport.calls), 1)
                    actual_method, url, headers, _ = transport.calls[0]
                    expected = urljoin(base_url, template.replace("{id}", quote(identifier, safe="")).lstrip("/"))
                    self.assertEqual(url, expected)
                    self.assertEqual(actual_method, method)
                    self.assertEqual(headers["Authorization"], "Bearer synthetic-token")
                    self.assertEqual((urlparse(url).query, urlparse(url).fragment), ("", ""))
    return check


for case in CASES:
    setattr(IdentifierTests, "test_segment_" + case[0], identifier_test(*case))
