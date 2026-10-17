import copy
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from swarm_sdk import (
    SwarmAPIError,
    SwarmClient,
    execute_agent_acquisition,
    run_agent_loop,
    run_agent_once,
)
from swarm_sdk import agent_runtime as runtime
from sdk_test_support import FakeTransport, json_response


def acquisition(run_id="run_1", ttl_seconds=300):
    return {
        "idle": False,
        "run": {
            "id": run_id, "space_id": "space_1", "status": "leased",
            "run_lease_id": "lease_" + run_id,
            "task_id": "task_shared", "goal_summary": "Compare the same sources",
            "run_requirements": {"execution_mode": "agent"},
            "context_refs": [{"kind": "artifact", "artifact_id": "source_1"}],
        },
        "run_lease": {
            "id": "lease_" + run_id, "run_id": run_id, "core_id": "",
            "leased_by_identity_id": "identity_1", "status": "active",
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat(),
        },
    }


class AgentClient:
    """Only the six allowed endpoints; scripts inject failures without a server."""

    def __init__(self, claims=None, ttl_seconds=300):
        self.claims = claims or [acquisition(ttl_seconds=ttl_seconds)]
        self.runs = {claim["run"]["id"]: copy.deepcopy(claim["run"])
                     for claim in self.claims if "run" in claim}
        self.leases = {claim["run_lease"]["id"]: copy.deepcopy(claim["run_lease"])
                       for claim in self.claims if "run_lease" in claim}
        self.calls = []
        self.scripts = {}
        self.lock = threading.Lock()

    def invoke(self, name, args, options, default):
        with self.lock:
            self.calls.append((name, copy.deepcopy(args), dict(options)))
            script = self.scripts.get(name, [])
            action = script.pop(0) if script else None
        if isinstance(action, BaseException):
            raise action
        if callable(action):
            return action()
        return copy.deepcopy(default() if action is None else action)

    def claim_next_run(self, space_id, **options):
        return self.invoke("claim", (space_id,), options, lambda: self.claims.pop(0))

    def start_run(self, run_id, lease_id, **options):
        def start():
            self.runs[run_id]["status"] = "running"
            return self.runs[run_id]
        return self.invoke("start", (run_id, lease_id), options, start)

    def renew_run_lease(self, lease_id, ttl_seconds, **options):
        def renew():
            self.leases[lease_id]["expires_at"] = (
                datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
            return self.leases[lease_id]
        return self.invoke("renew", (lease_id, ttl_seconds), options, renew)

    def get_run_context(self, run_id):
        return self.invoke("context", (run_id,), {}, lambda: {
            "run": self.runs[run_id], "task": {"id": "task_shared"},
            "context_refs": [{"artifact": {"body": "Exact source-bearing evidence"}}],
        })

    def complete_run(self, run_id, payload, **options):
        def complete():
            self.runs[run_id]["status"] = payload["outcome"]
            return self.runs[run_id]
        return self.invoke("complete", (run_id, payload), options, complete)

    def release_run_lease(self, lease_id, **options):
        return self.invoke("release", (lease_id,), options, lambda: {})

    def named(self, name):
        return [call for call in self.calls if call[0] == name]


def execute(run, context, stop_event):
    return {"title": "Comparison", "body": context["context_refs"][0]["artifact"]["body"]}


class AgentRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.retry_delay = runtime._retry_delay
        self.delay_patch = patch.object(runtime, "_retry_delay", return_value=0)
        self.delay_patch.start()
        self.addCleanup(self.delay_patch.stop)

    def assert_no_lease_threads(self):
        self.assertFalse([thread for thread in threading.enumerate()
                          if thread.name.startswith("swarm-agent-lease-")])

    def test_three_repeated_source_bearing_runs_and_bounded_published_callback(self):
        claims = [acquisition(f"run_{index}") for index in range(3)]
        client = AgentClient(claims)
        stop = threading.Event()
        executed, published = [], []

        def adapter(run, context, work_stop):
            self.assertEqual(run["status"], "running")
            self.assertEqual(context["run"]["status"], "running")
            self.assertEqual(context["context_refs"][0]["artifact"]["body"], "Exact source-bearing evidence")
            self.assertFalse(work_stop.is_set())
            executed.append(run["id"])
            return execute(run, context, work_stop)

        def on_execution(result):
            self.assertEqual(result["run"]["status"], "succeeded")
            self.assertEqual(len(client.named("complete")), len(published) + 1)
            published.append(result["run"]["id"])
            if len(published) == 3:
                stop.set()

        run_agent_loop(client, space_id="space_1", execute=adapter,
                       stop_event=stop, on_execution=on_execution)
        self.assertEqual(executed, [f"run_{index}" for index in range(3)])
        self.assertEqual(published, executed)
        self.assertEqual(len(client.named("claim")), 3)
        self.assertEqual(client.named("release"), [])
        keys = [options["idempotency_key"] for name, args, options in client.calls if name != "context"]
        self.assertEqual(len(keys), len(set(keys)))
        self.assert_no_lease_threads()

    def test_idle_cursor_is_returned_and_followed(self):
        idle = {"idle": True, "next_cursor": "page_a", "retry_after_ms": 0}
        client = AgentClient([idle, {**idle, "next_cursor": "page_b"}, acquisition()])
        stop = threading.Event()
        seen = []
        run_agent_loop(client, space_id="space_1", execute=execute, stop_event=stop,
                       on_idle=lambda result: seen.append(result["next_cursor"]),
                       on_execution=lambda result: stop.set())
        self.assertEqual(seen, ["page_a", "page_b"])
        self.assertEqual([call[2]["cursor"] for call in client.named("claim")], [None, "page_a", "page_b"])
        result = run_agent_once(AgentClient([idle]), space_id="space_1", execute=execute)
        self.assertTrue(result["idle"])
        self.assertEqual(result["next_cursor"], "page_a")

    def test_capacity_wait_preserves_cursor_and_resumes_with_fresh_intent(self):
        client = AgentClient([acquisition()])
        client.scripts["claim"] = [
            {"idle": True, "next_cursor": "tail", "retry_after_ms": 0},
            SwarmAPIError("full", status=409, body={"code": "lease_capacity_reached"}),
        ]
        class FastStop(threading.Event):
            def wait(self, timeout=None):
                return self.is_set()

        stop = FastStop()
        idle = []
        run_agent_loop(client, space_id="space_1", execute=execute, stop_event=stop,
                       on_idle=idle.append, on_execution=lambda result: stop.set())
        self.assertEqual(idle[1]["retry_after_ms"], 2000)
        polls = client.named("claim")
        self.assertEqual([call[2]["cursor"] for call in polls], [None, "tail", "tail"])
        self.assertEqual(len({call[2]["idempotency_key"] for call in polls}), 3)
        self.assertEqual(len(client.named("complete")), 1)
        self.assert_no_lease_threads()

    def test_capacity_hints_are_bounded_and_waits_can_be_stopped(self):
        for hint, expected in [(0, 2000), (4000, 4000), (1_000_000, 30_000)]:
            client = AgentClient()
            client.scripts["claim"] = [SwarmAPIError("full", status=409,
                body={"code": "lease_capacity_reached", "retry_after_ms": hint})]
            with patch.object(runtime, "_retry_delay", wraps=self.retry_delay):
                result = run_agent_once(client, space_id="space_1", execute=execute, cursor="tail")
            self.assertEqual(result["retry_after_ms"], expected)
            self.assertEqual(result["next_cursor"], "tail")
        client = AgentClient()
        client.scripts["claim"] = [SwarmAPIError("full", status=409, body={"code": "lease_capacity_reached"})]
        stop = threading.Event()
        run_agent_loop(client, space_id="space_1", execute=execute, stop_event=stop,
                       on_idle=lambda result: stop.set())
        self.assertEqual(len(client.named("claim")), 1)

    def test_capacity_compatibility_never_retries_lease_lifecycle_conflicts(self):
        client = AgentClient()
        client.scripts["renew"] = [SwarmAPIError("full", status=409, body={"code": "lease_capacity_reached"})]
        with self.assertRaises(SwarmAPIError):
            run_agent_loop(client, space_id="space_1", execute=execute)
        self.assertEqual(len(client.named("claim")), 1)
        self.assertEqual(len(client.named("complete")), 0)
        self.assert_no_lease_threads()

    def test_head_revisit_preserves_long_tail_progress(self):
        claims = [{"idle": True, "next_cursor": cursor, "retry_after_ms": 0}
                  for cursor in ["a", "b", "head", "d", "e"]]
        client = AgentClient(claims)
        stop = threading.Event()
        pages = []

        def on_idle(result):
            pages.append(result)
            if len(pages) == 5:
                stop.set()

        with patch.object(runtime, "_MAX_SCAN_PAGES", 2):
            run_agent_loop(client, space_id="space_1", execute=execute, stop_event=stop, on_idle=on_idle)
        self.assertEqual([call[2]["cursor"] for call in client.named("claim")], [None, "a", None, "b", None])

    def test_timed_head_revisit(self):
        claims = [{"idle": True, "next_cursor": "tail", "retry_after_ms": 0}] * 3
        client = AgentClient(claims)
        stop = threading.Event()
        count = []

        def on_idle(result):
            count.append(result)
            if len(count) == 3:
                stop.set()

        with patch.object(runtime.time, "monotonic", side_effect=[0, 0, 31, 31]):
            run_agent_loop(client, space_id="space_1", execute=execute, stop_event=stop, on_idle=on_idle)
        self.assertEqual([call[2]["cursor"] for call in client.named("claim")], [None, None, "tail"])

    def test_slow_polls_cannot_starve_the_tail_cursor(self):
        client = AgentClient([{"idle": True, "next_cursor": "tail", "retry_after_ms": 0}] * 5)
        stop = threading.Event()
        pages = []

        def on_idle(result):
            pages.append(result)
            if len(pages) == 5:
                stop.set()

        with patch.object(runtime.time, "monotonic", side_effect=range(0, 400, 31)):
            run_agent_loop(client, space_id="space_1", execute=execute, stop_event=stop, on_idle=on_idle)
        self.assertEqual([call[2]["cursor"] for call in client.named("claim")], [None, None, "tail", None, "tail"])

    def test_server_poll_delay_is_bounded_and_invalid_hints_use_default(self):
        client = AgentClient([{"idle": True, "retry_after_ms": hint} for hint in [999999999, "NaN", -1]])
        stop = threading.Event()
        count = []

        def on_idle(result):
            count.append(result)
            if len(count) == 3:
                stop.set()

        with patch.object(stop, "wait", return_value=False) as wait:
            run_agent_loop(client, space_id="space_1", execute=execute, stop_event=stop,
                           poll_interval_seconds=8, on_idle=on_idle)
        self.assertEqual([args.args[0] for args in wait.call_args_list], [30, 8, 8])

    def test_scope_mode_idle_and_stop_validation_prevent_adapter(self):
        cases = [
            ("acquisition space", lambda claim: claim["run"].update(space_id="other")),
            ("acquisition mode", lambda claim: claim["run"]["run_requirements"].update(execution_mode="model")),
            ("idle", lambda claim: claim.update(idle=True)),
            ("core", lambda claim: claim["run_lease"].update(core_id="core_1")),
            ("stopping", lambda claim: claim["run"].update(stop_requested_at="2026-10-12T00:00:00Z")),
            ("replacement lease", lambda claim: claim["run"].update(run_lease_id="other")),
            ("missing lease", lambda claim: claim["run"].pop("run_lease_id")),
        ]
        for name, alter in cases:
            with self.subTest(name=name):
                claim = acquisition()
                alter(claim)
                client = AgentClient([claim])
                adapter = unittest.mock.Mock(side_effect=AssertionError("must not execute"))
                with self.assertRaises((ValueError, RuntimeError)):
                    execute_agent_acquisition(client, claim, space_id="space_1", execute=adapter)
                adapter.assert_not_called()
                self.assertEqual(len(client.named("release")), 1)

    def test_context_and_started_scope_mode_stop_checks(self):
        for endpoint in ["context", "start"]:
            for field, value in [("space_id", "other"), ("id", "other"),
                                 ("run_requirements", {"execution_mode": "model"}),
                                 ("stop_requested_at", "now"), ("status", "leased"),
                                 ("run_lease_id", "replacement_lease"), ("run_lease_id", None)]:
                with self.subTest(endpoint=endpoint, field=field):
                    claim = acquisition()
                    run = {**claim["run"], "status": "running", field: value}
                    client = AgentClient([claim])
                    client.scripts[endpoint] = [{"run": run} if endpoint == "context" else run]
                    adapter = unittest.mock.Mock()
                    with self.assertRaises((ValueError, RuntimeError)):
                        execute_agent_acquisition(client, claim, space_id="space_1", execute=adapter)
                    adapter.assert_not_called()
                    self.assertEqual(len(client.named("release")), 1)
                    self.assertEqual(client.named("complete"), [])
                    self.assert_no_lease_threads()

    def test_validation_of_options_after_acquisition_also_releases(self):
        for options in [{"space_id": ""}, {"ttl_seconds": 0}, {"execute": None}]:
            with self.subTest(options=options):
                claim = acquisition()
                client = AgentClient([claim])
                with self.assertRaises((ValueError, TypeError)):
                    execute_agent_acquisition(client, claim, **{"space_id": "space_1", "execute": execute, **options})
                self.assertEqual(len(client.named("release")), 1)

    def test_context_start_and_initial_renew_error_preserved_despite_release_failure(self):
        for endpoint in ["context", "start", "renew"]:
            with self.subTest(endpoint=endpoint):
                claim = acquisition()
                client = AgentClient([claim])
                error = SwarmAPIError("primary", status=403)
                client.scripts[endpoint] = [error]
                client.scripts["release"] = [SwarmAPIError("cleanup", status=503)]
                adapter = unittest.mock.Mock()
                with self.assertRaises(SwarmAPIError) as caught:
                    execute_agent_acquisition(client, claim, space_id="space_1", execute=adapter)
                self.assertIs(caught.exception, error)
                adapter.assert_not_called()
                self.assertEqual(len(client.named("release")), 1)
                self.assertEqual(client.named("complete"), [])
                self.assert_no_lease_threads()

    def test_thread_start_failure_preserves_error_and_releases(self):
        original_start = threading.Thread.start
        for failing_name in ["swarm-agent-lease-watch", "swarm-agent-lease-renewal"]:
            with self.subTest(thread=failing_name):
                claim = acquisition()
                client = AgentClient([claim])
                error = RuntimeError("cannot start thread")

                def start(thread):
                    if thread.name == failing_name:
                        raise error
                    original_start(thread)

                with patch.object(threading.Thread, "start", start):
                    with self.assertRaises(RuntimeError) as caught:
                        execute_agent_acquisition(client, claim, space_id="space_1", execute=execute)
                self.assertIs(caught.exception, error)
                self.assertEqual(len(client.named("release")), 1)
                self.assert_no_lease_threads()

    def test_malformed_idle_claim_releases_known_lease(self):
        claim = acquisition()
        claim["idle"] = True
        client = AgentClient([claim])
        with self.assertRaisesRegex(ValueError, "non-idle"):
            run_agent_once(client, space_id="space_1", execute=execute)
        self.assertEqual(len(client.named("release")), 1)

    def test_adapter_exception_does_not_publish_exception_artifact_or_become_poll_retry(self):
        client = AgentClient()
        error = SwarmAPIError("unrecognized-internal-secret timeout", status=503)
        adapter = unittest.mock.Mock(side_effect=error)
        with self.assertRaises(SwarmAPIError) as caught:
            run_agent_loop(client, space_id="space_1", execute=adapter)
        self.assertIs(caught.exception, error)
        adapter.assert_called_once()
        self.assertEqual(len(client.named("claim")), 1)
        self.assertEqual(client.named("complete")[0][1][1],
                         {"lease_id": "lease_run_1", "outcome": "failed"})
        self.assert_no_lease_threads()

    def test_adapter_authority_loss_never_attempts_completion(self):
        for status in [401, 403, 409]:
            with self.subTest(status=status):
                client = AgentClient()
                error = SwarmAPIError("authority lost", status=status, body={"code": "credential_revoked"})
                adapter = unittest.mock.Mock(side_effect=error)
                with self.assertRaises(SwarmAPIError) as caught:
                    run_agent_once(client, space_id="space_1", execute=adapter)
                self.assertIs(caught.exception, error)
                self.assertTrue(adapter.call_args.args[2].is_set())
                self.assertEqual(client.named("complete"), [])
                self.assertEqual(len(client.named("release")), 1)
                self.assert_no_lease_threads()

    def test_adapter_error_preserved_when_failure_report_and_release_fail(self):
        claim = acquisition()
        client = AgentClient([claim])
        client.scripts["complete"] = [SwarmAPIError("revoked", status=403)]
        client.scripts["release"] = [TimeoutError("cleanup timeout")]
        error = ValueError("adapter failed")
        with self.assertRaises(ValueError) as caught:
            execute_agent_acquisition(client, claim, space_id="space_1", execute=unittest.mock.Mock(side_effect=error))
        self.assertIs(caught.exception, error)
        self.assertEqual(len(client.named("release")), 1)

    def test_success_requires_a_body_and_explicit_failed_or_canceled_results_are_not_success(self):
        for outcome in ["failed", "canceled"]:
            with self.subTest(outcome=outcome):
                result = run_agent_once(AgentClient(), space_id="space_1",
                                        execute=lambda *args: {"outcome": outcome})
                self.assertEqual(result["run"]["status"], outcome)
        client = AgentClient()
        with self.assertRaisesRegex(ValueError, "non-empty result body"):
            run_agent_once(client, space_id="space_1", execute=lambda *args: {"body": " "})
        self.assertEqual(client.named("complete")[0][1][1]["outcome"], "failed")

    def test_explicit_failure_text_is_sanitized(self):
        client = AgentClient()
        run_agent_once(client, space_id="space_1", execute=lambda *args: {
            "outcome": "failed", "body": "SWARM_TOKEN=swarm_agt_abcdefghijklmnop"})
        payload = client.named("complete")[0][1][1]
        self.assertNotIn("swarm_agt_", payload["body"])

    def test_renewal_failure_stops_adapter_and_never_completes_stale_lease(self):
        for status in [403, 409, 503]:
            with self.subTest(status=status):
                claim = acquisition(ttl_seconds=1)
                client = AgentClient([claim])
                error = SwarmAPIError("renewal failed", status=status)
                client.scripts["renew"] = [None] + [error] * 4

                def adapter(run, context, work_stop):
                    self.assertTrue(work_stop.wait(2))
                    return {"body": "must not publish"}

                with self.assertRaises(SwarmAPIError) as caught:
                    execute_agent_acquisition(client, claim, space_id="space_1", execute=adapter, ttl_seconds=1)
                self.assertIs(caught.exception, error)
                self.assertEqual(client.named("complete"), [])
                self.assertEqual(len(client.named("release")), 1)
                keys = [call[2]["idempotency_key"] for call in client.named("renew")]
                self.assertNotEqual(keys[0], keys[1])
                self.assertEqual(len(set(keys[1:])), 1)
                self.assert_no_lease_threads()

    def test_expiry_stops_adapter_even_with_a_blocked_renewal(self):
        claim = acquisition(ttl_seconds=1)
        client = AgentClient([claim])
        unblock = threading.Event()

        def blocked_renewal():
            self.assertTrue(unblock.wait(2))
            return copy.deepcopy(claim["run_lease"])

        client.scripts["renew"] = [None, blocked_renewal]

        def adapter(run, context, work_stop):
            try:
                self.assertTrue(work_stop.wait(2))
                return {"body": "must not publish"}
            finally:
                unblock.set()

        with self.assertRaisesRegex(RuntimeError, "expired"):
            execute_agent_acquisition(client, claim, space_id="space_1", execute=adapter, ttl_seconds=1)
        self.assertEqual(client.named("complete"), [])
        self.assert_no_lease_threads()

    def test_cancellation_releases_even_with_already_set_stop_event(self):
        for when in ["before", "context", "adapter", "after_claim"]:
            with self.subTest(when=when):
                claim = acquisition()
                client = AgentClient([claim])
                stop = threading.Event()

                def adapter(run, context, work_stop):
                    stop.set()
                    self.assertTrue(work_stop.wait(1))
                    return {"body": "must not publish"}

                if when == "before":
                    stop.set()
                if when == "context":
                    def context():
                        stop.set()
                        return {"run": {**claim["run"], "status": "running"}}
                    client.scripts["context"] = [context]
                if when == "after_claim":
                    def late_claim():
                        stop.set()
                        return claim
                    client.scripts["claim"] = [late_claim]
                    result = run_agent_once
                else:
                    result = lambda **options: execute_agent_acquisition(client, claim, **options)
                with self.assertRaisesRegex(RuntimeError, "stopped"):
                    if when == "after_claim":
                        result(client, space_id="space_1", execute=adapter, stop_event=stop)
                    else:
                        result(space_id="space_1", execute=adapter, stop_event=stop)
                self.assertEqual(client.named("complete"), [])
                self.assertEqual(len(client.named("release")), 1)
                self.assert_no_lease_threads()
        stopped = threading.Event()
        stopped.set()
        client = AgentClient()
        self.assertIsNone(run_agent_once(client, space_id="space_1", execute=execute, stop_event=stopped))
        self.assertEqual(client.calls, [])

    def test_claim_429_503_retries_keep_one_key_and_fail_closed_statuses_do_not_retry(self):
        client = AgentClient()
        client.scripts["claim"] = [SwarmAPIError("limited", status=429), SwarmAPIError("temporary", status=503)]
        run_agent_once(client, space_id="space_1", execute=execute)
        self.assertEqual(len(client.named("claim")), 3)
        self.assertEqual(len({call[2]["idempotency_key"] for call in client.named("claim")}), 1)
        for status in [401, 403, 409, 410]:
            with self.subTest(status=status):
                client = AgentClient()
                error = SwarmAPIError("lease/revocation", status=status)
                client.scripts["claim"] = [error]
                with self.assertRaises(SwarmAPIError) as caught:
                    run_agent_loop(client, space_id="space_1", execute=execute)
                self.assertIs(caught.exception, error)
                self.assertEqual(len(client.calls), 1)

    def test_each_start_renew_and_complete_lost_response_reuses_its_own_intent(self):
        client = AgentClient()
        for name in ["start", "renew", "complete"]:
            client.scripts[name] = [TimeoutError("response lost")]
        adapter = unittest.mock.Mock(side_effect=execute)
        run_agent_once(client, space_id="space_1", execute=adapter)
        adapter.assert_called_once()
        intent_keys = []
        for name in ["start", "renew", "complete"]:
            calls = client.named(name)
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0], calls[1])
            intent_keys.append(calls[0][2]["idempotency_key"])
        self.assertEqual(len(set(intent_keys)), 3)

    def test_committed_completion_response_loss_does_not_double_execute(self):
        client = AgentClient()

        def commit_then_lose_response():
            client.runs["run_1"]["status"] = "succeeded"
            raise TimeoutError("response lost after commit")

        client.scripts["complete"] = [commit_then_lose_response]
        adapter = unittest.mock.Mock(side_effect=execute)
        result = run_agent_once(client, space_id="space_1", execute=adapter)
        self.assertEqual(result["run"]["status"], "succeeded")
        adapter.assert_called_once()
        self.assertEqual(client.named("complete")[0], client.named("complete")[1])

    def test_completion_retry_exhaustion_terminates_loop_without_replaying_execution(self):
        client = AgentClient([acquisition(), acquisition("run_2")])
        error = SwarmAPIError("completion uncertain", status=503)
        client.scripts["complete"] = [error] * 4
        adapter = unittest.mock.Mock(side_effect=execute)
        with self.assertRaises(SwarmAPIError) as caught:
            run_agent_loop(client, space_id="space_1", execute=adapter)
        self.assertIs(caught.exception, error)
        adapter.assert_called_once()
        self.assertEqual(len(client.named("claim")), 1)
        self.assertEqual(len(client.named("complete")), 4)
        self.assertEqual(len({call[2]["idempotency_key"] for call in client.named("complete")}), 1)
        self.assertEqual(len(client.named("release")), 1)

    def test_missing_terminal_response_cannot_fake_success(self):
        client = AgentClient()
        client.scripts["complete"] = [{}]
        with self.assertRaises(ValueError):
            run_agent_once(client, space_id="space_1", execute=execute)
        self.assertEqual(len(client.named("complete")), 1)
        self.assertEqual(len(client.named("release")), 1)

    def test_public_client_transport_loss_preserves_retry_key(self):
        class LostResponseTransport(FakeTransport):
            def request(self, *args):
                response = super().request(*args)
                if response.status == 599:
                    raise TimeoutError("lost response")
                return response

        claim = acquisition()
        running = {**claim["run"], "status": "running"}
        transport = LostResponseTransport()
        transport.responses = [json_response(claim), json_response(running), json_response(claim["run_lease"]),
                               json_response({"run": running, "context_refs": []}),
                               json_response({}, status=599), json_response({**running, "status": "succeeded"})]
        client = SwarmClient(base_url="https://swarm.example", transport=transport)
        adapter = unittest.mock.Mock(return_value={"body": "Actual output"})
        result = run_agent_once(client, space_id="space_1", execute=adapter)
        self.assertEqual(result["run"]["status"], "succeeded")
        adapter.assert_called_once()
        self.assertEqual(transport.requests[-1], transport.requests[-2])
        self.assertTrue(all("core" not in request["url"] for request in transport.requests))

    def test_retry_delays_are_bounded_and_statusless_nontransport_errors_are_not_retried(self):
        self.delay_patch.stop()
        self.assertEqual(runtime._retry_delay(SwarmAPIError("busy", status=503, body={"retry_after_ms": 999999}), 0), 30)
        self.assertEqual(runtime._retry_delay(SwarmAPIError("busy", status=429, headers={"Retry-After": "999999"}), 0), 30)
        self.assertLessEqual(runtime._retry_delay(SwarmAPIError("busy", status=503), 20), 30)
        self.assertFalse(runtime._transient(ValueError("network timeout")))
        self.assertFalse(runtime._transient(SwarmAPIError("network timeout", status=403)))


if __name__ == "__main__":
    unittest.main()
