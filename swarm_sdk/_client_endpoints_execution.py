from __future__ import annotations

import threading
from typing import Any, Callable
from urllib import parse

from ._client_core import (
    CollectionPage,
    _encode_query,
    _normalize_prompt_launch_input,
    _path_segment,
    build_launch_run_idempotency_key,
)
from ._client_inputs import _normalize_launch_run_input


class ExecutionEndpoints:
    def list_space_agent_participants(self, space_id: str, **query: Any) -> CollectionPage:
        """Discover executor addresses; follow cursors even on empty pages. Not online readiness."""
        return self.list(f"/v1/spaces/{_path_segment(space_id)}/agent-participants", query=query)

    def list_tasks(self, **query: Any) -> CollectionPage:
        return self.list("/v1/tasks", query=query)

    def list_launch_templates(self, **query: Any) -> CollectionPage:
        return self.list("/v1/setups", query=query)

    def create_launch_template(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/setups", input_data)

    def get_launch_template(self, template_id: str) -> dict[str, Any]:
        return self.get(f"/v1/setups/{_path_segment(template_id)}")

    def update_launch_template(self, template_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post(f"/v1/setups/{_path_segment(template_id)}/update", input_data)

    def disable_launch_template(self, template_id: str) -> dict[str, Any]:
        return self.post(f"/v1/setups/{_path_segment(template_id)}/disable")

    def list_automation_profiles(self, **query: Any) -> CollectionPage:
        return self.list("/v1/automation-profiles", query=query)

    def create_automation_profile(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/automation-profiles", input_data)

    def get_automation_profile(self, profile_id: str) -> dict[str, Any]:
        return self.get(f"/v1/automation-profiles/{_path_segment(profile_id)}")

    def update_automation_profile(self, profile_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post(f"/v1/automation-profiles/{_path_segment(profile_id)}/update", input_data)

    def disable_automation_profile(self, profile_id: str) -> dict[str, Any]:
        return self.post(f"/v1/automation-profiles/{_path_segment(profile_id)}/disable")

    def create_task(self, input_data: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post("/v1/tasks", input_data, idempotency_key=idempotency_key)

    def list_runs(self, **query: Any) -> CollectionPage:
        """List Runs with optional run_group_id; any space_id must match that Group."""
        return self.list("/v1/runs", query=query)

    def launch_space_run(
        self,
        space_id: str,
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
        **input_data: Any,
    ) -> dict[str, Any]:
        """Launch hosted/device work and create its Task. Not the external-agent queue."""
        normalized = _normalize_launch_run_input(input_data)
        if input_data.keys() - (normalized.keys() | {"model_route_policy_id"}):
            raise ValueError("launch_space_run contains unsupported fields; use an explicit raw request")
        return self.post(
            f"/v1/spaces/{_path_segment(space_id)}/launch-run",
            normalized,
            idempotency_key=idempotency_key or build_launch_run_idempotency_key(space_id, normalized, intent_key=intent_key),
        )

    def launch_run(
        self,
        space_id: str,
        *,
        idempotency_key: str | None = None,
        **input_data: Any,
    ) -> dict[str, Any]:
        """Queue work in a new Run Group. Agent callers default to their own host."""
        return self.post(f"/v1/foundation/spaces/{_path_segment(space_id)}/runs:launch", input_data, idempotency_key=idempotency_key)

    def create_run_group(self, space_id: str, input_data: dict[str, Any] | None = None, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/foundation/spaces/{_path_segment(space_id)}/run-groups", input_data or {}, idempotency_key=idempotency_key)

    def create_group_run(self, run_group_id: str, input_data: dict[str, Any] | None = None, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/foundation/run-groups/{_path_segment(run_group_id)}/runs", input_data or {}, idempotency_key=idempotency_key)

    def pause_run_group(self, run_group_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/foundation/run-groups/{_path_segment(run_group_id)}:pause", {}, idempotency_key=idempotency_key)

    def resume_run_group(self, run_group_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/foundation/run-groups/{_path_segment(run_group_id)}:resume", {}, idempotency_key=idempotency_key)

    def cancel_run_group(self, run_group_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/foundation/run-groups/{_path_segment(run_group_id)}:cancel", {}, idempotency_key=idempotency_key)

    def launch_from_prompt(
        self,
        space_id: str,
        input_data: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
    ) -> dict[str, Any]:
        return self.launch_space_run(
            space_id,
            idempotency_key=idempotency_key,
            intent_key=intent_key,
            **_normalize_prompt_launch_input(input_data),
        )

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self.get(f"/v1/runs/{_path_segment(run_id)}")

    def acquire_run(self, run_id: str, *, ttl_seconds: int = 300, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/runs/{_path_segment(run_id)}/acquire", {"ttl_seconds": ttl_seconds}, idempotency_key=idempotency_key)

    def launch_task_run(self, task_id: str, input_data: dict[str, Any] | None = None, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/foundation/tasks/{_path_segment(task_id)}/runs:launch", input_data or {}, idempotency_key=idempotency_key)

    def spawn_subrun(self, run_id: str, input_data: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/foundation/runs/{_path_segment(run_id)}/subruns", input_data, idempotency_key=idempotency_key)

    def claim_next_run(self, space_id: str, *, ttl_seconds: int = 300, cursor: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/spaces/{_path_segment(space_id)}/runs:claim-next", {"ttl_seconds": ttl_seconds, "cursor": cursor or ""}, idempotency_key=idempotency_key)

    def declare_executor_capabilities(self, space_id: str, input_data: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/spaces/{_path_segment(space_id)}/executor-capabilities", input_data, idempotency_key=idempotency_key)

    def get_run_context(self, run_id: str) -> dict[str, Any]:
        return self.get(f"/v1/runs/{_path_segment(run_id)}/context")

    def get_task(self, task_id: str) -> dict[str, Any]:
        return self.get(f"/v1/tasks/{_path_segment(task_id)}")

    def update_task(self, task_id: str, input_data: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.patch(f"/v1/tasks/{_path_segment(task_id)}", input_data, idempotency_key=idempotency_key)

    def cancel_task(self, task_id: str, input_data: dict[str, Any] | None = None, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/tasks/{_path_segment(task_id)}/cancel", input_data or {}, idempotency_key=idempotency_key)

    def list_audit_events(self, **query: Any) -> CollectionPage:
        return self.list("/v1/audit-events", query=query)

    def search(self, **query: Any) -> CollectionPage:
        return self.list("/v1/search", query=query)

    def search_public(self, **query: Any) -> CollectionPage:
        return self.list("/v1/search/public", query=query)

    def list_chat_client_profiles(self) -> list[dict[str, Any]]:
        return self.get("/v1/chat-client-profiles")

    def stream_audit_events(
        self,
        on_event: Callable[[dict[str, Any], dict[str, Any]], None],
        *,
        last_event_id: str | None = None,
        on_stream_error: Callable[[dict[str, Any]], None] | None = None,
        strict: bool = False,
        stream_timeout: float | None = None,
        stream_idle_timeout: float | None = None,
        stop_event: threading.Event | None = None,
        **query: Any,
    ) -> str | None:
        url = parse.urljoin(self.base_url, "v1/audit-events/stream")
        query_string = _encode_query(query)
        if query_string:
            url = f"{url}?{query_string}"
        headers = dict(self.default_headers)
        headers["Accept"] = "text/event-stream"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        return self._stream_events(url, headers, on_event=on_event, json_events={"audit_event"},
                                   on_stream_error=on_stream_error, strict=strict, last_event_id=last_event_id,
                                   stream_timeout=stream_timeout, stream_idle_timeout=stream_idle_timeout,
                                   stop_event=stop_event)

    def list_runtime_hints(self, agent_id: str, **query: Any) -> CollectionPage:
        return self.list(f"/v1/agents/{_path_segment(agent_id)}/runtime-hints", query=query)

    def create_agent_capability_manifest(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/agent-capability-manifests", input_data)

    def list_agent_capability_manifests(self, **query: Any) -> CollectionPage:
        return self.list("/v1/agent-capability-manifests", query=query)

    def get_agent_capability_manifest(self, manifest_id: str) -> dict[str, Any]:
        return self.get(f"/v1/agent-capability-manifests/{_path_segment(manifest_id)}")

    def update_agent_capability_manifest(self, manifest_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post(f"/v1/agent-capability-manifests/{_path_segment(manifest_id)}/update", input_data)

    def heartbeat_agent(self, agent_id: str, input_data: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.post(f"/v1/agents/{_path_segment(agent_id)}/heartbeat", input_data or {})

    def stream_runtime_hints(
        self,
        agent_id: str,
        on_event: Callable[[dict[str, Any], dict[str, Any]], None],
        *,
        last_event_id: str | None = None,
        on_stream_error: Callable[[dict[str, Any]], None] | None = None,
        strict: bool = False,
        stream_timeout: float | None = None,
        stream_idle_timeout: float | None = None,
        stop_event: threading.Event | None = None,
        **query: Any,
    ) -> str | None:
        url = parse.urljoin(self.base_url, f"v1/agents/{_path_segment(agent_id)}/runtime-hints/stream")
        query_string = _encode_query(query)
        if query_string:
            url = f"{url}?{query_string}"
        headers = dict(self.default_headers)
        headers["Accept"] = "text/event-stream"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        return self._stream_events(url, headers, on_event=on_event, json_events={"runtime_hint"},
                                   on_stream_error=on_stream_error, strict=strict, last_event_id=last_event_id,
                                   stream_timeout=stream_timeout, stream_idle_timeout=stream_idle_timeout,
                                   stop_event=stop_event)

    def list_agent_presence(self, **query: Any) -> CollectionPage:
        return self.list("/v1/agent-presence", query=query)

    def get_agent_presence(self, agent_id: str) -> dict[str, Any]:
        return self.get(f"/v1/agents/{_path_segment(agent_id)}/presence")

    def record_core_runner_heartbeat(self, *, idempotency_key: str | None = None) -> dict[str, Any] | None:
        return self.post("/v1/core-runner/heartbeat", {}, idempotency_key=idempotency_key)

    def claim_next_core_run(
        self,
        ttl_seconds: int = 60,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any] | None:
        return self.post(
            "/v1/core-runner/runs/claim-next",
            {"ttl_seconds": ttl_seconds},
            idempotency_key=idempotency_key,
        )

    def acquire_core_run(
        self,
        run_id: str,
        ttl_seconds: int = 60,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        run_key = _path_segment(run_id)
        return self.post(
            f"/v1/core-runs/{run_key}/acquire",
            {"ttl_seconds": ttl_seconds},
            idempotency_key=idempotency_key,
        )

    def start_core_run(
        self,
        run_id: str,
        lease_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any] | None:
        run_key = _path_segment(run_id)
        lease_key = _path_segment(lease_id)
        return self.post(
            f"/v1/core-runs/{run_key}/leases/{lease_key}/start",
            {},
            idempotency_key=idempotency_key,
        )

    def execute_core_run(
        self,
        run_id: str,
        lease_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        run_key = _path_segment(run_id)
        lease_key = _path_segment(lease_id)
        return self.post(
            f"/v1/core-runs/{run_key}/leases/{lease_key}/execute",
            {},
            idempotency_key=idempotency_key,
        )

    def complete_core_run(
        self,
        run_id: str,
        lease_id: str,
        outcome: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any] | None:
        run_key = _path_segment(run_id)
        lease_key = _path_segment(lease_id)
        return self.post(
            f"/v1/core-runs/{run_key}/leases/{lease_key}/complete",
            {"outcome": outcome},
            idempotency_key=idempotency_key,
        )

    def renew_core_run_lease(
        self,
        run_id: str,
        lease_id: str,
        ttl_seconds: int = 60,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        run_key = _path_segment(run_id)
        lease_key = _path_segment(lease_id)
        return self.post(
            f"/v1/core-runs/{run_key}/leases/{lease_key}/renew",
            {"ttl_seconds": ttl_seconds},
            idempotency_key=idempotency_key,
        )

    def release_core_run_lease(
        self,
        run_id: str,
        lease_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any] | None:
        run_key = _path_segment(run_id)
        lease_key = _path_segment(lease_id)
        return self.post(
            f"/v1/core-runs/{run_key}/leases/{lease_key}/release",
            {},
            idempotency_key=idempotency_key,
        )

    def start_run(self, run_id: str, lease_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/runs/{_path_segment(run_id)}/start", {"lease_id": lease_id}, idempotency_key=idempotency_key)

    def complete_run(self, run_id: str, input_data: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/runs/{_path_segment(run_id)}/complete", input_data, idempotency_key=idempotency_key)

    def request_run_stop(self, run_id: str, reason: str | None = None, *, idempotency_key: str | None = None) -> dict[str, Any]:
        body = {"reason": reason} if reason else {}
        return self.post(f"/v1/runs/{_path_segment(run_id)}/stop", body, idempotency_key=idempotency_key)

    def get_run_lease(self, lease_id: str) -> dict[str, Any]:
        return self.get(f"/v1/run-leases/{_path_segment(lease_id)}")

    def renew_run_lease(self, lease_id: str, ttl_seconds: int, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/run-leases/{_path_segment(lease_id)}/renew", {"ttl_seconds": ttl_seconds}, idempotency_key=idempotency_key)

    def release_run_lease(self, lease_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.post(f"/v1/run-leases/{_path_segment(lease_id)}/release", {}, idempotency_key=idempotency_key)
