from __future__ import annotations

import hashlib
import re
import threading
from typing import Any

from ._client_core import CollectionPage, _path_segment
from ._client_transport import _response_byte_limit
from ._client_types import SwarmArtifactIntegrityError, SwarmResponseLimitError
from ._artifact_download import VerifiedArtifactDownload, download_artifact as _download_artifact


class OperationsEndpoints:
    def download_artifact(
        self, artifact_id: str, *, expected_sha256: str | None = None,
        max_artifact_bytes: int | None = None, max_response_bytes: int | None = None,
        max_error_response_bytes: int | None = None, operation_timeout: float | None = None,
        header_timeout: float | None = None, idle_timeout: float | None = None,
        stop_event: threading.Event | None = None,
    ) -> VerifiedArtifactDownload:
        """Return a caller-owned raw file only after full identity/size/SHA verification."""
        return _download_artifact(self, artifact_id, expected_sha256=expected_sha256,
                                  max_artifact_bytes=max_artifact_bytes, max_response_bytes=max_response_bytes,
                                  max_error_response_bytes=max_error_response_bytes, operation_timeout=operation_timeout,
                                  header_timeout=header_timeout, idle_timeout=idle_timeout, stop_event=stop_event)

    def create_artifact(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/artifacts", input_data)

    def get_artifact_content(self, artifact_id: str, **options: Any) -> dict[str, Any]:
        return self.get(f"/v1/artifacts/{_path_segment(artifact_id)}/content", **options)

    def get_verified_artifact_content(
        self, artifact_id: str, *, expected_sha256: str | None = None,
        max_artifact_bytes: int | None = None, max_response_bytes: int | None = None,
        max_error_response_bytes: int | None = None, operation_timeout: float | None = None,
        stop_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Verify the existing buffered JSON text response; this is not a download stream."""
        if not isinstance(artifact_id, str):
            raise ValueError("artifact_id must be a non-empty string")
        _path_segment(artifact_id)
        if expected_sha256 is not None and (not isinstance(expected_sha256, str) or
                                            re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None):
            raise ValueError("expected_sha256 must contain 64 hexadecimal characters")
        limit = _response_byte_limit(max_artifact_bytes, self.max_response_bytes, "max_artifact_bytes")
        timeout = self.operation_timeout if operation_timeout is None else operation_timeout
        with self.operation(timeout=timeout, stop_event=stop_event) as lifetime:
            content = self.get_artifact_content(artifact_id, max_response_bytes=max_response_bytes,
                                                max_error_response_bytes=max_error_response_bytes)
            lifetime.check()
            if not isinstance(content, dict) or content.get("artifact_id") != artifact_id or not isinstance(content.get("body"), str):
                raise SwarmArtifactIntegrityError()
            size, declared_hash = content.get("size_bytes"), content.get("sha256")
            if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= 9007199254740991:
                raise SwarmArtifactIntegrityError()
            if not isinstance(declared_hash, str) or re.fullmatch(r"[0-9a-fA-F]{64}", declared_hash) is None:
                raise SwarmArtifactIntegrityError()
            if size > limit:
                raise SwarmResponseLimitError(limit, 200)
            digest = hashlib.sha256()
            actual_size = 0
            text = content["body"]
            for offset in range(0, len(text), 16384):
                lifetime.check()
                invalid_utf8 = False
                try:
                    chunk = text[offset:offset + 16384].encode("utf-8")
                except UnicodeError:
                    invalid_utf8 = True
                if invalid_utf8:
                    raise SwarmArtifactIntegrityError() from None
                actual_size += len(chunk)
                if actual_size > limit:
                    raise SwarmResponseLimitError(limit, 200)
                digest.update(chunk)
            lifetime.check()
            actual_hash = digest.hexdigest()
            if actual_size != size or actual_hash != declared_hash.lower() or (
                expected_sha256 is not None and actual_hash != expected_sha256.lower()
            ):
                raise SwarmArtifactIntegrityError()
            lifetime.check()
            return content

    def create_evaluation(
        self,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self.post("/v1/evaluations", input_data, idempotency_key=idempotency_key)

    def create_token_usage_event(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/token-usage-events", input_data)

    def list_token_usage_events(self, **query: Any) -> CollectionPage:
        return self.list("/v1/token-usage-events", query=query)

    def get_token_usage_event(self, event_id: str) -> dict[str, Any]:
        event_key = _path_segment(event_id)
        return self.get(f"/v1/token-usage-events/{event_key}")

    def list_token_usage_rollups(self, **query: Any) -> list[dict[str, Any]]:
        data = self.get("/v1/token-usage-rollups", query=query)
        return data if isinstance(data, list) else []

    def append_agent_action_trace_batch(
        self,
        space_id: str,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(
            f"/v1/spaces/{space_key}/agent-action-traces/batches",
            input_data,
            idempotency_key=idempotency_key,
        )

    def publish_agent_checkpoint(
        self,
        space_id: str,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.post(
            f"/v1/spaces/{space_key}/checkpoints",
            input_data,
            idempotency_key=idempotency_key,
        )

    def list_agent_action_traces(self, **query: Any) -> CollectionPage:
        return self.list("/v1/agent-action-traces", query=query)

    def get_agent_action_trace(self, trace_id: str) -> dict[str, Any]:
        trace_key = _path_segment(trace_id)
        return self.get(f"/v1/agent-action-traces/{trace_key}")

    def get_space_action_intelligence(self, space_id: str, **query: Any) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.get(f"/v1/spaces/{space_key}/action-intelligence", query=query)

    def list_budget_accounts(self, **query: Any) -> CollectionPage:
        return self.list("/v1/budget-accounts", query=query)

    def get_budget_account(self, account_id: str) -> dict[str, Any]:
        return self.get(f"/v1/budget-accounts/{_path_segment(account_id)}")

    def create_budget_account(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/budget-accounts", input_data)

    def fund_budget_account(self, account_id: str, amount: int, reason: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"amount": amount}
        if reason:
            body["reason"] = reason
        return self.post(f"/v1/budget-accounts/{_path_segment(account_id)}/fund", body)

    def list_budget_ledger_entries(self, **query: Any) -> CollectionPage:
        return self.list("/v1/budget-ledger-entries", query=query)

    def get_budget_ledger_entry(self, entry_id: str) -> dict[str, Any]:
        return self.get(f"/v1/budget-ledger-entries/{_path_segment(entry_id)}")

    def list_budget_delegations(self, **query: Any) -> CollectionPage:
        return self.list("/v1/budget-delegations", query=query)

    def get_budget_delegation(self, delegation_id: str) -> dict[str, Any]:
        return self.get(f"/v1/budget-delegations/{_path_segment(delegation_id)}")

    def create_budget_delegation(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/budget-delegations", input_data)

    def consume_budget_delegation(self, delegation_id: str, amount: int, reason: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"amount": amount}
        if reason:
            body["reason"] = reason
        return self.post(f"/v1/budget-delegations/{_path_segment(delegation_id)}/consume", body)

    def revoke_budget_delegation(self, delegation_id: str, reason: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if reason:
            body["reason"] = reason
        return self.post(f"/v1/budget-delegations/{_path_segment(delegation_id)}/revoke", body)

    def list_approval_requests(self, **query: Any) -> CollectionPage:
        return self.list("/v1/approval-requests", query=query)

    def get_approval_request(self, request_id: str) -> dict[str, Any]:
        return self.get(f"/v1/approval-requests/{_path_segment(request_id)}")

    def create_approval_request(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/approval-requests", input_data)

    def approve_approval_request(self, request_id: str, resolution_note: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if resolution_note:
            body["resolution_note"] = resolution_note
        return self.post(f"/v1/approval-requests/{_path_segment(request_id)}/approve", body)

    def reject_approval_request(self, request_id: str, resolution_note: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if resolution_note:
            body["resolution_note"] = resolution_note
        return self.post(f"/v1/approval-requests/{_path_segment(request_id)}/reject", body)

    def list_webhook_actions(self) -> list[dict[str, Any]]:
        return self.get("/v1/webhook-actions")

    def list_webhook_action_presets(self) -> list[dict[str, Any]]:
        return self.get("/v1/webhook-action-presets")

    def list_webhook_targets(self, **query: Any) -> CollectionPage:
        return self.list("/v1/webhook-targets", query=query)

    def get_webhook_target(self, target_id: str) -> dict[str, Any]:
        return self.get(f"/v1/webhook-targets/{_path_segment(target_id)}")

    def create_webhook_target(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/webhook-targets", input_data)

    def create_webhook_target_from_preset(self, preset_key: str, input_data: dict[str, Any]) -> dict[str, Any]:
        normalized_key = str(preset_key or "").strip().lower()
        presets = self.list_webhook_action_presets()
        preset = next((candidate for candidate in presets if str(candidate.get("key", "")).strip().lower() == normalized_key), None)
        if preset is None:
            raise ValueError(f"Unknown Swarm webhook action preset: {preset_key}")
        request_body = dict(input_data)
        request_body["included_actions"] = list(preset.get("included_actions", []))
        return self.create_webhook_target(request_body)

    def update_webhook_target(self, target_id: str, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post(f"/v1/webhook-targets/{_path_segment(target_id)}/update", input_data)

    def disable_webhook_target(self, target_id: str) -> dict[str, Any]:
        return self.post(f"/v1/webhook-targets/{_path_segment(target_id)}/disable")

    def reconcile_webhook_target(self, target_id: str, limit: int | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if isinstance(limit, int) and limit > 0:
            body["limit"] = limit
        return self.post(f"/v1/webhook-targets/{_path_segment(target_id)}/reconcile", body)

    def list_webhook_deliveries(self, **query: Any) -> CollectionPage:
        return self.list("/v1/webhook-deliveries", query=query)

    def get_webhook_delivery(self, delivery_id: str) -> dict[str, Any]:
        return self.get(f"/v1/webhook-deliveries/{_path_segment(delivery_id)}")

    def redrive_webhook_delivery(self, delivery_id: str) -> dict[str, Any]:
        return self.post(f"/v1/webhook-deliveries/{_path_segment(delivery_id)}/redrive")

    def list_space_directory_entries(self, **query: Any) -> CollectionPage:
        return self.list("/v1/directory/spaces", query=query)

    def list_agent_directory_entries(self, **query: Any) -> CollectionPage:
        return self.list("/v1/directory/agents", query=query)

    def get_space_operator_summary(self, space_id: str) -> dict[str, Any]:
        return self.get(f"/v1/operator/spaces/{_path_segment(space_id)}/summary")

    def get_space_operator_live_snapshot(self, space_id: str, **query: Any) -> dict[str, Any]:
        return self.get(f"/v1/operator/spaces/{_path_segment(space_id)}/live", query=query)

    def get_control_plane_observability(self) -> dict[str, Any]:
        return self.get("/v1/operator/control-plane-observability")

    def list_space_operator_activity(self, space_id: str, **query: Any) -> CollectionPage:
        return self.list(f"/v1/operator/spaces/{_path_segment(space_id)}/activity", query=query)

    def list_space_operator_presence_entries(self, space_id: str, **query: Any) -> CollectionPage:
        return self.list(f"/v1/operator/spaces/{_path_segment(space_id)}/presence", query=query)

    def list_space_signed_status_entries(self, space_id: str, **query: Any) -> CollectionPage:
        return self.list(f"/v1/operator/spaces/{_path_segment(space_id)}/signed-status", query=query)
