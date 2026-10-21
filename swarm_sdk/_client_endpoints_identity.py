from __future__ import annotations

from typing import Any

from ._client_core import (
    CollectionPage,
    _normalize_bootstrap_current_user_entity_input,
    _normalize_bootstrap_space_input,
    _normalize_connect_current_user_and_launch_input,
    _normalize_connect_current_user_client_input,
    _path_segment,
    build_bootstrap_current_user_entity_idempotency_key,
    build_bootstrap_space_idempotency_key,
    build_connect_client_idempotency_key,
    build_connect_current_user_and_launch_idempotency_key,
    build_connect_current_user_client_idempotency_key,
)


class IdentityEndpoints:
    def create_entity(self, name: str, *, key: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name}
        if key is not None:
            body["key"] = key
        return self.post("/v1/entities", body)

    def get_current_user(self) -> dict[str, Any]:
        return self.get("/v1/me")

    def create_provider_session(
        self,
        *,
        provider: str,
        provider_subject: str,
        primary_email: str,
        display_name: str,
        asserted_at: int,
        signature: str,
        avatar_url: str | None = None,
        active_entity_id: str | None = None,
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "provider": provider,
            "provider_subject": provider_subject,
            "primary_email": primary_email,
            "display_name": display_name,
            "asserted_at": asserted_at,
            "signature": signature,
        }
        if avatar_url is not None:
            body["avatar_url"] = avatar_url
        if active_entity_id is not None:
            body["active_entity_id"] = active_entity_id
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds
        return self.post("/v1/auth/provider-sessions", body)

    def get_user_session(self, session_id: str) -> dict[str, Any]:
        return self.get(f"/v1/user-sessions/{_path_segment(session_id)}")

    def revoke_user_session(self, session_id: str, *, reason: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        return self.post(f"/v1/user-sessions/{_path_segment(session_id)}/revoke", body)

    def create_space(
        self,
        entity_id: str,
        key: str,
        name: str,
        *,
        description: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"entity_id": entity_id, "key": key, "name": name}
        if description is not None:
            body["description"] = description
        return self.post("/v1/spaces", body)

    def bootstrap_space(
        self,
        entity_id: str,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
    ) -> dict[str, Any]:
        normalized_input = _normalize_bootstrap_space_input(input_data)
        return self.post(
            f"/v1/entities/{_path_segment(entity_id)}/bootstrap-space",
            normalized_input,
            idempotency_key=idempotency_key
            or build_bootstrap_space_idempotency_key(entity_id, normalized_input, intent_key=intent_key),
        )

    def update_space(
        self,
        space_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if description is not None:
            body["description"] = description
        return self.patch(f"/v1/spaces/{_path_segment(space_id)}", body)

    def update_space_learning_policy(
        self,
        space_id: str,
        *,
        memory_policy: str,
        expected_updated_at: str,
        confirm_verified_auto_promotion: bool,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        space_key = _path_segment(space_id)
        return self.patch(
            f"/v1/spaces/{space_key}/learning-policy",
            {
                "memory_policy": memory_policy,
                "expected_updated_at": expected_updated_at,
                "confirm_verified_auto_promotion": confirm_verified_auto_promotion,
            },
            idempotency_key=idempotency_key,
        )

    def connect_space_client(
        self,
        space_id: str,
        input_data: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
    ) -> dict[str, Any]:
        return self.post(
            f"/v1/spaces/{_path_segment(space_id)}/connect-client",
            input_data or {},
            idempotency_key=idempotency_key
            or build_connect_client_idempotency_key(space_id, input_data, intent_key=intent_key),
        )

    def connect_client(
        self,
        space_id: str,
        input_data: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
    ) -> dict[str, Any]:
        return self.connect_space_client(
            space_id,
            input_data,
            idempotency_key=idempotency_key,
            intent_key=intent_key,
        )

    def connect_current_user_client(
        self,
        input_data: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
    ) -> dict[str, Any]:
        normalized_input = _normalize_connect_current_user_client_input(input_data)
        return self.post(
            "/v1/me/connect-client",
            normalized_input,
            idempotency_key=idempotency_key
            or build_connect_current_user_client_idempotency_key(normalized_input, intent_key=intent_key),
        )

    def bootstrap_current_user_entity(
        self,
        input_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
    ) -> dict[str, Any]:
        normalized_input = _normalize_bootstrap_current_user_entity_input(input_data)
        return self.post(
            "/v1/me/bootstrap-entity",
            normalized_input,
            idempotency_key=idempotency_key
            or build_bootstrap_current_user_entity_idempotency_key(normalized_input, intent_key=intent_key),
        )

    def connect_current_user_and_launch(
        self,
        input_data: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        intent_key: str | None = None,
    ) -> dict[str, Any]:
        normalized_input = _normalize_connect_current_user_and_launch_input(input_data)
        return self.post(
            "/v1/me/connect-and-launch",
            normalized_input,
            idempotency_key=idempotency_key
            or build_connect_current_user_and_launch_idempotency_key(input_data, intent_key=intent_key),
        )

    def create_agent(self, input_data: dict[str, Any]) -> dict[str, Any]:
        return self.post("/v1/agents", input_data)

    def list_agents(self, **query: Any) -> CollectionPage:
        return self.list("/v1/agents", query=query)

    def list_agent_runtime_credentials(self, agent_id: str, **query: Any) -> CollectionPage:
        return self.list(f"/v1/agents/{_path_segment(agent_id)}/runtime-credentials", query=query)

    def get_agent(self, agent_id: str) -> dict[str, Any]:
        return self.get(f"/v1/agents/{_path_segment(agent_id)}")
