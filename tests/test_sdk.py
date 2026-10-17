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


class SwarmSDKClientTests(unittest.TestCase):
    def test_urllib_transport_disables_redirects_for_credentialed_requests(self) -> None:
        handler = _NoRedirectHandler()
        req = request.Request("https://swarm.example/v1/spaces", method="GET")
        with self.assertRaises(request.HTTPError) as context:
            handler.redirect_request(req, None, 302, "Found", {}, "https://redirected.example/v1/spaces")

        self.assertEqual(context.exception.code, 302)
        self.assertIn("redirects are disabled", str(context.exception))

    def test_urllib_transport_uses_injected_opener_for_request_and_stream(self) -> None:
        opener = FakeOpener(body=b"[]")
        transport = UrllibTransport(opener=opener)

        response = transport.request("GET", "https://swarm.example/v1/spaces", {"Authorization": "Bearer token"}, None, 5.0)
        stream = transport.stream("GET", "https://swarm.example/v1/audit-events/stream", {"Authorization": "Bearer token"}, 3.0)

        self.assertEqual(response.status, 200)
        self.assertEqual(stream.status, 200)
        self.assertEqual(opener.requests[0]["timeout"], 5.0)
        self.assertEqual(opener.requests[1]["timeout"], 3.0)
        self.assertEqual(opener.requests[0]["url"], "https://swarm.example/v1/spaces")
        self.assertEqual(opener.requests[1]["url"], "https://swarm.example/v1/audit-events/stream")

    def test_list_spaces_preserves_collection_headers(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={
                    "content-type": "application/json",
                    "x-next-cursor": "cursor_2",
                    "x-order": "asc",
                },
                body=json.dumps(
                    [
                        {
                            "id": "ws_1",
                            "entity_id": "ent_1",
                            "key": "core",
                            "name": "Core",
                            "created_at": "2026-03-11T00:00:00Z",
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="agent-token", transport=transport)

        page = client.list_spaces(entity_id="ent_1", limit=10, order="asc")

        self.assertEqual(len(page.items), 1)
        self.assertEqual(page.next_cursor, "cursor_2")
        self.assertEqual(page.order, "asc")
        self.assertIn("Authorization", transport.requests[0]["headers"])

    def test_space_metadata_and_learning_policy_use_separate_routes(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "id": "ws_1",
                        "entity_id": "ent_1",
                        "key": "research",
                        "name": "Research",
                        "description": "Focused space for research and operator review.",
                        "created_at": "2026-03-11T00:00:00Z",
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
                        "id": "ws_1",
                        "entity_id": "ent_1",
                        "key": "research",
                        "name": "Research",
                        "description": "Updated space description.",
                        "memory_policy": "auto_promote_safe",
                        "created_at": "2026-03-11T00:00:00Z",
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
                        "id": "ws_1",
                        "entity_id": "ent_1",
                        "key": "research",
                        "name": "Research",
                        "description": "Updated space description.",
                        "created_at": "2026-03-11T00:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        created = client.create_space(
            "ent_1",
            "research",
            "Research",
            description="Focused space for research and operator review.",
        )
        updated = client.update_space("ws_1", description="Updated space description.")
        client.update_space_learning_policy(
            "ws_1",
            memory_policy="auto_promote_safe",
            expected_updated_at="2026-03-11T00:00:00Z",
            confirm_verified_auto_promotion=True,
            idempotency_key="learning-policy-1",
        )

        self.assertEqual(created["description"], "Focused space for research and operator review.")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/spaces"))
        self.assertEqual(
            transport.requests[0]["body"]["description"],
            "Focused space for research and operator review.",
        )
        self.assertTrue(transport.requests[1]["url"].endswith("/v1/spaces/ws_1"))
        self.assertEqual(transport.requests[1]["method"], "PATCH")
        self.assertEqual(transport.requests[1]["body"]["description"], "Updated space description.")
        self.assertTrue(transport.requests[2]["url"].endswith("/v1/spaces/ws_1/learning-policy"))
        self.assertEqual(transport.requests[2]["method"], "PATCH")
        self.assertEqual(transport.requests[2]["headers"]["Idempotency-Key"], "learning-policy-1")
        self.assertEqual(
            transport.requests[2]["body"],
            {
                "memory_policy": "auto_promote_safe",
                "expected_updated_at": "2026-03-11T00:00:00Z",
                "confirm_verified_auto_promotion": True,
            },
        )

    def test_request_redacts_sensitive_error_detail(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=401,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "error": "Authorization: Bearer swarm_agt_abcdefghijklmnop API_KEY=supersecretvalue"
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        with self.assertRaises(SwarmAPIError) as context:
            client.get_current_user()

        message = str(context.exception)
        self.assertNotIn("swarm_agt_abcdefghijklmnop", message)
        self.assertNotIn("supersecretvalue", message)
        self.assertIn("[REDACTED_SECRET]", message)

    def test_unavailable_artifact_content_is_a_safe_gone_error(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=410,
                headers={"content-type": "application/json"},
                body=json.dumps({"error": "artifact content is unavailable"}).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        with self.assertRaises(SwarmAPIError) as context:
            client.get_artifact_content("art_missing")

        self.assertEqual(context.exception.status, 410)
        self.assertEqual(context.exception.body, {"error": "artifact content is unavailable"})
        self.assertNotIn("payload_ref", str(context.exception).lower())
        self.assertNotIn("storage_key", str(context.exception).lower())

    def test_create_entity_accepts_an_optional_custom_key(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "id": "ent_1",
                        "key": "blackscale-research",
                        "name": "Blackscale Research",
                        "created_at": "2026-03-12T00:00:00Z",
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)

        entity = client.create_entity("Blackscale Research", key="blackscale-research")

        self.assertEqual(entity["key"], "blackscale-research")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/entities"))
        self.assertEqual(
            transport.requests[0]["body"],
            {"name": "Blackscale Research", "key": "blackscale-research"},
        )

    def test_human_session_helpers_target_canonical_self_and_session_endpoints(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "user": {
                            "id": "usr_1",
                            "primary_email": "operator@swarm.dev",
                            "display_name": "Operator",
                            "status": "active",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "session": {
                            "id": "ses_1",
                            "user_id": "usr_1",
                            "active_entity_id": "ent_1",
                            "created_at": "2026-03-12T00:00:00Z",
                            "last_seen_at": "2026-03-12T00:00:00Z",
                            "expires_at": "2026-03-12T12:00:00Z",
                        },
                        "active_entity_role": "entity_admin",
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
                        "id": "ses_2",
                        "user_id": "usr_2",
                        "active_entity_id": "ent_1",
                        "created_at": "2026-03-12T00:00:00Z",
                        "last_seen_at": "2026-03-12T00:00:00Z",
                        "expires_at": "2026-03-12T12:00:00Z",
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
                        "id": "ses_2",
                        "user_id": "usr_2",
                        "active_entity_id": "ent_1",
                        "created_at": "2026-03-12T00:00:00Z",
                        "last_seen_at": "2026-03-12T00:00:00Z",
                        "expires_at": "2026-03-12T12:00:00Z",
                        "revocation_reason": "logout",
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="human-session-token", transport=transport)

        current = client.get_current_user()
        session = client.get_user_session("ses_2")
        revoked = client.revoke_user_session("ses_2", reason="logout")

        self.assertEqual(current["user"]["id"], "usr_1")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/me"))
        self.assertTrue(transport.requests[1]["url"].endswith("/v1/user-sessions/ses_2"))
        self.assertEqual(session["id"], "ses_2")
        self.assertTrue(transport.requests[2]["url"].endswith("/v1/user-sessions/ses_2/revoke"))
        self.assertEqual(revoked["revocation_reason"], "logout")

    def test_create_provider_session_targets_public_provider_endpoint(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "user": {
                            "id": "usr_1",
                            "primary_email": "operator@swarm.dev",
                            "display_name": "Operator",
                            "status": "active",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "identity": {
                            "id": "uid_1",
                            "user_id": "usr_1",
                            "provider": "google",
                            "provider_subject": "google-subject-1",
                            "primary_email": "operator@swarm.dev",
                            "display_name": "Operator",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "session": {
                            "id": "ses_1",
                            "user_id": "usr_1",
                            "created_at": "2026-03-12T00:00:00Z",
                            "last_seen_at": "2026-03-12T00:00:00Z",
                            "expires_at": "2026-03-12T12:00:00Z",
                        },
                        "token": "swarm_usr_test",
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="human-session-token", transport=transport)

        issued = client.create_provider_session(
            provider="google",
            provider_subject="google-subject-1",
            primary_email="operator@swarm.dev",
            display_name="Operator",
            asserted_at=1741737600,
            signature="provider-signature",
        )

        self.assertEqual(issued["identity"]["provider"], "google")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/auth/provider-sessions"))
        self.assertEqual(transport.requests[0]["method"], "POST")

    def test_create_provider_session_does_not_fabricate_a_contract_key_requirement(self) -> None:
        transport = FakeTransport()
        transport.responses.extend(
            [
                HTTPResponse(
                    status=201,
                    headers={"content-type": "application/json"},
                    body=json.dumps({"token": "swarm_usr_test"}).encode("utf-8"),
                ),
                HTTPResponse(
                    status=201,
                    headers={"content-type": "application/json"},
                    body=json.dumps({"token": "swarm_usr_test"}).encode("utf-8"),
                ),
            ]
        )
        client = SwarmClient(base_url="https://swarm.example", transport=transport)

        payload = {
            "provider": "google",
            "provider_subject": "google-subject-1",
            "primary_email": "operator@swarm.dev",
            "display_name": "Operator",
            "asserted_at": 1741737600,
            "signature": "provider-signature",
        }

        client.create_provider_session(**payload)
        client.create_provider_session(**payload)

        self.assertNotIn("Idempotency-Key", transport.requests[0]["headers"])
        self.assertNotIn("Idempotency-Key", transport.requests[1]["headers"])
        self.assertEqual(transport.requests[0]["body"], transport.requests[1]["body"])

    def test_bootstrap_space_targets_entity_bootstrap_endpoint_with_stable_idempotency(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "space": {
                            "id": "ws_1",
                            "entity_id": "ent_1",
                            "key": "headless",
                            "name": "Headless",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "agent": {
                            "id": "agt_1",
                            "space_id": "ws_1",
                            "kind": "executor",
                            "name": "Primary Runtime",
                            "capabilities": ["artifact.write"],
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "credential": {
                            "credential": {
                                "id": "cred_1",
                                "agent_id": "agt_1",
                                "created_at": "2026-03-12T00:00:00Z",
                            },
                            "token": "swarm_agt_test",
                        },
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)
        input_data = {
            "space": {"key": "headless", "name": "Headless"},
            "agent": {"kind": "executor", "name": "Primary Runtime", "capabilities": ["artifact.write"]},
            "launch_template": {
                "name": "Research Launch",
                "title": "Research task",
                "description": "Produce a research draft.",
                "scoring_contract": {
                    "rubric_key": "research.review.v1",
                    "version": 1,
                    "pass_threshold": 85,
                    "fail_threshold": 50,
                    "strict_mode": True,
                    "dimensions": [
                        {"key": "safety", "weight_bps": 2000, "min_required": 85},
                        {"key": "factuality", "weight_bps": 3500, "min_required": 80},
                        {"key": "completeness", "weight_bps": 2500, "min_required": 65},
                        {"key": "actionability", "weight_bps": 2000, "min_required": 70},
                    ],
                },
            },
        }

        result = client.bootstrap_space("ent_1", input_data)

        self.assertEqual(result["space"]["id"], "ws_1")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/entities/ent_1/bootstrap-space"))
        self.assertEqual(transport.requests[0]["body"]["space"], {"key": "headless", "name": "Headless", "description": ""})
        self.assertEqual(
            transport.requests[0]["headers"]["Idempotency-Key"],
            build_bootstrap_space_idempotency_key("ent_1", input_data),
        )
        self.assertEqual(
            build_bootstrap_space_idempotency_key("ent_1", input_data),
            build_bootstrap_space_idempotency_key("ent_1", input_data),
        )
        self.assertEqual(
            [rule["key"] for rule in transport.requests[0]["body"]["launch_template"]["scoring_contract"]["dimensions"]],
            ["actionability", "completeness", "factuality", "safety"],
        )

    def test_connect_space_client_targets_space_scoped_connection_endpoint_with_stable_idempotency(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "space": {
                            "id": "ws_1",
                            "entity_id": "ent_1",
                            "key": "research",
                            "name": "Research",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "agent": {
                            "id": "agt_1",
                            "space_id": "ws_1",
                            "kind": "codex",
                            "name": "Codex Runtime",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "credential": {
                            "credential": {
                                "id": "cred_1",
                                "agent_id": "agt_1",
                                "created_at": "2026-03-12T00:00:00Z",
                            },
                            "token": "swarm_agt_test",
                        },
                        "connection": {
                            "mode": "direct_api",
                            "space_id": "ws_1",
                            "agent_id": "agt_1",
                            "authentication_type": "bearer",
                            "authorization_header": "Authorization",
                        },
                        "agent_created": True,
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="operator-token", transport=transport)
        input_data = {
            "kind": "codex",
            "name": "Codex Runtime",
            "capabilities": ["run.execute", "artifact.write"],
            "credential": {"description": "direct api", "ttl_seconds": 3600},
        }

        result = client.connect_space_client("ws_1", input_data)

        self.assertEqual(result["agent"]["id"], "agt_1")
        self.assertEqual(result["connection"]["mode"], "direct_api")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/spaces/ws_1/connect-client"))
        self.assertEqual(
            transport.requests[0]["headers"]["Idempotency-Key"],
            build_connect_space_client_idempotency_key("ws_1", input_data),
        )

    def test_connect_current_user_client_targets_personal_home_endpoint_with_stable_idempotency(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "user": {
                            "id": "usr_1",
                            "primary_email": "personal@blackscale.ai",
                            "display_name": "Personal Operator",
                            "default_entity_id": "ent_1",
                            "default_space_id": "ws_1",
                            "status": "active",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "entity": {
                            "id": "ent_1",
                            "name": "Personal Operator Personal",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "space": {
                            "id": "ws_1",
                            "entity_id": "ent_1",
                            "key": "personal",
                            "name": "Personal",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "agent": {
                            "id": "agt_1",
                            "space_id": "ws_1",
                            "kind": "codex",
                            "name": "Codex Chat",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "credential": {
                            "credential": {
                                "id": "cred_1",
                                "agent_id": "agt_1",
                                "created_at": "2026-03-12T00:00:00Z",
                            },
                            "token": "swarm_agt_test",
                        },
                        "connection": {
                            "mode": "direct_api",
                            "space_id": "ws_1",
                            "agent_id": "agt_1",
                            "authentication_type": "bearer",
                            "authorization_header": "Authorization",
                        },
                        "created_entity": True,
                        "created_space": True,
                        "agent_created": True,
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="human-token", transport=transport)
        input_data = {
            "space_id": "ws_1",
            "space_description": "Personal space for direct chat execution.",
            "kind": "codex",
            "name": "Codex Chat",
            "credential": {"description": "chat bootstrap", "ttl_seconds": 3600},
        }

        result = client.connect_current_user_client(input_data)

        self.assertEqual(result["space"]["key"], "personal")
        self.assertEqual(result["user"]["default_space_id"], "ws_1")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/me/connect-client"))
        self.assertEqual(
            transport.requests[0]["headers"]["Idempotency-Key"],
            build_connect_current_user_client_idempotency_key(input_data),
        )
        self.assertEqual(transport.requests[0]["body"]["space_id"], "ws_1")
        self.assertEqual(transport.requests[0]["body"]["space_description"], "Personal space for direct chat execution.")

    def test_bootstrap_current_user_entity_targets_self_serve_entity_bootstrap_endpoint(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "user": {"id": "usr_1"},
                        "session": {"id": "ses_1", "user_id": "usr_1", "active_entity_id": "ent_1"},
                        "entity": {"id": "ent_1", "key": "acme-research", "name": "Acme Research"},
                        "entity_role": "entity_owner",
                        "space": {"id": "ws_1", "entity_id": "ent_1", "key": "home", "name": "General"},
                        "space_role": "space_admin",
                        "session_activated": True,
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="human-token", transport=transport)

        input_data = {
            "key": "acme-research",
            "name": "Acme Research",
            "space": {"description": "Primary company space."},
        }
        result = client.bootstrap_current_user_entity(input_data)

        self.assertEqual(result["entity"]["id"], "ent_1")
        self.assertEqual(result["entity"]["key"], "acme-research")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/me/bootstrap-entity"))
        self.assertEqual(transport.requests[0]["body"]["key"], "acme-research")
        self.assertEqual(transport.requests[0]["body"]["space"]["description"], "Primary company space.")
        self.assertNotIn("connection", transport.requests[0]["body"])
        self.assertNotIn("launch_template", transport.requests[0]["body"])
        self.assertNotIn("automation_profile", transport.requests[0]["body"])
        self.assertEqual(
            transport.requests[0]["headers"]["Idempotency-Key"],
            build_bootstrap_current_user_entity_idempotency_key(input_data),
        )

    def test_connect_current_user_and_launch_targets_one_shot_current_user_chat_launch_endpoint(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "user": {
                            "id": "usr_1",
                            "primary_email": "operator@example.com",
                            "display_name": "Operator",
                            "status": "active",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "entity": {
                            "id": "ent_1",
                            "name": "Operator Personal",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "space": {
                            "id": "ws_1",
                            "entity_id": "ent_1",
                            "key": "personal",
                            "name": "Personal",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "agent": {
                            "id": "agt_1",
                            "space_id": "ws_1",
                            "kind": "codex",
                            "name": "Codex Chat",
                            "capabilities": ["artifact.write"],
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "credential": {
                            "id": "cred_1",
                            "agent_id": "agt_1",
                            "class": "connected_launch",
                            "description": "chat bootstrap",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "runtime_access": {
                            "credential": {
                                "id": "cred_1",
                                "agent_id": "agt_1",
                                "class": "connected_launch",
                                "description": "chat bootstrap",
                                "created_at": "2026-03-12T00:00:00Z",
                            },
                            "token": "swarm_agt_test",
                        },
                        "connection": {
                            "mode": "direct_api",
                            "space_id": "ws_1",
                            "agent_id": "agt_1",
                            "authentication_type": "bearer",
                            "authorization_header": "Authorization",
                        },
                        "runtime_presence": {
                            "agent_id": "agt_1",
                            "space_id": "ws_1",
                            "status": "online",
                            "last_heartbeat_at": "2026-03-12T00:00:00Z",
                            "protocol": "codex_chat",
                            "version": "desktop-v1",
                            "execution_mode": "sandboxed",
                            "sandbox_provider": "bwrap",
                            "sandbox_preset": "bwrap",
                            "metadata_trust": "self_reported",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "webhook_target": {
                            "id": "wht_1",
                            "space_id": "ws_1",
                            "name": "Chat Follow-Up",
                            "url": "https://hooks.example.com/swarm/chat",
                            "included_actions": ["run.launched"],
                            "has_signing_secret": False,
                            "max_attempts": 8,
                            "initial_backoff_seconds": 15,
                            "max_backoff_seconds": 900,
                            "dead_letter_count": 0,
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "task": {
                            "id": "task_1",
                            "space_id": "ws_1",
                            "title": "Research beautiful islands",
                            "description": "Create a ranked brief.",
                            "created_at": "2026-03-12T00:00:00Z",
                        },
                        "run": {
                            "id": "run_1",
                            "space_id": "ws_1",
                            "task_id": "task_1",
                            "status": "leased",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "run_lease": {
                            "id": "lease_1",
                            "run_id": "run_1",
                            "agent_id": "agt_1",
                            "status": "active",
                            "issued_at": "2026-03-12T00:00:00Z",
                            "expires_at": "2026-03-12T00:01:30Z",
                            "created_at": "2026-03-12T00:00:00Z",
                            "updated_at": "2026-03-12T00:00:00Z",
                        },
                        "created_entity": True,
                        "created_space": True,
                        "agent_created": True,
                    }
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", token="human-token", transport=transport)
        input_data = {
            "connection": {
                "kind": "codex",
                "name": "Codex Chat",
                "space_description": "Personal space for direct chat execution.",
            },
            "runtime_heartbeat": {
                "protocol": "codex_chat",
                "version": "desktop-v1",
                "executionMode": "sandboxed",
                "sandboxProvider": "bwrap",
                "sandboxPreset": "bwrap",
            },
            "webhook_follow_up": {
                "url": "https://hooks.example.com/swarm/chat",
            },
            "launch": {
                "title": "Research beautiful islands",
                "description": "Create a ranked brief.",
                "ttl_seconds": 90,
            },
        }

        launched = client.connect_current_user_and_launch(input_data)

        self.assertEqual(launched["space"]["id"], "ws_1")
        self.assertEqual(launched["run_lease"]["agent_id"], "agt_1")
        self.assertEqual(launched["webhook_target"]["url"], "https://hooks.example.com/swarm/chat")
        self.assertEqual(launched["runtime_presence"]["execution_mode"], "sandboxed")
        self.assertEqual(launched["runtime_presence"]["sandbox_provider"], "bwrap")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/me/connect-and-launch"))
        self.assertEqual(
            transport.requests[0]["headers"]["Idempotency-Key"],
            build_connect_current_user_and_launch_idempotency_key(input_data),
        )
        self.assertEqual(transport.requests[0]["body"]["runtime_heartbeat"]["execution_mode"], "sandboxed")
        self.assertEqual(transport.requests[0]["body"]["runtime_heartbeat"]["sandbox_provider"], "bwrap")
        self.assertEqual(transport.requests[0]["body"]["runtime_heartbeat"]["sandbox_preset"], "bwrap")

    def test_list_chat_client_profiles_targets_public_chat_client_registry(self) -> None:
        transport = FakeTransport()
        transport.responses.append(
            HTTPResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    [
                        {
                            "key": "codex_direct",
                            "label": "Codex Direct",
                            "description": "Codex profile",
                            "connection_mode": "direct_api",
                            "primary_entry_path": "/v1/me/connect-and-launch",
                            "same_client_supported": True,
                            "delegated_supported": True,
                            "follow_up_preset_key": "chat_follow_up",
                            "recommended_prompt_hints": ["Use Swarm for this space."],
                        }
                    ]
                ).encode("utf-8"),
            )
        )
        client = SwarmClient(base_url="https://swarm.example", transport=transport)

        profiles = client.list_chat_client_profiles()

        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0]["key"], "codex_direct")
        self.assertTrue(transport.requests[0]["url"].endswith("/v1/chat-client-profiles"))
