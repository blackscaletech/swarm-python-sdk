from __future__ import annotations

import copy
import hashlib
import json
import unittest
from importlib.resources import files

from swarm_sdk import (SwarmClient, build_bootstrap_current_user_entity_idempotency_key,
                       build_bootstrap_space_idempotency_key, build_launch_run_idempotency_key)
from swarm_sdk._client_inputs import (_normalize_run_requirements, _normalize_scoring_contract,
                                      _normalize_launch_run_input)
from swarm_sdk._client_inputs import _normalize_run_context_refs
from sdk_test_support import FakeTransport, json_response


REQUIREMENTS = {
    "execution_mode": "agent", "assignment_mode": "specific",
    "target_identity_ids": ["Identity_B", "Identity_A"], "excluded_identity_ids": ["Identity_X"],
    "target_agent_ids": ["Agent_B", "Agent_A"], "required_capabilities": ["SEARCH"],
    "required_tools": ["MCP.READ"], "required_models": ["MODEL-A"],
    "required_connectors": ["DRIVE"], "required_regions": ["CA"], "required_protocols": ["MCP"],
    "required_hosting_mode": " CONNECTED ", "required_runtime_human_action_tags": ["LOGIN"],
    "required_approval_actions": ["ARTIFACT:WRITE"], "result_approval_actions": ["ARTIFACT:PUBLISH"],
}

CONTRACT = json.loads(files("swarm_sdk").joinpath("client-contract.json").read_text(encoding="utf-8"))


class AuthorityNormalizationTests(unittest.TestCase):
    def test_all_canonical_launch_fields_survive_and_model_policy_changes_identity(self):
        transport = FakeTransport()
        transport.responses = [json_response({})]
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        input_data = {
            "model_route_policy_id": "mrp_chosen", "automation_profile_id": "aut_chosen", "launch_template_id": "lt_chosen",
            "title": "Review", "description": "Read evidence", "execution_policy_id": "exp_chosen",
            "prompt_capture_mode": "compiled_only", "prompt_visibility_mode": "private",
            "acknowledge_public_raw_prompt_visibility": False, "source_prompt": "User intent", "compiled_brief": "Evidence first",
            "run_requirements": {}, "run_context_refs": [{"resource_type": "artifact", "resource_id": "art_source"}],
        }
        self.assertEqual(set(input_data), set(CONTRACT["fields"]["launchRun"]))
        normalized = _normalize_launch_run_input(input_data)
        self.assertEqual(normalized["model_route_policy_id"], "mrp_chosen")
        client.launch_space_run("sp_test", **input_data)
        self.assertEqual(transport.requests[0]["body"], normalized)
        self.assertNotEqual(build_launch_run_idempotency_key("sp_test", input_data),
                            build_launch_run_idempotency_key("sp_test", {**input_data, "model_route_policy_id": "mrp_other"}))

    def test_boundary_fixture_fields_conform_to_public_contract(self):
        fields = CONTRACT["fields"]
        self.assertEqual(set(fields["runRequirements"]), set(REQUIREMENTS))
        scoring = {"rubric_key", "version", "pass_threshold", "fail_threshold", "strict_mode",
                   "dimensions", "accepted_evaluation_method_keys"}
        self.assertEqual(set(fields["scoringContract"]), scoring)
        self.assertEqual(set(fields["scoringDimension"]), {"key", "weight_bps", "min_required"})
        self.assertEqual(set(fields["runContextRef"]), {"resource_type", "resource_id"})

    def test_every_canonical_requirement_field_survives_and_changes_identity(self):
        fields = set(REQUIREMENTS)
        normalized = _normalize_run_requirements(REQUIREMENTS)
        self.assertEqual(set(normalized), fields)
        self.assertEqual(normalized["target_identity_ids"], ["Identity_A", "Identity_B"])
        self.assertEqual(normalized["excluded_identity_ids"], ["Identity_X"])
        self.assertEqual(normalized["required_hosting_mode"], "connected")
        self.assertEqual(normalized["required_approval_actions"], ["artifact:write"])
        payload = {"title": "A", "run_requirements": REQUIREMENTS}
        baseline = build_launch_run_idempotency_key("space", payload)
        for field in fields:
            changed = copy.deepcopy(payload)
            if isinstance(REQUIREMENTS[field], list):
                changed["run_requirements"][field] = ["different"]
            else:
                changed["run_requirements"][field] = {"execution_mode": "core", "assignment_mode": "self",
                                                       "required_hosting_mode": "swarm_cloud"}[field]
            with self.subTest(field=field):
                self.assertNotEqual(build_launch_run_idempotency_key("space", changed), baseline)
        self.assertEqual(REQUIREMENTS["required_hosting_mode"], " CONNECTED ")

    def test_legacy_defaults_and_canonical_enum_casing_are_not_silently_changed(self):
        defaults = _normalize_run_requirements(None)
        self.assertNotIn("execution_mode", defaults)
        self.assertNotIn("assignment_mode", defaults)
        self.assertNotIn("target_identity_ids", defaults)
        self.assertEqual(defaults, _normalize_run_requirements({"execution_mode": " ", "assignment_mode": ""}))
        for field, valid in (("execution_mode", ["agent", "core"]),
                             ("assignment_mode", ["self", "specific", "any", "others"])):
            for value in valid:
                self.assertEqual(_normalize_run_requirements({field: f" {value} "})[field], value)
                with self.assertRaises(ValueError):
                    _normalize_run_requirements({field: value.upper()})

    def test_equivalent_requirement_sets_have_one_exact_normalized_retry_identity(self):
        original = {"title": " A ", "run_requirements": copy.deepcopy(REQUIREMENTS)}
        equivalent = copy.deepcopy(original)
        for field, value in equivalent["run_requirements"].items():
            if isinstance(value, list):
                equivalent["run_requirements"][field] = [f" {item} " for item in reversed(value)] + [value[0]]
        normalized = _normalize_launch_run_input(original)
        self.assertEqual(normalized, _normalize_launch_run_input(equivalent))
        self.assertEqual(build_launch_run_idempotency_key("space", original),
                         build_launch_run_idempotency_key("space", equivalent))
        transport = FakeTransport()
        transport.responses = [json_response({}), json_response({})]
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        client.launch_space_run("space", **original)
        client.post("/v1/spaces/space/launch-run", transport.requests[0]["body"],
                    idempotency_key=transport.requests[0]["headers"]["Idempotency-Key"])
        self.assertEqual(transport.requests[0], transport.requests[1])
        self.assertEqual(transport.requests[0]["body"]["run_requirements"], normalized["run_requirements"])

    def test_unsupported_or_malformed_requirements_never_reach_transport(self):
        invalid = [{"future_deny_constraint": ["identity"]}, {"execution_mode": "unknown"},
                   {"assignment_mode": True}, {"required_hosting_mode": None}, [], "agent"]
        for field, value in REQUIREMENTS.items():
            if isinstance(value, list):
                invalid.extend([{field: "not-a-list"}, {field: ["ok", 1]}, {field: [" "]}])
        transport = FakeTransport()
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        for requirements in invalid:
            with self.subTest(requirements=requirements), self.assertRaises(ValueError):
                client.launch_space_run("space", title="A", run_requirements=requirements)
        self.assertEqual(transport.requests, [])

    def test_scoring_accepted_methods_are_preserved_case_sensitive_and_keyed(self):
        scoring = {"rubric_key": "rubric", "version": 1, "pass_threshold": 80, "fail_threshold": 20,
                   "strict_mode": True, "dimensions": [{"key": "quality", "weight_bps": 10000, "min_required": 1}],
                   "accepted_evaluation_method_keys": [" z.method ", "A.method", "A.method"]}
        normalized = _normalize_scoring_contract(scoring)
        self.assertEqual(normalized["accepted_evaluation_method_keys"], ["A.method", "z.method"])
        self.assertEqual(_normalize_scoring_contract({"accepted_evaluation_method_keys": ["method"]})[
            "accepted_evaluation_method_keys"], ["method"])
        payload = {"name": "Entity", "launch_template": {"name": "L", "run_requirements": REQUIREMENTS,
                                                          "scoring_contract": scoring}}
        baseline = build_bootstrap_current_user_entity_idempotency_key(payload)
        for field, value in scoring.items():
            changed = copy.deepcopy(payload)
            if field == "dimensions":
                changed["launch_template"]["scoring_contract"][field][0]["weight_bps"] = 9999
            elif isinstance(value, bool):
                changed["launch_template"]["scoring_contract"][field] = not value
            elif isinstance(value, int):
                changed["launch_template"]["scoring_contract"][field] = value + 1
            elif isinstance(value, list):
                changed["launch_template"]["scoring_contract"][field] = ["different"]
            else:
                changed["launch_template"]["scoring_contract"][field] = "different"
            with self.subTest(field=field):
                self.assertNotEqual(build_bootstrap_current_user_entity_idempotency_key(changed), baseline)
        for methods in ("method", [1], [" "], None):
            with self.assertRaises(ValueError):
                _normalize_scoring_contract({"accepted_evaluation_method_keys": methods})
        with self.assertRaises(ValueError):
            _normalize_scoring_contract({"unknown_authority_field": True})

    def test_bootstrap_entity_key_hashes_exact_sent_payload_including_entity_key_and_nested_authority(self):
        payload = {"key": "entity-a", "name": " Entity ", "activate_session": False,
                   "space": {"key": "space", "name": "S", "description": "D"},
                   "launch_template": {"name": "L", "run_requirements": REQUIREMENTS,
                                       "scoring_contract": {"accepted_evaluation_method_keys": ["manual"]}},
                   "automation_profile": {"key": "profile", "launch_template": {"run_requirements": REQUIREMENTS}}}
        transport = FakeTransport()
        transport.responses = [json_response({})]
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        client.bootstrap_current_user_entity(payload)
        sent = transport.requests[0]["body"]
        digest = hashlib.sha256(json.dumps({**sent, "intent_key": ""}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], f"swarm-me-bootstrap-entity-{digest[:32]}")
        baseline = build_bootstrap_current_user_entity_idempotency_key(payload)
        for field, value in (("key", "entity-b"), ("activate_session", True), ("name", "Other")):
            self.assertNotEqual(build_bootstrap_current_user_entity_idempotency_key({**payload, field: value}), baseline)
        for container in ("launch_template", "automation_profile"):
            changed = copy.deepcopy(payload)
            template = changed[container] if container == "launch_template" else changed[container]["launch_template"]
            template["run_requirements"]["excluded_identity_ids"] = ["another"]
            self.assertNotEqual(build_bootstrap_current_user_entity_idempotency_key(changed), baseline)
        self.assertNotEqual(build_bootstrap_current_user_entity_idempotency_key(payload, intent_key="new"), baseline)

    def test_bootstrap_space_nested_fields_use_exact_normalized_sent_payload(self):
        payload = {"space": {"key": "S"}, "launch": {"run_requirements": REQUIREMENTS},
                   "launch_template": {"scoring_contract": {"accepted_evaluation_method_keys": ["method"]}}}
        transport = FakeTransport()
        transport.responses = [json_response({})]
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        client.bootstrap_space("entity", payload)
        sent = transport.requests[0]["body"]
        digest = hashlib.sha256(json.dumps({"entity_id": "entity", **sent, "intent_key": ""},
                                           sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(transport.requests[0]["headers"]["Idempotency-Key"], f"swarm-bootstrap-{digest[:32]}")
        self.assertEqual(build_bootstrap_space_idempotency_key("entity", payload), f"swarm-bootstrap-{digest[:32]}")

    def test_raw_content_keys_distinguish_wire_whitespace_null_and_ordered_arrays(self):
        transport = FakeTransport()
        transport.responses = [json_response({}) for _ in range(7)]
        client = SwarmClient(base_url="https://local.invalid", transport=transport)
        for payload in ({"body": " x "}, {"body": "x"}, {"body": "x", "optional": None},
                        {"items": ["a", "b"]}, {"items": ["b", "a"]},
                        {"b": 2, "a": 1}, {"a": 1, "b": 2}):
            client.post("/v1/artifacts", payload)
        keys = [row["headers"]["Idempotency-Key"] for row in transport.requests]
        self.assertEqual(len(set(keys[:5])), 5)
        self.assertEqual(keys[5], keys[6])

    def test_context_refs_preserve_each_supported_evidence_type_and_reject_lossy_inputs(self):
        for kind in ("run", "task", "artifact", "evaluation"):
            refs = [{"resource_type": f" {kind.upper()} ", "resource_id": " Identity_A "}]
            normalized = [{"resource_type": kind, "resource_id": "Identity_A"}]
            self.assertEqual(_normalize_run_context_refs(refs + refs), normalized)
            first = build_launch_run_idempotency_key("space", {"run_context_refs": refs})
            self.assertEqual(first, build_launch_run_idempotency_key("space", {"run_context_refs": normalized}))
            self.assertNotEqual(first, build_launch_run_idempotency_key("space", {
                "run_context_refs": [{"resource_type": kind, "resource_id": "Other"}]}))
        invalid = ["not-a-list", [None], [{"resource_type": "repository", "resource_id": "a"}],
                   [{"resource_type": "run", "resource_id": 1}], [{"resource_type": "run", "resource_id": " "}],
                   [{"resource_type": "artifact", "resource_id": "a", "deny": True}]]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                _normalize_run_context_refs(value)

    def test_scoring_contract_and_dimension_malformed_fields_are_never_dropped(self):
        invalid = [{"strict_mode": "true"}, {"version": True}, {"version": 2**53},
                   {"pass_threshold": 1.5}, {"fail_threshold": "5"}, {"dimensions": {}},
                   {"dimensions": [None]}, {"dimensions": [{"key": "k", "deny": True}]},
                   {"dimensions": [{"key": " ", "weight_bps": 1}]},
                   {"dimensions": [{"key": "k", "weight_bps": True}]},
                   {"dimensions": [{"key": "k", "min_required": 2**53}]}]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                _normalize_scoring_contract(value)


if __name__ == "__main__":
    unittest.main()
