from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib import parse
import uuid


def _path_segment(value: Any) -> str:
    segment = str(value)
    if segment in {"", ".", ".."}:
        raise ValueError("SwarmClient requires a non-empty identifier, not a path traversal segment")
    return parse.quote(segment, safe="")


def _create_idempotency_key(prefix: str = "swarm-sdk-py") -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _canonicalize_idempotency_value(value: Any, *, preserve_strings: bool = False) -> Any:
    if isinstance(value, str):
        return value if preserve_strings else value.strip()
    if isinstance(value, list):
        return [_canonicalize_idempotency_value(item, preserve_strings=preserve_strings) for item in value]
    if isinstance(value, tuple):
        return [_canonicalize_idempotency_value(item, preserve_strings=preserve_strings) for item in value]
    if isinstance(value, dict):
        canonical: dict[str, Any] = {}
        for key in sorted(value.keys()):
            normalized = _canonicalize_idempotency_value(value[key], preserve_strings=preserve_strings)
            if preserve_strings or normalized is not None:
                canonical[key] = normalized
        return canonical
    return value


def _normalize_string_list(values: Any, *, lower: bool = False) -> list[str]:
    if not isinstance(values, (list, tuple)):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        if not isinstance(raw_value, str):
            continue
        value = raw_value.strip()
        if value == "":
            continue
        if lower:
            value = value.lower()
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    normalized.sort()
    return normalized


_REQUIREMENT_LIST_FIELDS = {
    "target_identity_ids", "excluded_identity_ids", "target_agent_ids", "required_capabilities",
    "required_tools", "required_models", "required_connectors", "required_regions", "required_protocols",
    "required_runtime_human_action_tags", "required_approval_actions", "result_approval_actions",
}
_REQUIREMENT_FIELDS = _REQUIREMENT_LIST_FIELDS | {"execution_mode", "assignment_mode", "required_hosting_mode"}


def _constraint_list(value: Any, name: str, *, lower: bool = False) -> list[str]:
    if not isinstance(value, (list, tuple)) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{name} must be a list of non-empty strings")
    return _normalize_string_list(value, lower=lower)


def _normalize_run_requirements(input_data: Any) -> dict[str, Any]:
    if input_data is None:
        input_data = {}
    if not isinstance(input_data, dict):
        raise ValueError("run_requirements must be an object")
    if input_data.keys() - _REQUIREMENT_FIELDS:
        raise ValueError("run_requirements contains unsupported fields")
    for field in _REQUIREMENT_LIST_FIELDS & input_data.keys():
        _constraint_list(input_data[field], f"run_requirements.{field}")
    for field in {"execution_mode", "assignment_mode", "required_hosting_mode"} & input_data.keys():
        if not isinstance(input_data[field], str):
            raise ValueError(f"run_requirements.{field} must be a string")
    required_hosting_mode = input_data.get("required_hosting_mode")
    runtime_human_action_tags_input = input_data.get("required_runtime_human_action_tags")
    required_runtime_human_action_tags = _normalize_string_list(
        runtime_human_action_tags_input if isinstance(runtime_human_action_tags_input, (list, tuple)) else [],
        lower=True,
    )
    normalized = {
        "target_agent_ids": _normalize_string_list(input_data.get("target_agent_ids")),
        "required_capabilities": _normalize_string_list(input_data.get("required_capabilities"), lower=True),
        "required_tools": _normalize_string_list(input_data.get("required_tools"), lower=True),
        "required_models": _normalize_string_list(input_data.get("required_models"), lower=True),
        "required_connectors": _normalize_string_list(input_data.get("required_connectors"), lower=True),
        "required_regions": _normalize_string_list(input_data.get("required_regions"), lower=True),
        "required_protocols": _normalize_string_list(input_data.get("required_protocols"), lower=True),
        "required_hosting_mode": required_hosting_mode.strip().lower() if isinstance(required_hosting_mode, str) else "",
        "required_runtime_human_action_tags": required_runtime_human_action_tags,
        "required_approval_actions": _normalize_string_list(input_data.get("required_approval_actions"), lower=True),
        "result_approval_actions": _normalize_string_list(input_data.get("result_approval_actions"), lower=True),
    }
    # Canonical enums are case-sensitive. Omitted fields keep legacy defaults.
    for field, allowed in (("execution_mode", {"agent", "core"}),
                           ("assignment_mode", {"self", "specific", "any", "others"})):
        value = input_data.get(field, "").strip()
        if value and value not in allowed:
            raise ValueError(f"run_requirements.{field} is unsupported")
        if value:
            normalized[field] = value
    for field in ("target_identity_ids", "excluded_identity_ids"):
        values = _normalize_string_list(input_data.get(field))
        if values:
            normalized[field] = values
    return normalized


def _normalize_run_context_refs(input_data: Any) -> list[dict[str, str]]:
    if input_data is None:
        return []
    if not isinstance(input_data, list):
        raise ValueError("run_context_refs must be a list")
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in input_data:
        if not isinstance(item, dict) or item.keys() - {"resource_type", "resource_id"}:
            raise ValueError("run_context_refs contains an unsupported reference")
        resource_type = item.get("resource_type")
        resource_id = item.get("resource_id")
        normalized_type = resource_type.strip().lower() if isinstance(resource_type, str) else ""
        normalized_id = resource_id.strip() if isinstance(resource_id, str) else ""
        if normalized_type not in {"run", "task", "artifact", "evaluation"} or not normalized_id:
            raise ValueError("run_context_refs requires a supported resource_type and non-empty resource_id")
        dedupe_key = f"{normalized_type}:{normalized_id}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append({
            "resource_type": normalized_type,
            "resource_id": normalized_id,
        })
    return normalized


def _normalize_launch_run_input(input_data: dict[str, Any] | None) -> dict[str, Any]:
    input_data = input_data or {}
    model_route_policy_id = input_data.get("model_route_policy_id")
    automation_profile_id = input_data.get("automation_profile_id")
    launch_template_id = input_data.get("launch_template_id")
    title = input_data.get("title")
    description = input_data.get("description")
    execution_policy_id = input_data.get("execution_policy_id")
    prompt_capture_mode = input_data.get("prompt_capture_mode")
    prompt_visibility_mode = input_data.get("prompt_visibility_mode")
    acknowledge_public_raw_prompt_visibility = input_data.get("acknowledge_public_raw_prompt_visibility")
    source_prompt = input_data.get("source_prompt")
    compiled_brief = input_data.get("compiled_brief")
    return {
        **({"model_route_policy_id": model_route_policy_id.strip()}
           if isinstance(model_route_policy_id, str) and model_route_policy_id.strip() else {}),
        "automation_profile_id": automation_profile_id.strip() if isinstance(automation_profile_id, str) else "",
        "launch_template_id": launch_template_id.strip() if isinstance(launch_template_id, str) else "",
        "title": title.strip() if isinstance(title, str) else "",
        "description": description.strip() if isinstance(description, str) else "",
        "execution_policy_id": execution_policy_id.strip() if isinstance(execution_policy_id, str) else "",
        "prompt_capture_mode": prompt_capture_mode.strip() if isinstance(prompt_capture_mode, str) else "",
        "prompt_visibility_mode": prompt_visibility_mode.strip() if isinstance(prompt_visibility_mode, str) else "",
        "acknowledge_public_raw_prompt_visibility": acknowledge_public_raw_prompt_visibility is True,
        "source_prompt": source_prompt.strip() if isinstance(source_prompt, str) else "",
        "compiled_brief": compiled_brief.strip() if isinstance(compiled_brief, str) else "",
        "run_requirements": _normalize_run_requirements(input_data.get("run_requirements")),
        "run_context_refs": _normalize_run_context_refs(input_data.get("run_context_refs")),
    }


def _normalize_automation_profile_task_input(input_data: Any) -> dict[str, Any]:
    if not isinstance(input_data, dict):
        input_data = {}
    title = input_data.get("title")
    description = input_data.get("description")
    execution_policy_id = input_data.get("execution_policy_id")
    return {
        "title": title.strip() if isinstance(title, str) else "",
        "description": description.strip() if isinstance(description, str) else "",
        "execution_policy_id": execution_policy_id.strip() if isinstance(execution_policy_id, str) else "",
    }


def _normalize_scoring_contract(input_data: Any) -> dict[str, Any] | None:
    if input_data is None:
        return None
    if not isinstance(input_data, dict):
        raise ValueError("scoring_contract must be an object")
    if input_data.keys() - {"rubric_key", "version", "pass_threshold", "fail_threshold", "strict_mode",
                            "dimensions", "accepted_evaluation_method_keys"}:
        raise ValueError("scoring_contract contains unsupported fields")
    methods = (_constraint_list(input_data["accepted_evaluation_method_keys"], "accepted_evaluation_method_keys")
               if "accepted_evaluation_method_keys" in input_data else [])
    if "rubric_key" in input_data and not isinstance(input_data["rubric_key"], str):
        raise ValueError("scoring_contract.rubric_key must be a string")
    for field in ("version", "pass_threshold", "fail_threshold"):
        if field in input_data and (isinstance(input_data[field], bool) or not isinstance(input_data[field], int)
                                    or abs(input_data[field]) > 9007199254740991):
            raise ValueError(f"scoring_contract.{field} must be a safe integer")
    if "strict_mode" in input_data and not isinstance(input_data["strict_mode"], bool):
        raise ValueError("scoring_contract.strict_mode must be a boolean")
    if "dimensions" in input_data:
        if not isinstance(input_data["dimensions"], list):
            raise ValueError("scoring_contract.dimensions must be a list")
        for rule in input_data["dimensions"]:
            if not isinstance(rule, dict) or rule.keys() - {"key", "weight_bps", "min_required"}:
                raise ValueError("scoring_contract.dimensions contains an unsupported rule")
            if not isinstance(rule.get("key"), str) or not rule["key"].strip():
                raise ValueError("scoring_contract.dimensions.key must be non-empty")
            for field in ("weight_bps", "min_required"):
                if field in rule and (isinstance(rule[field], bool) or not isinstance(rule[field], int)
                                      or abs(rule[field]) > 9007199254740991):
                    raise ValueError(f"scoring_contract.dimensions.{field} must be a safe integer")
    rubric_key = input_data.get("rubric_key")
    dimensions_input = input_data.get("dimensions")
    normalized_dimensions: list[dict[str, Any]] = []
    if isinstance(dimensions_input, list):
        for rule in dimensions_input:
            if not isinstance(rule, dict):
                continue
            key = rule.get("key")
            normalized_key = key.strip().lower() if isinstance(key, str) else ""
            if normalized_key == "":
                continue
            normalized_dimensions.append(
                {
                    "key": normalized_key,
                    "weight_bps": rule.get("weight_bps") if isinstance(rule.get("weight_bps"), int) else 0,
                    "min_required": rule.get("min_required") if isinstance(rule.get("min_required"), int) else 0,
                }
            )
    normalized_dimensions.sort(key=lambda rule: rule["key"])
    version = input_data.get("version")
    pass_threshold = input_data.get("pass_threshold")
    fail_threshold = input_data.get("fail_threshold")
    strict_mode = input_data.get("strict_mode")
    normalized = {
        "rubric_key": rubric_key.strip() if isinstance(rubric_key, str) else "",
        "version": version if isinstance(version, int) else 0,
        "pass_threshold": pass_threshold if isinstance(pass_threshold, int) else 0,
        "fail_threshold": fail_threshold if isinstance(fail_threshold, int) else 0,
        "strict_mode": strict_mode is True,
        "dimensions": normalized_dimensions,
    }
    if methods:
        normalized["accepted_evaluation_method_keys"] = methods
    if (
        normalized["rubric_key"] == ""
        and normalized["version"] == 0
        and normalized["pass_threshold"] == 0
        and normalized["fail_threshold"] == 0
        and normalized["strict_mode"] is False
        and len(normalized_dimensions) == 0
        and not methods
    ):
        return None
    return normalized


def _normalize_automation_profile_launch_template_input(input_data: Any) -> dict[str, Any]:
    if not isinstance(input_data, dict):
        input_data = {}
    name = input_data.get("name")
    summary = input_data.get("summary")
    title = input_data.get("title")
    description = input_data.get("description")
    execution_policy_id = input_data.get("execution_policy_id")
    normalized = {
        "name": name.strip() if isinstance(name, str) else "",
        "summary": summary.strip() if isinstance(summary, str) else "",
        "title": title.strip() if isinstance(title, str) else "",
        "description": description.strip() if isinstance(description, str) else "",
        "execution_policy_id": execution_policy_id.strip() if isinstance(execution_policy_id, str) else "",
        "run_requirements": _normalize_run_requirements(input_data.get("run_requirements")),
        "run_context_refs": _normalize_run_context_refs(input_data.get("run_context_refs")),
    }
    scoring_contract = _normalize_scoring_contract(input_data.get("scoring_contract"))
    if scoring_contract is not None:
        normalized["scoring_contract"] = scoring_contract
    return normalized


def _normalize_bootstrap_space_input(input_data: dict[str, Any] | None) -> dict[str, Any]:
    input_data = input_data or {}
    space_input = input_data.get("space")
    if isinstance(space_input, dict):
        space = space_input
    else:
        space = {}
    agent = input_data.get("agent") if isinstance(input_data.get("agent"), dict) else {}
    credential = input_data.get("credential") if isinstance(input_data.get("credential"), dict) else {}
    launch_template = input_data.get("launch_template") if isinstance(input_data.get("launch_template"), dict) else None
    automation_profile = input_data.get("automation_profile") if isinstance(input_data.get("automation_profile"), dict) else None
    launch = input_data.get("launch") if isinstance(input_data.get("launch"), dict) else None
    return {
        "space": {
            "key": space.get("key", "").strip() if isinstance(space.get("key"), str) else "",
            "name": space.get("name", "").strip() if isinstance(space.get("name"), str) else "",
            "description": space.get("description", "").strip() if isinstance(space.get("description"), str) else "",
        },
        "agent": {
            "kind": agent.get("kind", "").strip() if isinstance(agent.get("kind"), str) else "",
            "name": agent.get("name", "").strip() if isinstance(agent.get("name"), str) else "",
            "description": agent.get("description", "").strip() if isinstance(agent.get("description"), str) else "",
            "capabilities": _normalize_string_list(agent.get("capabilities"), lower=True),
        },
        "credential": {
            "description": credential.get("description", "").strip() if isinstance(credential.get("description"), str) else "",
            "ttl_seconds": credential.get("ttl_seconds")
            if isinstance(credential.get("ttl_seconds"), int) and credential.get("ttl_seconds") > 0
            else 0,
        },
        "launch_template": _normalize_automation_profile_launch_template_input(launch_template) if launch_template else None,
        "automation_profile": {
            "key": automation_profile.get("key", "").strip() if isinstance(automation_profile.get("key"), str) else "",
            "name": automation_profile.get("name", "").strip() if isinstance(automation_profile.get("name"), str) else "",
            "summary": automation_profile.get("summary", "").strip() if isinstance(automation_profile.get("summary"), str) else "",
            "compiler_key": automation_profile.get("compiler_key", "").strip()
            if isinstance(automation_profile.get("compiler_key"), str)
            else "",
            "review_threshold": automation_profile.get("review_threshold")
            if isinstance(automation_profile.get("review_threshold"), int) and automation_profile.get("review_threshold") >= 0
            else 0,
            "prompt_capture_mode": automation_profile.get("prompt_capture_mode", "").strip()
            if isinstance(automation_profile.get("prompt_capture_mode"), str)
            else "",
            "launch_template": _normalize_automation_profile_launch_template_input(automation_profile.get("launch_template")),
            "review_task": _normalize_automation_profile_task_input(automation_profile.get("review_task")),
            "remediation_task": _normalize_automation_profile_task_input(automation_profile.get("remediation_task")),
            "finalize_task": _normalize_automation_profile_task_input(automation_profile.get("finalize_task")),
        }
        if automation_profile
        else None,
        "launch": {
            "mode": launch.get("mode", "").strip() if isinstance(launch.get("mode"), str) else "",
            **_normalize_launch_run_input(launch),
        }
        if launch
        else None,
    }


def _normalize_connect_client_input(space_id: str, input_data: dict[str, Any] | None) -> dict[str, Any]:
    input_data = input_data or {}
    credential = input_data.get("credential") if isinstance(input_data.get("credential"), dict) else {}
    agent_id = ""
    if isinstance(input_data.get("agent_id"), str) and input_data.get("agent_id").strip():
        agent_id = input_data.get("agent_id").strip()
    elif isinstance(input_data.get("agentId"), str) and input_data.get("agentId").strip():
        agent_id = input_data.get("agentId").strip()
    kind = input_data.get("kind")
    return {
        "space_id": space_id.strip() if isinstance(space_id, str) else "",
        "agent_id": agent_id,
        "kind": (kind.strip() if isinstance(kind, str) and kind.strip() else "client") if agent_id == "" else "",
        "name": input_data.get("name", "").strip() if agent_id == "" and isinstance(input_data.get("name"), str) else "",
        "description": input_data.get("description", "").strip()
        if agent_id == "" and isinstance(input_data.get("description"), str)
        else "",
        "capabilities": _normalize_string_list(input_data.get("capabilities"), lower=True) if agent_id == "" else [],
        "credential": {
            "description": credential.get("description", "").strip() if isinstance(credential.get("description"), str) else "",
            "ttl_seconds": credential.get("ttl_seconds")
            if isinstance(credential.get("ttl_seconds"), int) and credential.get("ttl_seconds") > 0
            else 0,
        },
    }


def _normalize_connect_current_user_client_input(input_data: dict[str, Any] | None) -> dict[str, Any]:
    input_data = input_data or {}
    explicit_space_id = ""
    for field in ("space_id", "spaceId"):
        value = input_data.get(field)
        if isinstance(value, str) and value.strip():
            explicit_space_id = value
            break
    normalized = _normalize_connect_client_input(
        explicit_space_id,
        input_data,
    )
    space_name = input_data.get("space_name")
    if not isinstance(space_name, str):
        space_name = input_data.get("spaceName")
    space_description = input_data.get("space_description")
    if not isinstance(space_description, str):
        space_description = input_data.get("spaceDescription")
    return {
        "space_id": normalized["space_id"],
        "space_name": space_name.strip() if isinstance(space_name, str) else "",
        "space_description": space_description.strip() if isinstance(space_description, str) else "",
        "agent_id": normalized["agent_id"],
        "kind": normalized["kind"],
        "name": normalized["name"],
        "description": normalized["description"],
        "capabilities": normalized["capabilities"],
        "credential": normalized["credential"],
    }


def _normalize_bootstrap_current_user_entity_input(input_data: dict[str, Any] | None) -> dict[str, Any]:
    input_data = input_data or {}
    space = input_data.get("space") if isinstance(input_data.get("space"), dict) else {}
    launch_template = input_data.get("launch_template")
    if not isinstance(launch_template, dict):
        launch_template = input_data.get("launchTemplate")
    automation_profile = input_data.get("automation_profile")
    if not isinstance(automation_profile, dict):
        automation_profile = input_data.get("automationProfile")
    activate_session = input_data.get("activate_session")
    if not isinstance(activate_session, bool):
        activate_session = input_data.get("activateSession")
    normalized = {
        "key": input_data.get("key", "").strip() if isinstance(input_data.get("key"), str) else "",
        "name": input_data.get("name", "").strip() if isinstance(input_data.get("name"), str) else "",
        "activate_session": activate_session if isinstance(activate_session, bool) else True,
        "space": {
            "key": space.get("key", "").strip() if isinstance(space.get("key"), str) else "",
            "name": space.get("name", "").strip() if isinstance(space.get("name"), str) else "",
            "description": space.get("description", "").strip() if isinstance(space.get("description"), str) else "",
        },
    }
    if launch_template:
        normalized["launch_template"] = _normalize_automation_profile_launch_template_input(launch_template)
    if automation_profile:
        normalized["automation_profile"] = {
            "key": automation_profile.get("key", "").strip() if isinstance(automation_profile.get("key"), str) else "",
            "name": automation_profile.get("name", "").strip() if isinstance(automation_profile.get("name"), str) else "",
            "summary": automation_profile.get("summary", "").strip() if isinstance(automation_profile.get("summary"), str) else "",
            "compiler_key": automation_profile.get("compiler_key", "").strip()
            if isinstance(automation_profile.get("compiler_key"), str)
            else automation_profile.get("compilerKey", "").strip()
            if isinstance(automation_profile.get("compilerKey"), str)
            else "",
            "review_threshold": automation_profile.get("review_threshold")
            if isinstance(automation_profile.get("review_threshold"), int) and automation_profile.get("review_threshold") >= 0
            else 0,
            "prompt_capture_mode": automation_profile.get("prompt_capture_mode", "").strip()
            if isinstance(automation_profile.get("prompt_capture_mode"), str)
            else automation_profile.get("promptCaptureMode", "").strip()
            if isinstance(automation_profile.get("promptCaptureMode"), str)
            else "",
            "launch_template": _normalize_automation_profile_launch_template_input(automation_profile.get("launch_template")),
            "review_task": _normalize_automation_profile_task_input(automation_profile.get("review_task")),
            "remediation_task": _normalize_automation_profile_task_input(automation_profile.get("remediation_task")),
            "finalize_task": _normalize_automation_profile_task_input(automation_profile.get("finalize_task")),
        }
    return normalized


def _normalize_connect_current_user_and_launch_input(input_data: dict[str, Any] | None) -> dict[str, Any]:
    input_data = input_data or {}
    connection = _normalize_connect_current_user_client_input(
        input_data.get("connection") if isinstance(input_data.get("connection"), dict) else {}
    )
    runtime_heartbeat = input_data.get("runtime_heartbeat")
    if not isinstance(runtime_heartbeat, dict):
        runtime_heartbeat = input_data.get("runtimeHeartbeat")
    webhook_follow_up = input_data.get("webhook_follow_up")
    if not isinstance(webhook_follow_up, dict):
        webhook_follow_up = input_data.get("webhookFollowUp")
    launch = _normalize_connected_launch_input(
        "",
        input_data.get("launch") if isinstance(input_data.get("launch"), dict) else {},
    )
    launch.pop("space_id", None)
    normalized_webhook_follow_up = {
        "preset_key": "",
        "name": "",
        "url": "",
        "signing_secret": "",
    }
    if isinstance(webhook_follow_up, dict):
        preset_key = webhook_follow_up.get("preset_key")
        if not isinstance(preset_key, str):
            preset_key = webhook_follow_up.get("presetKey")
        url = webhook_follow_up.get("url")
        name = webhook_follow_up.get("name")
        signing_secret = webhook_follow_up.get("signing_secret")
        if not isinstance(signing_secret, str):
            signing_secret = webhook_follow_up.get("signingSecret")
        normalized_webhook_follow_up = {
            "preset_key": preset_key.strip() if isinstance(preset_key, str) and preset_key.strip() else ("chat_follow_up" if isinstance(url, str) and url.strip() else ""),
            "name": name.strip() if isinstance(name, str) else "",
            "url": url.strip() if isinstance(url, str) else "",
            "signing_secret": signing_secret.strip() if isinstance(signing_secret, str) else "",
        }
    return {
        "connection": connection,
        "runtime_heartbeat": {
            "status": runtime_heartbeat.get("status", "").strip() if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("status"), str) else "",
            "region": runtime_heartbeat.get("region", "").strip() if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("region"), str) else "",
            "protocol": runtime_heartbeat.get("protocol", "").strip() if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("protocol"), str) else "",
            "version": runtime_heartbeat.get("version", "").strip() if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("version"), str) else "",
            "execution_mode": runtime_heartbeat.get("execution_mode", "").strip()
            if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("execution_mode"), str)
            else (runtime_heartbeat.get("executionMode", "").strip() if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("executionMode"), str) else ""),
            "sandbox_provider": runtime_heartbeat.get("sandbox_provider", "").strip()
            if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("sandbox_provider"), str)
            else (runtime_heartbeat.get("sandboxProvider", "").strip() if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("sandboxProvider"), str) else ""),
            "sandbox_preset": runtime_heartbeat.get("sandbox_preset", "").strip()
            if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("sandbox_preset"), str)
            else (runtime_heartbeat.get("sandboxPreset", "").strip() if isinstance(runtime_heartbeat, dict) and isinstance(runtime_heartbeat.get("sandboxPreset"), str) else ""),
        },
        "webhook_follow_up": normalized_webhook_follow_up,
        "launch": {
            **({"model_route_policy_id": launch["model_route_policy_id"]} if launch.get("model_route_policy_id") else {}),
            "ttl_seconds": launch.get("ttl_seconds", 0),
            "automation_profile_id": launch.get("automation_profile_id", ""),
            "launch_template_id": launch.get("launch_template_id", ""),
            "title": launch.get("title", ""),
            "description": launch.get("description", ""),
            "execution_policy_id": launch.get("execution_policy_id", ""),
            "run_requirements": launch.get("run_requirements", {}),
            "run_context_refs": launch.get("run_context_refs", []),
            "prompt_capture_mode": launch.get("prompt_capture_mode", ""),
            "prompt_visibility_mode": launch.get("prompt_visibility_mode", ""),
            "acknowledge_public_raw_prompt_visibility": launch.get(
                "acknowledge_public_raw_prompt_visibility", False
            ),
            "source_prompt": launch.get("source_prompt", ""),
            "compiled_brief": launch.get("compiled_brief", ""),
        },
    }


def _normalize_prompt_launch_input(input_data: dict[str, Any] | None) -> dict[str, Any]:
    normalized = _normalize_launch_run_input(input_data)
    target_agent_id = input_data.get("target_agent_id") if isinstance(input_data, dict) else None
    if isinstance(target_agent_id, str) and target_agent_id.strip() and not normalized["run_requirements"]["target_agent_ids"]:
        normalized["run_requirements"]["target_agent_ids"] = [target_agent_id.strip()]
    if not normalized["prompt_capture_mode"]:
        normalized["prompt_capture_mode"] = "compiled_only"
    return normalized


def _normalize_connected_launch_input(space_id: str, input_data: dict[str, Any] | None) -> dict[str, Any]:
    spec = input_data if isinstance(input_data, dict) else {}
    agent_id = ""
    if isinstance(spec.get("agent_id"), str) and spec.get("agent_id", "").strip():
        agent_id = spec["agent_id"].strip()
    elif isinstance(spec.get("agentId"), str) and spec.get("agentId", "").strip():
        agent_id = spec["agentId"].strip()
    return {
        "space_id": space_id.strip() if isinstance(space_id, str) else "",
        "agent_id": agent_id,
        "ttl_seconds": spec.get("ttl_seconds") if isinstance(spec.get("ttl_seconds"), int) and spec.get("ttl_seconds") > 0 else 0,
        **_normalize_launch_run_input(spec),
    }


def build_launch_run_idempotency_key(
    space_id: str,
    input_data: dict[str, Any] | None = None,
    *,
    intent_key: str | None = None,
) -> str:
    payload = {
        "space_id": space_id.strip() if isinstance(space_id, str) else "",
        **_normalize_launch_run_input(input_data),
        "intent_key": intent_key.strip() if isinstance(intent_key, str) else "",
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"swarm-launch-{digest[:32]}"


def build_bootstrap_space_idempotency_key(
    entity_id: str,
    input_data: dict[str, Any] | None = None,
    *,
    intent_key: str | None = None,
) -> str:
    payload = {
        "entity_id": entity_id.strip() if isinstance(entity_id, str) else "",
        **_normalize_bootstrap_space_input(input_data),
        "intent_key": intent_key.strip() if isinstance(intent_key, str) else "",
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"swarm-bootstrap-{digest[:32]}"


def build_connect_client_idempotency_key(
    space_id: str,
    input_data: dict[str, Any] | None = None,
    *,
    intent_key: str | None = None,
) -> str:
    payload = {
        **_normalize_connect_client_input(space_id, input_data),
        "intent_key": intent_key.strip() if isinstance(intent_key, str) else "",
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"swarm-connect-{digest[:32]}"


def build_connect_space_client_idempotency_key(
    space_id: str,
    input_data: dict[str, Any] | None = None,
    *,
    intent_key: str | None = None,
) -> str:
    return build_connect_client_idempotency_key(space_id, input_data, intent_key=intent_key)


def build_connect_current_user_client_idempotency_key(
    input_data: dict[str, Any] | None = None,
    *,
    intent_key: str | None = None,
) -> str:
    payload = {
        **_normalize_connect_current_user_client_input(input_data),
        "intent_key": intent_key.strip() if isinstance(intent_key, str) else "",
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"swarm-me-connect-{digest[:32]}"


def build_bootstrap_current_user_entity_idempotency_key(
    input_data: dict[str, Any] | None = None,
    *,
    intent_key: str | None = None,
) -> str:
    payload = {
        **_normalize_bootstrap_current_user_entity_input(input_data),
        "intent_key": intent_key.strip() if isinstance(intent_key, str) else "",
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"swarm-me-bootstrap-entity-{digest[:32]}"


def build_connect_current_user_and_launch_idempotency_key(
    input_data: dict[str, Any] | None = None,
    *,
    intent_key: str | None = None,
) -> str:
    normalized = _normalize_connect_current_user_and_launch_input(input_data)
    payload = {
        "connection": normalized["connection"],
        "runtime_heartbeat": normalized["runtime_heartbeat"],
        "webhook_follow_up": normalized["webhook_follow_up"],
        "launch": normalized["launch"],
        "intent_key": intent_key.strip() if isinstance(intent_key, str) else "",
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"swarm-me-connect-launch-{digest[:32]}"


def _build_stable_mutation_idempotency_key(
    prefix: str,
    method: str,
    path: str,
    body: Any = None,
    *,
    intent_key: str | None = None,
    preserve_strings: bool = False,
) -> str:
    if preserve_strings:
        # Match the request serializer's JSON values before sorting object keys.
        body = json.loads(json.dumps(body))
    payload = _canonicalize_idempotency_value(
        {
            "method": method.strip().upper() if isinstance(method, str) else "",
            "path": path.strip() if isinstance(path, str) else "",
            "body": body,
            "intent_key": intent_key.strip() if isinstance(intent_key, str) else "",
        }, preserve_strings=preserve_strings,
    )
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:32]}"


def _default_mutation_idempotency_key(method: str, path: str, body: Any, *, operation_path: str | None = None, explicit_strategy: str | None = None) -> str | None:
    from ._operation_policy import _client_operation_key_strategy
    strategy = _client_operation_key_strategy(method, operation_path if operation_path is not None else path, explicit_strategy)
    if strategy is None:
        return None
    if strategy == "fresh-attempt":
        return _create_idempotency_key("swarm-sdk-py-op")
    return _build_stable_mutation_idempotency_key("swarm-sdk-py-v2", method, path, body, preserve_strings=True)
