from __future__ import annotations

import hashlib
import hmac
import re
from typing import Any


_SWARM_ASSIGNMENT_RE = re.compile(r"\b(SWARM_(?:HUMAN_TOKEN|RUNTIME_TOKEN|CHAT_TOKEN|CONNECT_CODE|AGENT_TOKEN|TOKEN|MCP_TOKEN|OPENCLAW_API_TOKEN))\b\s*[:=]\s*([^\s'\"\\]{8,})", re.IGNORECASE | re.MULTILINE)


_SWARM_TOKEN_RE = re.compile(r"\bswarm_(?:usr|agt|chat|pair)_[a-z0-9]{12,}\b", re.IGNORECASE)

_SWARM_CONNECTION_TOKEN_RE = re.compile(r"\b(?:sw(?:a|c|ac)_v2\.[a-f0-9]{16}\.[A-Za-z0-9_-]{43}|swr_ctf_[a-f0-9]{32}\.[0-9]+\.v2\.[a-f0-9]{16}\.[A-Za-z0-9_-]{43}|sw(?:u|ur|a|c)_(?!client_)[A-Za-z0-9_-]{43}|swr_ctf_[a-f0-9]{32}\.[A-Za-z0-9_-]{43})")


_AUTH_HEADER_BEARER_RE = re.compile(r"\bAuthorization\b\s*:\s*Bearer\s+[^\s'\"\\]{8,}", re.IGNORECASE | re.MULTILINE)


_BEARER_TOKEN_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._-]{12,}\b", re.IGNORECASE)


_API_KEY_ASSIGNMENT_RE = re.compile(r"\b(?:X-API-Key|Api-Key|API_KEY)\b\s*[:=]\s*([^\s'\"\\]{8,})", re.IGNORECASE | re.MULTILINE)


_PASSWORD_ASSIGNMENT_RE = re.compile(r"\b(?:password|passwd|pwd|secret|client_secret)\b\s*[:=]\s*([^\s'\"\\]{4,})", re.IGNORECASE | re.MULTILINE)


_PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----(?:.|\n|\r)*?-----END [A-Z0-9 ]*PRIVATE KEY-----", re.IGNORECASE)


_AWS_SECRET_RE = re.compile(r"\b(?:aws_secret_access_key|AWS_SECRET_ACCESS_KEY)\b\s*[:=]\s*([^\s'\"\\]{16,})", re.IGNORECASE | re.MULTILINE)


_AWS_SESSION_TOKEN_RE = re.compile(r"\b(?:aws_session_token|AWS_SESSION_TOKEN)\b\s*[:=]\s*([^\s'\"\\]{16,})", re.IGNORECASE | re.MULTILINE)


_AWS_ACCESS_KEY_ID_RE = re.compile(r"\bAKIA[0-9A-Z]{16}\b")


_SENTRY_DSN_RE = re.compile(r"https://[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+/\d+")


_URL_PASSWORD_RE = re.compile(r"(https?://[^:\s/]+:)([^@/\s]+)(@)", re.IGNORECASE)


_GCP_PRIVATE_KEY_RE = re.compile(r'"private_key"\s*:\s*"-----BEGIN PRIVATE KEY-----.*?-----END PRIVATE KEY-----\\n?"', re.IGNORECASE | re.DOTALL)


def _sanitize_sensitive_text(value: Any, *, max_len: int = 512) -> str:
    text = value.strip() if isinstance(value, str) else ""
    if text == "":
        return ""
    text = _SWARM_ASSIGNMENT_RE.sub(r"\1=[REDACTED_SECRET]", text)
    text = _SWARM_TOKEN_RE.sub("[REDACTED_SECRET]", text)
    text = _SWARM_CONNECTION_TOKEN_RE.sub("[REDACTED_SECRET]", text)
    text = _AUTH_HEADER_BEARER_RE.sub("Authorization: Bearer [REDACTED_SECRET]", text)
    text = _BEARER_TOKEN_RE.sub("Bearer [REDACTED_SECRET]", text)
    text = _API_KEY_ASSIGNMENT_RE.sub("API_KEY=[REDACTED_SECRET]", text)
    text = _PASSWORD_ASSIGNMENT_RE.sub("secret=[REDACTED_SECRET]", text)
    text = _AWS_SECRET_RE.sub("AWS_SECRET_ACCESS_KEY=[REDACTED_SECRET]", text)
    text = _AWS_SESSION_TOKEN_RE.sub("AWS_SESSION_TOKEN=[REDACTED_SECRET]", text)
    text = _AWS_ACCESS_KEY_ID_RE.sub("[REDACTED_SECRET]", text)
    text = _SENTRY_DSN_RE.sub("[REDACTED_SECRET_DSN]", text)
    text = _URL_PASSWORD_RE.sub(r"\1[REDACTED_SECRET]\3", text)
    text = _PRIVATE_KEY_RE.sub("[REDACTED_PRIVATE_KEY]", text)
    text = _GCP_PRIVATE_KEY_RE.sub('"private_key":"[REDACTED_PRIVATE_KEY]"', text)
    if max_len > 0 and len(text) > max_len:
        return f"{text[:max_len].rstrip()}..."
    return text


def _sanitize_sensitive_value(value: Any, *, max_len: int = 512) -> Any:
    if isinstance(value, str):
        return _sanitize_sensitive_text(value, max_len=max_len)
    if isinstance(value, list):
        return [_sanitize_sensitive_value(item, max_len=max_len) for item in value]
    if isinstance(value, dict):
        return {key: _sanitize_sensitive_value(item, max_len=max_len) for key, item in value.items()}
    return value


def _body_to_bytes(body: str | bytes | bytearray | memoryview) -> bytes:
    if isinstance(body, bytes):
        return body
    if isinstance(body, bytearray):
        return bytes(body)
    if isinstance(body, memoryview):
        return body.tobytes()
    if isinstance(body, str):
        return body.encode("utf-8")
    raise TypeError("Swarm webhook body must be str, bytes, bytearray, or memoryview")


def compute_webhook_signature(secret: str, body: str | bytes | bytearray | memoryview) -> str:
    if not isinstance(secret, str) or secret.strip() == "":
        raise ValueError("compute_webhook_signature requires a non-empty signing secret")
    payload = _body_to_bytes(body)
    return "sha256=" + hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def verify_webhook_signature(
    *,
    secret: str,
    body: str | bytes | bytearray | memoryview,
    signature: str,
    signature_version: str = "v1",
) -> bool:
    if signature_version != "v1":
        return False
    if not isinstance(signature, str) or signature.strip() == "":
        return False
    expected = compute_webhook_signature(secret, body)
    return hmac.compare_digest(expected, signature.strip())


def parse_webhook_headers(headers: dict[str, Any] | Any) -> dict[str, Any]:
    if headers is None:
        headers = {}
    if hasattr(headers, "items") and not isinstance(headers, dict):
        headers = {str(key).lower(): value for key, value in headers.items()}
    elif isinstance(headers, dict):
        headers = {str(key).lower(): value for key, value in headers.items()}
    else:
        raise TypeError("parse_webhook_headers requires a mapping of webhook headers")

    event_sequence_raw = headers.get("x-swarm-event-sequence", "")
    try:
        event_sequence = int(str(event_sequence_raw))
    except (TypeError, ValueError):
        event_sequence = None

    return {
        "delivery_id": str(headers.get("x-swarm-delivery-id", "") or ""),
        "audit_event_id": str(headers.get("x-swarm-audit-event-id", "") or ""),
        "event_action": str(headers.get("x-swarm-event-action", "") or ""),
        "event_sequence": event_sequence,
        "signature": str(headers.get("x-swarm-signature", "") or ""),
        "signature_version": str(headers.get("x-swarm-signature-version", "") or ""),
    }
