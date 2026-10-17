from __future__ import annotations

import hashlib
import json
import re
import tempfile
import threading
from dataclasses import dataclass, field
from typing import BinaryIO

from ._client_inputs import _path_segment
from ._client_lifetime import _Lifetime, _owned_response, _seconds, _transport_failure
from ._client_security import _sanitize_sensitive_value
from ._client_transport import UrllibTransport, _match_route_limits, _resolve_request_url, _response_byte_limit
from ._client_types import SwarmAPIError, SwarmArtifactIntegrityError, SwarmResponseLimitError

MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_DOWNLOAD_SECONDS = 120.0
_SPOOL_BYTES = 1024 * 1024
_READ_BYTES = 65536


@dataclass(frozen=True, slots=True)
class VerifiedArtifactDownload:
    """Caller-owned bytes verified against the authenticated response at return."""

    artifact_id: str
    content_id: str
    sha256: str
    size_bytes: int
    media_type: str | None
    etag: str
    _file: BinaryIO = field(repr=False)

    @property
    def file(self) -> BinaryIO:
        return self._file

    @property
    def closed(self) -> bool:
        return self._file.closed

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> VerifiedArtifactDownload:
        return self

    def __exit__(self, *_):
        self.close()


def _headers(response) -> dict[str, str]:
    headers = {}
    total = 0
    source = getattr(response, "headers", None)
    if not callable(getattr(source, "items", None)):
        raise SwarmArtifactIntegrityError()
    for key, value in source.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise SwarmArtifactIntegrityError()
        total += len(key) + len(value)
        if total > 16384 or len(headers) >= 100 or any(ord(c) < 32 or ord(c) == 127 for c in key + value):
            raise SwarmArtifactIntegrityError()
        name = key.lower()
        if name in headers:
            raise SwarmArtifactIntegrityError()
        headers[name] = value.strip()
    return headers


def _length(value: str | None) -> int:
    if not isinstance(value, str) or len(value) > 20 or re.fullmatch(r"[0-9]+", value) is None:
        raise SwarmArtifactIntegrityError()
    return int(value)


def _metadata(headers, artifact_id, expected_sha256, limit):
    digest = headers.get("x-swarm-content-sha256", "")
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise SwarmArtifactIntegrityError()
    content_id = "sha256:" + digest
    etag = '"' + content_id + '"'
    if (headers.get("content-type", "").lower() != "application/octet-stream" or
            headers.get("x-swarm-artifact-id") != artifact_id or
            headers.get("x-swarm-content-id") != content_id or headers.get("etag") != etag or
            (expected_sha256 is not None and expected_sha256.lower() != digest)):
        raise SwarmArtifactIntegrityError()
    if headers.get("content-encoding", "identity").lower() != "identity":
        raise SwarmArtifactIntegrityError()
    controls = {part.strip().lower() for part in headers.get("cache-control", "").split(",")}
    if not {"no-store", "no-transform"}.issubset(controls):
        raise SwarmArtifactIntegrityError()
    size = _length(headers.get("x-swarm-content-length"))
    if "content-length" in headers and _length(headers["content-length"]) != size:
        raise SwarmArtifactIntegrityError()
    transfer = headers.get("transfer-encoding")
    if transfer is not None and (transfer.lower() != "chunked" or "content-length" in headers):
        raise SwarmArtifactIntegrityError()
    if size > limit:
        raise SwarmResponseLimitError(limit, 200)
    return dict(artifact_id=artifact_id, content_id=content_id, sha256=digest,
                size_bytes=size, media_type=headers.get("x-swarm-content-media-type"), etag=etag)


def _read(response, size, lifetime, idle_timeout, guard):
    idle = _Lifetime(idle_timeout, kind="download_idle", parent=lifetime)
    if guard is not None:
        guard.lifetime = idle
        guard.check()
        if guard.socket is not None and guard.socket.fileno() >= 0:
            guard.socket.settimeout(idle.socket_timeout(None))
    idle.check()
    failure = None
    try:
        chunk = (getattr(response, "read1", None) or response.read)(size)
        idle.check()
        if guard is not None:
            guard.check()
    except Exception as exc:
        failure = _transport_failure(exc, idle, timeout_kind="download_idle")
    if guard is not None:
        guard.lifetime = lifetime
    if failure is not None:
        failure.__context__ = None
        raise failure from None
    if not isinstance(chunk, bytes):
        raise SwarmArtifactIntegrityError()
    return chunk


def _redact_rejection(value, token):
    if isinstance(value, dict):
        return {key: "[REDACTED_SECRET]" if re.search(r"token|secret|password|authorization|cookie|credential|body|chunk|payload", key, re.I)
                else _redact_rejection(item, token) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_rejection(item, token) for item in value]
    if isinstance(value, str) and token:
        value = value.replace(token, "[REDACTED_SECRET]")
    return _sanitize_sensitive_value(value)


def _rejection(response, headers, status, limit, lifetime, idle_timeout, guard, token):
    declared = headers.get("content-length")
    if declared is not None and _length(declared) > limit:
        raise SwarmResponseLimitError(limit, status)
    body = bytearray()
    while True:
        chunk = _read(response, min(_READ_BYTES, limit - len(body) + 1), lifetime, idle_timeout, guard)
        if not chunk:
            break
        if len(body) + len(chunk) > limit:
            raise SwarmResponseLimitError(limit, status)
        body.extend(chunk)
    payload = None
    if headers.get("content-encoding", "identity").lower() == "identity":
        try:
            payload = _redact_rejection(json.loads(body), token)
        except (ValueError, UnicodeError, RecursionError):
            pass
    # Never put a response chunk, redirect URL, or transport diagnostic in the message.
    safe_headers = _redact_rejection({key: headers[key] for key in ("content-type", "retry-after") if key in headers}, token)
    raise SwarmAPIError("Swarm Artifact download was rejected", status=status, body=payload, headers=safe_headers)


def download_artifact(client, artifact_id, *, expected_sha256=None, max_artifact_bytes=None,
                      max_response_bytes=None, max_error_response_bytes=None,
                      operation_timeout=None, header_timeout=None, idle_timeout=None, stop_event=None):
    if not isinstance(artifact_id, str) or any(ord(c) < 32 or ord(c) == 127 for c in artifact_id):
        raise ValueError("artifact_id must be a non-empty string")
    invalid_identifier = False
    try:
        segment = _path_segment(artifact_id)
    except (ValueError, UnicodeError):
        invalid_identifier = True
    if invalid_identifier:
        raise ValueError("artifact_id must be a valid non-empty identifier") from None
    if expected_sha256 is not None and (not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None):
        raise ValueError("expected_sha256 must contain 64 hexadecimal characters")
    if stop_event is not None and not isinstance(stop_event, threading.Event):
        raise ValueError("stop_event must be a threading.Event")
    total_timeout = _seconds(operation_timeout, "operation_timeout")
    if total_timeout is None:
        total_timeout = client.operation_timeout or MAX_DOWNLOAD_SECONDS
    total_timeout = min(total_timeout, MAX_DOWNLOAD_SECONDS)
    headers_timeout = _seconds(header_timeout, "header_timeout")
    if headers_timeout is None:
        headers_timeout = client.header_timeout or client.timeout or 30.0
    idle_timeout = _seconds(idle_timeout, "idle_timeout") or client.stream_idle_timeout or client.timeout or 30.0
    lifetime = _Lifetime(total_timeout, stop_event=stop_event, parent=client._operation.get())
    lifetime.check()
    url = _resolve_request_url(client.base_url, f"/v1/artifacts/{segment}/content")
    rules = _match_route_limits(client.response_byte_limits, "GET", client._operation_path(url))
    wire_limit = rules.get("max_response_bytes", client.max_response_bytes)
    limit = min(MAX_ARTIFACT_BYTES, wire_limit,
                _response_byte_limit(max_artifact_bytes, MAX_ARTIFACT_BYTES, "max_artifact_bytes"),
                _response_byte_limit(max_response_bytes, wire_limit, "max_response_bytes"))
    error_limit = min(client.max_error_response_bytes, rules.get("max_error_response_bytes", client.max_error_response_bytes),
                      _response_byte_limit(max_error_response_bytes, client.max_error_response_bytes, "max_error_response_bytes"))
    forbidden = {"accept", "accept-encoding", "range", "if-range", "if-match", "if-none-match",
                 "if-modified-since", "if-unmodified-since", "idempotency-key", "content-type"}
    if client.token:
        forbidden.add("authorization")
    request_headers = {key: value for key, value in client.default_headers.items() if key.lower() not in forbidden}
    request_headers.update({"Accept": "application/octet-stream", "Accept-Encoding": "identity"})
    if client.token:
        request_headers["Authorization"] = "Bearer " + client.token
    native = isinstance(client.transport, UrllibTransport)
    if not native and not callable(getattr(client.transport, "stream", None)):
        raise ValueError("download_artifact requires a streaming transport")
    spool = result = failure = None
    try:
        if native:
            owner = client.transport.download_with_lifetime("GET", url, request_headers, client.timeout,
                                                           lifetime=lifetime, header_timeout=headers_timeout)
        else:
            headers_lifetime = _Lifetime(headers_timeout, kind="headers", parent=lifetime)
            headers_lifetime.check()
            open_failure = None
            try:
                response = client.transport.stream("GET", url, request_headers, headers_lifetime.socket_timeout(client.timeout))
            except Exception as exc:
                open_failure = _transport_failure(exc, headers_lifetime)
            if open_failure is not None:
                open_failure.__context__ = None
                raise open_failure from None
            owner = _owned_response(response)
        with owner as owned:
            response, guard = owned if native else (owned, None)
            if not native:
                headers_lifetime.check()
            lifetime.check()
            geturl = getattr(response, "geturl", None)
            if callable(geturl) and geturl() != url:
                raise SwarmArtifactIntegrityError()
            status = getattr(response, "status", None)
            if isinstance(status, bool) or not isinstance(status, int) or not 100 <= status <= 599:
                raise SwarmArtifactIntegrityError()
            response_headers = _headers(response)
            if status != 200:
                if 200 <= status < 300:
                    raise SwarmArtifactIntegrityError()
                _rejection(response, response_headers, status, error_limit, lifetime, idle_timeout, guard, client.token)
            metadata = _metadata(response_headers, artifact_id, expected_sha256, limit)
            spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_BYTES, mode="w+b")
            digest = hashlib.sha256()
            size = 0
            while True:
                chunk = _read(response, min(_READ_BYTES, limit - size + 1), lifetime, idle_timeout, guard)
                if not chunk:
                    break
                if len(chunk) > _READ_BYTES or size + len(chunk) > limit:
                    raise SwarmResponseLimitError(limit, 200)
                size += len(chunk)
                if size > metadata["size_bytes"]:
                    raise SwarmArtifactIntegrityError()
                digest.update(chunk)
                lifetime.check()
                spool.write(chunk)
                lifetime.check()
            if size != metadata["size_bytes"] or digest.hexdigest() != metadata["sha256"]:
                raise SwarmArtifactIntegrityError()
            spool.seek(0)
        lifetime.check()
        result = VerifiedArtifactDownload(**metadata, _file=spool)
    except (SwarmAPIError, SwarmResponseLimitError, SwarmArtifactIntegrityError) as exc:
        failure = exc
    except Exception as exc:
        failure = _transport_failure(exc, lifetime)
    finally:
        if result is None and spool is not None:
            try:
                spool.close()
            except Exception:
                pass
    if failure is not None:
        failure.__context__ = None
        raise failure from None
    return result
