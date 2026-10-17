from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from urllib.parse import urlparse
from urllib.error import URLError
from http.client import HTTPException
from typing import Any
from ._operation_policy import _client_operation_key_strategy

from ._client_inputs import (
    _default_mutation_idempotency_key,
    _normalize_bootstrap_current_user_entity_input,
    _normalize_bootstrap_space_input,
    _normalize_connect_current_user_and_launch_input,
    _normalize_connect_current_user_client_input,
    _normalize_prompt_launch_input,
    _path_segment,
    build_bootstrap_current_user_entity_idempotency_key,
    build_bootstrap_space_idempotency_key,
    build_connect_client_idempotency_key,
    build_connect_current_user_and_launch_idempotency_key,
    build_connect_current_user_client_idempotency_key,
    build_connect_space_client_idempotency_key,
    build_launch_run_idempotency_key,
)
from ._client_pagination import CollectionPage, _encode_query
from ._client_security import (
    _sanitize_sensitive_text,
    _sanitize_sensitive_value,
    compute_webhook_signature,
    parse_webhook_headers,
    verify_webhook_signature,
)
from ._client_streaming import (
    DEFAULT_SSE_MAX_BUFFER_BYTES,
    DEFAULT_SSE_MAX_EVENT_BYTES,
    _consume_sse_stream,
    _decode_body,
    SwarmStreamError,
)
from ._client_transport import (DEFAULT_MAX_RESPONSE_BYTES, DEFAULT_MAX_ERROR_RESPONSE_BYTES,
                               UrllibTransport, _NoRedirectHandler, _normalize_base_url,
                               _resolve_request_url, _response_byte_limit, _route_byte_limits, _match_route_limits)
from ._client_types import HTTPResponse, SwarmAPIError, SwarmResponseLimitError, SwarmTransportError
from ._client_lifetime import _Lifetime, _owned_response, _seconds, _transport_failure


def _normalize_request_effect(method: str, effect: str | None, idempotency_key: str | None) -> str:
    normalized_method = str(method or "").upper()
    default_effect = "read" if normalized_method in {"GET", "HEAD"} else "mutation"
    normalized_effect = default_effect if effect is None else str(effect).strip().lower()
    if normalized_effect not in {"read", "mutation"}:
        raise ValueError("SwarmClient request effect must be read or mutation")
    if normalized_method in {"GET", "HEAD"} and normalized_effect != "read":
        raise ValueError(f"{normalized_method} requests cannot declare mutation effect")
    if normalized_effect == "read" and idempotency_key:
        raise ValueError("Read requests cannot declare an idempotency key")
    return normalized_effect


class SwarmClientCore:
    def __init__(
        self,
        *,
        base_url: str,
        token: str = "",
        timeout: float | None = 30.0,
        transport: Any | None = None,
        headers: dict[str, str] | None = None,
        allow_insecure_transport: bool = False,
        sse_max_event_bytes: int = DEFAULT_SSE_MAX_EVENT_BYTES,
        sse_max_buffer_bytes: int = DEFAULT_SSE_MAX_BUFFER_BYTES,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        max_error_response_bytes: int = DEFAULT_MAX_ERROR_RESPONSE_BYTES,
        operation_timeout: float | None = None,
        stream_idle_timeout: float | None = None,
        stream_timeout: float | None = None,
        retry_timeout: float | None = 150.0,
        response_byte_limits: list[dict[str, Any]] | None = None,
        header_timeout: float | None = None,
    ) -> None:
        self.base_url = _normalize_base_url(base_url, allow_insecure_transport=allow_insecure_transport)
        self.token = token
        self.timeout = _seconds(timeout, "timeout")
        self.header_timeout = _seconds(header_timeout, "header_timeout")
        self.operation_timeout = _seconds(operation_timeout, "operation_timeout")
        self.stream_idle_timeout = _seconds(stream_idle_timeout, "stream_idle_timeout")
        self.stream_timeout = _seconds(stream_timeout, "stream_timeout")
        self.retry_timeout = _seconds(retry_timeout, "retry_timeout")
        self.response_byte_limits = _route_byte_limits(response_byte_limits)
        self._operation: ContextVar[_Lifetime | None] = ContextVar("swarm_operation", default=None)
        self.transport = transport or UrllibTransport()
        self.default_headers = headers or {}
        self.allow_insecure_transport = allow_insecure_transport is True
        self.sse_max_event_bytes = sse_max_event_bytes if sse_max_event_bytes > 0 else DEFAULT_SSE_MAX_EVENT_BYTES
        self.sse_max_buffer_bytes = sse_max_buffer_bytes if sse_max_buffer_bytes > 0 else DEFAULT_SSE_MAX_BUFFER_BYTES
        self.max_response_bytes = _response_byte_limit(max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES, "max_response_bytes")
        self.max_error_response_bytes = _response_byte_limit(max_error_response_bytes, DEFAULT_MAX_ERROR_RESPONSE_BYTES, "max_error_response_bytes")

    @contextmanager
    def operation(self, *, timeout: float | None = None, stop_event: threading.Event | None = None,
                  _kind: str = "operation", _check_exit: bool = True):
        """Share one monotonic budget across requests and caller-managed retries."""
        lifetime = _Lifetime(_seconds(timeout, "timeout"), kind=_kind,
                             stop_event=stop_event, parent=self._operation.get())
        lifetime.check()
        token = self._operation.set(lifetime)
        try:
            yield lifetime
            if _check_exit:
                lifetime.check()
        finally:
            self._operation.reset(token)

    def request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotency_policy: str | None = None,
        effect: str | None = None,
        max_response_bytes: int | None = None,
        max_error_response_bytes: int | None = None,
        operation_timeout: float | None = None,
        stop_event: threading.Event | None = None,
    ) -> tuple[Any, HTTPResponse]:
        lifetime = _Lifetime(self.operation_timeout if operation_timeout is None else _seconds(operation_timeout, "operation_timeout"),
                             stop_event=stop_event, parent=self._operation.get())
        lifetime.check()
        url = _resolve_request_url(self.base_url, path)
        operation_path = self._operation_path(url)
        route_limits = _match_route_limits(self.response_byte_limits, method, operation_path)
        success_limit = _response_byte_limit(max_response_bytes, route_limits.get("max_response_bytes", self.max_response_bytes), "max_response_bytes")
        error_limit = _response_byte_limit(max_error_response_bytes, route_limits.get("max_error_response_bytes", self.max_error_response_bytes), "max_error_response_bytes")
        query_string = _encode_query(query)
        if query_string:
            url = f"{url}?{query_string}"

        request_effect = _normalize_request_effect(method, effect, idempotency_key)
        request_headers = dict(self.default_headers)
        request_headers["Accept"] = "application/json"
        if self.token:
            request_headers["Authorization"] = f"Bearer {self.token}"
        if headers:
            request_headers.update({key: str(value) for key, value in headers.items()})

        payload = None
        if body is not None:
            request_headers["Content-Type"] = "application/json"
            payload = json.dumps(body).encode("utf-8")
        if request_effect == "mutation":
            if idempotency_key and idempotency_policy is not None:
                _client_operation_key_strategy(method, operation_path, idempotency_policy)
            key = idempotency_key or _default_mutation_idempotency_key(method, path + ("?" + query_string if query_string else ""), body,
                                                                     operation_path=operation_path, explicit_strategy=idempotency_policy)
            if key:
                request_headers["Idempotency-Key"] = key
            else:
                request_headers.pop("Idempotency-Key", None)

        failure = None
        try:
            bounded = getattr(self.transport, "request_bounded", None)
            if callable(bounded):
                options = {"max_response_bytes": success_limit, "max_error_response_bytes": error_limit}
                if isinstance(self.transport, UrllibTransport):
                    options["lifetime"] = lifetime
                    options["header_timeout"] = self.header_timeout
                response = bounded(method, url, request_headers, payload, lifetime.socket_timeout(self.timeout), **options)
            else:
                response = self.transport.request(method, url, request_headers, payload, lifetime.socket_timeout(self.timeout))
        except SwarmResponseLimitError:
            raise
        except Exception as exc:
            failure = _transport_failure(exc, lifetime)
        if failure is not None:
            failure.__context__ = None
            raise failure from None

        lifetime.check()
        limit = success_limit if 200 <= response.status < 300 else error_limit
        if len(response.body) > limit:
            raise SwarmResponseLimitError(limit, response.status)
        invalid_body = False
        try:
            data = _decode_body(response)
        except (ValueError, UnicodeError):
            invalid_body = True
        if invalid_body:
            if not 200 <= response.status < 300:
                raise SwarmAPIError("Swarm API rejection contains invalid JSON or UTF-8",
                                    status=response.status, headers=_sanitize_sensitive_value(response.headers)) from None
            raise ValueError("Swarm response contains invalid JSON or UTF-8") from None
        lifetime.check()
        if response.status < 200 or response.status >= 300:
            raise SwarmAPIError(
                f"{method} {path} failed ({response.status}): {_sanitize_sensitive_text(json.dumps(data) if not isinstance(data, str) else data)}",
                status=response.status,
                body=_sanitize_sensitive_value(data),
                headers=_sanitize_sensitive_value(response.headers),
            )
        return data, response

    def _operation_path(self, url):
        prefix = urlparse(self.base_url).path.rstrip("/")
        path = urlparse(url).path
        return path[len(prefix):] if prefix and path.startswith(prefix + "/") else path

    def _stream_events(self, url, headers, *, on_event, json_events, on_stream_error,
                       strict, last_event_id, stream_timeout, stream_idle_timeout, stop_event):
        lifetime = _Lifetime(self.stream_timeout if stream_timeout is None else _seconds(stream_timeout, "stream_timeout"),
                             kind="stream", stop_event=stop_event, parent=self._operation.get())
        idle_timeout = self.stream_idle_timeout if stream_idle_timeout is None else _seconds(stream_idle_timeout, "stream_idle_timeout")
        try:
            lifetime.check()
            if isinstance(self.transport, UrllibTransport):
                owner = self.transport.stream_with_lifetime(
                    "GET", url, headers, self.timeout, lifetime=lifetime, idle_timeout=idle_timeout,
                    header_timeout=self.header_timeout,
                    max_error_response_bytes=_match_route_limits(self.response_byte_limits, "GET", self._operation_path(url)).get(
                        "max_error_response_bytes", self.max_error_response_bytes))
            else:
                try:
                    response = self.transport.stream("GET", url, headers, lifetime.socket_timeout(self.timeout))
                except Exception as exc:
                    raise _transport_failure(exc, lifetime) from None
                owner = _owned_response(response)
            with owner as response:
                lifetime.check()
                if isinstance(response, HTTPResponse):
                    try:
                        body = _decode_body(response)
                    except (ValueError, UnicodeError):
                        body = None
                    raise SwarmAPIError("Swarm stream request was rejected", status=response.status,
                                        body=_sanitize_sensitive_value(body), headers=_sanitize_sensitive_value(response.headers))
                return _consume_sse_stream(
                    response, on_event=on_event, json_events=json_events, on_stream_error=on_stream_error,
                    max_event_bytes=self.sse_max_event_bytes, max_buffer_bytes=self.sse_max_buffer_bytes,
                    strict=strict, last_event_id=last_event_id, lifetime=lifetime, idle_timeout=idle_timeout)
        except SwarmTransportError as failure:
            if failure.last_event_id is None:
                failure.last_event_id = last_event_id
            if failure.last_received_event_id is None:
                failure.last_received_event_id = last_event_id
            pending_failure = failure
        except SwarmStreamError as failure:
            pending_failure = failure
        except (SwarmAPIError, SwarmResponseLimitError):
            raise
        except (OSError, URLError, HTTPException) as exc:
            failure = _transport_failure(exc, lifetime)
            failure.last_event_id = failure.last_received_event_id = last_event_id
            pending_failure = failure
        pending_failure.__context__ = None
        raise pending_failure from None

    def list(self, path: str, *, query: dict[str, Any] | None = None) -> CollectionPage:
        data, response = self.request("GET", path, query=query)
        return CollectionPage(
            items=data if isinstance(data, list) else [],
            next_cursor=response.headers.get("x-next-cursor"),
            order=response.headers.get("x-order"),
            headers=response.headers,
        )

    def data(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        body: Any = None,
        headers: dict[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotency_policy: str | None = None,
        effect: str | None = None,
        max_response_bytes: int | None = None,
        max_error_response_bytes: int | None = None,
        operation_timeout: float | None = None,
        stop_event: threading.Event | None = None,
    ) -> Any:
        data, _ = self.request(
            method,
            path,
            query=query,
            body=body,
            headers=headers,
            idempotency_key=idempotency_key,
            idempotency_policy=idempotency_policy,
            effect=effect,
            max_response_bytes=max_response_bytes,
            max_error_response_bytes=max_error_response_bytes,
            operation_timeout=operation_timeout,
            stop_event=stop_event,
        )
        return data

    def get(self, path: str, **options: Any) -> Any:
        return self.data("GET", path, **options)

    def post(self, path: str, body: Any = None, **options: Any) -> Any:
        return self.data("POST", path, body=body, **options)

    def patch(self, path: str, body: Any = None, **options: Any) -> Any:
        return self.data("PATCH", path, body=body, **options)
