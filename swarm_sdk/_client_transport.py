from __future__ import annotations

import http.client
import re
from contextlib import contextmanager
from typing import Any
from urllib import error, parse, request

from ._client_types import HTTPResponse, SwarmResponseLimitError
from ._client_lifetime import _Lifetime, _NATIVE_GUARD, _SocketGuard, _owned_response, _transport_failure

DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_ERROR_RESPONSE_BYTES = 1024 * 1024


def _route_byte_limits(rules):
    if rules is None:
        return []
    if not isinstance(rules, (list, tuple)):
        raise ValueError("response_byte_limits must be a list")
    normalized = []
    seen = set()
    for rule in rules:
        if not isinstance(rule, dict) or rule.keys() - {"method", "path", "max_response_bytes", "max_error_response_bytes"}:
            raise ValueError("response_byte_limits contains an invalid rule")
        method, path = rule.get("method"), rule.get("path")
        if not isinstance(method, str) or method.upper() not in {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}:
            raise ValueError("response_byte_limits method must be an HTTP method")
        if not isinstance(path, str) or not path.startswith("/v1/"):
            raise ValueError("response_byte_limits path must be an absolute /v1/ path template")
        segments = path.split("/")[1:]
        template = []
        for segment in segments:
            if re.fullmatch(r"\{[A-Za-z_][A-Za-z0-9_]*\}", segment):
                template.append(None)
            elif segment not in {"", ".", ".."} and re.fullmatch(r"[A-Za-z0-9._~:@%+-]+", segment):
                template.append(segment)
            else:
                raise ValueError("response_byte_limits path must contain literal segments or named placeholders")
        key = (method.upper(), tuple(template))
        if key in seen:
            raise ValueError("response_byte_limits contains duplicate templates")
        seen.add(key)
        limits = {}
        for field in ("max_response_bytes", "max_error_response_bytes"):
            if field in rule:
                if rule[field] is None:
                    raise ValueError(f"{field} must be a positive safe integer")
                limits[field] = _response_byte_limit(rule[field], 0, field)
        if not limits:
            raise ValueError("response_byte_limits rule must supply a byte ceiling")
        normalized.append((method.upper(), template, limits))
    return normalized


def _match_route_limits(rules, method, path):
    segments = path.split("/")[1:]
    for rule_method, template, limits in rules:
        if rule_method == method.upper() and len(template) == len(segments) and all(
            actual and (expected is None or expected == actual) for expected, actual in zip(template, segments)
        ):
            return limits
    return {}


def _response_byte_limit(value: int | None, fallback: int, name: str) -> int:
    if value is None:
        return fallback
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= 9007199254740991:
        raise ValueError(f"{name} must be a positive safe integer")
    return value


class _NoRedirectHandler(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise request.HTTPError(
            req.full_url,
            code,
            "redirects are disabled for credentialed Swarm requests",
            headers,
            fp,
        )


class _ObservedConnection:
    def connect(self):
        try:
            super().connect()
            guard = _NATIVE_GUARD.get()
            if guard is not None and self.sock is not None:
                guard.bind(self.sock)
        except Exception:
            self.close()
            raise

    def getresponse(self):
        try:
            return super().getresponse()
        except Exception:
            self.close()
            raise


class _HTTPConnection(_ObservedConnection, http.client.HTTPConnection):
    pass


class _HTTPSConnection(_ObservedConnection, http.client.HTTPSConnection):
    pass


class _HTTPHandler(request.HTTPHandler):
    def http_open(self, req):
        return self.do_open(_HTTPConnection, req)


class _HTTPSHandler(request.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(_HTTPSConnection, req, context=self._context)


class UrllibTransport:
    def __init__(self, opener: Any | None = None) -> None:
        self._opener = opener or request.build_opener(_NoRedirectHandler(), _HTTPHandler(), _HTTPSHandler())

    def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float | None,
    ) -> HTTPResponse:
        return self.request_bounded(method, url, headers, body, timeout)

    def request_bounded(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float | None,
        *, max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        max_error_response_bytes: int = DEFAULT_MAX_ERROR_RESPONSE_BYTES,
        lifetime: _Lifetime | None = None,
        header_timeout: float | None = None,
    ) -> HTTPResponse:
        max_response_bytes = _response_byte_limit(max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES, "max_response_bytes")
        max_error_response_bytes = _response_byte_limit(max_error_response_bytes, DEFAULT_MAX_ERROR_RESPONSE_BYTES, "max_error_response_bytes")
        lifetime = lifetime or _Lifetime()
        header_lifetime = _Lifetime(header_timeout, kind="headers", parent=lifetime)
        with _SocketGuard(header_lifetime) as guard:
            try:
                response = self._open(method, url, headers, body, header_lifetime.socket_timeout(timeout))
                with _owned_response(response):
                    guard.check()
                    guard.lifetime = lifetime
                    if guard.socket is not None:
                        guard.socket.settimeout(lifetime.socket_timeout(timeout))
                    return self._read_response(response, max_response_bytes, max_error_response_bytes, guard)
            except SwarmResponseLimitError:
                raise
            except Exception as exc:
                failure = _transport_failure(exc, guard.lifetime)
            failure.__context__ = None
            raise failure from None

    def _open(self, method, url, headers, body, timeout):
        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            return self._opener.open(req, timeout=timeout)
        except error.HTTPError as failure:
            # API rejections remain bounded responses, including stream-open errors.
            return failure

    def _read_response(self, response, success_limit, error_limit, guard):
        response_headers = {key.lower(): value for key, value in (response.headers or {}).items()}
        limit = success_limit if 200 <= response.status < 300 else error_limit
        declared = response_headers.get("content-length", "")
        if declared.isdigit() and int(declared) > limit:
            raise SwarmResponseLimitError(limit, response.status)
        payload = bytearray()
        read = getattr(response, "read1", None) or response.read
        while True:
            guard.check()
            chunk = read(min(65536, limit - len(payload) + 1))
            guard.check()
            if not chunk:
                break
            if len(payload) + len(chunk) > limit:
                raise SwarmResponseLimitError(limit, response.status)
            payload.extend(chunk)
        if declared.isdigit() and len(payload) < int(declared):
            raise http.client.IncompleteRead(b"")
        return HTTPResponse(status=response.status, headers=response_headers, body=bytes(payload))

    @contextmanager
    def download_with_lifetime(self, method, url, headers, timeout, *, lifetime, header_timeout):
        """Own the raw response and native socket; verification owns body reads."""
        header_lifetime = _Lifetime(header_timeout, kind="headers", parent=lifetime)
        with _SocketGuard(header_lifetime) as guard:
            failure = None
            try:
                response = self._open(method, url, headers, None, header_lifetime.socket_timeout(timeout))
            except Exception as exc:
                failure = _transport_failure(exc, header_lifetime)
            if failure is not None:
                failure.__context__ = None
                raise failure from None
            with _owned_response(response):
                guard.check()
                guard.lifetime = lifetime
                yield response, guard

    @contextmanager
    def stream_with_lifetime(self, method, url, headers, timeout, *, lifetime,
                             idle_timeout, max_error_response_bytes, header_timeout=None):
        header_lifetime = _Lifetime(header_timeout, kind="headers", parent=lifetime)
        with _SocketGuard(header_lifetime) as guard:
            try:
                response = self._open(method, url, headers, None, header_lifetime.socket_timeout(timeout))
            except Exception as exc:
                raise _transport_failure(exc, header_lifetime) from None
            with _owned_response(response):
                guard.check()
                guard.lifetime = lifetime
                if guard.socket is not None:
                    guard.socket.settimeout(lifetime.socket_timeout(timeout))
                if not 200 <= response.status < 300:
                    try:
                        buffered = self._read_response(response, max_error_response_bytes, max_error_response_bytes, guard)
                    except SwarmResponseLimitError:
                        raise
                    except Exception as exc:
                        raise _transport_failure(exc, lifetime) from None
                    yield buffered
                else:
                    # A subscription has its own idle policy, not the JSON timeout.
                    if guard.socket is not None:
                        guard.socket.settimeout(idle_timeout)
                    yield response

    def stream(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        timeout: float | None,
    ):
        req = request.Request(url, headers=headers, method=method)
        return self._opener.open(req, timeout=timeout)


def _is_loopback_host(hostname: str) -> bool:
    normalized = hostname.strip().lower()
    return normalized in {"localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]"} or normalized.endswith(".localhost")


def _normalize_base_url(base_url: str, *, allow_insecure_transport: bool = False) -> str:
    if not base_url:
        raise ValueError("SwarmClient requires a non-empty base_url")
    normalized = base_url if base_url.endswith("/") else f"{base_url}/"
    parsed = parse.urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("SwarmClient base_url must be a valid absolute URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("SwarmClient URLs must not include credentials")
    if parsed.scheme != "https" and not _is_loopback_host(parsed.hostname or "") and not allow_insecure_transport:
        raise ValueError(
            "SwarmClient requires HTTPS unless the base_url is loopback/local. Pass allow_insecure_transport=True to override."
        )
    return normalized


def _resolve_request_url(base_url: str, path: str) -> str:
    url = parse.urljoin(base_url, path.lstrip("/"))
    base = parse.urlparse(base_url)
    parsed = parse.urlparse(url)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("SwarmClient URLs must not include credentials")

    def origin(value):
        port = value.port if value.port is not None else (443 if value.scheme == "https" else 80)
        return (value.scheme.lower(), (value.hostname or "").lower(), port)

    if origin(parsed) != origin(base):
        raise ValueError("SwarmClient requests must stay on the configured server origin")
    return url
