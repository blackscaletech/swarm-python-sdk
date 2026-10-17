from __future__ import annotations

import codecs
import json
from typing import Any, Callable

from ._client_security import _sanitize_sensitive_text
from ._client_types import HTTPResponse
from ._client_types import SwarmTransportError
from ._client_lifetime import _Lifetime, _transport_failure


DEFAULT_SSE_MAX_EVENT_BYTES = 256 * 1024


DEFAULT_SSE_MAX_BUFFER_BYTES = 512 * 1024


def _decode_body(response: HTTPResponse) -> Any:
    if response.status == 204 or response.body == b"":
        return None
    content_type = response.headers.get("content-type", "")
    text = response.body.decode("utf-8")
    if "application/json" in content_type:
        return json.loads(text)
    return text


def _report_stream_error(on_stream_error: Callable[[dict[str, Any]], None] | None, details: dict[str, Any]) -> None:
    if callable(on_stream_error):
        on_stream_error(details)


class SwarmStreamError(Exception):
    """Strict consumption stopped; last_event_id is the last applied cursor."""

    def __init__(self, details: dict[str, Any]) -> None:
        super().__init__(details["message"])
        self.code = details["code"]
        self.last_event_id = details["last_event_id"]
        self.last_received_event_id = details["last_received_event_id"]


def _stream_chunks(response: Any):
    # HTTPResponse.read1 returns available bytes without waiting for a full buffer.
    if callable(getattr(response, "read1", None)):
        while chunk := response.read1(16384):
            yield chunk
    else:
        yield from response


def _reject_json_constant(value: str) -> None:
    raise ValueError("Non-finite JSON constant is not permitted")


def _consume_sse_stream(
    response: Any,
    *,
    on_event: Callable[[Any, dict[str, Any]], None],
    json_events: set[str],
    on_stream_error: Callable[[dict[str, Any]], None] | None,
    max_event_bytes: int,
    max_buffer_bytes: int,
    strict: bool = False,
    last_event_id: str | None = None,
    lifetime: _Lifetime | None = None,
    idle_timeout: float | None = None,
) -> str | None:
    lifetime = lifetime or _Lifetime()
    if strict and not callable(on_event):
        raise TypeError("Strict event consumption requires an application callback")
    decoder = codecs.getincrementaldecoder("utf-8-sig")(errors="strict" if strict else "replace")
    event_limit = max_event_bytes if max_event_bytes > 0 else DEFAULT_SSE_MAX_EVENT_BYTES
    buffer_limit = max_buffer_bytes if max_buffer_bytes > 0 else DEFAULT_SSE_MAX_BUFFER_BYTES
    received_id = applied_id = pending_id = last_event_id
    event_type = "message"
    data_lines: list[str] = []
    line: list[str] = []
    line_has_chars = False
    frame_bytes = 0
    dropping = skip_lf = False

    def report(details: dict[str, Any], cause: Exception | None = None) -> None:
        diagnostic = {**details, "last_event_id": applied_id, "last_received_event_id": received_id}
        try:
            lifetime.check()
            _report_stream_error(on_stream_error, diagnostic)
            lifetime.check()
        except SwarmTransportError:
            raise
        except Exception as exc:
            if not strict:
                raise
            raise SwarmStreamError({**diagnostic, "code": "error_callback_failed", "message": "Swarm stream error callback failed"}) from None
        if strict:
            raise SwarmStreamError(diagnostic) from None

    def finish_line() -> None:
        nonlocal line, line_has_chars, received_id, applied_id, pending_id, event_type, data_lines, frame_bytes, dropping
        if line_has_chars:
            if not dropping:
                text = "".join(line)
                if not text.startswith(":"):
                    field, _, value = text.partition(":")
                    if value.startswith(" "):
                        value = value[1:]
                    if field == "id" and "\0" not in value:
                        pending_id = value
                    elif field == "event":
                        event_type = value or "message"
                    elif field == "data":
                        data_lines.append(value)
            line = []
            line_has_chars = False
            return

        # Only complete frames can advance the received cursor.
        received_id = pending_id
        if not dropping and data_lines:
            payload = "\n".join(data_lines)
            data: Any = payload
            valid = True
            if event_type in json_events:
                try:
                    data = json.loads(payload, parse_constant=_reject_json_constant)
                except ValueError as exc:
                    valid = False
                    report({
                        "code": "invalid_json", "id": received_id, "event": event_type,
                        "message": _sanitize_sensitive_text(str(exc)),
                        "raw_data": _sanitize_sensitive_text(payload, max_len=256),
                    }, exc)
            if valid:
                lifetime.check()
                try:
                    on_event(data, {"id": received_id, "event": event_type})
                    lifetime.check()
                except SwarmTransportError:
                    raise
                except Exception as exc:
                    if not strict:
                        raise
                    report({"code": "callback_failed", "id": received_id, "event": event_type,
                            "message": "Swarm event application callback failed"}, exc)
                applied_id = received_id
        data_lines = []
        event_type = "message"
        frame_bytes = 0
        dropping = False

    def consume_text(text: str) -> None:
        nonlocal skip_lf, frame_bytes, dropping, line, data_lines, line_has_chars
        for char in text:
            if skip_lf:
                skip_lf = False
                if char == "\n":
                    continue
            if not dropping:
                frame_bytes += len(char.encode("utf-8"))
                if frame_bytes > event_limit or frame_bytes > buffer_limit:
                    dropping = True
                    line = []
                    data_lines = []
                    report({
                        "code": "event_too_large" if event_limit <= buffer_limit else "buffer_limit_exceeded",
                        "id": pending_id, "event": event_type, "event_bytes": frame_bytes,
                        "message": "Skipped an oversized Swarm SSE event frame",
                    })
            if char in ("\r", "\n"):
                finish_line()
                skip_lf = char == "\r"
            else:
                line_has_chars = True
                if not dropping:
                    line.append(char)

    def decode(chunk: bytes, final: bool = False) -> None:
        try:
            text = decoder.decode(chunk, final=final)
        except UnicodeDecodeError as exc:
            report({"code": "invalid_utf8", "message": "Swarm stream contains invalid UTF-8"}, exc)
            return
        consume_text(text)

    chunks = iter(_stream_chunks(response))
    try:
        while True:
            lifetime.check()
            idle = _Lifetime(idle_timeout, kind="stream_idle", parent=lifetime)
            try:
                raw = next(chunks)
            except StopIteration:
                idle.check()
                break
            except Exception as exc:
                raise _transport_failure(exc, lifetime, timeout_kind="stream_idle" if idle_timeout else "socket") from None
            idle.check()
            chunk = raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8")
            for offset in range(0, len(chunk), 16384):
                lifetime.check()
                decode(chunk[offset:offset + 16384])
        lifetime.check()
        decode(b"", final=True)
        if strict and (frame_bytes > 0 or dropping):
            report({"code": "truncated_frame", "message": "Swarm stream ended before its event delimiter"})
    except SwarmTransportError as failure:
        failure.last_event_id = applied_id
        failure.last_received_event_id = received_id
        diagnostic = {"code": failure.code, "message": str(failure), "last_event_id": applied_id,
                      "last_received_event_id": received_id}
        try:
            _report_stream_error(on_stream_error, diagnostic)
        except Exception:
            pass
        raise failure from None
    return applied_id if strict else received_id
