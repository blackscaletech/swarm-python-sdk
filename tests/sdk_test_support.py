from __future__ import annotations

import io
import json

from swarm_sdk.client import HTTPResponse


class FakeTransport:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []
        self.stream_payload = b""
        self.responses: list[HTTPResponse] = []

    def request(self, method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float | None) -> HTTPResponse:
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "body": json.loads(body.decode("utf-8")) if body else None,
                "timeout": timeout,
            }
        )
        if not self.responses:
            raise AssertionError("unexpected request without queued response")
        return self.responses.pop(0)

    def stream(self, method: str, url: str, headers: dict[str, str], timeout: float | None):
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "body": None,
                "timeout": timeout,
            }
        )
        return io.BytesIO(self.stream_payload)


class FakeOpener:
    def __init__(self, body: bytes = b"{}", status: int = 200, headers: dict[str, str] | None = None) -> None:
        self.requests: list[dict[str, object]] = []
        self.body = body
        self.status = status
        self.headers = headers or {"content-type": "application/json"}

    def open(self, req, timeout=None):
        self.requests.append(
            {
                "method": req.get_method(),
                "url": req.full_url,
                "headers": dict(req.header_items()),
                "timeout": timeout,
            }
        )

        class _Response:
            def __init__(self, status, headers, body):
                self.status = status
                self.headers = headers
                self._body = body
                self._offset = 0

            def read(self, amount=-1):
                end = len(self._body) if amount < 0 else self._offset + amount
                chunk = self._body[self._offset:end]
                self._offset += len(chunk)
                return chunk

            def close(self):
                return None

        return _Response(self.status, self.headers, self.body)

def json_response(payload: dict[str, object], *, status: int = 200) -> HTTPResponse:
    return HTTPResponse(
        status=status,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
    )


def json_list_response(payload: list[object], *, status: int = 200) -> HTTPResponse:
    return HTTPResponse(
        status=status,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
    )


def on_run(ctx):
    ctx.create_artifact(
        {
            "kind": "report",
            "media_type": "text/plain",
            "body": "hello",
        }
    )
    ctx.create_evaluation(
        {
            "outcome": "pass",
            "score": 97,
            "summary": "Strong result",
        }
    )
    return {"outcome": "succeeded"}
