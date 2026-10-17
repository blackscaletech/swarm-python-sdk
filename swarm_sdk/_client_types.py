from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypedDict


class ExecutionParticipant(TypedDict):
    """Current connection authorization, not a durable Agent or online readiness."""

    id: str
    executor_identity_id: str
    name: str
    kind: Literal["client", "agent"]
    is_self: bool
    can_execute: bool
    created_at: str


@dataclass(slots=True)
class HTTPResponse:
    status: int
    headers: dict[str, str]
    body: bytes


class SwarmAPIError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, body: Any = None, headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.body = body
        self.headers = headers or {}


class SwarmTransportError(SwarmAPIError):
    """A local transport failure, never an API response or raw exception chain."""

    code = "transport_failed"
    retryable = False

    def __init__(self, message: str = "Swarm transport failed") -> None:
        super().__init__(message)
        self.last_event_id: str | None = None
        self.last_received_event_id: str | None = None


class SwarmTimeoutError(SwarmTransportError):
    code = "timeout"

    def __init__(self, kind: str = "socket") -> None:
        super().__init__(f"Swarm {kind} timeout")
        self.kind = kind
        self.retryable = kind in {"socket", "stream_idle"}


class SwarmConnectionError(SwarmTransportError):
    code = "connection_failed"
    retryable = True

    def __init__(self) -> None:
        super().__init__("Swarm connection was interrupted")


class SwarmTLSError(SwarmTransportError):
    code = "tls_failed"

    def __init__(self) -> None:
        super().__init__("Swarm TLS verification or configuration failed")


class SwarmCancelledError(SwarmTransportError):
    code = "cancelled"

    def __init__(self) -> None:
        super().__init__("Swarm operation stopped (cancelled)")


class SwarmResponseLimitError(RuntimeError):
    def __init__(self, max_bytes: int, status: int) -> None:
        super().__init__(f"Swarm response exceeds the {max_bytes}-byte limit")
        self.code = "response_too_large"
        self.max_bytes = max_bytes
        self.status = status


class SwarmArtifactIntegrityError(RuntimeError):
    code = "artifact_integrity_failed"

    def __init__(self) -> None:
        super().__init__("Swarm artifact content failed integrity verification")
